import logging
import sys
from datetime import datetime, time, timezone
from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

# Usual constraints on nodes' properties
NODE_CONSTRAINTS = {
        'AS': {
                'asn': set(['UNIQUE', 'NOT NULL'])
                } ,

        'PREFIX': {
                'prefix': set(['UNIQUE', 'NOT NULL']), 
                #                'af': set(['NOT NULL'])
                },
        
        'IP': {
                'ip': set(['UNIQUE', 'NOT NULL']),
                #'af': set(['NOT NULL'])
                },

        'DOMAIN_NAME': {
                'name': set(['UNIQUE', 'NOT NULL'])
                },

        'COUNTRY': {
                'country_code': set(['UNIQUE', 'NOT NULL'])
                },

        'ORGANIZATION': {
                'name': set(['NOT NULL'])
                },
    }

# Properties that may be frequently queried and that are not constraints
NODE_INDEXES = {
        'PEERINGDB_ORG_ID': [ 'id' ]
        }

# Set of node labels with constrains (ease search for node merging)
NODE_CONSTRAINTS_LABELS = set(NODE_CONSTRAINTS.keys())

BATCH_SIZE = 50000

def format_properties(prop):
    """Make sure certain properties are always formatted the same way.
    For example IPv6 addresses are stored in lowercase, or ASN are kept as 
    integer not string."""

    prop = dict(prop)

    # asn is stored as an int
    if 'asn' in prop:
        prop['asn'] = int(prop['asn'])

    # ipv6 is stored in lowercase
    if 'ip' in prop:
        prop['ip'] = prop['ip'].lower()
    if 'prefix' in prop:
        prop['prefix'] = prop['prefix'].lower()

    # country code is kept in capital letter
    if 'country_code' in prop:
        prop['country_code'] = prop['country_code'].upper()

    return prop


def dict2str(d, eq=':', pfx=''):
    """Converts a python dictionary to a Cypher map."""

    data = [] 
    for key, value in d.items():
        if isinstance(value, str) and '"' in value:
            escaped = value.replace("'", r"\'")
            data.append(f"{pfx+key}{eq} '{escaped}'")
        elif isinstance(value, str) or isinstance(value, datetime):
            data.append(f'{pfx+key}{eq} "{value}"')
        else:
            data.append(f'{pfx+key}{eq} {value}')

    return '{'+','.join(data)+'}'


class IYP(object):

    def __init__(self):

        logging.debug('IYP: Enter initialization')
        self.neo4j_enterprise = False

        # TODO: get config from configuration file
        self.server = 'localhost'
        self.port = 7687
        self.login = "neo4j"
        self.password = "password"

        # Connect to the database
        uri = f"neo4j://{self.server}:{self.port}"
        self.db = GraphDatabase.driver(uri, auth=(self.login, self.password))

        if self.db is None:
            sys.exit('Could not connect to the Neo4j database!')
        else:
            self.session = self.db.session()

        self._db_init()
        self.tx = self.session.begin_transaction()


    def _db_init(self):
        """Add constraints and indexes."""

        # Create constraints (implicitly add corresponding indexes)
        for label, prop_constraints in NODE_CONSTRAINTS.items():
            for property, constraints in prop_constraints.items():

                for constraint in constraints:
                    # neo4j-community only implements the UNIQUE constraint
                    if not self.neo4j_enterprise and constraint != 'UNIQUE':
                        continue

                    constraint_formated = constraint.replace(' ', '')
                    self.session.run(
                        f" CREATE CONSTRAINT {label}_{constraint_formated}_{property} IF NOT EXISTS "
                        f" FOR (n:{label}) "
                        f" REQUIRE n.{property} IS {constraint} ")

        # Create indexes
        for label, indexes in NODE_INDEXES.items():
            for index in indexes:
                self.session.run(
                    f" CREATE INDEX {label}_INDEX_{index} IF NOT EXISTS "
                    f" FOR (n:{label}) "
                    f" ON (n.{index}) ")

    def commit(self):
        """Commit all pending queries (node/link creation) and start a new
        transaction."""

        self.tx.commit()
        self.tx = self.session.begin_transaction()

    def rollback(self):
        """Rollback all pending queries (node/link creation) and start a new
        transaction."""

        self.tx.rollback()
        self.tx = self.session.begin_transaction()

    def batch_get_nodes(self, type, prop_name, prop_set=set(), all=True):
        """Find the ID of all nodes in the graph for the given type (label)
        and check that a node exists for each value in prop_set for the property
        prop. Create these nodes if they don't exist.

        Notice: this is a costly operation if there is a lot of nodes for the
        given type. To return only the nodes corresponding to prop_set values 
        set all=False.
        This method commit changes to neo4j.
       """

        if all:
            existing_nodes = self.tx.run(f"MATCH (n:{type}) RETURN n.{prop_name} as {prop_name}, ID(n) as _id")
        else:
            list_prop = list(prop_set)
            existing_nodes = self.tx.run(f"""
            WITH $list_prop as list_prop
            MATCH (n:{type}) 
            WHERE n.{prop_name} IN list_prop
            RETURN n.{prop_name} as {prop_name}, ID(n) as _id""", list_prop=list_prop)

        ids = {node[prop_name]: node['_id'] for node in existing_nodes }
        existing_nodes_set = set(ids.keys())
        missing_props = prop_set.difference(existing_nodes_set)
        missing_nodes = [{prop_name: val} for val in missing_props]
        
        # Create missing nodes
        for i in range(0, len(missing_nodes), BATCH_SIZE):
            batch = missing_nodes[i:i+BATCH_SIZE]

            create_query = f"""WITH $batch as batch 
            UNWIND batch as item CREATE (n:{type}) 
            SET n += item RETURN n.{prop_name} as {prop_name}, ID(n) as _id"""

            new_nodes = self.tx.run(create_query, batch=batch)

            for node in new_nodes:
                ids[node[prop_name]] = node['_id']

            self.commit()

        return ids 

    def get_node(self, type, prop, create=False):
        """Find the ID of a node in the graph  with the possibility to create it
        if it is not in the graph. 

        type: either a string or list of strings giving the type(s) of the node.
        prop: dictionary of attributes for the node.
        create: if the node doesn't exist, the node can be added to the database
        by setting create=True.

        Return the node ID or None if the node does not exist and create=False."""

        prop = format_properties(prop)

        # put type in a list
        type_str = str(type)
        if isinstance(type, list):
            type_str = ':'.join(type)
        else:
            type = [type]

        if create:
            has_constraints = NODE_CONSTRAINTS_LABELS.intersection(type)
            if len( has_constraints ):
                ### MERGE node with constraints
                ### Search on the constraints and set other values
                label = has_constraints.pop()
                constraint_prop = dict([ (c, prop[c]) for c in NODE_CONSTRAINTS[label].keys() ]) 
                #values = ', '.join([ f"a.{p} = {val}" for p, val in prop.items() ])
                labels = ', '.join([ f"a:{l}" for l in type])

                # TODO: fix this. Not working as expected. e.g. getting prefix
                # with a descr in prop
                try:
                    result = self.tx.run(
                    f"""MERGE (a:{label} {dict2str(constraint_prop)}) 
                        ON MATCH
                            SET {dict2str(prop, eq='=', pfx='a.')[1:-1]}, {labels}
                        ON CREATE
                            SET {dict2str(prop, eq='=', pfx='a.')[1:-1]}, {labels}
                        RETURN ID(a)"""
                        ).single()
                except ConstraintError:
                    sys.stderr.write(f'cannot merge {prop}')
                    result = self.tx.run(
                    f"""MATCH (a:{label} {dict2str(constraint_prop)}) RETURN ID(a)""").single()

            else:
                ### MERGE node without constraints
                result = self.tx.run(f"MERGE (a:{type_str} {dict2str(prop)}) RETURN ID(a)").single()
        else:
            ### MATCH node
            result = self.tx.run(f"MATCH (a:{type_str} {dict2str(prop)}) RETURN ID(a)").single()

        if result is not None:
            return result[0]
        else:
            return None

    def batch_get_node_extid(self, id_type):
        """Find all nodes in the graph which have an EXTERNAL_ID relationship with
        the given id_type. Return None if the node does not exist."""

        result = self.tx.run(f"MATCH (a)-[:EXTERNAL_ID]->(i:{id_type}) RETURN i.id as extid, ID(a) as nodeid")

        ids = {}
        for node in result:
            ids[node['extid']] = node['nodeid']


        return ids


    def get_node_extid(self, id_type, id):
        """Find a node in the graph which has an EXTERNAL_ID relationship with
        the given ID. Return None if the node does not exist."""

        result = self.tx.run(f"MATCH (a)-[:EXTERNAL_ID]->(:{id_type} {{id:{id}}}) RETURN ID(a)").single()

        if result is not None:
            return result[0]
        else:
            return None

    def batch_add_links(self, type, links, action='create'):
        """Create links of the given type in batches (this is faster than add_links).
        The links parameter is a list of {"src_id":int, "dst_id":int, "props":[dict].
        The dictionary prop_dict should at least contain a 'source', 'point in time', 
        and 'reference URL'. Keys in this dictionary should contain no space.
        To merge links with existing ones set action='merge'

        Notice: this method commit changes to neo4j """


        # Create links in batches
        for i in range(0, len(links), BATCH_SIZE):
            batch = links[i:i+BATCH_SIZE]

            create_query = f"""WITH $batch as batch 
            UNWIND batch as link 
                MATCH (x), (y)
                WHERE ID(x) = link.src_id AND ID(y) = link.dst_id
                CREATE (x)-[l:{type}]->(y) 
                WITH l, link
                UNWIND link.props as prop 
                    SET l += prop """

            if action == 'merge':
                create_query = f"""WITH $batch as batch 
                UNWIND batch as link 
                    MATCH (x), (y)
                    WHERE ID(x) = link.src_id AND ID(y) = link.dst_id
                    MERGE (x)-[l:{type}]-(y) 
                    WITH l,  link
                    UNWIND link.props as prop 
                        SET l += prop """


            res = self.tx.run(create_query, batch=batch)
            res.consume()
            self.commit()


    def add_links(self, src_node, links):
        """Create links from src_node to the destination nodes given in parameter
        links. This parameter is a list of [link_type, dst_node_id, prop_dict].
        The dictionary prop_dict should at least contain a 'source', 'point in time', 
        and 'reference URL'. Keys in this dictionary should contain no space.

        By convention link_type is written in UPPERCASE and keys in prop_dict are
        in lowercase."""

        matches = ' MATCH (x)' 
        where = f" WHERE ID(x) = {src_node}"
        merges = ''
        
        for i, (type, dst_node, prop) in enumerate(links):

            assert 'reference_org' in prop
            assert 'reference_url' in prop
            assert 'reference_name' in prop
            assert 'reference_time' in prop

            prop = format_properties(prop)

            matches += f", (x{i})"
            where += f" AND ID(x{i}) = {dst_node}"
            merges += f" MERGE (x)-[:{type}  {dict2str(prop)}]->(x{i}) "

        self.tx.run( matches+where+merges).consume()


    def close(self):
        """Commit pending queries and close IYP"""
        self.tx.commit()
        self.session.close()
        self.db.close()

class BasePostProcess(object):
    def __init__(self):
        """IYP and references initialization"""

        self.reference = {
            'reference_org': 'Internet Yellow Pages',
            'reference_url': 'https://iyp.iijlab.net',
            'reference_name': 'iyp',
            'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
            }

        # connection to IYP database
        self.iyp = IYP()
    
    
    def close(self):
        # Commit changes to IYP
        self.iyp.close()


class BaseCrawler(object):
    def __init__(self, organization, url, name):
        """IYP and references initialization"""

        self.reference = {
            'reference_name': name,
            'reference_org': organization,
            'reference_url': url,
            'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
            }

        # connection to IYP database
        self.iyp = IYP()
    
    
    def close(self):
        # Commit changes to IYP
        self.iyp.close()

