import glob
import logging
import os
import sys
from datetime import datetime, time, timezone
from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

# Usual constraints on nodes' properties
NODE_CONSTRAINTS = {
        'AS': {
                'asn': set(['UNIQUE', 'NOT NULL'])
                } ,

        'Prefix': {
                'prefix': set(['UNIQUE', 'NOT NULL']), 
                #                'af': set(['NOT NULL'])
                },
        
        'IP': {
                'ip': set(['UNIQUE', 'NOT NULL']),
                #'af': set(['NOT NULL'])
                },

        'DomainName': {
                'name': set(['UNIQUE', 'NOT NULL'])
                },

        'Country': {
                'country_code': set(['UNIQUE', 'NOT NULL'])
                },

        'Organization': {
                'name': set(['NOT NULL'])
                },
    }

# Properties that may be frequently queried and that are not constraints
NODE_INDEXES = {
        'PeeringdbOrgID': [ 'id' ]
        }

# Set of node labels with constrains (ease search for node merging)
NODE_CONSTRAINTS_LABELS = set(NODE_CONSTRAINTS.keys())

BATCH_SIZE = 50000

prop_formatters = {
    # asn is stored as an int
    'asn': int,
    # ipv6 is stored in lowercase
    'ip': str.lower,
    'prefix': str.lower,
    # country code is kept in capital letter
    'country_code': str.upper
}


def format_properties(prop):
    """Make sure certain properties are always formatted the same way.
    For example IPv6 addresses are stored in lowercase, or ASN are kept as 
    integer not string."""

    prop = dict(prop)

    for prop_name, formatter in prop_formatters.items():
        if prop_name in prop:
            prop[prop_name] = formatter(prop[prop_name])

    return prop


def batch_format_link_properties(links: list, inplace=True) -> list:
    """Helper function that applies format_properties to the
    relationship properties.

    Warning: Formats properties in-place to save memory by default.
    Use inplace=False to create a copy.

    links: List of relationships as defined in batch_add_links"""
    if inplace:
        for l in links:
            for idx, prop_dict in enumerate(l['props']):
                l['props'][idx] = format_properties(prop_dict)
        return links
    return [{'src_id': l['src_id'],
             'dst_id': l['dst_id'],
             'props': [format_properties(d) for d in l['props']]}
            for l in links]


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

        if prop_set and prop_name in prop_formatters:
            prop_set = set(map(prop_formatters[prop_name], prop_set))

        if all:
            existing_nodes = self.tx.run(f"MATCH (n:{type}) RETURN n.{prop_name} AS {prop_name}, ID(n) AS _id")
        else:
            list_prop = list(prop_set)
            existing_nodes = self.tx.run(f"""
            WITH $list_prop AS list_prop
            MATCH (n:{type}) 
            WHERE n.{prop_name} IN list_prop
            RETURN n.{prop_name} AS {prop_name}, ID(n) AS _id""", list_prop=list_prop)

        ids = {node[prop_name]: node['_id'] for node in existing_nodes }
        existing_nodes_set = set(ids.keys())
        missing_props = prop_set.difference(existing_nodes_set)
        missing_nodes = [{prop_name: val} for val in missing_props]
        
        # Create missing nodes
        for i in range(0, len(missing_nodes), BATCH_SIZE):
            batch = missing_nodes[i:i+BATCH_SIZE]

            create_query = f"""WITH $batch AS batch 
            UNWIND batch AS item CREATE (n:{type}) 
            SET n += item RETURN n.{prop_name} AS {prop_name}, ID(n) AS _id"""

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

        result = self.tx.run(f"MATCH (a)-[:EXTERNAL_ID]->(i:{id_type}) RETURN i.id AS extid, ID(a) AS nodeid")

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

        batch_format_link_properties(links, inplace=True)

        # Create links in batches
        for i in range(0, len(links), BATCH_SIZE):
            batch = links[i:i+BATCH_SIZE]

            create_query = f"""WITH $batch AS batch 
            UNWIND batch AS link 
                MATCH (x), (y)
                WHERE ID(x) = link.src_id AND ID(y) = link.dst_id
                CREATE (x)-[l:{type}]->(y) 
                WITH l, link
                UNWIND link.props AS prop 
                    SET l += prop """

            if action == 'merge':
                create_query = f"""WITH $batch AS batch 
                UNWIND batch AS link 
                    MATCH (x), (y)
                    WHERE ID(x) = link.src_id AND ID(y) = link.dst_id
                    MERGE (x)-[l:{type}]-(y) 
                    WITH l,  link
                    UNWIND link.props AS prop 
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

        if len(links) == 0:
            return 

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
        """IYP and references initialization. The crawler name should be unique."""

        self.organization = organization
        self.url = url
        self.name = name

        self.reference = {
            'reference_name': name,
            'reference_org': organization,
            'reference_url': url,
            'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
            }

        # connection to IYP database
        self.iyp = IYP()

    def create_tmp_dir(self, root='./tmp/'):
        """Create a temporary directory for this crawler. If the directory 
        already exists all and contains files then all files are deleted.

        return: path to the temporary directory
        """

        path = self.get_tmp_dir(root)

        try: 
            os.makedirs( path, exist_ok=False )
        except OSError:
            files = glob.glob(path+'*')
            for file in files:
                os.remove(file)

        return path

    def get_tmp_dir(self, root='./tmp/'):
        """Return the path to the temporary directory for this crawler. The
        directory may not exist yet."""

        assert self.name != ''

        return  f'{root}/{self.name}/'

    def fetch(self):
        """Large datasets may be pre-fetched using this method. Currently the 
        BaseCrawler does nothing for this method. Note that all crawlers may
        fetch data at the same time, hence it may cause API rate limiting issues."""

        pass
    
    def count_relations(self):
        """
        count the number of relations in the graph with the reference name of crawler 
        """
        
        result = self.iyp.tx.run(f"MATCH ()-[r]->() WHERE r.reference_name = '{self.name}' RETURN count(r) AS count").single()
        
        return result['count']
    
    def unit_test(self,logging):
        relation_count = self.count_relations()
        logging.info("Relations before starting: %s" % relation_count)
        self.run()
        relation_count_new = self.count_relations()
        logging.info("Relations after starting: %s" % relation_count_new)
        self.close()
        print("assertion failed") if relation_count_new <= relation_count else print("assertion passed")
        assert relation_count_new > relation_count
        
    
    
    def close(self):
        # Commit changes to IYP
        self.iyp.close()

