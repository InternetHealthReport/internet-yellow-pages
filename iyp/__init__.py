import bz2
import ipaddress
import json
import logging
import os
import pickle
from datetime import datetime, timezone
from shutil import rmtree
from typing import Optional

import requests
from github import Github
from neo4j import GraphDatabase, NotificationMinimumSeverity

BATCH_SIZE = 50000

prop_formatters = {
    # asn is stored as an int
    'asn': int,
    'ip': lambda s: ipaddress.ip_address(s).compressed,
    'prefix': lambda s: ipaddress.ip_network(s).compressed,
    # country code is kept in capital letter
    'country_code': lambda s: str.upper(str.strip(s))
}


def format_properties(prop):
    """Make sure certain properties are always formatted the same way.

    For example IPv6 addresses are stored in lowercase, or ASN are kept as integer not
    string.
    """

    prop = dict(prop)

    for prop_name, formatter in prop_formatters.items():
        if prop_name in prop:
            prop[prop_name] = formatter(prop[prop_name])

    return prop


def batch_format_link_properties(links: list, inplace=True) -> Optional[list]:
    """Helper function that applies format_properties to the relationship properties.

    Warning: Formats properties in-place to save memory by default.
    Use inplace=False to create a copy.

    links: List of relationships as defined in batch_add_links
    """
    if inplace:
        for link in links:
            for idx, prop_dict in enumerate(link['props']):
                link['props'][idx] = format_properties(prop_dict)
        return None
    return [{'src_id': link['src_id'],
             'dst_id': link['dst_id'],
             'props': [format_properties(d) for d in link['props']]}
            for link in links]


def dict2str(d, eq=':', pfx=''):
    """Converts a python dictionary to a Cypher map."""

    data = []
    for key, value in d.items():
        if isinstance(value, str):
            escaped = value.replace('\\', '\\\\').replace("'", "\\'")
            data.append(f"{pfx + key}{eq} '{escaped}'")
        elif isinstance(value, datetime):
            data.append(f'{pfx + key}{eq} datetime("{value.isoformat()}")')
        elif value is None:
            # Neo4j does not have the concept of empty properties.
            pass
        else:
            data.append(f'{pfx + key}{eq} {value}')

    return '{' + ','.join(data) + '}'


def get_commit_datetime(repo, file_path):
    """Get the datetime of the latest commit modifying a file in a GitHub repository.

    repo: The name of the repository in org/repo format, e.g.,
    "InternetHealthReport/internet-yellow-pages"
    file_path: The path to the file relative to the repository root, e.g.,
    "iyp/__init__.py"
    """
    return Github().get_repo(repo).get_commits(path=file_path)[0].commit.committer.date


def set_modification_time_from_last_modified_header(reference, response):
    """Set the reference_time_modification field of the specified reference dict to the
    datetime parsed from the Last-Modified header of the specified response if
    possible."""
    try:
        last_modified_str = response.headers['Last-Modified']
        # All HTTP dates are in UTC:
        # https://www.rfc-editor.org/rfc/rfc2616#section-3.3.1
        last_modified = datetime.strptime(last_modified_str,
                                          '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=timezone.utc)
        reference['reference_time_modification'] = last_modified
    except KeyError:
        logging.warning('No Last-Modified header; will not set modification time.')
    except ValueError as e:
        logging.error(f'Failed to parse Last-Modified header "{last_modified_str}": {e}')


class RequestStatusError(requests.HTTPError):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class JSONDecodeError(ValueError):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class MissingKeyError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ConnectionError(requests.exceptions.ConnectionError):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class AddressValueError(ValueError):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DataNotAvailableError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class IYP(object):

    def __init__(self):

        logging.debug('IYP: Enter initialization')
        self.neo4j_enterprise = False

        # Load configuration file
        with open('config.json', 'r') as fp:
            conf = json.load(fp)

        auth = None
        if 'login' in conf['neo4j'] and 'password' in conf['neo4j']:
            auth = (conf['neo4j']['login'], conf['neo4j']['password'])

        # Connect to the database
        uri = f'neo4j://{conf["neo4j"]["server"]}:{conf["neo4j"]["port"]}'
        self.db = GraphDatabase.driver(uri,
                                       auth=auth,
                                       notifications_min_severity=NotificationMinimumSeverity.WARNING)

        if self.db is None:
            raise ConnectionError('Could not connect to the Neo4j database!')
        # Raises an exception if there is a problem.
        # "Best practice" is to just let the program
        # crash: https://neo4j.com/docs/python-manual/current/connect/
        self.db.verify_connectivity()

        self.session = self.db.session()

        self.tx = self.session.begin_transaction()

    def __create_unique_constraint(self, label, prop):
        """Create a UNIQUE constraint on the given properties for the given node label.

        label: a string specifying the node label.
        property: a string or list of strings specifying the property name(s). A list of
        properties with more than one entry will create a combined constraint.
        """
        # The Neo4j Community Edition only supports UNIQUE constraints, i.e., no reason
        # to make this function more flexible.
        if isinstance(prop, list):
            require_str = '(' + ','.join([f'a.{p}' for p in prop]) + ')'
            prop = '_'.join(prop)
        else:
            require_str = f'a.{prop}'

        # Schema modifications are not allowed in the same transaction as writes.
        self.commit()
        self.tx.run(f"""CREATE CONSTRAINT {label}_UNIQUE_{prop} IF NOT EXISTS
                        FOR (a:{label})
                        REQUIRE {require_str} IS UNIQUE""")
        self.commit()

    def __create_range_index(self, label_type, prop, on_relationship):
        """Create a RANGE index (the default) on the given properties for the given node
        label or relationship type.

        label_type: a string specifying a node label or a relationship type.
        prop: a string or list of strings specifying the property name(s). A list of
        properties with more than one entry will create a combined index.
        on_relationship: a bool specifying if label_type refers to a relationship type
        (True) or a node label (False).
        """
        if isinstance(prop, list):
            on_str = '(' + ','.join([f'n.{p}' for p in prop]) + ')'
            prop = '_'.join(prop)
        else:
            on_str = f'a.{prop}'

        if on_relationship:
            for_str = f'()-[a:{label_type}]-()'
        else:
            for_str = f'(a:{label_type})'

        # Schema modifications are not allowed in the same transaction as writes.
        self.commit()
        self.tx.run(f"""CREATE INDEX {label_type}_INDEX_{prop} IF NOT EXISTS
                        FOR {for_str}
                        ON {on_str}""")
        self.commit()

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

    def close(self):
        """Commit pending queries and close IYP."""
        self.tx.commit()
        self.session.close()
        self.db.close()

    def batch_get_nodes_by_single_prop(self, label, prop_name, prop_set=set(), all=True, create=True, batch_size=0):
        """Find the ID of all nodes in the graph for the given label and check that a
        node exists for each value in prop_set for the property prop. Create these nodes
        if they don't exist.

        Notice: this is a costly operation if there is a lot of nodes for the
        given type. To return only the nodes corresponding to prop_set values
        set all=False.
        This method commits changes to the database.
        """
        if isinstance(label, list) and create:
            raise NotImplementedError('Can not implicitly create multi-label nodes.')

        if create:
            # Ensure UNIQUE constraint on id property.
            self.__create_unique_constraint(label, prop_name)

        # Assemble label
        label_str = str(label)
        if isinstance(label, list):
            label_str = ':'.join(label)

        if prop_set and prop_name in prop_formatters:
            prop_set = set(map(prop_formatters[prop_name], prop_set))

        if all:
            logging.info(f'Fetching all {label_str} nodes.')
            existing_nodes = self.tx.run(f"""
                MATCH (n:{label_str})
                RETURN n.{prop_name} AS {prop_name}, elementId(n) AS _id
                """)
            ids = {node[prop_name]: node['_id'] for node in existing_nodes}
        else:
            logging.info(f'Fetching up to {len(prop_set)} {label_str} nodes.')
            list_prop = list(prop_set)
            ids = dict()
            query = f"""
                    WITH $list_prop AS list_prop
                    MATCH (n:{label_str})
                    WHERE n.{prop_name} IN list_prop
                    RETURN n.{prop_name} AS {prop_name}, elementId(n) AS _id
                    """
            if batch_size > 0:
                logging.info(f'Fetching in batches of {batch_size} nodes')
                for i in range(0, len(list_prop), batch_size):
                    batch = list_prop[i:i + batch_size]
                    existing_nodes = self.tx.run(query, list_prop=batch)
                    ids.update({node[prop_name]: node['_id'] for node in existing_nodes})
            else:
                existing_nodes = self.tx.run(query, list_prop=list_prop)
                ids = {node[prop_name]: node['_id'] for node in existing_nodes}
        existing_nodes_set = set(ids.keys())
        missing_props = prop_set.difference(existing_nodes_set)
        missing_nodes = [{prop_name: val} for val in missing_props]

        # Create missing nodes
        if create and missing_nodes:
            logging.info(f'Creating {len(missing_nodes)} {label_str} nodes.')
            for i in range(0, len(missing_nodes), BATCH_SIZE):
                batch = missing_nodes[i:i + BATCH_SIZE]

                create_query = f"""WITH $batch AS batch
                UNWIND batch AS item CREATE (n:{label_str})
                SET n = item RETURN n.{prop_name} AS {prop_name}, elementId(n) AS _id"""

                new_nodes = self.tx.run(create_query, batch=batch)

                for node in new_nodes:
                    ids[node[prop_name]] = node['_id']
                self.commit()

        return ids

    def batch_get_nodes(self, label, properties, id_properties=list(), create=True):
        """Find the IDs of all nodes in the graph for the given label and properties.

        label: a str for a single label or a list of str for multiple labels. Multiple
        labels are only supported with create=False.
        properties: a list of dicts containing the node properties that should be
        fetched/set.
        id_properties: a list of keys from properties that should be used as the search
        predicate. Can be empty if only one node property is given. The order of keys in
        this list also defines the order of values for the returned id map.
        create: a bool specifying if new nodes shall be created for missing properties.
        """
        # HOW TO USE THIS FUNCTION
        #
        # To _only get_ nodes:
        #   Call with create=False.
        #   You can specify a list of labels.
        #   When getting nodes based on a single property, id_properties can be empty as
        #   the property name will be inferred automatically.
        #   When getting nodes based on multiple properties, all of them have to be
        #   specified in id_properties. PROPERTIES THAT ARE NOT LISTED IN id_properties
        #   WILL BE IGNORED!
        #
        #   For example:
        #     properties = [{'id': 1, 'asn_v4': 64496}, {'id': 2, 'asn_v4': 64497}]
        #     batch_get_nodes('AtlasProbe', properties, ['id', 'asn_v4'], create=False)
        #   This would return the node ids for these nodes (if they exist) as a dict
        #   like this (assuming x and y are the node's ids):
        #     {(1, 64496): x, (2, 64497): y}
        #
        #
        # To get/update/create nodes:
        #   Call with create=True.
        #   Only a single label string can be specified.
        #   This function guarantees that all properties are assigned to nodes. If
        #   needed, nodes are created.
        #   Like above, if there is only one property specified, id_properties can be
        #   empty.
        #   In contrast to above, if there are multiple properties not all of them have
        #   to be present in id_properties. id_properties specifies which properties are
        #   used as a filtering predicate, whereas all of them will be assigned.
        #
        #   For example:
        #     properties = [{'id': 1, 'asn_v4': 64496}, {'id': 2, 'asn_v4': 64497}]
        #     batch_get_nodes('AtlasProbe', properties, ['id'])
        #   Assuming (:AtlasProbe {'id': 1}) already exists, then this function would
        #   set the asn_v4 property of the existing node to 64496 and it would create a
        #   new node (:AtlasProbe {'id': 2}) and set the asn_v4 property of that node to
        #   64497.
        #   The returned id map would be:
        #     {1: x, 2: y}

        if isinstance(label, list) and create:
            raise NotImplementedError('Can not implicitly create multi-label nodes.')

        properties = [format_properties(props) for props in properties]

        # Assemble label
        label_str = str(label)
        if isinstance(label, list):
            label_str = ':'.join(label)

        if not id_properties:
            # We assume that all property dicts have the same keys.
            example_props = properties[0]
            # Implicit id property.
            if len(example_props) != 1:
                # In the single get_node case we return the id of the node directly, but
                # here we return a map of id_properties to id. If there is more than one
                # property, the order of the keys in the dictionary is not really clear,
                # so the user should pass an explicit order in id_properties instead.
                raise ValueError('batch_get_nodes only supports implicit id property if a single property is passed.')
            id_properties = list(example_props.keys())

        # Assemble "WHERE" and RETURN clauses.
        # The WHERE clause in this case in not an explicit WHERE clause, but the
        # predicate that is contained within the node specification.
        # For id_properties = ['x', 'y'] this will result in
        #   {x: prop.x, y: prop.y}
        # The RETURN clause is actually only a part of it, namely
        #   a.x AS x, a.y AS y
        # for the example above.
        where_clause = ['{']
        return_clause = list()
        for prop in id_properties:
            where_clause += [f'{prop}: prop.{prop}', ',']
            return_clause += [f'a.{prop} AS {prop}', ',']
        where_clause.pop()
        where_clause.append('}')
        where_clause_str = ''.join(where_clause)
        return_clause.pop()
        return_clause_str = ''.join(return_clause)

        action = 'MATCH'
        set_line = str()
        if create:
            action = 'MERGE'
            set_line = 'SET a += prop'
            self.__create_unique_constraint(label, id_properties)

        query = f"""UNWIND $props AS prop
                    {action} (a:{label_str} {where_clause_str})
                    {set_line}
                    RETURN {return_clause_str}, elementId(a) AS _id"""

        ids = dict()
        for i in range(0, len(properties), BATCH_SIZE):
            props = properties[i: i + BATCH_SIZE]
            results = self.tx.run(query, props=props)
            if len(id_properties) == 1:
                # Single id property results in a simple key-to-value mapping.
                for r in results:
                    ids[r[id_properties[0]]] = r['_id']
            else:
                # Multiple id properties result in a tuple-to-value mapping where the
                # order of values in the tuple is defined by the order of keys in
                # id_properties.
                for r in results:
                    id_key = tuple([r[prop] for prop in id_properties])
                    ids[id_key] = r['_id']
            self.commit()
        return ids

    def get_node(self, label, properties, id_properties=list(), create=True):
        """Find the ID of a node in the graph  with the possibility to create it if it
        is not in the graph.

        label: either a string or list of strings giving the node label(s). A list
        (multiple labels) can only be used with create=False.
        properties: dictionary of node properties.
        id_properties: list of keys from properties that should be used as the search
        predicate. If empty, all properties will be used.
        create: if the node doesn't exist, the node can be added to the database
        by setting create=True.

        Return the node ID or None if the node does not exist and create=False.
        """

        if isinstance(label, list) and create:
            raise NotImplementedError('Can not implicitly create multi-label nodes.')

        properties = format_properties(properties)

        # put type in a list
        label_str = str(label)
        if isinstance(label, list):
            label_str = ':'.join(label)

        if create:
            # No explicit id properties means all specified properties should be treated
            # as id properties.
            if not id_properties:
                id_property_dict = properties
            else:
                id_property_dict = {prop: properties[prop] for prop in id_properties}
            self.__create_unique_constraint(label, list(id_property_dict.keys()))
            result = self.tx.run(
                f"""MERGE (a:{label} {dict2str(id_property_dict)})
                SET a += {dict2str(properties)}
                RETURN elementId(a)"""
            ).single()
        else:
            # MATCH node
            result = self.tx.run(f'MATCH (a:{label_str} {dict2str(properties)}) RETURN elementId(a)').single()

        if result is not None:
            return result[0]
        else:
            return None

    def batch_add_node_label(self, node_ids, label):
        """Add additional labels to existing nodes.

        node_ids: list of node ids
        label: label string or list of label strings
        """
        label_str = str(label)
        if isinstance(label, list):
            label_str = ':'.join(label)

        logging.info(f'Adding label "{label_str}" to {len(node_ids)} nodes.')

        for i in range(0, len(node_ids), BATCH_SIZE):
            batch = node_ids[i:i + BATCH_SIZE]

            self.tx.run(f"""WITH $batch AS batch
                        MATCH (n)
                        WHERE elementId(n) IN batch
                        SET n:{label_str}""",
                        batch=batch)
            self.commit()

    def batch_get_node_extid(self, id_type):
        """Find all nodes in the graph which have an EXTERNAL_ID relationship with the
        given id_type.

        Return None if the node does not exist.
        """

        result = self.tx.run(f'MATCH (a)-[:EXTERNAL_ID]->(i:{id_type}) RETURN i.id AS extid, elementId(a) AS nodeid')

        ids = {}
        for node in result:
            ids[node['extid']] = node['nodeid']

        return ids

    def get_node_extid(self, id_type, id):
        """Find a node in the graph which has an EXTERNAL_ID relationship with the given
        ID.

        Return None if the node does not exist.
        """

        result = self.tx.run(f'MATCH (a)-[:EXTERNAL_ID]->(:{id_type} {{id:{id}}}) RETURN elementId(a)').single()

        if result is not None:
            return result[0]
        else:
            return None

    def batch_add_links(self, type, links, action='create'):
        """Create links of the given type in batches (this is faster than add_links).
        The links parameter is a list of {"src_id":int, "dst_id":int, "props":[dict].
        The dictionary prop_dict should at least contain a 'source', 'point in time',
        and 'reference URL'. Keys in this dictionary should contain no space. To merge
        links with existing ones set action='merge'.

        Notice: this method commit changes to neo4j
        """

        batch_format_link_properties(links, inplace=True)

        self.__create_range_index(type, 'reference_name', on_relationship=True)

        action_str = 'Creating' if action == 'create' else 'Merging'
        logging.info(f'{action_str} {len(links)} {type} relationships.')

        # Create links in batches
        for i in range(0, len(links), BATCH_SIZE):
            batch = links[i:i + BATCH_SIZE]

            create_query = f"""WITH $batch AS batch
            UNWIND batch AS link
                MATCH (x), (y)
                WHERE elementId(x) = link.src_id AND elementId(y) = link.dst_id
                CREATE (x)-[l:{type}]->(y)
                WITH l, link
                UNWIND link.props AS prop
                    SET l += prop """

            if action == 'merge':
                create_query = f"""WITH $batch AS batch
                UNWIND batch AS link
                    MATCH (x), (y)
                    WHERE elementId(x) = link.src_id AND elementId(y) = link.dst_id
                    MERGE (x)-[l:{type}]-(y)
                    WITH l,  link
                    UNWIND link.props AS prop
                        SET l += prop """

            res = self.tx.run(create_query, batch=batch)
            res.consume()
            self.commit()

    def add_links(self, src_node, links):
        """Create links from src_node to the destination nodes given in parameter links.
        This parameter is a list of [link_type, dst_node_id, prop_dict]. The dictionary
        prop_dict should at least contain a 'source', 'point in time', and 'reference
        URL'. Keys in this dictionary should contain no space.

        By convention link_type is written in UPPERCASE and keys in prop_dict are in
        lowercase.
        """

        if len(links) == 0:
            return

        relationship_types = {e[0] for e in links}
        for relationship_type in relationship_types:
            self.__create_range_index(relationship_type, 'reference_name', on_relationship=True)

        matches = ' MATCH (x)'
        where = f' WHERE elementId(x) = "{src_node}"'
        merges = ''

        for i, (type, dst_node, prop) in enumerate(links):

            assert 'reference_org' in prop
            assert 'reference_url_data' in prop
            assert 'reference_name' in prop
            assert 'reference_time_fetch' in prop

            prop = format_properties(prop)

            matches += f', (x{i})'
            where += f' AND elementId(x{i}) = "{dst_node}"'
            merges += f' MERGE (x)-[:{type}  {dict2str(prop)}]->(x{i}) '

        self.tx.run(matches + where + merges).consume()
        self.commit()

    def batch_add_properties(self, id_prop_list):
        """Add properties to existing nodes.

        id_prop_list should be a list of (int, dict) tuples, where the int refers to the
        node id and the dict contains the properties that should be added to the node.
        """
        # Ensure proper formatting and transform into dict.
        formatted_props = [{'id': node_id, 'props': format_properties(props)} for node_id, props in id_prop_list]

        for i in range(0, len(formatted_props), BATCH_SIZE):
            batch = formatted_props[i: i + BATCH_SIZE]

            add_query = """WITH $batch AS batch
            UNWIND batch AS item
            MATCH (n)
            WHERE elementId(n) = item.id
            SET n += item.props"""

            res = self.tx.run(add_query, batch=batch)
            res.consume()
            self.commit()


class BasePostProcess(object):
    def __init__(self, name):
        """IYP and references initialization."""

        self.reference = {
            'reference_name': f'iyp.{name}',
            'reference_org': 'Internet Yellow Pages',
            'reference_url_data': 'https://iyp.iijlab.net',
            'reference_url_info': str(),
            'reference_time_fetch': datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            'reference_time_modification': None
        }

        # connection to IYP database
        self.iyp = IYP()

    def close(self):
        # Commit changes to IYP
        self.iyp.close()

    def run(self):
        raise NotImplementedError()

    def unit_test(self):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()

    def rerun(self):
        self.delete()
        self.run()


class BaseCrawler(object):
    def __init__(self, organization, url, name):
        """IYP and references initialization.

        The crawler name should be unique.
        """

        self.organization = organization
        self.url = url
        self.name = name

        self.reference = {
            'reference_name': name,
            'reference_org': organization,
            'reference_url_data': url,
            'reference_url_info': str(),
            'reference_time_fetch': datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            'reference_time_modification': None
        }

        # connection to IYP database
        self.iyp = IYP()

    def create_tmp_dir(self, root='./tmp/', remove=False):
        """Create a temporary directory for this crawler.

        If remove is True, the directory is removed and recreated.
        If remove is False (default), the directory is kept if it exists
        (preserves cache).

        return: path to the temporary directory
        """

        path = self.get_tmp_dir(root)

        if remove and os.path.exists(path):
            rmtree(path)

        os.makedirs(path, exist_ok=True)

        return path

    def get_tmp_dir(self, root='./tmp/'):
        """Return the path to the temporary directory for this crawler.

        The directory may not exist yet.
        """

        assert self.name != ''
        if not root.endswith('/'):
            root += '/'

        return f'{root}{self.name}/'

    def fetch(self):
        """Large datasets may be pre-fetched using this method.

        Currently the BaseCrawler does nothing for this method. Note that all crawlers
        may fetch data at the same time, hence it may cause API rate limiting issues.
        """

    def count_relations(self):
        """Count the number of relations in the graph with the reference name of
        crawler."""

        result = self.iyp.tx.run(
            f"MATCH ()-[r]->() WHERE r.reference_name = '{self.name}' RETURN count(r) AS count").single()

        return result['count']

    def unit_test(self, relation_types):
        """Check for existence of relationships created by this crawler.

        relation_types should be a list of types for which existence is checked.
        """
        logging.info(f'Running existence test for {relation_types}')
        passed = True
        for relation_type in relation_types:
            existenceQuery = f"""MATCH ()-[r:{relation_type}]-()
                                USING INDEX r:{relation_type}(reference_name)
                                WHERE r.reference_name = '{self.reference['reference_name']}'
                                RETURN 0 LIMIT 1"""
            result = self.iyp.tx.run(existenceQuery)
            if len(list(result)) == 0:
                passed = False
                logging.error(f'Missing data for relation {relation_type}')
        return passed

    def close(self):
        # Commit changes to IYP
        self.iyp.close()


class CacheHandler:
    def __init__(self, dir: str, prefix: str) -> None:
        self.cache_dir = dir
        self.cache_file_prefix = dir + prefix
        self.cache_file_suffix = '.pickle.bz2'

    def cached_object_exists(self, object_name: str) -> bool:
        cache_file = f'{self.cache_file_prefix}{object_name}{self.cache_file_suffix}'
        return os.path.exists(cache_file)

    def load_cached_object(self, object_name: str):
        cache_file = f'{self.cache_file_prefix}{object_name}{self.cache_file_suffix}'
        with bz2.open(cache_file, 'rb') as f:
            return pickle.load(f)

    def save_cached_object(self, object_name: str, object) -> None:
        cache_file = f'{self.cache_file_prefix}{object_name}{self.cache_file_suffix}'
        with bz2.open(cache_file, 'wb') as f:
            pickle.dump(object, f)

    def clear_cache(self) -> None:
        rmtree(self.cache_dir)
