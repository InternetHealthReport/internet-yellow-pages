import logging
import datetime
from neo4j import GraphDatabase


def format_properties(prop):
    """Make sure certain properties are always formatted the same way.
    For example IPv6 addresses are stored in lowercase, or ASN are kept as 
    integer not string."""

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


def dict2str(d):
    """Converts a python dictionary to a Cypher map."""

    data = [] 
    for key, value in d.items():
        if isinstance(value, str) and '"' in value:
            escaped = value.replace("'", r"\'")
            data.append(f"{key}: '{escaped}'")
        elif isinstance(value, str) or isinstance(value, datetime.datetime):
            data.append(f'{key}: "{value}"')
        else:
            data.append(f'{key}: {value}')

    return '{'+','.join(data)+'}'


class IYP(object):

    def __init__(self):

        logging.debug('Wikihandy: Enter initialization')

        # TODO: get config from configuration file
        self.server = 'localhost'
        self.port = 7687
        self.login = "neo4j"
        self.password = "password"

        # Connect to the database
        uri = f"neo4j://{self.server}:{self.port}"
        self.db = GraphDatabase.driver(uri, auth=(self.login, self.password))
        self.session = self.db.session()

        self._db_init()

    def _db_init(self):
        """Add usual constrains"""

        # Constrains for ASes
        self.session.run(
            " CREATE CONSTRAINT AS_UNIQUE_ASN IF NOT EXISTS "
            " FOR (n:AS) "
            " REQUIRE n.asn IS UNIQUE ")
        self.session.run(
            " CREATE CONSTRAINT AS_NOTNULL_ASN IF NOT EXISTS "
            " FOR (n:AS) "
            " REQUIRE n.asn IS NOT NULL ")

        # Constrains for prefixes
        self.session.run(
            " CREATE CONSTRAINT PREFIX_UNIQUE_PREFIX IF NOT EXISTS "
            " FOR (n:PREFIX) "
            " REQUIRE n.prefix IS UNIQUE ")
        self.session.run(
            " CREATE CONSTRAINT PREFIX_NOTNULL_PREFIX IF NOT EXISTS "
            " FOR (n:PREFIX) "
            " REQUIRE n.prefix IS NOT NULL ")
        self.session.run(
            " CREATE CONSTRAINT PREFIX_NOTNULL_AF IF NOT EXISTS "
            " FOR (n:PREFIX) "
            " REQUIRE n.af IS NOT NULL ")

        # Constrains for IPs
        self.session.run(
            " CREATE CONSTRAINT IP_UNIQUE_IP IF NOT EXISTS "
            " FOR (n:IP) "
            " REQUIRE n.ip IS UNIQUE ")
        self.session.run(
            " CREATE CONSTRAINT IP_NOTNULL_IP IF NOT EXISTS "
            " FOR (n:IP) "
            " REQUIRE n.ip IS NOT NULL ")
        self.session.run(
            " CREATE CONSTRAINT IP_NOTNULL_AF IF NOT EXISTS "
            " FOR (n:IP) "
            " REQUIRE n.af IS NOT NULL ")

        # Constrains for domain names
        self.session.run(
            " CREATE CONSTRAINT DN_UNIQUE_NAME IF NOT EXISTS "
            " FOR (n:DOMAIN_NAME) "
            " REQUIRE n.name IS UNIQUE ")
        self.session.run(
            " CREATE CONSTRAINT DN_NOTNULL_NAME IF NOT EXISTS "
            " FOR (n:DOMAIN_NAME) "
            " REQUIRE n.name IS NOT NULL ")

        # Constrains for countries
        self.session.run(
            " CREATE CONSTRAINT COUNTRY_UNIQUE_CC IF NOT EXISTS "
            " FOR (n:COUNTRY) "
            " REQUIRE n.country_code IS UNIQUE ")
        self.session.run(
            " CREATE CONSTRAINT COUNTRY_NOTNULL_CC IF NOT EXISTS "
            " FOR (n:COUNTRY) "
            " REQUIRE n.country_code IS NOT NULL ")


    def get_node(self, type, prop, create=False):
        """Find the ID of a node in the graph. Return None if the node does not
        exist or create the node if create=True."""

        prop = format_properties(prop)

        type_str = str(type)
        if isinstance(type, list):
            type_str = ':'.join(type)

        if create:
            result = self.session.run(f"MERGE (a:{type_str} {dict2str(prop)}) RETURN ID(a)").single()
        else:
            result = self.session.run(f"MATCH (a:{type_str} {dict2str(prop)}) RETURN ID(a)").single()

        if result is not None:
            return result[0]
        else:
            return None

    def get_node_extid(self, id_type, id):
        """Find a node in the graph which has an EXTERNAL_ID relationship with
        the given ID. Return None if the node does not exist."""

        result = self.session.run(f"MATCH (a)-[:EXTERNAL_ID]->(:{id_type} {{id:{id}}}) RETURN ID(a)").single()
        print(f"MATCH (a)-[:EXTERNAL_ID]->(:{id_type} {{id:{id}}}) RETURN ID(a)")

        if result is not None:
            return result[0]
        else:
            return None



    def add_links(self, src_node, links):
        """Create links from src_node to the destination nodes given in parameter
        links. This parameter is a list of [link_type, dst_node_id, prop_dict].
        The dictionary prop_dict should at least contain a 'source', 'point in time', 
        and 'reference URL'. Keys in this dictionary should contain no space.

        By convention link_type is written in UPPERCASE and keys in prop_dict are
        in lowercase."""

        
        for type, dst_node, prop in links:

            assert 'source' in prop
            assert 'reference_url' in prop
            assert 'point_in_time' in prop

            prop = format_properties(prop)

            self.session.run(
                " MATCH (a), (b) " 
                " WHERE ID(a) = $src_node AND ID(b) = $dst_node "
                f" MERGE (a)-[:{type}  {dict2str(prop)}]->(b) ",
                src_node=src_node, dst_node=dst_node).consume()


    def close(self):
        self.session.close()
        self.db.close()



