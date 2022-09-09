import logging
from neo4j import GraphDatabase

# TODO: get config from configuration file

def dict2str(d):

    data = [] 
    for key, value in d.items():
        if isinstance(value, str) and '"' in value:
            data.append(f"{key}: '{value}'")
        elif isinstance(value, str):
            data.append(f'{key}: "{value}"')
        else:
            data.append(f'{key}: {value}')

    return '{'+','.join(data)+'}'

class IYP(object):

    def __init__(self):

        logging.debug('Wikihandy: Enter initialization')

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
            " CREATE CONSTRAINT PREFIX_NOTNULL_af IF NOT EXISTS "
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
            " CREATE CONSTRAINT IP_NOTNULL_IP IF NOT EXISTS "
            " FOR (n:IP) "
            " REQUIRE n.af IS NOT NULL ")



    def get_node(self, type, prop, create=False):
        """Find the ID of a node in the graph. Return None if the node does not
        exist or create the node if create=True."""

        if create:
            result = self.session.run(f"MERGE (a:{type} {dict2str(prop)}) RETURN ID(a)").single()
        else:
            result = self.session.run(f"MATCH (a:{type} {dict2str(prop)}) RETURN ID(a)").single()

        if result is not None:
            return result[0]
        else:
            return None


    def add_links(self, src_node, links):
        """Create links from src_node to the destination nodes given in parameter
        links. This parameter is a list of [link_type, dst_node_id, prop_dict]."""

        for type, dst_node, prop in links:
            self.session.run(
                f"MERGE (a)-[:{type}]->(b {prop})" 
                "WHERE ID(a) = $src_node ID(b) = $dst_node",
                src_node=src_node, dst_node=dst_node)

    def close(self):
        self.session.close()
        self.db.close()



