import logging
from neo4j import GraphDatabase

# TODO: get config from configuration file


class IYP(object):

    def __init__(self):

        logging.debug('Wikihandy: Enter initialization')

        self.server = 'localhost'
        self.port = 7687
        self.login = "neo4j"
        self.password = "password"

        # Connect to the database
        uri = "neo4j://{self.server}:{self.port}"
        self.db = GraphDatabase.driver(uri, auth=(self.login, self.password))
        self.session = self.db.session() 

    def get_node(self, type, prop, create=False):
        """Find the ID of a node in the graph. Return None if the node does not
        exist or create the node if create=True."""

        if create:
            result = self.session.run("MERGE (a:$type $prop) RETURN ID(a)", type=type, prop=prop).single()
        else:
            result = self.session.run("MATCH (a:$type $prop) RETURN ID(a)", type=type, prop=prop).single()

        return result


    def add_links(self, src_node, links):
        """Create links from src_node to the destination nodes given in parameter
        links. This parameter is a list of [link_type, dst_node_id, prop_dict]."""

        for type, dst_node, prop in links:
            self.session.run("MATCH (a) WHERE ID(a) = $src_node "
                "CREATE (a)-[:$type]->(b $prop) WHERE ID(b) = $dst_node",
                src_node=src_node, type=type, prop=prop)


    def close(self):
        self.session.close()
        self.db.close()



