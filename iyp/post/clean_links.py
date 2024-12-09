import argparse
import logging
import os
import sys

from iyp import BasePostProcess


class PostProcess(BasePostProcess):
    def get_links_of_type(self, link_type, prop_dict=None):
        """Returns a list of all links of a given type with optional properties,
        including the source and destination nodes.

        Parameters:
        - link_type: The type of links to return.
        - prop_dict: Optional dictionary of properties to return.

        Returns:
        - List of links with the specified type and properties, including source and
        destination nodes.
        """
        prop_conditions = (
            ' AND '.join([f"r.{k} = '{v}'" for k, v in prop_dict.items()])
            if prop_dict
            else 'TRUE'
        )
        prop_str = ', '.join([f'r.{k}' for k in prop_dict.keys()]) if prop_dict else '*'

        query = f"""
        MATCH (src)-[r:{link_type}]->(dst)
        WHERE {prop_conditions}
        RETURN elementId(r) AS link_id, elementId(src) AS src_id, elementId(dst) AS dst_id, {prop_str}
        """
        result = self.iyp.tx.run(query)
        if result:
            return [record for record in result]
        else:
            return None

    def delete_links(self, link_ids):
        """Deletes all links in the given list.

        Parameters:
        - link_ids: List of link IDs to delete.

        Returns:
        - None
        """
        query = """
        UNWIND $link_ids AS link_id
        MATCH ()-[r]->()
        WHERE elementId(r) = link_id
        DELETE r
        """
        self.iyp.tx.run(query, link_ids=link_ids)

    def clean_links_of_type(self, link_type, prop_dict=None):
        links = self.get_links_of_type(link_type, prop_dict)
        link_dict = {}
        for link in links:
            key = (link['src_id'], link['dst_id'])
            if key not in link_dict:
                link_dict[key] = []
            link_dict[key].append(link['link_id'])

        # Create the new list of link IDs excluding the first one for each (src_id,
        # dst_id) pair
        filtered_link_ids = []
        for key, link_ids in link_dict.items():
            if len(link_ids) > 1:
                filtered_link_ids.extend(link_ids[1:])

        self.delete_links(filtered_link_ids)

    def run(self):
        # Clean links of all types with the reference_org 'OONI'
        link_types = ['COUNTRY', 'RESOLVES_TO', 'PART_OF', 'CATEGORIZED']
        for link_type in link_types:
            self.clean_links_of_type(link_type, {'reference_org': 'OONI'})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.info(f'Started: {sys.argv}')

    post = PostProcess()
    if args.unit_test:
        post.unit_test()
    else:
        post.run()
        post.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
