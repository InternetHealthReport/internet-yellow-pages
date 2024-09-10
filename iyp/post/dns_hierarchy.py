import argparse
import logging
import os
import sys

from iyp import BasePostProcess


class PostProcess(BasePostProcess):
    def run(self):
        """Connect existing DomainName nodes with PART_OF relationships to model the DNS
        hierarchy.

        For example: (:DomainName {name: 'ihr.iijlab.net'}) results in
            ihr.iijlab.net -[:PART_OF]-> iijlab.net -[:PART_OF]-> net
        where the nodes in between are DomainName nodes.
        Create intermediate DomainName nodes if needed.
        """

        self.reference['reference_name'] = 'iyp.post.dns_hierarchy'
        logging.info('Building DNS hierarchy.')

        # Fetch all existing DomainName nodes.
        dns_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name')

        # Build hierarchical relationships and keep track of new nodes that have to be
        # created.
        new_nodes = set()
        link_tuples = set()
        for dns_name in dns_id:
            while '.' in dns_name:
                _, parent = dns_name.split('.', maxsplit=1)
                link_tuple = (dns_name, parent)
                if link_tuple not in link_tuples:
                    link_tuples.add(link_tuple)
                if parent not in dns_id and parent not in new_nodes:
                    new_nodes.add(parent)
                dns_name = parent

        # Create new nodes.
        dns_id.update(self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', new_nodes, all=False))

        # Build relationships and push to IYP.
        part_of_links = list()
        for child, parent in link_tuples:
            part_of_links.append({'src_id': dns_id[child], 'dst_id': dns_id[parent], 'props': [self.reference]})

        self.iyp.batch_add_links('PART_OF', part_of_links)

    def count_relation(self):
        count = self.iyp.tx.run("""
            MATCH (:DomainName)-[r:PART_OF {reference_name: 'iyp.post.dns_hierarchy'}]->(:DomainName)
            RETURN count(r) AS count
            """).single()
        return count

    def unit_test(self):
        result_before = self.count_relation()
        logging.info('relations before: %s' % result_before)
        self.run()
        result_after = self.count_relation()
        logging.info('relations after: %s' % result_after)
        self.close()
        print('assertion error ') if result_after <= result_before else print('assertion success')
        assert result_after > result_before


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
        datefmt='%Y-%m-%d %H:%M:%S'
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
