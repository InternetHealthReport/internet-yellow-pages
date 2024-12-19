import argparse
import logging
import os
import sys

import radix

from iyp import BasePostProcess

NAME = 'post.ip2prefix'


class PostProcess(BasePostProcess):
    @staticmethod
    def __get_network_and_prefixlen(prefix):
        """Split an IP prefix into its network and prefix length.

        Return the prefix length as an integer. Return None if the prefix is invalid.
        """
        prefix_split = prefix.split('/')
        if len(prefix_split) != 2 or not prefix_split[1].isdigit():
            logging.error(f'Invalid prefix: {prefix}')
            return None
        return (prefix_split[0], int(prefix_split[1]))

    def run(self):
        """Fetch all IP and Prefix nodes, then link IPs to their most specific
        prefix."""

        # Get all prefixes in a radix tree
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix')
        additional_properties = list()

        rtree = radix.Radix()
        for prefix, prefix_qid in prefix_id.items():
            rnode = rtree.add(prefix)
            rnode.data['id'] = prefix_qid
            prefix_split = self.__get_network_and_prefixlen(prefix)
            if prefix_split is not None:
                additional_properties.append((prefix_qid, {'network': prefix_split[0], 'prefixlen': prefix_split[1]}))

        # Add network and prefixlen properties
        self.iyp.batch_add_properties(additional_properties)

        # Get all IP nodes
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip')

        # Compute links for IPs
        links = []
        for ip, ip_qid in ip_id.items():
            if ip:
                rnode = rtree.search_best(ip)

                if rnode:
                    links.append({
                        'src_id': ip_qid,
                        'dst_id': rnode.data['id'],
                        'props': [self.reference]
                    })

        # push IP to prefix links to IYP
        self.iyp.batch_add_links('PART_OF', links)

        # Compute links sub-prefix and covering prefix
        links = []
        for rnode in rtree:
            covering = rnode.parent

            if covering:
                links.append({
                    'src_id': rnode.data['id'],
                    'dst_id': covering.data['id'],
                    'props': [self.reference]
                })

        # push sub-prefix to covering-prefix links
        self.iyp.batch_add_links('PART_OF', links)

    def unit_test(self):
        raise NotImplementedError()


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

    post = PostProcess(NAME)
    if args.unit_test:
        post.unit_test()
    else:
        post.run()
        post.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
