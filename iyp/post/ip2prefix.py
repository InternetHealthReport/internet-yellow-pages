import argparse
import logging
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

        # Find all different types of prefixes
        prefixes_labels = self.iyp.tx.run(
            'MATCH (pfx:Prefix) RETURN DISTINCT labels(pfx) AS pfx_labels;')

        all_labels = set([label for row in prefixes_labels for label in row['pfx_labels']])
        rtrees = {}

        for label in all_labels:
            if label == 'Prefix':
                continue
            # Get all prefixes in a radix tree
            prefix_id = self.iyp.batch_get_nodes_by_single_prop(label, 'prefix', all=True)
            additional_properties = list()

            rtrees[label] = radix.Radix()
            for prefix, prefix_qid in prefix_id.items():
                rnode = rtrees[label].add(prefix)
                rnode.data['id'] = prefix_qid
                prefix_split = self.__get_network_and_prefixlen(prefix)
                if prefix_split is not None:
                    additional_properties.append(
                        (prefix_qid, {'network': prefix_split[0], 'prefixlen': prefix_split[1]}))

            # Add network and prefixlen properties
            self.iyp.batch_add_properties(additional_properties)

        # Get all IP nodes
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip')

        # Compute links for IPs
        # We use a dictionary to avoid having duplicate links
        links = {}
        for ip, ip_qid in ip_id.items():
            if ip:
                for rtree in rtrees.values():
                    rnode = rtree.search_best(ip)
                    if rnode:
                        src = ip_qid
                        dst = rnode.data['id']
                        links[(src, dst)] = {
                            'src_id': src,
                            'dst_id': dst,
                            'props': [self.reference]
                        }

        # push IP to prefix links to IYP
        self.iyp.batch_add_links('PART_OF', list(links.values()))

        # Compute links sub-prefix and covering prefix
        links = []
        for rtree in rtrees.values():
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

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
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
