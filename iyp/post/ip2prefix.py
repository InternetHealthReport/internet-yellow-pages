import argparse
import logging
import os
import sys

import radix

from iyp import BasePostProcess


class PostProcess(BasePostProcess):
    def run(self):
        """Fetch all IP and Prefix nodes, then link IPs to their most specific
        prefix."""

        # Get all prefixes in a radix trie
        prefix_id = self.iyp.batch_get_nodes('Prefix', 'prefix')

        rtree = radix.Radix()
        for prefix, prefix_qid in prefix_id.items():
            rnode = rtree.add(prefix,)
            rnode.data['id'] = prefix_qid

        # Get all IP nodes
        ip_id = self.iyp.batch_get_nodes('IP', 'ip')

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

    def count_relation(self):
        count = self.iyp.tx.run('MATCH (ip:IP)-[r]->()  RETURN count(r) AS count').single()
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
