import argparse
import logging
import sys

import radix

from iyp import BasePostProcess

# based on post/ip2prefix.py
NAME = 'post.laces'


class PostProcess(BasePostProcess):

    def run(self):
        """Fetch all LACeS AnycastPrefix nodes (created in crawler/ut-dacs/laces.py), then link IPs to those prefixes."""

        # Find all different types of prefixes
        prefixes_labels = self.iyp.tx.run('MATCH (apfx:AnycastPrefix) RETURN DISTINCT labels(apfx) AS apfx_labels')

        all_labels = set([label for row in prefixes_labels for label in row['apfx_labels']])
        all_labels.remove('AnycastPrefix')

        rtrees = dict()
        for label in all_labels:
            # Get all prefixes in a radix tree
            prefix_id = self.iyp.batch_get_nodes_by_single_prop(label, 'aprefix', all=True)

            rtrees[label] = radix.Radix()
            for prefix, prefix_qid in prefix_id.items():
                rnode = rtrees[label].add(prefix)
                rnode.data['id'] = prefix_qid

        # Get all IP nodes
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', batch_size=100000)

        # Compute links for IPs
        links = list()
        for ip, ip_qid in ip_id.items():
            for rtree in rtrees.values():
                rnode = rtree.search_best(ip)
                if rnode:
                    src = ip_qid
                    dst = rnode.data['id']
                    links.append(
                        {
                            'src_id': src,
                            'dst_id': dst,
                            'props': [self.reference]
                        }
                    )

        # push IP to prefix links to IYP
        self.iyp.batch_add_links('IS_ANYCAST', links)

    def unit_test(self):
        raise NotImplementedError()

    def delete(self):
        logging.info('Deleting existing relationships.')
        self.iyp.tx.commit()
        self.iyp.session.run("""
            MATCH ()-[r:PART_OF {reference_name: 'iyp.laces'}]->(:AnycastPrefix)
            CALL (r) {
                DELETE r
            } IN TRANSACTIONS OF 100000 ROWS
        """)
        self.iyp.tx = self.iyp.session.begin_transaction()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    parser.add_argument('--rerun', action='store_true')
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
    if args.rerun:
        post.rerun()
        post.close()
    else:
        post.run()
        post.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
