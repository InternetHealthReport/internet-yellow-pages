import sys
import logging
from iyp import BasePostProcess
pip install

class PostProcess(BasePostProcess):
    def run(self):
        """Fetch all IP and Prefix nodes, then link IPs to their most specific prefix."""

        # Get all prefixes in a radix trie
        prefix_id = self.iyp.batch_get_nodes('Prefix', 'prefix')

        rtree = radix.Radix()
        for prefix, prefix_qid in prefix_id.items():
            rnode = rtree.add(prefix,)
            rnode.data['id'] = prefix_qid

        # Get all IP nodes
        ip_id = self.iyp.batch_get_nodes('IP', 'ip')

        # Compute links
        links = []
        for ip, ip_qid in  ip_id.items():
            if ip:
                rnode = rtree.search_best(ip)

                if rnode:
                    links.append( {
                        'src_id': ip_qid,
                        'dst_id': rnode.data['id'],
                        'props': [self.reference]
                        })

        # push links to IYP
        self.iyp.batch_add_links('PART_OF', links)


if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Start: %s" % sys.argv)

    post = PostProcess()
    post.run()
    post.close()

    logging.info("End: %s" % sys.argv)
