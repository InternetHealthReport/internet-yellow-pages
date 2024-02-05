import logging
import sys

import tldextract

from iyp import BasePostProcess


class PostProcess(BasePostProcess):
    def run(self):
        """Link URLs and their corresponding HostNames."""

        # Get all URL nodes.
        url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url')

        # Get all HostName Nodes
        hostname_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name')

        # Compute links
        links = []
        for url, url_qid in url_id.items():
            # Extract host name from URL
            hostname = tldextract.extract(url).fqdn

            # Get HostName node for the fqdn of the URL
            hostname_qid = hostname_id.get(hostname)

            if hostname_qid is not None:
                links.append({
                    'src_id': url_qid,
                    'dst_id': hostname_qid,
                    'props': [self.reference]
                })

        # push links to IYP
        self.iyp.batch_add_links('PART_OF', links)


if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info('Start: %s' % sys.argv)

    post = PostProcess()
    post.run()
    post.close()

    logging.info('End: %s' % sys.argv)
