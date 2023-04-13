import sys
import logging
from iyp import BasePostProcess
import tldextract

class PostProcess(BasePostProcess):
    def run(self):
        """Link URLs and their corresponding DomainNames."""

        # Get all URL nodes.
        url_id = self.iyp.batch_get_nodes('URL', 'url')

        #Get all DomainName Nodes 
        domain_id = self.iyp.batch_get_nodes('DomainName','name')

        # Compute links
        links = []
        for url, url_qid in url_id.items():
            # Extract domain name from URL
            domain = tldextract.extract(url).registered_domain

            # Get DomainName node for the domain
            domain_qid = domain_id.get(domain)
            
            if domain_qid is not None:
                links.append({
                    'src_id': url_qid,
                    'dst_id': domain_qid,
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
