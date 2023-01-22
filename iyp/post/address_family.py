import sys
import logging
from iyp import BasePostProcess
import radix

class PostProcess(BasePostProcess):
    def run(self):
        """Add address family (4 or 6 for IPv4 or IPv6) to all IP and Prefix nodes."""

        # Update prefixes
        self.iyp.tx.run("match (pfx:Prefix) where pfx.prefix contains '.' SET pfx.af = 4;")
        self.iyp.tx.run("match (pfx:Prefix) where pfx.prefix contains ':' SET pfx.af = 6;")

        # Update IP addresses
        self.iyp.tx.run("match (ip:IP) where ip.ip contains '.' SET ip.af = 4;")
        self.iyp.tx.run("match (ip:IP) where ip.ip contains ':' SET ip.af = 6;")


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
