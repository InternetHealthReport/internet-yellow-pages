import sys
import logging
from iyp import BasePostProcess
import radix

class PostProcess(BasePostProcess):
    def run(self):
        """Add address family (4 or 6 for IPv4 or IPv6) to all IP and Prefix nodes."""

        # Update prefixes
        self.iyp.tx.run("MATCH (pfx:Prefix) WHERE pfx.prefix CONTAINS '.' SET pfx.af = 4;")
        self.iyp.tx.run("MATCH (pfx:Prefix) WHERE pfx.prefix CONTAINS ':' SET pfx.af = 6;")

        # Update IP addresses
        self.iyp.tx.run("MATCH (ip:IP) WHERE ip.ip CONTAINS '.' SET ip.af = 4;")
        self.iyp.tx.run("MATCH (ip:IP) WHERE ip.ip CONTAINS ':' SET ip.af = 6;")
    def unit_test(self):
        
        self.run()
        # test the prefix tree for IPv4 and IPv6 and return count
        result_prefix = self.iyp.tx.run("MATCH (pfx:Prefix) WHERE pfx.af <> 4 and pfx.af <> 6 RETURN count(pfx);").data()
        
        # test the IP tree for IPv4 and IPv6 and return count
        result_ip = self.iyp.tx.run("MATCH (ip:IP) WHERE ip.af <> 4 and ip.af <> 6 RETURN count(ip);").data()
        
        result = result_prefix[0]['count(pfx)'] + result_ip[0]['count(ip)']
        logging.info("Count of the remaining prefex/IP which is not IPv4 or IPv6: %s and the assert result is %s" % (result, result == 0))
        post.close()
        print("assertion error ") if result != 0 else print("assertion success")
        assert result == 0
        
        
        
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
    
    if len(sys.argv) > 1 and sys.argv[1] == 'unit_test':
        post.unit_test()
    else :
        post.run()
        post.close()
        
    logging.info("End: %s" % sys.argv)

