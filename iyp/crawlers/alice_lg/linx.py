import sys
import logging
from iyp.crawlers.alice_lg import Crawler

URL = 'https://alice-rs.linx.net/api/v1/'
        
# Main program
if __name__ == '__main__':


    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    crawler = Crawler(URL)
    crawler.run()
