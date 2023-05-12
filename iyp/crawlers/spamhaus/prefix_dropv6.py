import logging
import sys

from iyp.crawlers.spamhaus.prefix_drop import Crawler

URL = 'https://www.spamhaus.org/drop/dropv6.txt'


# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info('Started: %s' % sys.argv)

    crawler = Crawler(URL)
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        crawler.unit_test(logging)
    else:
        crawler.run()
