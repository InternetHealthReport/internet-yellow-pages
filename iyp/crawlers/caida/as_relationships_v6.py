import argparse
import logging
import os
import sys

from iyp.crawlers.caida import ASRelCrawler

URL = 'https://publicdata.caida.org/datasets/as-relationships/serial-1/'
ORG = 'CAIDA'
NAME = 'caida.as_relationships_v6'


class Crawler(ASRelCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 6)


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

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
