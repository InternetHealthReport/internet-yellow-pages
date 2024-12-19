import argparse
import logging
import sys

from iyp.crawlers.openintel import DnsgraphCrawler

URL = 'https://storage.dacs.utwente.nl/sommeser-dnsdep/NL'
ORG = 'OpenINTEL'
NAME = 'openintel.dnsgraph_nl'


class Crawler(DnsgraphCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
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
