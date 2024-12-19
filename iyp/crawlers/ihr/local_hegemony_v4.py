import argparse
import logging
import sys

from iyp.crawlers.ihr import HegemonyCrawler

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/hegemony/ipv4/local/%Y/%m/%d/ihr_hegemony_ipv4_local_%Y-%m-%d.csv.lz4'
ORG = 'IHR'
NAME = 'ihr.local_hegemony_v4'


class Crawler(HegemonyCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, af=4)


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
