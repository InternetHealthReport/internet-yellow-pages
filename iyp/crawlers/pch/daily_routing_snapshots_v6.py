import argparse
import logging
import sys

from iyp.crawlers.pch import RoutingSnapshotCrawler

ORG = 'Packet Clearing House'
URL = 'https://www.pch.net/resources/Routing_Data/IPv6_daily_snapshots/'
NAME = 'pch.daily_routing_snapshots_v6'


class Crawler(RoutingSnapshotCrawler):
    def __init__(self, organization, url, name):
        self.name = name
        super().__init__(organization, url, name, af=6)


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
