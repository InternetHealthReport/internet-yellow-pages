import argparse
import logging
import sys

from iyp.crawlers.bgptools import AnycastPrefixesCrawler

ORG = 'bgp.tools'
URL = 'https://github.com/bgptools/anycast-prefixes'
NAME = 'bgptools.anycast_prefixes_v6'


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

    crawler = AnycastPrefixesCrawler(ORG, URL, NAME, 6)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
