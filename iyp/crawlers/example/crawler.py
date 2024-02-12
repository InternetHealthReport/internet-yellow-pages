import argparse
import logging
import os
import sys

import requests

from iyp import BaseCrawler, RequestStatusError

# Organization name and URL to data
ORG = 'Example Org'
URL = 'https://example.com/data.csv'
NAME = 'example.crawler'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference

    def run(self):
        """Fetch data and push to IYP."""

        # Fetch data
        req = requests.get(self.reference['reference_url_data'])
        if req.status_code != 200:
            logging.error('Cannot download data {req.status_code}: {req.text}')
            raise RequestStatusError('Error while fetching data file')

        # Process line one after the other
        for i, line in enumerate(req.text.splitlines()):
            self.update(line)
            sys.stderr.write(f'\rProcessed {i} lines')

        sys.stderr.write('\n')

    def update(self, one_line):
        """Add the entry to IYP if it's not already there and update its properties."""

        asn, value = one_line.split(',')

        # create node for value
        val_qid = self.iyp.get_node(
            'EXAMPLE_NODE_LABEL',
            {
                'example_property_0': value,
                'example_property_1': value,
            }
        )

        # set relationship
        statements = [['EXAMPLE_RELATIONSHIP_LABEL', val_qid, self.reference]]

        # Commit to IYP
        # Get the AS's node ID (create if it is not yet registered) and commit changes
        as_qid = self.iyp.get_node('AS', {'asn': asn})
        self.iyp.add_links(as_qid, statements)


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
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
