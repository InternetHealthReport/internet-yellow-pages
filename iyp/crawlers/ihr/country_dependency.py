import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import arrow
import iso3166
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from iyp import BaseCrawler

# URL to the API
URL = 'https://ihr.iijlab.net/ihr/api/hegemony/countries/?country={country}&af=4'
# Name of the organization providing the data
ORG = 'IHR'
NAME = 'ihr.country_dependency'
MIN_HEGE = 0.01


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialize IYP."""

        # list of countries
        self.countries = iso3166.countries_by_alpha2

        # Session object to fetch peeringdb data
        retries = Retry(total=15,
                        backoff_factor=0.2,
                        status_forcelist=[104, 500, 502, 503, 504])

        self.http_session = requests.Session()
        self.http_session.mount('https://', HTTPAdapter(max_retries=retries))

        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ihr.iijlab.net/ihr/en-us/documentation#Country_s_network_dependency'  # noqa: E501

    def run(self):
        """Fetch data from API and push to IYP."""

        for cc, _ in self.countries.items():
            # Query IHR
            self.url = URL.format(country=cc)
            req = self.http_session.get(self.url + '&format=json')
            req.raise_for_status()
            data = json.loads(req.text)
            ranking = data['results']
            if not ranking:
                continue

            # Setup rankings' node
            country_qid = self.iyp.get_node('Country',
                                            {
                                                'country_code': cc,
                                            }
                                            )

            # Find the latest timebin in the data
            last_timebin = '1970-01-01'
            for r in ranking:
                if arrow.get(r['timebin']) > arrow.get(last_timebin):
                    last_timebin = r['timebin']
            self.reference['reference_url_data'] = self.url + f'&timebin={last_timebin}'
            self.reference['reference_time_modification'] = None
            try:
                date = datetime.strptime(last_timebin, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                self.reference['reference_time_modification'] = date
            except ValueError as e:
                logging.warning(f'Failed to get modification time: {e}')

            countryrank_statements = []
            if country_qid is not None:
                countryrank_statements = [('COUNTRY', country_qid, self.reference.copy())]

            # Make ranking and push data
            links = []
            for metric, weight in [('Total eyeball', 'eyeball'), ('Total AS', 'as')]:

                self.countryrank_qid = self.iyp.get_node('Ranking',
                                                         {'name': f'IHR country ranking: {metric} ({cc})'}
                                                         )
                self.iyp.add_links(self.countryrank_qid, countryrank_statements)

                # Filter out unnecessary data
                selected = [r for r in ranking
                            if (r['weightscheme'] == weight
                                and r['transitonly'] is False
                                and r['hege'] > MIN_HEGE
                                and r['timebin'] == last_timebin)
                            ]

                # Make sure the ranking is sorted and add rank field
                selected.sort(key=lambda x: x['hege'], reverse=True)
                asns = set()
                for i, asn in enumerate(selected):
                    asns.add(asn['asn'])
                    asn['rank'] = i + 1

                self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)

                # Compute links
                for asn in selected:
                    links.append({
                        'src_id': self.asn_id[asn['asn']],
                        'dst_id': self.countryrank_qid,
                        'props': [self.reference.copy(), asn]
                    })

            # Push links to IYP
            self.iyp.batch_add_links('RANK', links)

    def unit_test(self):
        return super().unit_test(['RANK', 'COUNTRY'])


# Main program
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
