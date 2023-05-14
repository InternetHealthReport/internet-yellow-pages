import argparse
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from iyp import BaseCrawler


def get_latest_dataset_url(inetintel_data_url: str, file_name_format: str):
    pattern = re.compile(r'^\d{4}-\d{2}$')
    response = requests.get(inetintel_data_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    date_elements = soup.find_all('a', string=pattern)
    all_date = []
    for date_element in date_elements:
        all_date.append(date_element.text)
    latest_date = datetime.strptime(all_date[-1], '%Y-%m')
    dateset_file_name: str = latest_date.strftime(file_name_format)
    inetintel_data_url_formated: str = inetintel_data_url.replace('github.com', 'raw.githubusercontent.com')
    inetintel_data_url_formated = inetintel_data_url_formated.replace('tree/', '')
    full_url: str = f'{inetintel_data_url_formated}/{all_date[-1]}/{dateset_file_name}'
    return full_url


# Organization name and URL to data
ORG = 'Internet Intelligence Lab'
URL = get_latest_dataset_url('https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping/tree/master/data',
                             'ii.as-org.v01.%Y-%m.json')
NAME = 'inetintel.as_org'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and set up a dictionary with the org/url/today's date in self.reference

    def run(self):
        """Fetch data and push to IYP."""

        # Create a temporary directory
        self.tmpdir = tempfile.mkdtemp()

        # Filename to save the JSON file as
        self.filename = os.path.join(self.tmpdir, 'inetintel_as_org.json')

        # Fetch data
        try:
            req = requests.get(URL)
        except requests.exceptions.ConnectionError as e:
            logging.error(e)
            sys.exit('Connection error while fetching data file')
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            sys.exit('Error while fetching data file')

        with open(self.filename, 'w') as file:
            file.write(req.text)

        print('Dataset crawled and saved in a temporary file.')

        # The dataset is very large. Pandas has the ability to read JSON, and, in
        # theory, it could do it in a more memory-efficient way.
        df = pd.read_json(self.filename, orient='index')
        print('Dataset has {} rows.'.format(len(df)))

        # Optimized code
        batch_size = 10000
        if len(df) < batch_size:
            batch_size = len(df)

        count_rows_global = 0
        count_relationships_global = 0
        connections = defaultdict(set)  # Remember the relationship between the "AS" and its Sibling.
        connections_org = defaultdict(set)  # Remember the relationship between the organizations
        org_id = self.iyp.batch_get_node_extid('PeeringdbOrgID')

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i + batch_size]
            batch_lines = []
            batch_asns = set()
            pdb_orgs = set()
            batch_urls = set()
            count_rows = 0
            count_relationships = 0

            for index, row in df_batch.iterrows():
                asn = int(index)
                batch_asns.add(asn)
                sibling_asns = set()

                for sibling_asn in row['Sibling ASNs']:
                    sibling_asns.add(int(sibling_asn))
                    batch_asns.add(int(sibling_asn))

                pdb_orgs = set([int(org['org_id']) for org in row['Reference Orgs'] if org['source'] == 'PDB'])

                url = row['Website']
                if len(url) > 1:
                    batch_urls.add(url)

                batch_lines.append([asn, url, sibling_asns, pdb_orgs])
                count_rows += 1

            asn_id = self.iyp.batch_get_nodes('AS', 'asn', batch_asns, all=False)
            url_id = self.iyp.batch_get_nodes('URL', 'url', batch_urls, all=False)

            asn_to_url_links = []
            asn_to_sibling_asn_links = []
            org_to_sibling_org_links = []

            for (asn, url, siblings, pdb_orgs) in batch_lines:
                asn_qid = asn_id[asn]

                if len(url) > 1:
                    url_qid = url_id[url]
                    asn_to_url_links.append({'src_id': asn_qid, 'dst_id': url_qid, 'props': [self.reference]})

                for org0 in pdb_orgs:
                    for org1 in pdb_orgs:
                        if org0 in org_id and org1 in org_id:
                            if org0 != org1 and org1 not in connections_org[org0]:
                                org0_qid = org_id[org0]
                                org1_qid = org_id[org1]
                                org_to_sibling_org_links.append(
                                    {'src_id': org0_qid, 'dst_id': org1_qid, 'props': [self.reference]})

                                connections_org[org0].add(org1)
                                connections_org[org1].add(org0)

                for sibling in siblings:
                    sibling_qid = asn_id[sibling]
                    if asn_qid != sibling_qid:
                        # A check whether asn and sibling are connected already.
                        if asn in connections and sibling in connections[asn]:
                            continue
                        elif sibling in connections and asn in connections[sibling]:
                            continue
                        else:
                            connections[asn].add(sibling)
                            asn_to_sibling_asn_links.append(
                                {'src_id': asn_qid, 'dst_id': sibling_qid, 'props': [self.reference]})
                            count_relationships += 1

            # Push all links to IYP
            if len(asn_to_url_links) > 0:
                self.iyp.batch_add_links('WEBSITE', asn_to_url_links)

            if len(asn_to_sibling_asn_links) > 0:
                self.iyp.batch_add_links('SIBLING_OF', asn_to_sibling_asn_links)

            if len(org_to_sibling_org_links) > 0:
                self.iyp.batch_add_links('SIBLING_OF', org_to_sibling_org_links)

            count_rows_global += count_rows
            count_relationships_global += count_relationships
            print('processed: {} rows and {} relationships'
                  .format(count_rows_global, count_relationships_global))

    def close(self):
        super().close()

        os.remove(self.filename)
        os.rmdir(self.tmpdir)


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
