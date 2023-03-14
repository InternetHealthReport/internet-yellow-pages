import os
import sys
import logging
import requests
import tempfile
import json
from iyp import BaseCrawler

import neo4j.exceptions

# Organization name and URL to data
ORG = 'Internet Intelligence Lab'
URL = 'https://raw.githubusercontent.com/InetIntel/Improving-Inference-of-Sibling-ASes/master/data/output/output_dataset.json'
NAME = 'inetintel.siblings_asdb'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference

    def run(self):
        """Fetch data and push to IYP. """

        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Filename to save the JSON file as
        filename = os.path.join(tmpdir, 'output_dataset.json')

        # Fetch data
        try:
            req = requests.get(URL)
        except requests.exceptions.ConnectionError as e:
            logging.error(e)
            sys.exit('Connection error while fetching data file')
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            sys.exit('Error while fetching data file')

        with open(filename, "w") as file:
            file.write(req.text)

        with open(r'/home/pando-roopesh/ihr-org/internet-yellow-pages/siblings_asn.json', "r") as file:
            data = json.load(file)

        lines = []
        asns = set()
        sibling_asns = set()
        urls = set()

        for key, value in data.items():
            asn = key
            sibling_asns_set = value.get('Sibling ASNs')
            sibling_asns_list = list(sibling_asns_set)
            for sibling_asn in sibling_asns_list:
                sibling_asns.add(sibling_asn)
            url = value.get('Website')
            asns.add(asn)
            if len(url) > 1:
                urls.add(url)
            lines.append([asn, url, sibling_asns])

        print(lines)
        asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        sibling_id = self.iyp.batch_get_nodes('AS', 'asn', sibling_asns)
        url_id = self.iyp.batch_get_nodes('URL', 'url', urls)

        asn_to_url_links = []
        asn_to_sibling_asn_links = []
        count = 0
        for (asn, url, sibling_asns) in lines:
            print(asn)
            print(url)
            print(sibling_asns)
            asn_qid = asn_id[asn]
            url_qid = url_id[url]
            if len(url) > 1:
                asn_to_url_links.append({'src_id': asn_qid, 'dst_id': url_qid, 'props': [self.reference]})
            for sibling in sibling_asns:
                sibling_qid = sibling_id[sibling]
                asn_to_sibling_asn_links.append(
                    {'src_id': asn_qid, 'dst_id': sibling_qid, 'props': [self.reference]})
                print({'src_id': asn_qid, 'dst_id': sibling_qid, 'props': [self.reference]})
            count += 1

        # Push all links to IYP
        try:
            self.iyp.batch_add_links('WEBSITE', asn_to_url_links)
        except neo4j.exceptions.Neo4jError as e:
            print(e)

        try:
            self.iyp.batch_add_links('SIBLING_OF', asn_to_sibling_asn_links)
        except neo4j.exceptions.Neo4jError as e:
            print(e)

        print('processed: ')
        print(count)


# Main program
if __name__ == '__main__':
    scriptname = sys.argv[0].replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.WARNING,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Started: %s" % sys.argv)

    siblings_asdb = Crawler(ORG, URL, NAME)
    # if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
    #     siblingdb.unit_test(logging)
    # else:
    #     siblingdb.run()
    #     siblingdb.close()

    siblings_asdb.run()
    siblings_asdb.close()

    logging.info("End: %s" % sys.argv)
