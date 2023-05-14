import gzip
import json
import logging
import os
import pickle
import sys
from collections import defaultdict

import requests
import tldextract

from iyp.wiki.ip2asn import ip2asn
from iyp.wiki.wikihandy import Wikihandy

# URL to Rapid7 open data
# TODO automatically fetch filename
# TODO remove all data with URL regex
# TODO remove downloaded file
URL = 'https://opendata.rapid7.com/sonar.fdns_v2/2021-02-26-1614298023-fdns_a.json.gz'
# URL = 'https://opendata.rapid7.com/sonar.fdns_v2/2021-02-26-1614297920-fdns_aaaa.json.gz'  # noqa: W505


def download_file(url, local_filename):
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename


class Crawler(object):
    def __init__(self, fdns_url=URL):
        """Fetch QID for Rapid7 and initialize wikihandy."""

        sys.stderr.write('Initialization...\n')
        self.fdns_url = fdns_url
        # Helper for wiki access
        self.wh = Wikihandy()

        self.org_qid = self.wh.get_qid('Rapid7',
                                       create={  # Create it if it doesn't exist
                                           # Commit message
                                           'summary': 'add Rapid7 forward DNS data',
                                           # Item description
                                           'description': 'Rapid7, a security company that provides unified '
                                                          'vulnerability management solutions',
                                           'statements': [
                                               [self.wh.get_pid('instance of'), self.wh.get_qid('organization')],
                                               [self.wh.get_pid('website'), 'https://www.rapid7.com/'],
                                           ]
                                       })

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
            (self.wh.get_pid('source'), self.org_qid),
            (self.wh.get_pid('reference URL'), fdns_url),
            (self.wh.get_pid('point in time'), today)
        ]

        self.ia = ip2asn(wikihandy=self.wh)

        # keep track of all resolved prefixes so we just make one push per
        # domain
        self.tld_pfx = defaultdict(set)

    def match_domain_prefix(self, line):
        """Parse a line from the rapid7 dataset, extract the domain and ip, and find the
        corresponding IP prefix.

        return: (domain name, prefix) or None, None if the domain is not in the wiki
        """

        tld = None
        prefix = None

        datapoint = json.loads(line)
        if (datapoint['type'] in ['a', 'aaaa']
            and 'value' in datapoint
                and 'name' in datapoint):

            ext = tldextract.extract(datapoint['name'])
            tld = ext[-2] + '.' + ext[-1]

            # skip domains not in the wiki
            if self.wh.domain2qid(tld) is None:
                return tld, None

            ip_info = self.ia.lookup(datapoint['value'])
            if ip_info is None:
                return tld, None

            prefix = ip_info['prefix']
            self.tld_pfx[tld].add(prefix)

        return tld, prefix

    def run(self):
        """Fetch Rapid7 DNS forward data, find corresponding BGP prefixes and push
        resolution for domains already in the wikibase."""

        # download rapid7 data and find corresponding prefixes
        sys.stderr.write('Downloading Rapid7 dataset...\n')
        fname = self.fdns_url.split('/')[-1]
        if not os.path.exists(fname):
            fname = download_file(self.fdns_url, fname)

        sys.stderr.write('Processing dataset...\n')
        if os.path.exists(fname + '.pickle'):
            sys.stderr.write('Load data from cache!')
            self.tld_pfx = pickle.load(open(fname + '.pickle', 'rb'))
        else:
            with gzip.open(fname, 'rt') as finput:
                for line in finput:
                    self.match_domain_prefix(line)

            pickle.dump(self.tld_pfx, open(fname + '.pickle', 'wb'))

        sys.stderr.write(f'Found {len(self.tld_pfx)} domain names in Rapid7 dataset out of the '
                         f'{len(self.wh._domain2qid)} domain names in wiki\n')
        # push data to wiki
        for i, (tld, pfxs) in enumerate(self.tld_pfx.items()):
            sys.stderr.write(f'\33[2K\rUpdating iyp... {i+1}/{len(self.tld_pfx)}\t{tld} {len(pfxs)} prefixes')
            self.update(tld, pfxs)

        sys.stderr.write('\n')

    def update(self, tld, pfxs):
        """Update statements for the given domain name."""

        # make all statements
        statements = []
        for pfx in pfxs:
            pfx_qid = self.wh.prefix2qid(pfx, create=True)
            statements.append(
                [self.wh.get_pid('forward DNS'), pfx_qid, self.reference]
            )

        # Commit to wikibase
        # Get the domain name QID  and commit changes
        dn_qid = self.wh.domain2qid(tld)
        try:
            # TODO remove old data with URL regex
            self.wh.upsert_statements('update from Rapid7 forward DNS data', dn_qid, statements)
        except Exception as e:
            logging.error(f'Could not update domain {dn_qid}')
            logging.error(str(e))


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

    crawler = Crawler()
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        crawler.unit_test(logging)
    else:
        crawler.run()
