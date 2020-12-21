import sys
import datetime
import requests
import cgi
import wikihandy 
import iso3166
from concurrent.futures import ThreadPoolExecutor

URL_RIPE_AS_NAME = 'https://ftp.ripe.net/ripe/asnames/asn.txt'
EXOTIC_CC = {'ZZ': 'unknown country', 'EU': 'Europe', 'AP': 'Asia-Pacific'}

class ASNames(object):
    def __init__(self):

        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Qualifiers for added properties
        self.qualifiers = [
            (self.wh.get_pid('source'), self.wh.get_qid('RIPE NCC')),
            (self.wh.get_pid('reference URL'), URL_RIPE_AS_NAME),
            (self.wh.get_pid('point in time'), self.wh.today())
            ]

    def run(self):
        """Fetch the AS name file from RIPE website and process lines one by one"""

        req = requests.get(URL_RIPE_AS_NAME)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        self.wh.login() # Login once for all threads

        pool = ThreadPoolExecutor(max_workers=32)
        for i, res in enumerate(pool.map(self.update_asn, req.text.splitlines())):
            sys.stderr.write(f'\rProcessed {i} ASes')

        pool.shutdown()
            

    def update_asn(self, one_line):
        new_as = False
        # Get ASN, name, and country code
        asn, _, name_cc = one_line.partition(' ')
        name, _, cc = name_cc.rpartition(', ')
        name = cgi.escape(name)     # Needed for wiki API

        asn_qid = self.asn_qid(asn)
        cc_qid = self.country_qid(cc)

        self.wh.upsert_statement('country found in RIPE AS names', asn_qid, self.wh.get_pid('country'), cc_qid, self.qualifiers)
        self.wh.upsert_statement('name compiled by RIPE', asn_qid, self.wh.get_pid('name'), name, self.qualifiers)

        return asn_qid


    def asn_qid(self, asn):
        """Find an AS QID or add it to wikibase if it doesn't exists."""

        # Find this AS QID or add it to wikibase
        qid = self.wh.asn2qid(asn)
        if qid is None:
            new_as = True
            # if this AS is unknown, create corresponding item
            qid = self.wh.add_item('add new AS', f'AS{asn}')
            self.wh.upsert_statement('AS found in RIPE names', qid, self.wh.get_pid('instance of'), self.wh.get_qid('Autonomous System'))
            self.wh.upsert_statement('AS found in RIPE names', qid, self.wh.get_pid('autonomous system number'), asn)

        return qid
        

    def country_qid(self, cc):
        """Find a country QID or add the country to wikibase if it doesn't exist 
        (the label is the country name as defined by iso3166)."""

        # Check if country page exists
        cc_label = 'unknown country'
        if cc in EXOTIC_CC:
            cc_label = EXOTIC_CC[cc]
        else:
            cc_label = iso3166.countries_by_alpha2[cc].name

        # Create the country page if it doesn't exists
        cc_qid = self.wh.get_qid(cc_label)
        if cc_qid is None:
            cc_qid = self.add_country(cc_label)
        cc_qid = self.wh.add_item('add new country', cc_label, None, cc)
        self.wh.upsert_statement('country from RIPE AS name file', 
                cc_qid, self.wh.get_pid('instance of'), self.wh.get_qid('country'))

        return cc_qid


if __name__ == '__main__':
    asnames = ASNames()
    asnames.run()
