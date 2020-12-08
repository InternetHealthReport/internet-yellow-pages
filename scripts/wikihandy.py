import pywikibot
from pywikibot import pagegenerators as pg
from pywikibot.data import api
from SPARQLWrapper import SPARQLWrapper, JSON
import logging


class wikihandy(object):

    def __init__(self, wikidata_site, sparql="https://query.wikidata.org/sparql"):
        self._label_pid = {}
        self._label_qid = {}
        self._asn2qid = None
        self.wikidata_site = wikidata_site

        self.sparql = SPARQLWrapper(sparql)

    def get_items(self, label):
        """ Return the list of items for the given label"""
        params = {'action': 'wbsearchentities', 'format': 'json',
                'language': 'en', 'type': 'item', 
                'search': label}
        request = api.Request(site=self.wikidata_site, parameters=params)
        result = request.submit()
        return result['search'] if len(result['search'])>0 else None


    def label2qid(self, label, lang='en'):
        """Retrieve item id based on the given label"""

        if label not in self._label_qid:
            items = self.get_items(label)
            if items is None:
                return None

            self._label_qid[label] = items[0]['id']

        return self._label_qid[label]

    def label2pid(self, label, lang='en'):
        """Retrieve property id based on the given label"""

        if label not in self._label_pid:

            params = {'action': 'wbsearchentities', 'format': 'json',
                    'language': lang, 'type': 'property', 
                    'search': label}
            request = api.Request(site=self.wikidata_site, parameters=params)
            result = request.submit()

            if len(result['search']) == 0:
                return None

            if len(result['search']) > 1:
                logging.warning(f'Several properties have the label: {label}')
                logging.warning(result)

            self._label_pid[label] = result['search'][0]['id']


        return self._label_pid[label]

    def asn2qid(self, asn):
        """Retrive QID of items assigned with the given Autonomous System Number"""

        if self._asn2qid is None:
            QUERY = """
            #Items that have a pKa value set
            SELECT ?item ?asn
            WHERE 
            {
                    # ?item wdt:%s wdt:%s .
                    ?item wdt:%s ?asn .
            } 
            """ % (
                    self.label2pid('instance of'), 
                    self.label2qid('Autonomous System') , 
                    self.label2pid('autonomous system number')
                  )

            import requests

            self.sparql.setQuery(QUERY)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            
            self._asn2qid = {}
            for res in results['results']['bindings']:
                res_qid = res['item']['value'].rpartition('/')[2]
                res_asn = int(res['asn']['value'])

                self._asn2qid[res_asn] = res_qid

        return self._asn2qid.get(int(asn),None)


if __name__ == '__main__':
    wikidata_site = pywikibot.Site("wikidata", "wikidata")
    wh = wikihandy(wikidata_site)

    import IPython
    IPython.embed()
