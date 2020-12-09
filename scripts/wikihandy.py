import pywikibot
from pywikibot import pagegenerators as pg
from pywikibot.data import api
from SPARQLWrapper import SPARQLWrapper, JSON
import logging

DEFAULT_WIKI_SPARQL = 'https://exp1.iijlab.net/wdqs/bigdata/namespace/wdq/sparql'
DEFAULT_WIKI_PROJECT = 'iyp'

class Wikihandy(object):

    def __init__(self, wikidata_project=DEFAULT_WIKI_PROJECT, sparql=DEFAULT_WIKI_SPARQL):
        self._label_pid = {}
        self._label_qid = {}
        self._asn2qid = None
        self.site = pywikibot.Site("en", wikidata_project)
        self.repo = pywikibot.DataSite("en", wikidata_project)

        self.sparql = SPARQLWrapper(sparql)


    def get_pid(self, label):
        """ Return the PID of the first property found with the given label"""

        return self.get_properties(label)[0]['id']

    def get_qid(self, label):
        """ Return the PID of the first item found with the given label"""

        return self.get_items(label)[0]['id']


    def get_items(self, label):
        """ Return the list of items for the given label"""
        params = {'action': 'wbsearchentities', 'format': 'json',
                'language': 'en', 'type': 'item', 
                'search': label}
        request = api.Request(site=self.site, parameters=params)
        result = request.submit()
        return result['search'] 

    def get_properties(self, label):
        """ Return the list of properties for the given label"""
        params = {'action': 'wbsearchentities', 'format': 'json',
                'language': 'en', 'type': 'property', 
                'search': label}
        request = api.Request(site=self.site, parameters=params)
        result = request.submit()
        return result['search']

    def add_property(self, label, description, aliases, data_type):
        """Create new property if it doesn't already exists. Return the property
        PID."""

        properties = self.get_properties(label)
        if len(properties) > 0:
            return properties[0]['id']

        new_prop = pywikibot.PropertyPage(self.repo, datatype=data_type)
        new_prop.editLabels(labels={'en':label}, summary="Setting labels")
        new_prop.editAliases({'en': aliases})
        new_prop.editDescriptions({'en': description})
        return new_prop.getID()

    def add_item(self, label, description, aliases):
        """Create new item if it doesn't already exists. Return the item QID"""

        items = self.get_items(label)
        if len(items) > 0:
            return items[0]['id']
        
        new_item = pywikibot.ItemPage(self.repo) 
        new_item.editLabels(labels={'en':label}, summary="Setting labels")
        new_item.editAliases({'en': aliases})
        new_item.editDescriptions({'en': description})
        return new_item.getID()

    def upsert_statement(self, item_id, property_id, target, datatype='wikibase-item', summary='upsert claim'):
        """Update statement value if the property is already assigned to the item,
        create it otherwise.
        If the property datatype is 'wikibase-item' then the target is expected to be the
        item PID. For properties with a different datatype the value of target
        is used as is."""
        
        item = pywikibot.ItemPage(self.repo, item_id)
        claims = item.get(u'claims') 

        if property_id in claims['claims']:
            claim = claims['claims'][property_id]

            if datatype == 'item': 
                target_value = pywikibot.ItemPage(self.repo, target)
                if target_value.getID() != claim.getTarget().id:
                    claim.changeTarget(target_value)
            else:
                target_value = target
                if target_value != claim.getTarget():
                    claim.changeTarget(target_value)

        else:
            self.add_statement(item_id, property_id, target, datatype, summary)

    def add_statement(self, item_id, property_id, target, datatype='wikibase-item', summary='adding claim'):
        """Create new property if it doesn't already exists. 
        If the property datatype is 'wikibase-item' then the target is expected to be the
        item PID. For properties with a different datatype the value of target
        is used as is."""

        item = pywikibot.ItemPage(self.repo, item_id)
        claim = pywikibot.Claim(self.repo, property_id)
        target_value = target
        if datatype == 'item': 
            target_value = pywikibot.ItemPage(self.repo, target_id)
        claim.setTarget(target_value)
        item.addClaim(claim, summary=summary)



        items = get_items(label)
        if len(items) > 0:
            return items[0]['id']
        
        new_item = pywikibot.ItemPage(self.repo) 
        new_item.editLabels(labels={'en':label}, summary="Setting labels")
        new_item.editAliases({'en': aliases})
        new_item.editDescriptions({'en': description})
        return new_item.getID()


    def label2qid(self, label):
        """Retrieve item id based on the given label"""

        if label not in self._label_qid:
            items = self.get_items(label)
            if len(items) == 0:
                return None

            self._label_qid[label] = items[0]['id']

        return self._label_qid[label]


    def label2pid(self, label):
        """Retrieve property id based on the given label"""

        if label not in self._label_pid:

            properties = self.get_properties(label)
            if len(properties) == 0:
                return None

            if len(properties) > 1:
                logging.warning(f'Several properties have the label: {label}')
                logging.warning(properties)

            self._label_pid[label] = properties[0]['id']

        return self._label_pid[label]


    def asn2qid(self, asn):
        """Retrive QID of items assigned with the given Autonomous System Number"""

        if self._asn2qid is None:
            QUERY = """
            #Items that have a pKa value set
            SELECT ?item ?asn
            WHERE 
            {
                    ?item wdt:%s wd:%s .
                    ?item wdt:%s ?asn .
            } 
            """ % (
                    self.label2pid('instance of'), 
                    self.label2qid('Autonomous System') , 
                    self.label2pid('autonomous system number')
                  )

            print(QUERY)
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
    wh = Wikihandy()

    import IPython
    IPython.embed()
