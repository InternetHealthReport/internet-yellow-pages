import pywikibot
from pywikibot import pagegenerators as pg
from pywikibot.data import api
from SPARQLWrapper import SPARQLWrapper, JSON
import logging

DEFAULT_WIKI_SPARQL = 'https://exp1.iijlab.net/wdqs/bigdata/namespace/wdq/sparql'
DEFAULT_WIKI_PROJECT = 'iyp'
DEFAULT_LANG = 'en'

class Wikihandy(object):

    def __init__(self, wikidata_project=DEFAULT_WIKI_PROJECT, lang=DEFAULT_LANG, 
            sparql=DEFAULT_WIKI_SPARQL):
        self._label_pid = {}
        self._label_qid = {}
        self._asn2qid = None
        self.site = pywikibot.Site(lang, wikidata_project)
        self.repo = pywikibot.DataSite(lang, wikidata_project)

        self.sparql = SPARQLWrapper(sparql)


    def get_pid(self, label):
        """ Return the PID of the first property found with the given label"""

        return self.get_properties(label)[0]['id']

    def get_qid(self, label):
        """ Return the PID of the first item found with the given label"""

        return self.get_items(label)[0]['id']

    #TODO implement a different way to find entities
    def get_items(self, label, lang='en'):
        """ Return the list of items for the given label"""
        params = {'action': 'wbsearchentities', 'format': 'json',
                'language': lang, 'type': 'item', 
                'search': label}
        request = api.Request(site=self.repo, parameters=params)
        result = request.submit()
        return result['search'] 

    def get_properties(self, label, lang='en'):
        """ Return the list of properties for the given label"""
        params = {'action': 'wbsearchentities', 'format': 'json',
                'language': lang, 'type': 'property', 
                'search': label}
        request = api.Request(site=self.repo, parameters=params)
        result = request.submit()
        print(result)
        return result['search']

    def add_property(self, summary, label, description, aliases, data_type):
        """Create new property if it doesn't already exists. Return the property
        PID."""

        properties = self.get_properties(label)
        if len(properties) > 0:
            return properties[0]['id']

        new_prop = pywikibot.PropertyPage(self.repo, datatype=data_type)
        data = {
                'labels': {'en':label},
                'aliases': {'en': aliases},
                'descriptions': {'en': description}
                }
        new_prop.editEntity(data, summary=summary)
        return new_prop.getID()

    def add_item(self, summary, label=None, description=None, aliases=None, sitelinks=None):
        """Create new item if it doesn't already exists. Return the item QID"""

        print('Adding item: ', label, description, aliases, sitelinks)

        items = self.get_items(label)
        if len(items) > 0:
            print('!!! item already exists')
            print (items)
            return items[0]['id']
        
        data = {}
        new_item = pywikibot.ItemPage(self.repo) 
        if label is not None:
            data['labels'] = {'en':label}
        if aliases is not None:
            data['aliases'] = {'en':aliases}
        if description is not None:
            data['descriptions'] = {'en':description}
        if False and sitelinks is not None:
            #FIXME: always raising exception?
            data['sitelinks'] = sitelinks
        new_item.editEntity(data, summary=summary)
        return new_item.getID()


    def upsert_statement(self, summary, item_id, property_id, target, datatype='wikibase-item'):
        """Update statement value if the property is already assigned to the item,
        create it otherwise.

        Notices:
        - If the property datatype is 'wikibase-item' then the target is expected 
        to be the item PID. For properties with a different datatype the value 
        of target is used as is.
        - If the item has multiple times the given property only the first one
        is modified."""
        
        item = pywikibot.ItemPage(self.repo, item_id)
        claims = item.get(u'claims') 

        if property_id in claims['claims']:
            claim = claims['claims'][property_id][0]
            print(claim)

            if datatype == 'wikibase-item': 
                target_value = pywikibot.ItemPage(self.repo, target)
                if target_value.getID() != claim.getTarget().id:
                    claim.changeTarget(target_value)
            else:
                target_value = target
                if target_value != claim.getTarget():
                    claim.changeTarget(target_value)

        else:
            self.add_statement(summary, item_id, property_id, target, datatype)

    def add_statement(self, summary, item_id, property_id, target, datatype='wikibase-item'):
        """Create new property if it doesn't already exists. 
        If the property datatype is 'wikibase-item' then the target is expected to be the
        item PID. For properties with a different datatype the value of target
        is used as is."""

        item = pywikibot.ItemPage(self.repo, item_id)
        claim = pywikibot.Claim(self.repo, property_id)
        target_value = target
        if datatype == 'wikibase-item': 
            target_value = pywikibot.ItemPage(self.repo, target)
        claim.setTarget(target_value)
        item.addClaim(claim, summary=summary)

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

            self.sparql.setQuery(QUERY)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            
            self._asn2qid = {}
            for res in results['results']['bindings']:
                res_qid = res['item']['value'].rpartition('/')[2]
                res_asn = int(res['asn']['value'])

                self._asn2qid[res_asn] = res_qid

        return self._asn2qid.get(int(asn),None)


    def _delete_all_items(self):
        # Reduce throttling delay
        wh.site.throttle.setDelays(0,1)

        QUERY="""SELECT ?item ?itemLabel
            WHERE { 
            ?item rdfs:label ?itemLabel. 
            } """

        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        
        for res in results['results']['bindings']:
            qid = res['item']['value'].rpartition('/')[2]
            if qid.startswith('Q'):
                item = pywikibot.ItemPage(self.repo, qid)
                item.delete(reason='delete all', prompt=False)


    def get_all_entities(self):
        QUERY="""SELECT ?item ?itemLabel
            WHERE { 
            ?item rdfs:label ?itemLabel. 
            } """

        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        
        entities = []
        for res in results['results']['bindings']:
            id = res['item']['value'].rpartition('/')[2]
            label = res['itemLabel']['value']
            entities.append([id,label])


        return entities

    def get_all_properties_items(self):
        """Return two dictionaries (properties and items) with labels as keys
        and Q/P IDs as values. For entities that have the same label only the
        first entity found is given, the other are ignored."""
        properties = {}
        items = {}
        # Fetch existing entities
        for id, label in self.get_all_entities():
            if id.startswith('P'):
                if label not in properties:
                    properties[label]=id
            elif id.startswith('Q'):
                if label not in items:
                    items[label]=id

        return properties, items

if __name__ == '__main__':
    wh = Wikihandy()

    import IPython
    IPython.embed()
