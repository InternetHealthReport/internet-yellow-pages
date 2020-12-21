import pywikibot
from pywikibot import pagegenerators as pg
from pywikibot.data import api
from SPARQLWrapper import SPARQLWrapper, JSON
import logging

DEFAULT_WIKI_SPARQL = 'http://localhost:8989/bigdata/namespace/wdq/sparql' #'https://exp1.iijlab.net/wdqs/bigdata/namespace/wdq/sparql'
DEFAULT_WIKI_PROJECT = 'local'
# DEFAULT_WIKI_SPARQL = 'https://exp1.iijlab.net/wdqs/bigdata/namespace/wdq/sparql'
# DEFAULT_WIKI_PROJECT = 'iyp'
DEFAULT_LANG = 'en'

#TODO add method to efficiently get countries
#TODO add method to efficiently get prefixes

class Wikihandy(object):

    def __init__(self, wikidata_project=DEFAULT_WIKI_PROJECT, lang=DEFAULT_LANG, 
            sparql=DEFAULT_WIKI_SPARQL, preload=True):
        self._asn2qid = None
        self.repo = pywikibot.DataSite(lang, wikidata_project)

        self.sparql = SPARQLWrapper(sparql)
        self.label_pid, self.label_qid = self._label2id()
        # TODO this is not neded?? already cached by pywikibot
        self.cache = {}

        if preload:
            self.asn2qid(1)

    def login(self):
        """Login to the wikibase."""
        
        return self.repo.login()

    def today(self):
        """Return a wikibase time object with current date."""

        now = self.repo.server_time()
        return pywikibot.WbTime(year=now.year, month=now.month, day=now.day,
            calendarmodel="http://www.wikidata.org/entity/Q1985727")
    
    def get_item(self, label=None, qid=None):
        """ Return the first item with the given label."""

        if qid is not None:
            if qid not in self.cache:
                self.cache[qid] = pywikibot.ItemPage(self.repo, qid)
            return self.cache[qid]

        if label is not None:
            if label in self.label_qid:
                qid = self.label_qid[label]
                if qid not in self.cache: 
                    self.cache[qid] = pywikibot.ItemPage(self.repo, qid)
                return self.cache[qid]

        return None

        # params = {'action': 'wbsearchentities', 'format': 'json',
                # 'language': lang, 'type': 'item', 
                # 'search': label}
        # request = api.Request(site=self.repo, parameters=params)
        # result = request.submit()
        # return result['search'] 

    def get_property(self, label=None, pid=None):
        """ Return the fisrt property with the given label"""

        if pid is not None:
            if pid not in self.cache:
                self.cache[pid] = pywikibot.PropertyPage(self.repo, pid)
            return [self.cache[pid]]

        if label is not None:
            if label in self.label_pid:
                pid = self.label_pid[label]
                if pid not in self.cache: 
                    self.cache[pid] = pywikibot.PropertyPage(self.repo, pid)
                return [self.cache[pid]]

        return None

        # params = {'action': 'wbsearchentities', 'format': 'json',
                # 'language': lang, 'type': 'property', 
                # 'search': label}
        # request = api.Request(site=self.repo, parameters=params)
        # result = request.submit()
        # print(result)
        # return result['search']

    def add_property(self, summary, label, description, aliases, data_type):
        """Create new property if it doesn't already exists. Return the property
        PID."""

        pid = self.get_pid(label)
        if pid is not None:
            return pid

        new_prop = pywikibot.PropertyPage(self.repo, datatype=data_type)
        data = {
                'labels': {'en':label},
                'aliases': {'en': aliases},
                'descriptions': {'en': description}
                }
        new_prop.editEntity(data, summary=summary)
        pid = new_prop.getID()

        # Keep it in the cache
        self.label_pid[label] = pid
        self.cache[pid] = new_prop

        return pid 

    def add_item(self, summary, label=None, description=None, aliases=None, sitelinks=None):
        """Create new item if it doesn't already exists. Return the item QID"""

        qid = self.get_qid(label)
        if qid is not None:
            return qid
        
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
        qid = new_item.getID()

        # Keep it in the cache
        self.label_qid[label] = qid
        self.cache[qid] = new_item

        return qid

    def _update_statement_local(self, claims, target, ref_urls=None):

        ref_url_pid = self.label_pid['reference URL']
        selected_claim = claims[0]

        # search for a claim with the same reference url
        if ref_urls is not None:
            for claim in claims:
                if ref_url_pid in claim.qualifiers: 
                    for qualifier in claim.qualifiers[ref_url_pid]:
                        if qualifier.getTarget() == ref_urls:
                            selected_claim = claim
                            break

        if selected_claim.type == 'wikibase-item': 
            target_value = self.get_item(qid=target)
            if target_value.getID() != selected_claim.getTarget().id:
                # selected_claim.changeTarget(target_value)                    # API access
                selected_claim.setTarget(target_value)                    # no API access!
        else:
            target_value = target
            if target_value != selected_claim.getTarget():
                # selected_claim.changeTarget(target_value)                    # API access
                selected_claim.setTarget(target_value)                    # no API access!

        return selected_claim



    def upsert_statement(self, summary, item_id, property_id, target, qualifiers={}):
        """Update statement value if the property is already assigned to the item,
        create it otherwise.
        Qualifiers is a list of pairs (PID, value (e.g QID, string, URL)).
        If an existing claim has the same 'reference URL' has the one given in 
        the qualifiers then this claim value and qualifiers will be updated.
        Otherwise the first claim will be updated.

        TODO: shall we keep history if 'point in time' is given?

        Notices:
        - If the property datatype is 'wikibase-item' then the target is expected 
        to be the item PID. For properties with a different datatype the value 
        of target is used as is.
        - If the item has multiple times the given property only the first one
        is modified."""

        ref_url_pid = self.label_pid['reference URL']
        given_ref_urls = [val for pid, val in qualifiers if pid==ref_url_pid]
        
        item = self.get_item(qid=item_id)
        claims = item.get(u'claims')['claims']

        selected_claim = None
        if property_id in claims:
            # update the main statement value
            selected_claim = self._update_statement_local(claims[property_id], target, given_ref_urls)
        else:
            # create a new claim
            selected_claim = pywikibot.Claim(self.repo, property_id)
            target_value = target
            if selected_claim.type == 'wikibase-item': 
                target_value = self.get_item(qid=target)
            selected_claim.setTarget(target_value)

        # update qualifiers
        claims = selected_claim.qualifiers 
        new_qualifiers = [] 
        for pid, value in qualifiers:
            if pid in claims:
                self._update_statement_local(claims[pid], value)
            else:
                new_qualifiers.append( [pid, value] )

        # Add new qualifiers
        updated_claim = selected_claim.toJSON()
        for pid, value in new_qualifiers:
            qualifier = pywikibot.Claim(self.repo, pid)
            target_value = value
            if qualifier.type == 'wikibase-item':
                target_value = self.get_item(qid=target_value)
            qualifier.setTarget(target_value)
            updated_claim['qualifiers'][pid] = [qualifier.toJSON()['mainsnak']]

        # Commit changes
        item.editEntity({'claims':[updated_claim]},
                asynchronous=True, callback=self.on_delivery)


    # TODO: add a upsert_statements to add multiple statements with only one
    # API call?



    def on_delivery(self, entity, error):
        """"""

        if error is not None:
            print('!!! ERROR (on_delivery)!!!')
            print(error)


    def add_statement(self, summary, item_id, property_id, target, qualifiers=[]):
        """Create new claim, if the property datatype is 'wikibase-item' then 
        the target is expected to be the item PID. For properties with a 
        different datatype the value of target is used as is.
        Qualifiers is a list of pairs (PID, value (e.g QID, string, URL))
        """

        item = self.get_item(qid=item_id)
        claim = pywikibot.Claim(self.repo, property_id)
        target_value = target
        if claim.type == 'wikibase-item': 
            target_value = self.get_item(qid=target)
        claim.setTarget(target_value)
        item.addClaim(claim, summary=summary)

        for pid, value in qualifiers:
            qualifier = pywikibot.Claim(self.repo, pid)
            target_value = value
            if qualifier.type == 'wikibase-item':
                target_value = self.get_item(qid=value)
            qualifier.setTarget(target_value)
            claim.addQualifier(qualifier, summary=summary)

    def get_qid(self, label):
        """Retrieve item id based on the given label. Returns None if the label
        is unknown."""

        return self.label_qid.get(label, None)

    def get_pid(self, label):
        """Retrieve property id based on the given label. Returns None if the label
        is unknown."""

        return self.label_pid.get(label, None)

    def extid2qid(self, label=None, qid=None):
        """Find items that have an external ID for the given type of IDs.
        return: dict where keys are the external ids and values are the QIDs

        warning: assumes there is only one item per external IDs
        """

        extid_qid = qid
        if qid is None and label is not None:
            ext_qid = self.get_qid(label)

        if extid_qid is None:
            print('Error: could not find the item corresponding to this external ID')
            return None

        QUERY = """
        #Items that have a pKa value set
        SELECT ?item ?extid
        WHERE 
        {
                ?item p:%s ?extidStatement .
                ?extidStatement ps:%s ?extid .
                ?extidStatement pq:%s ?%s .
        } 
        """ % (
                self.get_pid('external ID'), 
                self.get_pid('external ID'), 
                self.get_pid('instance of'), 
                extid_qid
                )

        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        
        extid2qid = {}
        for res in results['results']['bindings']:
            res_qid = res['item']['value'].rpartition('/')[2]
            res_extid = res['extid']['value']

            extid2qid[res_extid] = res_qid

        return extid2qid
        

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
                    self.get_pid('instance of'), 
                    self.get_qid('Autonomous System') , 
                    self.get_pid('autonomous system number')
                  )

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
        wh.repo.throttle.setDelays(0,1)

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
                item = self.get_item(qid=qid)
                item.delete(reason='delete all', prompt=False)


    def _label2id(self):
        """Return two dictionaries, one for properties and  one for items, with 
        labels as keys and Q/P IDs as values. For entities that have the same 
        label only the first entity found is given, the other are ignored."""
        properties = {}
        items = {}
        QUERY="""SELECT ?item ?itemLabel
            WHERE { 
                ?item rdfs:label ?itemLabel. 
            } """

        # Fetch existing entities
        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        
        entities = []
        for res in results['results']['bindings']:
            id = res['item']['value'].rpartition('/')[2]
            label = res['itemLabel']['value']

            # sort properties/items in dictionaries
            if id.startswith('P'):
                if label not in properties:
                    properties[label] = id
            elif id.startswith('Q'):
                if label not in items:
                    items[label] = id

        return properties, items

if __name__ == '__main__':
    wh = Wikihandy()

    import IPython
    IPython.embed()
