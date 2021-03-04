import os
import pywikibot
from SPARQLWrapper import SPARQLWrapper, JSON
import json
import logging
import iso3166
import arrow
from collections import defaultdict
from threading import RLock


# DEFAULT_WIKI_SPARQL = 'http://localhost:8989/bigdata/namespace/wdq/sparql' #'https://exp1.iijlab.net/wdqs/bigdata/namespace/wdq/sparql'
# DEFAULT_WIKI_PROJECT = 'local'
DEFAULT_WIKI_SPARQL = 'http://iyp-proxy.iijlab.net/bigdata/namespace/wdq/sparql'
DEFAULT_WIKI_PROJECT = 'iyp'
DEFAULT_LANG = 'en'
MAX_PENDING_REQUESTS = 100

EXOTIC_CC = {'ZZ': 'unknown country', 'EU': 'Europe', 'AP': 'Asia-Pacific'}

#TODO add method to efficiently get countries
#TODO label2QID should not include AS, prefixes, countries

from functools import wraps

# Decorator for making a method thread safe and fix concurrent pywikibot cache
# accesses
def thread_safe(method):
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        with self.lock:
            res = method(self, *method_args, **method_kwargs)
        return res
    return _impl


class Wikihandy(object):

    def __init__(self, wikidata_project=DEFAULT_WIKI_PROJECT, lang=DEFAULT_LANG, 
            sparql=DEFAULT_WIKI_SPARQL, preload=True):

        logging.debug('Wikihandy: Enter initialization')

        # used to make pywikibot cache access thread-safe
        self.lock = RLock()

        self._asn2qid = None
        self._prefix2qid = None
        self.repo = pywikibot.DataSite(lang, wikidata_project, 
                user=pywikibot.config.usernames[wikidata_project][lang])

        self.sparql = SPARQLWrapper(sparql)
        self.label_pid, self.label_qid = self._label2id()
        # TODO this is not neded?? already cached by pywikibot
        self.cache = {}
        self.pending_requests = 0

        if preload:
            self.asn2qid(1)
            self.prefix2qid('10.0.0.0/8')

        logging.debug('Wikihandy: Leave initialization')

    def login(self):
        """Login to the wikibase."""
        
        return self.repo.login()

    def today(self):
        """Return a wikibase time object with current date."""

        now = self.repo.server_time()
        return pywikibot.WbTime(year=now.year, month=now.month, day=now.day,
            calendarmodel="http://www.wikidata.org/entity/Q1985727")
    
    def to_wbtime(self, datetime):
        """Convert a string, timestamp, or datetime object to a pywikibot WbTime
        object"""

        dt = arrow.get(datetime)
        dtstr = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        return pywikibot.WbTime.fromTimestr(dtstr,
            calendarmodel="http://www.wikidata.org/entity/Q1985727")
    
    @thread_safe
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

    @thread_safe
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

    @thread_safe
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
        self.editEntity(new_prop, data, summary)
        pid = new_prop.getID()

        # Keep it in the cache
        self.label_pid[label] = pid
        self.cache[pid] = new_prop

        return pid 

    @thread_safe
    def add_item(self, summary, label, description=None, aliases=None, statements=None):
        """Create new item if it doesn't already exists. Return the item QID"""

        qid = self.get_qid(label)
        if qid is not None:
            self.upsert_statements(summary, qid, statements)
            return qid
        
        data = {}
        new_item = pywikibot.ItemPage(self.repo) 
        if label is not None:
            data['labels'] = {'en':label.strip()}
        if aliases is not None:
            data['aliases'] = {'en':aliases}
        if description is not None and label != description:
            data['descriptions'] = {'en':description}
        if statements is not None:
            data['claims'] = self.upsert_statements(summary,new_item,statements,commit=False)['claims']

        self.editEntity(new_item, data, summary)
        qid = new_item.getID()

        # Keep it in the cache
        self.label_qid[label] = qid
        self.cache[qid] = new_item

        return qid

    def __dict2quantity(self, target):
        """Transform a dictionnary representing a quantity (value, unit: a QID
        to the item representing the unit, lowerBound, upperBound) to a wikibase 
        quantity object."""

        target_tmp = dict(target)
        if 'unit' in target_tmp:
            target_tmp['unit'] = self.get_item(qid=target['unit'])
        target_tmp['site'] = self.repo
        return pywikibot.WbQuantity(**target_tmp)



    def _update_statement_local(self, claims, target, ref_urls=None, 
            sources=None, reference=False):
        """Update a statement locally (changed are not pushed to wikibase). 
        If a reference URL is given, then it will update the statement that have
        the same reference URL. Otherwise it will update the first statement that
        has no reference URL."""

        ref_url_pid = self.label_pid['reference URL']
        source_pid = self.label_pid['source']
        selected_claim = None

        # search for a claim with the same reference url
        if ref_urls is not None:
            for claim in claims:
                for source in claim.sources:
                    if ref_url_pid in source:
                        for ref in source[ref_url_pid]:
                            if ref.getTarget() in ref_urls:
                                selected_claim = claim
                                break

        # search for a claim with the same source
        elif sources is not None:
            for claim in claims:
                for source in claim.sources:
                    if source_pid in source:
                        for ref in source[source_pid]:
                            if ref.getTarget() in sources:
                                selected_claim = claim
                                break

        # search for the first claim without a reference url
        else:
            for claim in claims:
                for source in claim.sources:
                    if ref_url_pid not in source:
                        selected_claim = claim
                        break

        if selected_claim is None:
            # Couldn't find a matching claim
            return None


        # Update statement target value
        if selected_claim.type == 'wikibase-item': 
            target_value = self.get_item(qid=target)
            if target_value.getID() != selected_claim.getTarget().id:
                # selected_claim.changeTarget(target_value)                    # API access
                selected_claim.setTarget(target_value)                    # no API access!
        elif selected_claim.type == 'quantity': 
            target_value = self.__dict2quantity(target)
            if target_value != selected_claim.getTarget():
                selected_claim.setTarget(target_value)                    # no API access!
        else:
            target_value = target
            if target_value != selected_claim.getTarget():
                # selected_claim.changeTarget(target_value)                    # API access
                selected_claim.setTarget(target_value)                    # no API access!

        return selected_claim
    
    def _update_qualifiers_local(self, qualifiers, new_qualifiers_list):
        new_qualifiers = [] 
        for pid, value in new_qualifiers_list:
            if pid in qualifiers:
                self._update_statement_local(qualifiers[pid], value)
            else:
                new_qualifiers.append( [pid, value] )

        return new_qualifiers

    def _update_references_local(self, sources, new_ref_list):
        new_sources = [] 
        for pid, value in new_ref_list:
            updated = False
            for source in sources:
                if pid in source:
                    source = self._update_statement_local(source[pid], value)
                    updated = True
            if not updated:
                new_sources.append( [pid, value] )

        return new_sources

    def _insert_qualifiers_local(self, claim, new_qualifiers):
        # Add new qualifiers
        if 'qualifiers' not in claim and new_qualifiers:
            claim['qualifiers'] = {}

        for pid, value in new_qualifiers:
            qualifier = pywikibot.Claim(self.repo, pid)
            target_value = value
            if qualifier.type == 'wikibase-item':
                target_value = self.get_item(qid=value)
            elif qualifier.type == 'quantity': 
                target_value = self.__dict2quantity(value)
            qualifier.setTarget(target_value)
            claim['qualifiers'][pid] = [qualifier.toJSON()['mainsnak']]


    def _insert_references_local(self, claim, new_references):
        # Add new sources
        if 'references' not in claim and new_references:
            claim['references'] = []

        refs = defaultdict(list)
        for pid, value in new_references:
            reference = pywikibot.Claim(self.repo, pid)
            target_value = value
            if reference.type == 'wikibase-item':
                target_value = self.get_item(qid=value)
            elif reference.type == 'quantity': 
                target_value = self.__dict2quantity(value)
            reference.setTarget(target_value)
            reference.isReference = True
            refs[pid].append(reference.toJSON())

        if refs:
            claim['references'].append({'snaks':refs})


    @thread_safe
    def upsert_statements(self, summary, item_id, statements, commit=True, 
            checkRefURL=True, checkSource=False, delete_ref_url=None):
        """
        Update statements that have the same reference URLs or create new
        statements. All of this in only one or two API calls.

        This method finds statements based on the given item_id and reference 
        URLs, it updates statements with new values, and delete outdated 
        statements (i.e. statements with same references but not seen in the
        given statements).
        
        The statements parameter is a list of statement where each statement is
        a list in the form ['pid', 'target', 'references', 'qualifiers'].  
        References and qualifiers have the same format, both are a list of 
        pairs (PID, value (e.g QID, string, URL)).  If checkRefURL is
        True and an existing claim has the same 'reference URL' has the one
        given in the qualifiers then this claim value and qualifiers will be
        updated.  If checkSource is True, it will update a claim from the same 
        source. Otherwise the first claim will be updated.

        Notices:
        - If the property datatype is 'wikibase-item' then the target is expected 
        to be the item PID. For properties with a different datatype the value 
        of target is used as is.
        - If the item has multiple times the given property only the first one
        is modified.

        When delete_ref_url is not None, all statements with the given URL will
        also be removed. This is useful if the reference URL has changed."""

        updates = {'claims':[]}
        # Retrieve item and claims objects
        item = None
        if isinstance(item_id, pywikibot.ItemPage):
            item = item_id
        else:
            item = self.get_item(qid=item_id)

        # Retrieve claims objects
        if item.getID() != '-1':
            item_claims = dict(item.get()['claims'])
        else:
            item_claims = {}

        for statement in statements:

            references = []
            qualifiers = []
            if len(statement) == 2:
                property_id, target = statement
            elif len(statement) == 3:
                property_id, target, references = statement
            else:
                property_id, target, references, qualifiers = statement

            given_ref_urls = None
            if checkRefURL:
                ref_url_pid = self.label_pid['reference URL']
                given_ref_urls = [val for pid, val in references if pid==ref_url_pid]

            given_sources = None
            if checkSource:
                source_pid = self.label_pid['source']
                given_sources = [val for pid, val in references if pid==source_pid]
            
            # Find the matching claim or create a new one
            selected_claim = None
            if property_id in item_claims:
                # update the main statement value
                selected_claim = self._update_statement_local(
                        item_claims[property_id], target, given_ref_urls, 
                        given_sources)

            if selected_claim is None:
                # create a new claim
                selected_claim = pywikibot.Claim(self.repo, property_id)
                target_value = target
                if selected_claim.type == 'wikibase-item': 
                    target_value = self.get_item(qid=target)
                elif selected_claim.type == 'quantity': 
                    target_value = self.__dict2quantity(target)
                selected_claim.setTarget(target_value)
            else:
                # Remove the claim from the list, so we won't select it anymore
                item_claims[property_id].remove(selected_claim)

            # update current qualifiers and references
            new_qualifiers = self._update_qualifiers_local(selected_claim.qualifiers, qualifiers)
            new_references = self._update_references_local(selected_claim.sources, references)

            # add new qualifiers and references
            updated_claim = selected_claim.toJSON()

            # update references
            self._insert_qualifiers_local(updated_claim, new_qualifiers)
            self._insert_references_local(updated_claim, new_references)

            # Add updated claims
            updates['claims'].append(updated_claim)

        # Commit changes
        if commit and item is not None:
            self.editEntity(item, updates, summary)

        return updates

    
    @thread_safe
    def editEntity(self, entity, data, summary):
        """Update entity in asynchronous manner if MAX_PENDING_REQUESTS permits,
        use synchronous call otherwise."""

        # logging.info(f'wikihandy: editEntity entity={entity}, data={data}')

        if self.pending_requests < MAX_PENDING_REQUESTS:
            self.pending_requests += 1
            entity.editEntity(data, summary=summary, asynchronous=True, callback=self.on_delivery)
        else:
            logging.warn('Too many pending requests. Doing a synchronous request.')
            entity.editEntity(data, summary=summary)


    def on_delivery(self, entity, error):
        """Print errors if a commit didn't succeed"""

        self.pending_requests -= 1
        if error is not None:
            print('!!! ERROR (on_delivery)!!!')
            print(entity)
            print(error)


    @thread_safe
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

    def get_qid(self, label, create=None):
        """Retrieve item id based on the given label. Returns None if the label
        is unknown or create it with values passed in the create parameter.
        The create parameter is a dictionary with keys:
            - summary (mandatory)
            - label (optional, reusing the label parameter is not given)
            - description (optional)
            - aliases (optional)
            - statements (optional)
        """

        qid = self.label_qid.get(label, None)
        if qid is None and create is not None:
            # Create the item 
            if 'label' not in create:
                create['label'] = label
            qid = self.add_item(**create)

        return qid

    def get_pid(self, label):
        """Retrieve property id based on the given label. Returns None if the label
        is unknown."""

        return self.label_pid.get(label, None)

    @thread_safe
    def extid2qid(self, label=None, qid=None):
        """Find items that have an external ID for the given type of IDs.
        return: dict where keys are the external ids and values are the QIDs

        warning: assumes there is only one item per external IDs
        """

        extid_qid = qid
        if qid is None and label is not None:
            extid_qid = self.get_qid(label)

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
                ?extidStatement pq:%s wd:%s .
        } 
        """ % (
                self.get_pid('external ID'), 
                self.get_pid('external ID'), 
                self.get_pid('instance of'), 
                extid_qid
                )

        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        response = self.sparql.query().convert()
        results = response['results']
        
        extid2qid = {}
        for res in results['bindings']:
            res_qid = res['item']['value'].rpartition('/')[2]
            res_extid = res['extid']['value']

            extid2qid[res_extid] = res_qid

        return extid2qid
        

    @thread_safe
    def asn2qid(self, asn, create=False):
        """Retrive QID of items assigned with the given Autonomous System Number.

        param: asn (int)"""

        if isinstance(asn, str) and asn.startswith('AS'):
            asn = asn[2:]

        if int(asn) < 0:
            print('Error: ASN value should be positive.')
            return None

        if self._asn2qid is None:
            logging.info('Wikihandy: downloading AS QIDs')
            # Bootstrap : retrieve all existing ASN/QID pairs
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
                    self.get_qid('autonomous system') , 
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

            logging.info('Wikihandy: download complete (AS QIDs)')

        # Find the AS QID or add it to wikibase
        qid = self._asn2qid.get(int(asn), None)
        if create and qid is None:
            # if this AS is unknown, create corresponding item
            qid = self.add_item('new AS', f'AS{asn}',
                    statements=[
                        [self.get_pid('instance of'), self.get_qid('autonomous system'), []],
                        [self.get_pid('autonomous system number'), str(asn), []]
                    ])

        return qid
        
    # FIXME: decide on a proper type for IP routing prefixes
    @thread_safe
    def prefix2qid(self, prefix, create=False):
        """Retrive QID of items assigned with the given routing IP prefix.

        param: prefix (str)"""

        prefix = prefix.strip()

        # TODO use a proper regex
        if ('.' not in prefix and ':' not in prefix) or '/' not in prefix:
            print('Error: wrong format: ', prefix)
            return None

        # IP version
        af=4
        if ':' in prefix:
            af=6

        if self._prefix2qid is None:
            # Bootstrap : retrieve all existing prefix/QID pairs

            logging.info('Wikihandy: downloading prefix QIDs')

            QUERY = """
            #Items that have a pKa value set
            SELECT ?item ?prefix
            WHERE 
            {
                    ?item wdt:%s ?type.
                    ?item rdfs:label ?prefix. 
                    FILTER(?type IN (wd:%s, wd:%s, wd:%s))
            } 
            """ % (
                    self.get_pid('instance of'), 
                    self.get_qid(f'IPv4 routing prefix') , 
                    self.get_qid(f'IPv6 routing prefix') , 
                    self.get_qid(f'IP routing prefix') , 
                  )

            self.sparql.setQuery(QUERY)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            
            self._prefix2qid = {}
            for res in results['results']['bindings']:
                res_qid = res['item']['value'].rpartition('/')[2]
                res_prefix = res['prefix']['value']

                self._prefix2qid[res_prefix] = res_qid

            logging.info('Wikihandy: download complete (prefix QIDs)')

        # Find the prefix QID or add it to wikibase
        qid = self._prefix2qid.get(prefix, None)
        if create and qid is None:
            # if this prefix is unknown, create corresponding item
            qid = self.add_item('new prefix', prefix,
                    statements=[
                        [self.get_pid('instance of'), self.get_qid('IP routing prefix'), []],
                        [self.get_pid('IP version'), self.get_qid(f'IPv{af}'), []],
                    ])

        return qid
        

    def country2qid(self, cc, create=True):
        """Find a country QID or add the country to wikibase if it doesn't exist
        and create is set to True.

        param: cc (string) Two-character country code.

        Notice: the label of a created country is the country name as defined 
        by iso3166)."""

        # Check if country page exists
        cc = cc.upper()
        cc_label = 'unknown country'
        if cc in EXOTIC_CC:
            cc_label = EXOTIC_CC[cc]
        elif cc in iso3166.countries_by_alpha2:
            cc_label = iso3166.countries_by_alpha2[cc].name
        else:
            return None

        # Create the country page if it doesn't exists
        cc_qid = self.get_qid(cc_label)
        if create and cc_qid is None:
            cc_qid = self.add_item('add new country', cc_label, aliases=cc,
                statements=[[self.get_pid('instance of'), self.get_qid('country'),[]]])

        return cc_qid


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
        results = []

        # Load cached data if available
        if os.path.exists('.wikihandy_label2id.json'):
            results = json.load(open('.wikihandy_label2id.json','r'))
        else:
            QUERY="""SELECT ?item ?itemLabel
                WHERE { 
                    ?item rdfs:label ?itemLabel. 
                } """

            # Fetch existing entities
            self.sparql.setQuery(QUERY)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            json.dump(results, open('.wikihandy_label2id.json','w'))
        
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
    wh = Wikihandy(preload=False)

    import IPython
    IPython.embed()
