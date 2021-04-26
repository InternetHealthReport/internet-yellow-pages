import sys
import pywikibot
from SPARQLWrapper import SPARQLWrapper, JSON
import logging
import iso3166
import arrow
from collections import defaultdict
from threading import RLock
from iyp.wiki import decorators 


# DEFAULT_WIKI_SPARQL = 'http://localhost:8989/bigdata/namespace/wdq/sparql' #'https://exp1.iijlab.net/wdqs/bigdata/namespace/wdq/sparql'
# DEFAULT_WIKI_PROJECT = 'local'
DEFAULT_WIKI_SPARQL = 'http://iyp-proxy.iijlab.net/bigdata/namespace/wdq/sparql'
DEFAULT_WIKI_PROJECT = 'iyp'
DEFAULT_LANG = 'en'
MAX_PENDING_REQUESTS = 250
MAX_CLAIM_EDIT = 300

EXOTIC_CC = {'ZZ': 'unknown country', 'EU': 'Europe', 'AP': 'Asia-Pacific'}

#TODO add method to efficiently get countries
#TODO make a generic function for all the XXX2qid() functions


class Wikihandy(object):

    def __init__(self, wikidata_project=DEFAULT_WIKI_PROJECT, lang=DEFAULT_LANG, 
            sparql=DEFAULT_WIKI_SPARQL, preload=False):

        logging.debug('Wikihandy: Enter initialization')

        # used to make pywikibot cache access thread-safe
        self.lock = RLock()

        self._asn2qid = {}
        self._prefix2qid = {}
        self._domain2qid = {}
        self.repo = pywikibot.DataSite(lang, wikidata_project, 
                user=pywikibot.config.usernames[wikidata_project][lang])

        self.sparql = SPARQLWrapper(sparql)
        self.label_pid, self.label_qid = self.id4alllabels()
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
        return pywikibot.WbTime(year=dt.year, month=dt.month, day=dt.day,
            calendarmodel="http://www.wikidata.org/entity/Q1985727")
    
    @decorators.thread_safe
    def get_item(self, label=None, qid=None):
        """ Return the first item with the given label."""

        if qid is not None:
            return pywikibot.ItemPage(self.repo, qid)

        if label is not None:
            if label in self.label_qid:
                qid = self.label_qid[label]
                return pywikibot.ItemPage(self.repo, qid)

        return None

        # params = {'action': 'wbsearchentities', 'format': 'json',
                # 'language': lang, 'type': 'item', 
                # 'search': label}
        # request = api.Request(site=self.repo, parameters=params)
        # result = request.submit()
        # return result['search'] 

    @decorators.thread_safe
    def get_property(self, label=None, pid=None):
        """ Return the fisrt property with the given label"""

        if pid is not None:
            return pywikibot.PropertyPage(self.repo, pid)

        if label is not None:
            if label in self.label_pid:
                pid = self.label_pid[label]
                return pywikibot.PropertyPage(self.repo, pid)

        return None

        # params = {'action': 'wbsearchentities', 'format': 'json',
                # 'language': lang, 'type': 'property', 
                # 'search': label}
        # request = api.Request(site=self.repo, parameters=params)
        # result = request.submit()
        # print(result)
        # return result['search']

    @decorators.thread_safe
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
        self.editEntity(new_prop, data, summary, asynchronous=False)
        pid = new_prop.getID()

        # Keep it in the cache
        self.label_pid[label] = pid

        return pid 

    @decorators.thread_safe
    def add_item(self, summary, label, description=None, aliases=None, 
            statements=None):
        """Create new item if it doesn't already exists. 

        - summary (string): a commit message
        - label (string): the item name
        - description (string): the item description in english
        - aliases: item's aliases
        - statements: list of statements for the created item

        Return the item QID"""

        qid = self.label_qid.get(label, None)
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

        self.editEntity(new_item, data, summary, asynchronous=False)
        qid = new_item.getID()

        # Keep it in the cache
        self.label_qid[label] = qid

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



    def _update_statement_local(self, claims, target, ref_urls=None, sources=None):
        """Update a statement locally (changed are not pushed to wikibase). 
        If a reference URL is given, then it will update the statement that have
        the same reference URL. Otherwise it will update the first statement that
        has no reference URL."""

        ref_url_pid = self.label_pid['reference URL']
        source_pid = self.label_pid['source']
        selected_claim = None

        # search for a claim with the same reference url
        if ref_urls is not None:
            selected_claim = select_first_claim(claims, ref_url_pid, ref_urls)

        # search for a claim with the same source
        elif sources is not None:
            selected_claim = select_first_claim(claims, source_pid, sources)

        # search for the first claim without a reference url
        else:
            for claim in claims:
                if claim.sources:
                    for source in claim.sources:
                        if ref_url_pid not in source:
                            selected_claim = claim
                            break
                else:
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


    @decorators.thread_safe
    def upsert_statements(self, summary, item_id, statements, commit=True, 
            checkRefURL=True, checkSource=False, delete_ref_url=None,
            asynchronous=True):
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

        When the list delete_ref_url is not None, all statements with the given 
        URLs will also be removed. This is useful if the reference URLs has 
        changed."""

        all_updated_claims = []
        ref_url_pid = self.get_pid('reference URL')
        source_pid = self.get_pid('source')
        # Retrieve item and claims objects
        item = None
        if isinstance(item_id, pywikibot.ItemPage):
            item = item_id
        else:
            item = self.get_item(qid=item_id)

        # Select claims objects
        if item.getID() != '-1':
            all_claims = dict(item.get()['claims'])
            # find all reference URLs in given statements
            all_given_ref_urls = set([
                val for statement in statements
                    for pid, val in unpack_statement(statement)[2] if pid==ref_url_pid
                    ])

            # add outdated URLs
            if delete_ref_url is not None:
                all_given_ref_urls.update(delete_ref_url)
            selected_claims = select_claims(all_claims, ref_url_pid, all_given_ref_urls)

        else:
            all_claims = {}
            selected_claims = {}

        for statement in statements:

            property_id, target, references, qualifiers = unpack_statement(statement)

            claims = selected_claims
            given_ref_urls = None
            if checkRefURL:
                given_ref_urls = set([val for pid, val in references if pid==ref_url_pid])
                if not given_ref_urls:
                    given_ref_urls = None
                    claims = all_claims

            given_sources = None
            if checkSource:
                given_sources = set([val for pid, val in references if pid==source_pid])
            
            # Find the matching claim or create a new one
            selected_claim = None
            if property_id in claims:
                # update the main statement value
                selected_claim = self._update_statement_local(
                        claims[property_id], target, given_ref_urls, 
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
                claims[property_id].remove(selected_claim)

            # update current qualifiers and references
            new_qualifiers = self._update_qualifiers_local(selected_claim.qualifiers, qualifiers)
            new_references = self._update_references_local(selected_claim.sources, references)

            # add new qualifiers and references
            updated_claim = selected_claim.toJSON()

            # update references
            self._insert_qualifiers_local(updated_claim, new_qualifiers)
            self._insert_references_local(updated_claim, new_references)

            # Add updated claims
            all_updated_claims.append(updated_claim)

        # Commit changes
        if commit and item is not None:
            self.editEntity(item, all_updated_claims, summary, asynchronous=asynchronous)
            claims_to_remove = [claim for claims_list in selected_claims.values()
                                    for claim in claims_list]
            if claims_to_remove:
                if len(claims_to_remove) > MAX_CLAIM_EDIT:
                    # Remove in batches if there is too many to do
                    batch_size = MAX_CLAIM_EDIT - 1
                    for i in range(0, len(claims_to_remove), batch_size):
                        batch = claims_to_remove[i:min(i+batch_size, len(claims_to_remove))]
                        item.removeClaims( batch )
                else:
                    item.removeClaims(claims_to_remove)
            else:
                pass

        return {'claims':all_updated_claims}

    @decorators.thread_safe
    def editEntity(self, entity, data, summary, asynchronous=True):
        """Update entity in the database.

        data: should be either a dictionary that may give all informationi (e.g.
        label, description, claims) or a list with only updated claims.

        Update is done in asynchronous manner if MAX_PENDING_REQUESTS permits,
        use synchronous call otherwise."""

        if isinstance(data, list):
            claims = data
            data = { 'claims': claims }
            if len(claims) == 0:
                # Nothing to do
                return

            # API limits the number of claims to 500
            if len(claims) > MAX_CLAIM_EDIT:
                batch_size = MAX_CLAIM_EDIT - 1
                self.editEntity(entity, claims[batch_size:],summary, asynchronous)
                data['claims'] = claims[:batch_size]

        if asynchronous and self.pending_requests < MAX_PENDING_REQUESTS:
            self.pending_requests += 1
            entity.editEntity(data, summary=summary, asynchronous=True, callback=self.on_delivery)
        else:
            logging.debug('Too many pending requests. Doing a synchronous request.')
            entity.editEntity(data, summary=summary)


    def on_delivery(self, entity, error):
        """Print errors if a commit didn't succeed"""

        self.pending_requests -= 1
        if error is not None:
            print('!!! ERROR (on_delivery)!!!')
            print(entity)
            print(error)


    @decorators.thread_safe
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
        elif qid is None:
            # Try a query with this label
            res = self.label2id(label, type='Q')
            if res is not None and res.startswith('Q'):
                self.label_qid[label] = res
                qid = res

        return qid

    def get_pid(self, label):
        """Retrieve property id based on the given label. Returns None if the label
        is unknown."""

        pid = self.label_pid.get(label, None)
        if pid is None:
            # Try a query with this label
            res = self.label2id(label, type='P')
            if res is not None and res.startswith('P'):
                self.label_pid[label] = res
                pid = res

        return pid

    @decorators.thread_safe
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
        

    @decorators.thread_safe
    def asn2qid(self, asn, create=False):
        """Retrive QID of items assigned with the given Autonomous System Number.

        param: asn (int)"""

        if isinstance(asn, str) and asn.startswith('AS'):
            asn = asn[2:]

        if int(asn) < 0:
            print('Error: ASN value should be positive.')
            return None

        if len( self._asn2qid ) == 0:
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
            
            for res in results['results']['bindings']:
                res_qid = res['item']['value'].rpartition('/')[2]
                res_asn = int(res['asn']['value'])

                self._asn2qid[res_asn] = res_qid

            logging.info(f'Wikihandy: downloaded QIDs for {len(self._asn2qid)} ASes')

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
        
    @decorators.thread_safe
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

        if len( self._prefix2qid ) == 0:
            # Bootstrap : retrieve all existing prefix/QID pairs

            logging.info('Wikihandy: downloading prefix QIDs')

            QUERY = """
            #Items that have a pKa value set
            SELECT ?item ?prefix
            WHERE 
            {
                    ?item wdt:%s wd:%s.
                    ?item rdfs:label ?prefix. 
            } 
            """ % (
                    self.get_pid('instance of'), 
                    self.get_qid(f'IP routing prefix') , 
                  )

            self.sparql.setQuery(QUERY)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            
            for res in results['results']['bindings']:
                res_qid = res['item']['value'].rpartition('/')[2]
                res_prefix = res['prefix']['value']

                self._prefix2qid[res_prefix] = res_qid

            logging.info(f'Wikihandy: downloaded QIDs for {len(self._prefix2qid)} prefixes ')

        # Find the prefix QID or add it to wikibase
        qid = self._prefix2qid.get(prefix, None)
        if create and qid is None:
            # if this prefix is unknown, create corresponding item
            qid = self.add_item('new prefix', prefix,
                    statements=[
                        [self.get_pid('instance of'), self.get_qid('IP routing prefix'), []],
                        [self.get_pid('IP version'), self.get_qid(f'IPv{af}'), []],
                    ],)

        return qid
        
    @decorators.thread_safe
    def domain2qid(self, domain, create=False):
        """Retrive QID of items assigned to the given domain name.

        param: domain (str)"""

        domain = domain.strip()

        if len( self._domain2qid ) == 0:
            # Bootstrap : retrieve all existing prefix/QID pairs

            logging.info('Wikihandy: downloading domain QIDs')

            QUERY = """
            #Items that have a pKa value set
            SELECT ?item ?domain
            WHERE 
            {
                    ?item wdt:%s wd:%s.
                    ?item rdfs:label ?domain. 
            } 
            """ % (
                    self.get_pid('instance of'), 
                    self.get_qid('domain name') , 
                  )

            self.sparql.setQuery(QUERY)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()['results']
            
            self._domain2qid = {}
            for res in results['bindings']:
                res_qid = res['item']['value'].rpartition('/')[2]
                res_domain = res['domain']['value']

                self._domain2qid[res_domain] = res_qid

            logging.info(f'Wikihandy: downloaded QIDs for {len(self._domain2qid)} domains')

        # Find the domain QID or add it to wikibase
        qid = self._domain2qid.get(domain, None)
        if create and qid is None:
            # if this domain is unknown, create corresponding item
            qid = self.add_item('new domain', domain,
                    statements=[
                        [self.get_pid('instance of'), self.get_qid('domain'), []],
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


    def print_all_items(self):
        # Reduce throttling delay

        QUERY="""SELECT ?item ?itemLabel
            WHERE { 
            ?item rdfs:label ?itemLabel. 
            } """

        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()['results']
        
        for res in results['bindings']:
            qid = res['item']['value'].rpartition('/')[2]
            if qid.startswith('Q'):
                item = self.get_item(qid=qid)
                print(f'# {item}')
                #item.delete(reason='delete all', prompt=False)

    def label2id(self, label, type=None):
        """Return the qid or pid corresponding to the given label using a sparql 
        query."""

        QUERY="""SELECT ?item 
            WHERE { 
                ?item rdfs:label "%s"@en. 
            } """ % label

        # Fetch existing entities
        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()['results']
        
        for res in results['bindings']:
            id = res['item']['value'].rpartition('/')[2]
            if type is None or id.startswith(type):
                return id

        return None


    def id4alllabels(self):
        """Return two dictionaries, one for properties and  one for items, with 
        labels as keys and Q/P IDs as values. For entities that have the same 
        label only the first entity found is given, the other are ignored.

        Also ignore items that are instance of:
            - autonomous system
            - IP routing prefix
            - domain name
        Use dedicated method for these items (e.g. asn2qid)
        """

        properties = {}
        items = {}
        results = []

        # Query Sparql for label of items that are not AS, prefix, domain
        QUERY="""SELECT ?item ?itemLabel
            WHERE { 
                ?item rdfs:label ?itemLabel. 
                OPTIONAL{
                    ?item wdt:%s ?instance.
                }
                FILTER( !bound(?instance) || ?instance NOT IN (wd:%s, wd:%s, wd:%s))
                OPTIONAL{
                    ?item wdt:%s ?extID.
                }
                FILTER( bound(?extID) )
                
            } """ % (
                    self.label2id('instance of'), 
                    self.label2id('autonomous system'),
                    self.label2id('IP routing prefix'),
                    self.label2id('domain name'),
                    self.label2id('external ID'), 
                    ) 

        print(QUERY)

        # Fetch existing entities
        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        
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

        logging.info(f'Wikihandy: loaded {len(properties)} PIDs and {len(items)} QIDs')

        return properties, items

###### Handy functions
def select_claims(claims_dict, ref_pid, ref_values):
    """Select claims from claims_dict that have a reference statement ref_pid
    with one of the values given in ref_values"""

    selected_claims = defaultdict(list) 

    for pid, claims in claims_dict.items():
        for claim in claims:
            for source in claim.sources:
                if ref_pid in source:
                    for ref in source[ref_pid]:
                        if ref.getTarget() in ref_values:
                            selected_claims[pid].append(claim)

    return selected_claims
    

def select_first_claim(claims_list, ref_pid, ref_values):
    """Select first claim from all_claims that have a reference statement ref_pid
    with one of the values given in ref_values. Return None if such claim is not
    found."""

    for claim in claims_list:
        for source in claim.sources:
            if ref_pid in source:
                for ref in source[ref_pid]:
                    if ref.getTarget() in ref_values:
                        return claim

    return None
    

def unpack_statement(statement):
    '''Return four objects (pid, target, references, qualifiers) regardless
    of the length of the given statement'''

    references = []
    qualifiers = []
    if len(statement) == 2:
        property_id, target = statement
    elif len(statement) == 3:
        property_id, target, references = statement
    else:
        property_id, target, references, qualifiers = statement

    return property_id, target, references, qualifiers


if __name__ == '__main__':

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/wikihandy.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    wh = Wikihandy()

    import IPython
    IPython.embed()
