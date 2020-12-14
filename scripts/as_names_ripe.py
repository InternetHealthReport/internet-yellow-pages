import sys
import requests
import wikihandy 
import iso3166

URL_RIPE_AS_NAME = 'https://ftp.ripe.net/ripe/asnames/asn.txt'

wh = wikihandy.Wikihandy()
#TODO create function to efficiently get properties?
properties, items = wh.get_all_properties_items()

# Find properties ids we are going to use
asn_pid = properties['autonomous system number']
reg_cc_pid = properties['registered country'] 
reg_name_pid = properties['registered name'] 

# Fetch AS names
req = requests.get(URL_RIPE_AS_NAME)
if req.status_code != 200:
    sys.exit('Error while fetching AS names')

for line in req.text.splitlines():
    print(line)
    # Get ASN, name, and country code
    asn, _, name_cc = line.partition(' ')
    name, _, cc = name_cc.partition(', ')

    # Find this AS QID or add it to wikibase
    qid = wh.asn2qid(asn)
    if qid is None:
        # if this AS is unknown, create corresponding item
        qid = wh.add_item('add new AS', f'AS{asn}', name )
        wh.upsert_statement('AS found in RIPE names', qid, properties['instance of'], items['Autonomous System'])
        wh.upsert_statement('AS found in RIPE names', qid, asn_pid, asn, 'external-id')
    
    # Check if country page exists
    cc_label = 'unknown country'
    if cc != 'ZZ':
        cc_label = iso3166.countries_by_alpha2[cc].name

    # Create the country page if it doesn't exists
    #TODO create function to efficiently get country
    reg_cc_qid = items.get(cc_label, None)
    if reg_cc_qid is None:
        reg_cc_qid = wh.add_item('add new country', cc_label, '', cc)
        wh.upsert_statement('country from as names', reg_cc_qid, properties['instance of'], items['country'])
        # keep track of this QID
        items[cc_label] = reg_cc_qid

    #
    wh.upsert_statement('country found in names', qid, reg_cc_pid, reg_cc_qid)
    wh.upsert_statement('name compiled by RIPE', qid, reg_name_pid, name, 'string')

    #TODO add AS name as alias?
