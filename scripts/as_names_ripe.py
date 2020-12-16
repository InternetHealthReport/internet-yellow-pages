import sys
import datetime
import requests
import wikihandy 
import iso3166

URL_RIPE_AS_NAME = 'https://ftp.ripe.net/ripe/asnames/asn.txt'

wh = wikihandy.Wikihandy()
wh.repo.throttle.setDelays(0,1)
#TODO create function to efficiently get properties?
properties, items = wh.get_all_properties_items()
exotic_cc = {'ZZ': 'unknown country', 'EU': 'Europe', 'AP': 'Asia-Pacific'}

# Find properties ids we are going to use 
asn_pid = properties['autonomous system number']
cc_pid = properties['country'] 
name_pid = properties['name'] 
source_pid = properties['source'] 
ref_url_pid = properties['reference URL'] 
time_pid = properties['point in time'] 

# Find items ids we are going to use
ripe_qid = items['RIPE NCC']

# Qualifiers for added properties
qualifiers = [
        (source_pid, ripe_qid),
        (ref_url_pid, URL_RIPE_AS_NAME),
        (time_pid, wh.today())
        ]

# Fetch AS names
req = requests.get(URL_RIPE_AS_NAME)
if req.status_code != 200:
    sys.exit('Error while fetching AS names')

for line in req.text.splitlines():
    print(line)
    new_as = False
    # Get ASN, name, and country code
    asn, _, name_cc = line.partition(' ')
    name, _, cc = name_cc.rpartition(', ')

    # Find this AS QID or add it to wikibase
    qid = wh.asn2qid(asn)
    if qid is None:
        new_as = True
        # if this AS is unknown, create corresponding item
        qid = wh.add_item('add new AS', f'AS{asn}')
        wh.upsert_statement('AS found in RIPE names', qid, properties['instance of'], items['Autonomous System'])
        wh.upsert_statement('AS found in RIPE names', qid, asn_pid, asn)
    
    # Check if country page exists
    cc_label = 'unknown country'
    if cc in exotic_cc:
        cc_label = exotic_cc[cc]
    else:
        cc_label = iso3166.countries_by_alpha2[cc].name

    # Create the country page if it doesn't exists
    #TODO create function to efficiently get country
    reg_cc_qid = items.get(cc_label, None)
    if reg_cc_qid is None:
        reg_cc_qid = wh.add_item('add new country', cc_label, '', cc)
        wh.upsert_statement('country from as names', reg_cc_qid, properties['instance of'], items['country'])
        # keep track of this QID
        items[cc_label] = reg_cc_qid

    wh.upsert_statement('country found in RIPE AS names', qid, cc_pid, reg_cc_qid, qualifiers)
    wh.upsert_statement('name compiled by RIPE', qid, name_pid, name, qualifiers)
