import sys
import requests
import json
import wikihandy
import iso3166

URL_PDB_ORGS = 'https://peeringdb.com/api/org'
URL_PDB_NETS = 'https://peeringdb.com/api/net'

# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PeeringDB organization ID' 

wh = wikihandy.Wikihandy()
# Property PIDs and item QIDs available in the wikibase
properties, items = wh.get_all_properties_items()
today = wh.today()

# Check if there is an item representing the organization IDs
# Create it if it doesn't exist
if ORGID_LABEL not in items:
    items[ORGID_LABEL] = wh.add_item(
            'add PeeringDB org IDs',                                      # Commit message
            ORGID_LABEL,                                                  # Label 
            'Identifier for an organization in the PeeringDB database')   # Description

# Load the QIDs for organzations already available in the wikibase
orgid2qid = wh.extid2qid(items[ORGID_LABEL])

# Fetch organizations information from PeeringDB
req = requests.get(URL_PDB_ORGS)
if req.status_code != 200:
    sys.exit('Error while fetching AS names')
organizations = json.loads(req.text)['data']

for organization in organizations:
    # Check if the organization is in the wikibase
    if str(organization['id']) not in orgid2qid :
        # Add this organization to the wikibase
        org_qid = wh.add_item('add new peeringDB organization', organization['name'])
        qualifiers = [
                (properties['instance of'], items[ORGID_LABEL]),
                (properties['reference URL'], URL_PDB_ORGS),
                (properties['source'], items['PeeringDB'])
                ]
        wh.upsert_statement('add new peeringDB organization ID', 
                org_qid, properties['external ID'], str(organization['id']), qualifiers)
        # keep track of this QID
        orgid2qid[str(organization['id'])] = org_qid

    # Update name, website, and country for this organization
    org_qid = orgid2qid[str(organization['id'])]
    qualifiers = [
            (properties['reference URL'], URL_PDB_ORGS),
            (properties['source'], items['PeeringDB']),
            (properties['point in time'], today)
            ]

    wh.upsert_statement('update peeringDB organization', 
            org_qid, properties['name'], organization['name'], qualifiers)

    if organization['website']:
        wh.upsert_statement('update peeringDB organization', 
            org_qid, properties['website'], organization['website'], qualifiers)

    if organization['country'] in iso3166.countries_by_alpha2:
        country_qid = items[iso3166.countries_by_alpha2[organization['country']].name]
        wh.upsert_statement('update peeringDB organization', 
            org_qid, properties['country'], country_qid, qualifiers)
