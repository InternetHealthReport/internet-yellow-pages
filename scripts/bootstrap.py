import pywikibot
import wikihandy 
import csv
from collections import defaultdict

BASIC_PROPERTY_FNAME = 'basic/properties.csv'
BASIC_ITEMS_FNAME = 'basic/items.csv'

wh = wikihandy.Wikihandy() 
# Reduce throttling delay
wh.site.throttle.setDelays(0,1)

properties = {}
items = {}

def decomment(csvfile):
    """Ignore lines with comments"""
    for row in csvfile:
        if not '#' in row: yield row

# Fetch existing entities
for id, label in wh.get_all_entities():
    print(id,label)
    if id.startswith('P'):
        properties[label]=id
    elif id.startswith('Q'):
        items[label]=id

print('Adding properties')
with open(BASIC_PROPERTY_FNAME, 'r') as fp:
    csvdata = csv.reader(decomment(fp), skipinitialspace=True)

    for row in csvdata:
        if not row:    
            continue

        label, description, aliases, data_type = [col.strip() for col in row]
        if label not in properties:
            properties[label] = wh.add_property('bootstrap', label, description, aliases, data_type)


print('Adding items')
statements=defaultdict(list)
wikidata = wikihandy.Wikihandy(wikidata_project='wikidata', lang='wikidata')

with open(BASIC_ITEMS_FNAME, 'r') as fp:
    csvdata = csv.reader(decomment(fp),  skipinitialspace=True)

    for row in csvdata:
        if not row:    
            continue

        print(row)
        label, description, aliases, statements = [col.strip() for col in row]
        print(label)
        if label in items:
            continue

        #TODO remove this. We have all we need in the csv files
        wikidata_item_list = False #wikidata.get_items(label)
        if wikidata_item_list:
            wikidata_qid = wikidata_item_list[0]['id']
            wikidata_item = pywikibot.ItemPage(wikidata.repo, wikidata_qid).get() 
            wikidata_label = wikidata_item['labels']['en']
            print('Found corresponding wikidata item')
            sitelinks = [val for key, val in wikidata_item['sitelinks'].toJSON().items()]
            aliases = wikidata_item['aliases'].get('en','')
            description = wikidata_item['descriptions'].get('en','')

            items[label] = wh.add_item(
                "bootstrap",
                label,
                description,
                aliases,
                sitelinks
                )


            # Keep track of wikidata QID
            pid = properties['wikidata qid']
            wh.upsert_statement('bootstrap', items[label], pid,  wikidata_qid, 'external-id' )

        else:
            # Label not found in wikidata
            items[label] = wh.add_item(
                "bootstrap",
                label,
                description,
                aliases
                )

        # Add statements from the csv file
        # Assume all properties have the 'wikidata-item' datatype
        for statement in statements.split('|'):
            try:
                property, target = statement.split(':')
            except ValueError:
                # skip lines with no statement
                continue

            pid = properties[property.strip()]
            wh.upsert_statement('bootstrap', items[label], pid, items[target]) 

