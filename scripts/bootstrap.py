import pywikibot
import wikihandy 
import csv
from collections import defaultdict

BASIC_PROPERTY_FNAME = 'basic/properties.csv'
BASIC_ITEMS_FNAME = 'basic/items.csv'

wh = wikihandy.Wikihandy() 
repo = wh.repo

properties = {}
items = {}

print('Adding properties')
with open(BASIC_PROPERTY_FNAME, 'r') as fp:
    csvdata = csv.reader(fp)

    for row in csvdata:
        print(row)
        label, description, aliases, data_type = row
        properties[label] = wh.add_property(label, description, aliases, data_type)


print('Adding items')
statements=defaultdict(list)
wikidata = wikihandy.Wikihandy('wikipedia')

with open(BASIC_ITEMS_FNAME, 'r') as fp:
    csvdata = csv.reader(fp, delimiter=' ', quotechar='|')

    for label, statements in csvdata:
        wikidata_item = wikidata.get_items(label)[0]

        items[label] = wh.add_item(
                label, 
                wikidata_item['description']['en'], 
                wikidata_item['aliases']['en'], 
                )


        # Keep track of wikidata QID
        pid = wh.get_pid('wikidata qid')
        wh.upsert_statement(items[label], pid,  wikidata_item['id'], 'external-id' )

        for statement in statements.split('|'):
            property, target = statement.split(':')
            pid = wh.get_pid('wikidata qid')
            wh.upsert_statement(items[label], pid, items[target]) 
