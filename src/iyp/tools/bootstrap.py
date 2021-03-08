import csv
import sys
import time
from collections import defaultdict
from iyp.wiki.wikihandy import Wikihandy

BASIC_PROPERTY_FNAME = 'basic/properties.csv'
BASIC_ITEMS_FNAME = 'basic/items.csv'

wh = Wikihandy(preload=False) 

def decomment(csvfile):
    """Ignore lines with comments"""
    for row in csvfile:
        if not '#' in row: yield row

print('Adding properties')
with open(BASIC_PROPERTY_FNAME, 'r') as fp:
    csvdata = csv.reader(decomment(fp), skipinitialspace=True)

    for row in csvdata:
        if not row:    
            continue

        label, description, aliases, data_type = [col.strip() for col in row]
        pid = wh.add_property('bootstrap', label, description, aliases, data_type)
        print(pid, label)

print('Adding items')
statements=defaultdict(list)
# wikidata = wikihandy.Wikihandy(wikidata_project='wikidata', lang='wikidata')

with open(BASIC_ITEMS_FNAME, 'r') as fp:
    csvdata = csv.reader(decomment(fp),  skipinitialspace=True)

    for row in csvdata:
        if not row:    
            continue

        label, description, aliases, statements = [col.strip() for col in row]
        print(label)

        # Retrive statements from the csv file
        # Assume all properties have the 'wikidata-item' datatype
        claims = []
        for statement in statements.split('|'):
            try:
                property, target = statement.split(':')
            except ValueError:
                # skip lines with no statement
                continue

            claims.append( [ wh.get_pid(property.strip()), wh.get_qid(target), [] ] ) 

        wh.add_item(
            "bootstrap",
            label,
            description,
            aliases,
            claims
            )
