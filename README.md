# internet-yellow-pages

## Quick-start
- [Install and setup pywikibot](https://github.com/InternetHealthReport/internet-yellow-pages/blob/main/documentation/install_pywikibot.md)
- Install required lib
```
sudo pip3 install -r requirements.txt
```
- Example to populate iyp with one dataset
```
python scripts/spamhaus_asn_drop.py
```
- You can also use this script ([scripts/spamhaus_asn_drop.py](https://github.com/InternetHealthReport/internet-yellow-pages/blob/main/scripts/spamhaus_asn_drop.py)) as a template to create your own.

### Tips and Tricks
- Revert last changes: https://www.mediawiki.org/wiki/Manual:Pywikibot/revertbot.py

## Useful ressources
- Example using python library: https://www.wikidata.org/wiki/Wikidata:Pywikibot_-_Python_3_Tutorial
- Create new property: https://marc.info/?l=pywikipediabot-users&m=145893355707437&w=2 
- wikidata homepage: https://wikiba.se/

## Candidate data sources
- RIS
- Atlas
- Regulators: start with ARCEP's open data
- peeringdb
- openIPmap
- AS Hegemony
- Country hegemony
- dns tags
- rpki


## Examples:
RIS1.2.3.4 is an instance bgp collector peer
RIS1.2.3.4 is part of RIS project


