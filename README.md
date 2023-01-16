# Internet Yellow Pages


## Loading a dump and playing with it (docker)

A preliminary database dump is available at https://exp1.iijlab.net/wip/iyp/dumps/2023/01/15/iyp-2023-01-15.dump

### Loading a dump
Download a dump and rename it neo4j.dump. Assuming this file is in $HOME/iyp/dumps/,
load the database with this command:
```
docker run --interactive --tty --rm   --volume=$HOME/iyp/data:/data --volume=$HOME/iyp/dumps/:/backups neo4j/neo4j-admin:5.1.0 neo4j-admin database load neo4j --from-path=/backups --verbose
```
Then create a neo4j container with the new database:
```
docker run -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/password  -v $HOME/iyp/data:/data --name iyp neo4j:5.1.0 
```
(Add these options if you are planning to execute large transactions: -e NEO4J_server_memory_heap_initial__size=8G -e NEO4J_server_memory_heap_max__size=8G)

### Querying the database
Open http://localhost:7474 in your favorite browser. To connect the interface to the database give the default login and password: neo4j and password.
Then enter your query in the top input field. 

For example, this finds the IXPs and corresponding country codes where IIJ (AS2497) is:
```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:COUNTRY) 
RETURN iij, ix, cc
```
![Countries of IXPs where AS2497 is present](/documentation/assets/gallery/as2497ixpCountry.svg)

### IYP gallery
See more query examples in [IYP gallery](/documentation/gallery.md)

### Save modified database
If you modify the database and want to make a new dump, use the following command:
```
docker run --interactive --tty --rm   --volume=$HOME/iyp/data:/data --volume=$HOME/iyp/dumps/:/backups neo4j/neo4j-admin:5.1.0 neo4j-admin database dump neo4j --to-path=/backups --verbose
```


## How to create a new dump from scratch
Clone this repository.
```
git clone https://github.com/InternetHealthReport/internet-yellow-pages.git
cd internet-yellow-pages
```

Create python environment and install python libraries:
```
python3 -m venv .
source bin/activate
pip install -r requirements.txt
```

Configuration file, rename example file and add API keys:
```
cp config.json.example config.json
# Edit as needed
```

Create and populate a new database:
```
python3 create_db.py
```
This will take a couple of hours to download all datasets and push them to neo4j.

### Tips and Tricks


## Candidate data sources
- Atlas
- Regulators: start with ARCEP's open data
- openIPmap
- dns tags
- CERT/ NOG per countries
- mobile prefixes (Japan)

