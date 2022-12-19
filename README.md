# Internet Yellow Pages


## Loading a dump and playing with it

A preliminary database dump is available at https://exp1.iijlab.net/wip/iyp/dumps/2022/12/09/iyp-2022-12-09.dump

### Using docker
Download a dump a rename it neo4j.dump. Assuming this file is in $HOME/iyp/dumps/,
load the database with this command:
```
docker run --interactive --tty --rm   --volume=$HOME/iyp/data:/data --volume=$HOME/iyp/dumps/:/backups neo4j/neo4j-admin:5.1.0 neo4j-admin database load neo4j --from-path=/backups --verbose
```
Then run neo4j with the new database:
```
docker run -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/password  -v $HOME/iyp/data:/data --name iyp neo4j:5.1.0 
```
Add these options if you are planning to execute large transactions: -e NEO4J_server_memory_heap_initial__size=8G -e NEO4J_server_memory_heap_max__size=8G 

If you modify the database and want to make a new dump, use the following command:
```
docker run --interactive --tty --rm   --volume=$HOME/iyp/data:/data --volume=$HOME/iyp/dumps/:/backups neo4j/neo4j-admin:5.1.0 neo4j-admin database dump neo4j --to-path=/backups --verbose
```


## How to create a new dump
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
cp config.conf.example config.conf
# Edit as needed
```

Create and populate a new database:
```
python3 create_db.py
```
This will take a couple of hours to download all datasets and push them to neo4j.

### Tips and Tricks


## Candidate data sources
- RIS peers
- Atlas
- Regulators: start with ARCEP's open data
- openIPmap
- AS Hegemony
- dns tags
- CERT/ NOG per countries
- mobile prefixes (Japan)
- 

