# Internet Yellow Pages

## Public IYP prototype

Visit http://iyp.iijlab.net to try our online prototype. No password is required, just click the 'connect' button to get started. Don't know how to use IYP ? You'll find a guide after clicking the 'connect' button, see also examples [here](https://github.com/InternetHealthReport/internet-yellow-pages/blob/main/documentation/gallery.md).

## Deploying a local IYP instance

### Prerequisites
- [Curl](https://curl.se/download.html)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- about 30GB of free disk space

### Downloading the Database dump

#### Explore and Download Dumps

Visit the database dumps repository at:
```
https://exp1.iijlab.net/wip/iyp/dumps/
```

#### Specific Dump Format

Dumps are organized by year, month, and day in this format:
```
https://exp1.iijlab.net/wip/iyp/dumps/YYYY/MM/DD/iyp-YYYY-MM-DD.dump
```

Replace `YYYY`, `MM`, and `DD` in the URL with the desired date to access a specific database dump.

#### Download Instructions

1. **Create a Directory:**

   Execute the following command to create a `dumps` directory in your current working directory:
   ```
   mkdir dumps
   ```

2. **Download the Database Dump:**

   Use `curl` to download the database dump and save it in the `dumps/neo4j.dump` path:
   ```
   curl https://exp1.iijlab.net/wip/iyp/dumps/YYYY/MM/DD/iyp-YYYY-MM-DD.dump -o dumps/neo4j.dump
   ```

Remember to replace `YYYY`, `MM`, and `DD` in the download command with the specific date you require.

### Setting up IYP
To uncompress the dump and start the database run the following command:
```
docker compose --profile local up
```
This creates a `data` directory containing the database. 
This initial setup needs be done only once. 
It won't work if this directory already contains a database.

Afterwards, you can simply [start/stop](#startstop-iyp) IYP to use it. 
To update the database with a new dump see [Updating an existing database](#updating-an-existing-database).


### Start/Stop IYP
To stop the database, run the following command:
```
docker stop iyp
```

To restart the database, run the following command:
```
docker start iyp
```


### Querying the database

Open http://localhost:7474 in your favorite browser. To connect the interface to the database give
the default login and password: `neo4j` and `password` respectively. Then enter your query in the top input field.

For example, this finds the IXPs and corresponding country codes where IIJ (AS2497) is:
```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:Country)
RETURN iij, ix, cc
```
![Countries of IXPs where AS2497 is present](/documentation/assets/gallery/as2497ixpCountry.svg)

### IYP gallery

See more query examples in [IYP gallery](/documentation/gallery.md)

### Save modified database

If you modify the database and want to make a new dump, use the following command. Run the following command for updating an existing database. **Note: This command writes the dump to `backups/neo4j.dump` and overwrites this file if it exists.** 
```
docker compose run -it iyp_loader neo4j-admin database dump neo4j --to-path=/backups --verbose --overwrite-destination
```

### Updating an existing database

To update the database with a new dump remove the existing `data` directory and 
reload a dump with the following commands:
```
docker stop iyp
sudo rm -rf data
docker start iyp_loader -i
```

### Viewing Neo4j logs
To view the logs of the Neo4j container, use the following command:
```
docker compose logs -f iyp
```


## Creating a new dump from scratch

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

## Changelog

See: https://github.com/InternetHealthReport/internet-yellow-pages/releases

