# Internet Yellow Pages


## Loading a dump and playing with it

A preliminary database dump is available at https://exp1.iijlab.net/wip/iyp/dumps/2023/01/15/iyp-2023-01-15.dump

### Usage
#### Prerequisites
- [Curl](https://curl.se/download.html)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/install/)

#### Downloading the Database dump
Before starting the database dumping pipeline, you need to download the database dump using the following commands. **Note this downloads a database dump with a size of ~4GB.**
:
```
mkdir dumps
curl https://exp1.iijlab.net/wip/iyp/dumps/2023/01/15/iyp-2023-01-15.dump -o dumps/neo4j.dump
```

This will create directory named `dumps` and download the dataset to `dumps/neo4j.dump`

#### Starting the database dumping pipeline
To start the pipeline using docker-compose, navigate to the root directory of the repository and run the following command:
```
docker-compose up
```
You can also start only a subset of services by specifying the service name:
```
docker-compose up <service1> <service2>
```
You can then replace the placeholders with the actual service names when running the command.

This command will start all services defined in the `docker-compose.yaml` file and run the pipeline. To start the pipeline in detached mode, use the `-d` flag:

```
docker-compose up -d
```

#### Stopping the database dumping pipeline
To stop the pipeline, run the following command:
```
docker-compose down
```
This command will stop all running services and remove their containers.

This initial setup needs only be done once. Afterwards, you can simply start/stop the container to
use it. To later overwrite the existing database with a new dump check [Updating an existing
database](#updating-an-existing-database).
#### Viewing Logs
To view the logs for a specific service, use the logs command and specify the service name:
```
docker-compose logs -f <service1>
```
You can then replace the placeholders with the actual service names when running the command.


#### Querying the database

Open http://localhost:7474 in your favorite browser. To connect the interface to the database give
the default login and password: `neo4j` and `password` respectively. Then enter your query in the top input field.

For example, this finds the IXPs and corresponding country codes where IIJ (AS2497) is:
```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:Country)
RETURN iij, ix, cc
```
![Countries of IXPs where AS2497 is present](/documentation/assets/gallery/as2497ixpCountry.svg)

#### IYP gallery

See more query examples in [IYP gallery](/documentation/gallery.md)

#### Save modified database

If you modify the database and want to make a new dump, use the following command. Run the following command for updating an existing database. **Note: This command writes the dump to `backups/neo4j.dump` and overwrites this file if it exists.** 
```
docker-compose run -it neo4j_admin neo4j-admin database dump neo4j --to-path=/backups --verbose --overwrite-destination
```

### Updating an existing database

To update the database with a new dump without deleting the docker container, simply run the
first command with the `--overwrite-destination` parameter.
```
docker-compose run -it neo4j_admin neo4j-admin database load neo4j --from-path=/dumps --verbose --overwrite-destination
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

## Changelog

See: https://github.com/InternetHealthReport/internet-yellow-pages/releases

