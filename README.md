# Internet Yellow Pages


## Loading a dump and playing with it (docker)

A preliminary database dump is available at https://exp1.iijlab.net/wip/iyp/dumps/2023/01/15/iyp-2023-01-15.dump

### Quickstart

If you simply want to copy & paste to get started, execute this script, otherwise continue at [Slow
start](#slow-start). **Note: This script assumes that `curl` is installed on your system and
downloads a database dump with a size of ~4GB.**
```
mkdir data dumps
curl https://exp1.iijlab.net/wip/iyp/dumps/2023/01/15/iyp-2023-01-15.dump -o dumps/neo4j.dump
sudo docker run --interactive --tty --rm --volume=$PWD/data/:/data --volume=$PWD/dumps/:/dumps neo4j/neo4j-admin:5.1.0 neo4j-admin database load neo4j --from-path=/dumps
sudo docker run -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 -e NEO4J_AUTH=neo4j/password -v $PWD/data:/data --name iyp neo4j:5.1.0
```
The database is now running and can be stopped with `Ctrl+C`. Continue at [Querying the
database](#querying-the-database) to start playing.

### Slow start

Create the data directory (containing the neo4j data) and the dumps directory (for compressed
database dumps).
```
mkdir data dumps
```
Download the database dump and place it (or a link) in the `dumps` directory with the name
`neo4j.dump`. Example using `curl`:
```
curl https://exp1.iijlab.net/wip/iyp/dumps/2023/01/15/iyp-2023-01-15.dump -o dumps/neo4j.dump
```
Load the dump to create a working database. neo4j assumes that dump files end with `.dump` so we
specify the location of our dump using the `--from-path=/dumps` parameter and the name with `load
neo4j` (omitting the `.dump` suffix). For more information on the docker parameters see
[here](https://docs.docker.com/engine/reference/commandline/run/).

**Note: You might need to execute the docker commands with `sudo`.**
```
docker run --interactive --tty --rm  \
    --volume=$PWD/data:/data \
    --volume=$PWD/dumps/:/dumps \
    neo4j/neo4j-admin:5.1.0 \
    neo4j-admin database load neo4j \
        --from-path=/dumps \
        --verbose
```
Then create a neo4j docker container named `iyp` with the new database. This container will bind
ports `7474` (required for the web interface) and `7687` (required for the neo4j driver) to listen
on the loopback interface. The `NEO4J_AUTH=neo4j/password` environment variable is used to set the
initial username (`neo4j`) and password (`password`) of the database.
```
docker create \
    -p 127.0.0.1:7474:7474 \
    -p 127.0.0.1:7687:7687 \
    --env NEO4J_AUTH=neo4j/password \
    --volume $PWD/data:/data \
    --name iyp \
    neo4j:5.1.0
```
This initial setup needs only be done once. Afterwards, you can simply start/stop the container to
use it. To later overwrite the existing database with a new dump check [Overwriting an existing
database](#overwriting-an-existing-database).
```
# Start
docker start iyp
# Stop
docker stop iyp
```

### Querying the database

Open http://localhost:7474 in your favorite browser. To connect the interface to the database give
the default login and password: `neo4j` and `password`.
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

If you modify the database and want to make a new dump, use the following command. **Note: This
command writes the dump to `backups/neo4j.dump` and overwrites this file if it exists.**
```
docker run --interactive --tty --rm --volume=$PWD/data:/data --volume=$PWD/backups/:/backups neo4j/neo4j-admin:5.1.0 neo4j-admin database dump neo4j --to-path=/backups --verbose --overwrite-destination
```

### Overwriting an existing database

To overwrite the database with a new dumb without deleting the docker container, simply run the
first command with the `--overwrite-destination` parameter.
```
docker run --interactive --tty --rm --volume=$PWD/data:/data --volume=$PWD/dumps/:/dumps neo4j/neo4j-admin:5.1.0 neo4j-admin database load neo4j --from-path=/dumps --verbose --overwrite-destination
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

