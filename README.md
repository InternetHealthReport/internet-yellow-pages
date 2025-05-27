# Internet Yellow Pages

The Internet Yellow Pages (IYP) is a knowledge database that gathers information about
Internet resources (for example ASNs, IP prefixes, and domain names).

## Public IYP prototype

Visit <https://iyp.iijlab.net> to try our online prototype. You will find instructions
on how to connect to the prototype and some example queries there. For even more
examples, check out the [IYP
gallery](documentation/gallery.md).

## Deploy a local IYP instance

We describe the basic process of deploying a local IYP instance below. For more advanced
commands see the [database documentation](documentation/database-management.md).

### Prerequisites

- [Curl](https://curl.se/download.html)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- about 100GB of free disk space
- at least 2GB of RAM

### Download the database dump

Visit the [database dump repository](https://archive.ihr.live/ihr/iyp/).

Dumps are organized by year, month, and day in this format:

```text
https://archive.ihr.live/ihr/iyp/YYYY/MM/DD/iyp-YYYY-MM-DD.dump
```

Replace `YYYY`, `MM`, and `DD` in the URL with the desired date to access a specific
database dump.

The dump file needs to be called `neo4j.dump` and needs to be put in a folder called
`dumps` (`dumps/neo4j.dump`).
To create the folder and download a dump with `curl`:

```bash
mkdir dumps
curl https://archive.ihr.live/ihr/iyp/YYYY/MM/DD/iyp-YYYY-MM-DD.dump -o dumps/neo4j.dump
```

### Set up IYP

To uncompress the dump and start the database run the following command:

```bash
mkdir -p data
uid="$(id -u)" gid="$(id -g)" docker compose --profile local up
```

This creates a `data` directory containing the database, load the database dump, and
start the local IYP instance. This initial setup needs be done only once but it takes
some time to completely load the database and start IYP. Please wait until IYP is fully
loaded. Also this step won't work if the data directory already contains a database.

This setup keeps the database instance running in the foreground. It can be stopped with
`Ctrl+C`. Afterwards, you can simply [start/stop](#startstop-iyp) IYP in the background
to use it. To update the database with a new dump see [Update existing
database](documentation/database-management.md#update-existing-database).

### Start/Stop IYP

To start the database, run the following command:

```bash
docker start iyp
```

To stop the database, run the following command:

``` bash
docker stop iyp
```

### Query the database

Open <http://localhost:7474> in your favorite browser. To connect the interface to the database give
the default login and password: `neo4j` and `password` respectively. Then enter your query in the top input field.

For example, this finds the IXPs and corresponding country codes where IIJ (AS2497) is:

```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:Country)
RETURN iij, ix, cc
```

![Countries of IXPs where AS2497 is present](/documentation/assets/gallery/as2497ixpCountry.svg)

### IYP gallery

See more query examples in [IYP gallery](/documentation/gallery.md)

## Contributing

Want to [propose a new dataset](documentation/README.md#add-new-datasets) or [implement
a crawler](documentation/writing-a-crawler.md)? Checkout the
[documentation](documentation/README.md) for more info.

## Changelog

See: <https://github.com/InternetHealthReport/internet-yellow-pages/releases>

## External links

- [Public instance of IYP](https://iyp.iijlab.net)
- [RIPE86 presentation](https://ripe86.ripe.net/archives/video/1073/)
- [APNIC blog article](https://blog.apnic.net/2023/09/06/understanding-the-japanese-internet-with-the-internet-yellow-pages/)
