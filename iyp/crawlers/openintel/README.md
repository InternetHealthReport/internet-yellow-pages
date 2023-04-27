# OpenINTEL -- https://www.openintel.nl/

The OpenINTEL measurement platform captures daily snapshots of the state of large parts of the
global Domain Name System (DNS) by running a number of forward and reverse DNS measurements.

While OpenINTEL runs measurements to a variety of domain names, IYP currently only fetches data for
the [Tranco top 1 million list](https://data.openintel.nl/data/tranco1m/) as the ranks of this
dataset are already fetched by the `tranco.top1m` crawler.

IYP uses only `A` queries to add IP resolution for DomainName nodes.

## Graph representation

```Cypher
(:DomainName {name: 'google.com'})-[:RESOLVES_TO]->(:IP {ip: '142.250.179.142'})
```

## Dependence

This crawler is not depending on other crawlers.
