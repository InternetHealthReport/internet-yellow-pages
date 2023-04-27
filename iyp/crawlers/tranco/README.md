# Tranco -- https://tranco-list.eu/

The Tranco list is a research-oriented top sites ranking hardened against manipulation. It [combines
the rankings of several source lists](https://tranco-list.eu/methodology) to produce a daily list
that is based on data of the past 30 days.

IYP uses this data to create and annotate DomainName nodes.

## Graph representation

The rank of the domain is indicated by the `rank` property of the relationship.

```Cypher
(:DomainName {name: 'google.com'})-[:RANK {rank: 1}]->(:Ranking {name: 'Tranco top 1M'})
```

## Dependence

This crawler is not depending on other crawlers.
