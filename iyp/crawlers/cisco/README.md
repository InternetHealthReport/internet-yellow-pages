# Cisco Umbrella -- https://umbrella-static.s3-us-west-1.amazonaws.com/index.html

The popularity list contains most queried domains based on passive DNS usage across the Umbrella global network.

IYP uses this data to create and annotate DomainName nodes.

## Graph representation

The rank of the domain is indicated by the `rank` property of the relationship.

```Cypher
(:DomainName {name: 'com'})-[:RANK {rank: 1}]->(:Ranking {name: 'CISCO Umbrella Top 1 million TLD'})
```

## Dependence

This crawler is not depending on other crawlers.
