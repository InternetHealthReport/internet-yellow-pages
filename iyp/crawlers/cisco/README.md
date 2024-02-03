# Cisco Umbrella -- https://umbrella-static.s3-us-west-1.amazonaws.com/index.html

The popularity list contains most queried domains (ranging from TLDs to FQDNs)
based on passive DNS usage across the Umbrella global network.

IYP uses this data to create and annotate DomainName and HostName nodes.

## Graph representation

The rank of the domain is indicated by the `rank` property of the relationship.

```Cypher
(:DomainName {name: 'google.com'})-[:RANK {rank: 1}]->(:Ranking {name: 'Cisco Umbrella Top 1 million'})
(:HostName {name: 'www.google.com'})-[:RANK {rank: 8}]->(:Ranking {name: 'Cisco Umbrella Top 1 million'})
```

## Dependence

This crawler dependents on openintel/umbrella1m.
