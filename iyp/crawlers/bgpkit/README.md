# BGPKIT data -- https://data.bgpkit.com

Data inferred from RouteViews and RIPE RIS BGP data, including:
- AS relationship
- prefix to ASN mappings
- BGP collectors' peers stats


## Graph representation

### AS relationship
Connect ASes that are peering with each other. The 'rel' attribute and the link
direction gives the type of relationship between the two ASes:
- rel=0: peer to peer relationship
- rel=1: provider/customer relationship. A->B means A is the provider of B.

```
(:AS {asn:2497})-[:PEERS_WITH {rel: 0, af: 4}]-(:AS {asn:2914})
```


### Peers stats
Connect AS nodes to BGP route collector nodes, meaning that an AS peers with
a route collector hence participating in the RIS or RouteViews projects.

```
(:AS {asn:2497})-[:PEERS_WITH]-(:BGPCollector {project: 'riperis', name:'rrc06'})
```

### Prefix to ASN
Connect AS nodes to prefix nodes representing the prefixes originated by an AS.
For example:
```
(:AS  {asn:2497})-[:ORIGINATE]-(:Prefix {prefix: '101.128.128.0/17'})
```

## Dependence

This crawler is not depending on other crawlers.
