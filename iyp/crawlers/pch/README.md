# Packet Clearing House -- https://www.pch.net/

Packet Clearing House (PCH) is an international organization responsible for providing operational
support and security to critical Internet infrastructure, including Internet exchange points and the
core of the domain name system.

PCH operates route collectors at more than 100 Internet Exchange Points around the world.
[Data](https://www.pch.net/resources/Routing_Data/) from these route collectors is made available
publicly for the benefit of the Internet's operational and research communities.

IYP fetches the *Daily snapshots of the results of "show ip bgp" on PCH route collectors*, which
indicate the state of the routing table on PCH route collectors at the moment in time that the
snapshot is taken.

IYP uses the announced routes to infer the origin ASes of announced prefixes, some of which might
not be visible in route collectors from Route Views or RIPE RIS.

## Graph representation

```Cypher
(:AS {asn: 2497})-[:ORIGINATE {count: 4}]->(:BGPPrefix {prefix: '101.128.128.0/17'})

```

The `ORIGINATE` relationship contains the property `count` that, similar to the relationship
produced by `bgpkit.pfx2asn`, indicates by how many route collectors the announcement was seen.
A detailed list of collector names is also available via the `seen_by_collectors` property.

## Dependence

This crawler may create new `BGPPrefix` nodes that miss the `af` property, so the
`iyp.post.address_family` postprocessing script should be run after this.
