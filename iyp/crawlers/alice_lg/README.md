# Alice-LG -- https://github.com/alice-lg/alice-lg

Alice-LG is a BGP looking glass which gets its data from external APIs.

It is used by some large IXPs (e.g., DE-CIX, LINX, AMS-IX) and IYP imports membership
information by reading the route server neighbors.

The crawler *can* also import the received routes of all neighbors, however testing has
shown that this takes an unreasonable amount of time for most IXPs due to the tiny
pagination size (250 routes per page). Therefore this functionality is disabled by default.

List of supported IXPs:

- AMS-IX (`amsix.py`)
- BCIX (`bcix.py`)
- DE-CIX (`decix.py`)
- IX.br (`ixbr.py`)
- LINX (`linx.py`)
- Megaport (`megaport.py`)
- Netnod (`netnod.py`)

## Graph representation

```Cypher
(:AS {asn: 2497})-[:MEMBER_OF {address: '80.81.193.136', routeserver_id: 'rs1_fra_ipv4'}]->(:IXP {name: 'DE-CIX Frankfurt'})(:AS {asn: 2497})-[:MEMBER_OF]->(:IXP {name: 'DE-CIX Frankfurt'})
// Routes are not crawled by default
(:AS {asn: 3333})-[:ORIGINATE {neighbor_id: 'pb_0280_as20562', routeserver_id: 'rs01-bcix-v4'}]->(:Prefix {prefix: '193.0.0.0/21'})
```

There is the possibility of multiple relationships between the same node. However, these
contain different information, e.g., a member is present with multiple interfaces
(`address`) or the information is seen by different route servers (`routeserver_id`).
Similarly, a route can be seen via multiple neighbors (`neighbor_id`) or different route
servers (`routeserver_id`).

## Dependence

This crawler requires peering LAN information to map the neighbor IP to an IXP.
Therefore, it should be run after crawlers that create

```Cypher
(:Prefix)-[:MANAGED_BY]->(:IXP)
```

relationships:

- `iyp.crawlers.peeringdb.ix`
