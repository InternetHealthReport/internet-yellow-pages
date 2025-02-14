# CAIDA -- https://caida.org

## ASRank (asrank.py)

AS rank in terms of customer cone size, meaning that large transit providers are
higher ranked.

### Graph representation

Ranking:

Connect ASes nodes to a single ranking node corresponding to ASRank. The rank is
given as a link attribute.
For example:

```cypher
(:AS  {asn:2497})-[:RANK {rank:87}]-(:Ranking {name:'CAIDA ASRank'})
```

Country:

Connect AS to country nodes, meaning that the AS is registered in that country.

```cypher
(:AS)-[:COUNTRY]-(:Country)
```

AS name:

Connect AS to names nodes, providing the name of an AS.
For example:

```cypher
(:AS {asn:2497})-[:NAME]-(:Name {name:'IIJ'})
```

### Dependence

The asrank crawler is not depending on other crawlers.

## IXPs (ixs.py)

List of IXPs obtained from PeeringDB, Hurricane Electric, Packet Clearing House.

### Graph representation

Nodes:

- `(:IXP {name})`: IXP node
- `(:Name {name})`: Name of IXP
- `(:Prefix {prefix})`: Prefix of IXP peering LAN
- `(:CaidaIXID {id})`: ID of the IXP assigned by CAIDA
- `(:Country {country_code})`: Country code
- `(:URL {url})`: Website of IXP

Relationships:

```Cypher
(:IXP)-[:COUNTRY]->(:Country)
(:IXP)-[:EXTERNAL_ID]->(:CaidaIXID)
(:IXP)-[:NAME]->(:Name)
(:IXP)-[:WEBSITE]->(:URL)
(:Prefix)-[:MANAGED_BY]->(:IXP)
```

### Dependence

The ixs crawler depends on the peeringdb.ix crawler.

## IXP memberships (ix_asns.py)

List of ASes present at each IXP.

### Graph representation

Relationships:

```cypher
(:AS)-[:MEMBER_OF]->(:IXP)
```

### Dependence

The ix_asns crawler depends on the ixs crawler.

## AS relationships (as_relationships_v[4|6].py)

Inferred AS relationships (peer-to-peer or customer-provider).

### Graph representation

```cypher
(:AS {asn: 2497})-[r:PEERS_WITH {af: 4, rel: -1}]->(:AS {asn: 7500})
```

Either the `reference_name` or `af` properties can be used to distinguish between IPv4
and IPv6.

`rel: -1` indicates customer-provider, and the direction of the relationship is modeled
as `provider -> customer` to be consistent with `bgpkit.as2rel`.

`rel: 0` indicates peer-to-peer relationship.

**Note:** While both CAIDA and BGPKIT use `rel: 0` to indicate a peer-to-peer
relationship, BGPKIT uses `rel: 1` for customer-provider, whereas CAIDA uses `rel: -1`.

### Dependence

The as_relatonship crawler does not depend on other crawlers.

## AS Organizations (as2org.py)

Mapping of ASes to their respective organizations

### Graph representation


This integration introduces no new node or relationship types, it only connects existing nodes as follows:
```cypher
(:AS)-[:MANAGED_BY]->(:Organization)
(:Organization)-[:COUNTRY]->(:Country)
(:Organization)-[:NAME]->(:Name)
```

### Dependence

The as2org crawler doesn't depend on other datasets