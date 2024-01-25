# CAIDA -- https://caida.org

## ASRank (asrank.py)
AS rank in terms of customer cone size, meaning that large transit providers are
higher ranked.

### Graph representation

Ranking:

Connect ASes nodes to a single ranking node corresponding to ASRank. The rank is
given as a link attribute.
For example:
```
(:AS  {asn:2497})-[:RANK {rank:87}]-(:Ranking {name:'CAIDA ASRank'})
```

Country:

Connect AS to country nodes, meaning that the AS is registered in that country.

```
(:AS)-[:COUNTRY]-(:Country)
```

AS name:

Connect AS to names nodes, providing the name of an AS.
For example:
```
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

```Cypher
(:AS)-[:MEMBER_OF]->(:IXP)
```


### Dependence
The ix_asns crawler dependends on the ixs crawler.
