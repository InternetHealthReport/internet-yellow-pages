# IYP Gallery

Querying the IYP database requires to be familiar with:
- Cypher, Neo4j's query langage https://neo4j.com/docs/getting-started/current/cypher-intro/
- Basic networking knowledge (IP, prefixes, ASes, etc..)
- IYP ontology (TODO link to ontology)

Below are examples queries that you can copy/paste in Neo4j browser:
1. [Names for AS2497](#names-for-as2497)
2. [All nodes related to 8.8.8.0/24](#all-nodes-related-to-888024)
3. [Country code of AS2497 in delegated files](#country-code-of-as2497-in-delegated-files)
4. [Countries of IXPs where AS2497 is present](#countries-of-ixps-where-as2497-is-present)
5. [Top domain names hosted by AS2497](#top-domain-names-hosted-by-as2497)
6. [ASes hosting top domain names in Japan](#ases-hosting-top-domain-names-in-japan)


### Names for AS2497
Find 'Name' nodes directly connected to the node corresponding to AS2497.
```cypher
MATCH (a:AS {asn:2497})--(n:Name) RETURN a,n
```
![Names for AS2497](/documentation/assets/gallery/as2497names.svg)


### All nodes related to 8.8.8.0/24
Find nodes of any type that are connected to the node corresponding to prefix 
8.8.8.0/24.
```cypher
MATCH (gdns:Prefix {prefix:'8.8.8.0/24'})--(neighbor)
RETURN gdns, neighbor
```
![All nodes related to 8.8.8.0/24](/documentation/assets/gallery/prefixAllRelated.svg)


### Country code of AS2497 in delegated files
Here we search for a country node directly connected to AS2497's node and that
comes from NRO's delegated stats.
```cypher
MATCH (iij:AS {asn:2497})-[{reference_name:'nro.delegated_stats'}]-(cc:Country)
RETURN iij, cc
```
![Country code of AS2497 in delegated files](/documentation/assets/gallery/as2497country.svg)


### Countries of IXPs where AS2497 is present
Starting from the node corresponding to AS2497, find IXPs where AS2497 is member
of, and then the country corresponding to each IXP.
```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:Country)
RETURN iij, ix, cc
```
![Countries of IXPs where AS2497 is present](/documentation/assets/gallery/as2497ixpCountry.svg)


### Top domain names hosted by AS2497
Select domain names in top 50k rankings that resolves to an IP originated by
AS2497.
```cypher
MATCH (:Ranking)-[r:RANK]-(dn:DomainName)--(ip:IP)--(pfx:Prefix)-[:ORIGINATE]-(iij:AS {asn:2497})
WHERE r.rank < 50000
RETURN dn, ip, pfx, iij
```
![Top domain names hosted by AS2497](/documentation/assets/gallery/as2497domainNames.svg)


### ASes hosting top domain names in Japan
From the top 10k domain names select domain names that ends with '.jp', the
corresponding IP, prefix, and ASN.
```cypher
MATCH (:Ranking)-[r:RANK]-(dn:DomainName)--(ip:IP)--(pfx:Prefix)-[:ORIGINATE]-(net:AS)
WHERE dn.name ends with '.jp' and r.rank<10000
RETURN dn, ip, pfx, net
```
![ASes hosting top domain names in Japan](/documentation/assets/gallery/top10kJapanAS.svg)
