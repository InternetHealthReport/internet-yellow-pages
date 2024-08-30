# IYP Gallery

Below are examples queries that you can copy/paste in [Neo4j browser](https://iyp.iijlab.net/iyp/browser/?dbms=iyp-bolt.iijlab.net:443).

Querying the IYP database requires to be familiar with:
- Cypher, Neo4j's query langage https://neo4j.com/docs/getting-started/current/cypher-intro/
- Basic networking knowledge (IP, prefixes, ASes, etc..)
- [IYP ontology](./README.md)

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
MATCH (:Ranking)-[r:RANK]-(dn:DomainName)-[:PART_OF]-(hn:HostName)-[:RESOLVES_TO]-(ip:IP)--(pfx:Prefix)-[:ORIGINATE]-(iij:AS {asn:2497})
WHERE r.rank < 50000 AND dn.name = hn.name
RETURN hn, ip, pfx, iij
```
![Top domain names hosted by AS2497](/documentation/assets/gallery/as2497domainNames.svg)


### ASes hosting top domain names in Japan
From the top 10k domain names select domain names that ends with '.jp', the
corresponding IP, prefix, and ASN.
```cypher
MATCH (:Ranking)-[r:RANK]-(dn:DomainName)-[:PART_OF]-(hn:HostName)-[rt:RESOLVES_TO]-(ip:IP)-[po:PART_OF]-(pfx:Prefix)-[o:ORIGINATE]-(net:AS)
WHERE dn.name ENDS WITH '.jp' AND r.rank<10000 AND dn.name = hn.name
RETURN hn, ip, pfx, net, rt, po, o
```
![ASes hosting top domain names in Japan](/documentation/assets/gallery/top10kJapanAS.svg)

### Topology for top ASes in Iran
Select IHR's top 20 ASes in Iran and show how they are connected to each other using AS relationships.
```cypher
MATCH (a:AS)-[ra:RANK]->(:Ranking {name: 'IHR country ranking: Total AS (IR)'})<-[rb:RANK]-(b:AS)-[p:PEERS_WITH]-(a)
WHERE ra.rank < 20 AND rb.rank < 20 AND p.rel = 0
RETURN a, p, b
```
![Top ASes connecting Iran](/documentation/assets/gallery/top20IranAS.svg)

### Topology for AS2501's dependencies
Select AS dependencies for AS2501 and find the shortest PEERS_WITH relationship to these ASes.
```cypher
MATCH (a:AS {asn:2501})-[h:DEPENDS_ON {af:4}]->(d:AS)
WITH a, COLLECT(DISTINCT d) AS dependencies
UNWIND dependencies as d
MATCH p = allShortestPaths((a)-[:PEERS_WITH*]-(d))
WHERE a.asn <> d.asn AND all(r IN relationships(p) WHERE r.af = 4) AND all(n IN nodes(p) WHERE n IN dependencies)
RETURN p
```
![Dependencies for AS2501](/documentation/assets/gallery/as2501dependencies.svg)

### List of IPs for RIPE RIS full feed peers (more than 800k prefixes)

```cypher
MATCH (n:BGPCollector)-[p:PEERS_WITH]-(a:AS) 
WHERE n.project = 'riperis' AND p.num_v4_pfxs > 800000 
RETURN n.name, COUNT(DISTINCT p.ip) AS nb_full, COLLECT(DISTINCT p.ip) AS ips_full
```

### Active RIPE Atlas probes for the top 5 ISPs in Japan

```cypher
MATCH (pb:AtlasProbe)-[:LOCATED_IN]-(a:AS)-[pop:POPULATION]-(c:Country) 
WHERE c.country_code = 'JP' AND pb.status_name = 'Connected' AND pop.rank <= 5 
RETURN pop.rank, a.asn, COLLECT(pb.id) AS probe_ids ORDER BY pop.rank
```
