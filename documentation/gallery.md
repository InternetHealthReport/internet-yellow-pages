# IYP Gallery

*Based on dump [2025-05-15](https://archive.ihr.live/ihr/iyp/2025/05/15/)*

Below are examples queries that you can copy/paste in the [Neo4j
Browser](https://iyp.iijlab.net/iyp/browser/?dbms=iyp-bolt.iijlab.net:443).

Querying the IYP database requires to be familiar with:

- [Cypher, Neo4j's query language](https://neo4j.com/docs/getting-started/cypher/)
- Basic networking knowledge (IP, prefixes, ASes, etc..)
- [IYP ontology](./README.md)

## Names for AS2497

Find 'Name' nodes directly connected to the node corresponding to AS2497.

```cypher
MATCH p = (:AS {asn: 2497})--(:Name)
RETURN p
```

![Names for AS2497](/documentation/assets/gallery/as2497names.svg)

## All nodes related to 8.8.8.0/24

Find nodes of any type that are connected to the node corresponding to prefix
8.8.8.0/24.

```cypher
MATCH p = (:Prefix {prefix: '8.8.8.0/24'})--()
RETURN p
```

![All nodes related to 8.8.8.0/24](/documentation/assets/gallery/prefixAllRelated.svg)

## Country code of AS2497 in delegated files

Here we search for a country node directly connected to AS2497's node and that
comes from NRO's delegated stats.

```cypher
MATCH p = (:AS {asn:2497})-[{reference_name: 'nro.delegated_stats'}]-(:Country)
RETURN p
```

![Country code of AS2497 in delegated files](/documentation/assets/gallery/as2497country.svg)

## Countries of IXPs where AS2497 is present

Starting from the node corresponding to AS2497, find IXPs where AS2497 is member of, and
then the country corresponding to each IXP.

```cypher
MATCH p = (:AS {asn:2497})-[:MEMBER_OF]->(ix:IXP)--(:Country)
RETURN p
```

![Countries of IXPs where AS2497 is present](/documentation/assets/gallery/as2497ixpCountry.svg)

## Top domain names hosted by AS2497

Select domain names in top 10k rankings that resolves to an IP originated by
AS2497. Use the OpenINTEL Tranco Top 1M dataset for name resolution and host-domain
mapping, and BGPKIT for announced prefixes.

```cypher
MATCH p = (:Ranking)<-[r:RANK]-(dn:DomainName)
    <-[:PART_OF {reference_name: 'openintel.tranco1m'}]-(hn:HostName)
    -[:RESOLVES_TO {reference_name: 'openintel.tranco1m'}]->(:IP)
    -[:PART_OF]->(pfx:Prefix)
    <-[:ORIGINATE {reference_name: 'bgpkit.pfx2asn'}]-(:AS {asn:2497})
WHERE r.rank < 10000
AND dn.name = hn.name
RETURN p
```

![Top domain names hosted by AS2497](/documentation/assets/gallery/as2497domainNames.svg)

### ASes hosting top domain names in Japan

From the top 5k domain names select domain names that ends with '.jp', the
corresponding IP, prefix, and ASN. Use OpenINTEL for name resolution and BGPKIT for
announced prefixes.

```cypher
MATCH (:Ranking)<-[r:RANK]-(dn:DomainName)<-[:PART_OF]-(hn:HostName)
WHERE dn.name ENDS WITH '.jp'
    AND r.rank < 5000
    AND dn.name = hn.name
MATCH q = (hn)-[:RESOLVES_TO {reference_name: 'openintel.tranco1m'}]->(:IP)
    -[po:PART_OF]->(:Prefix)
    <-[:ORIGINATE {reference_name: 'bgpkit.pfx2asn'}]-(:AS)
WHERE 'BGPPrefix' in po.prefix_types
RETURN q
```

![ASes hosting top domain names in Japan](/documentation/assets/gallery/top10kJapanAS.svg)

## Topology for top ASes in Iran

Select IHR's top 20 ASes in Iran and show how they are connected to each other using
BGPKIT's AS relationships.

```cypher
MATCH (a:AS)-[ra:RANK]->(:Ranking {name: 'IHR country ranking: Total AS (IR)'})
    <-[rb:RANK]-(b:AS)
WHERE ra.rank < 20
    AND rb.rank < 20
MATCH q = (b)-[pw:PEERS_WITH {reference_name: 'bgpkit.as2rel_v4'}]-(a)
WHERE pw.rel = 0 // Peer-to-peer
RETURN q
```

![Top ASes connecting Iran](/documentation/assets/gallery/top20IranAS.svg)

## Topology for AS2501's dependencies

Select IPv4 AS dependencies for AS2501 and find the shortest PEERS_WITH relationship to these
ASes.

```cypher
MATCH (a:AS {asn:2501})-[:DEPENDS_ON {af: 4}]->(d:AS)
WITH a, COLLECT(DISTINCT d) AS dependencies
UNWIND dependencies as d
MATCH p = allShortestPaths((a)-[:PEERS_WITH*]-(d))
WHERE a.asn <> d.asn
    AND all(r IN relationships(p) WHERE r.af = 4)
    AND all(n IN nodes(p) WHERE n IN dependencies)
RETURN p
```

![Dependencies for AS2501](/documentation/assets/gallery/as2501dependencies.svg)

## List of IPs for RIPE RIS full feed peers (more than 800k prefixes)

```cypher
MATCH (n:BGPCollector)-[p:PEERS_WITH]-(a:AS)
WHERE n.project = 'riperis' AND p.num_v4_pfxs > 800000
RETURN n.name, COUNT(DISTINCT p.ip) AS nb_full, COLLECT(DISTINCT p.ip) AS ips_full
```

## Active RIPE Atlas probes for the top 5 ISPs in Japan

```cypher
MATCH (pb:AtlasProbe)-[:LOCATED_IN]-(a:AS)-[pop:POPULATION]-(c:Country)
WHERE c.country_code = 'JP' AND pb.status_name = 'Connected' AND pop.rank <= 5
RETURN pop.rank, a.asn, COLLECT(pb.id) AS probe_ids ORDER BY pop.rank
```
