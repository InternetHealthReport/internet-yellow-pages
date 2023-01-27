# Examples  for

## Which country an AS maps to
### Registration country code (administrative)

### Presence at IXP (biased by peering db)

### Country code of its peers

### Geoloc of prefixes


## Find all nodes that have the word 'crimea' in their name
MATCH (x)--(n:Name) WHERE toLower(n.name) CONTAINS 'crimea' RETURN  x, n;

## Crimean neighbors network dependencies
MATCH (crimea:Name)--(:AS)-[:PEERS_WITH]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:Name) WHERE toLower(crimea.name) CONTAINS 'crimea' AND toFloat(r.hegemony)>0.2 RETURN transit_as, collect(DISTINCT transit_name.name), count(DISTINCT r) AS nb_links ORDER BY nb_links DESC;

## Crimean IXP members dependencies
MATCH (crimea:Name)--(:IXP)-[:MEMBER_OF]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:Name) WHERE toLower(crimea.name) CONTAINS 'crimea' AND toFloat(r.hegemony)>0.2 RETURN transit_as, collect(DISTINCT transit_name.name), count(DISTINCT r) AS nb_links ORDER BY nb_links DESC;

## who is at Crimea ixp but not any other ixp


# Rostelecom

## Top domain names that are hosted by Rostelecom
```
MATCH (n:AS {asn:12389})-[:ORIGINATE]-(p:Prefix)--(i:IP)--(d:DomainName)-[r:RANK]-(:Ranking) WHERE r.rank<1000 RETURN n,p,i,d
```

## Top domain names that depends on Rostelecom
```
MATCH (n:AS {asn:12389})--(p:Prefix)--(i:IP)--(d:DomainName)-[r:RANK]-(:Ranking) WHERE r.rank<1000 RETURN n,p,i,d
```

## all ases assigned to same org


## which top domain names map to this ASes
```
MATCH (oid:OpaqueID)--(net:AS)--(pfx:Prefix)--(ip:IP)--(dname:DomainName)-[r]-(:Ranking), (net)-[{reference_org:'RIPE NCC'}]-(asname:Name) WHERE (oid:OpaqueID)--(:AS {asn:12389}) AND r.rank < 10000 AND net.asn<>12389 RETURN net, pfx, ip, dname, asname
```

## which prefixes map to other counties
```
MATCH (:AS {asn:12389})--(oid:OpaqueID)--(net:AS)--(pfx:Prefix)--(cc:Country), (net)-[{reference_org:'RIPE NCC'}]-(asname:Name) WHERE net.asn<>12389 AND cc.country_code <> 'RU' RETURN net, pfx, asname, cc, oid
```

## which IXPs they are member of:
```
MATCH (oid:OpaqueID)--(n:AS)--(ix:IXP) WHERE (oid:OpaqueID)--(:AS {asn:12389}) RETURN n, ix
```

## which IXPs are they operating to?
```
MATCH (n:AS {asn:12389})--(ix:IXP)--(cc:Country) RETURN n,ix,cc
```

## prefixes assigned to this org and ASes announcing it

!
## Country code of ASes hosting top domain names
MATCH (:Ranking)-[r:RANK]-(domain:DomainName)--(:IP)--(:Prefix)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country) WHERE r.rank<10000 AND domain.name ENDS WITH '.ru' RETURN cc.country_code, count(DISTINCT domain.name) AS dm_count ORDER BY dm_count DESC
MATCH (:Ranking)-[r:RANK]-(domain:DomainName)--(:IP)--(:Prefix)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country) WHERE r.rank<10000 AND domain.name ENDS WITH '.jp' RETURN cc.country_code, count(DISTINCT domain.name) AS dm_count ORDER BY dm_count DESC
MATCH (:Ranking)-[r:RANK]-(domain:DomainName)--(:IP)--(:Prefix)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country) WHERE r.rank<10000 AND domain.name ENDS WITH '.jp' RETURN cc.country_code, count(DISTINCT domain.name) AS dm_count ORDER BY dm_count DESC

# Interesting queries:
## Orange presence
```cypher
MATCH (oid)--(n:AS)--(ix:IXP)--(cc:Country) WHERE (oid:OpaqueID)--(:AS {asn:5511}) RETURN n,ix,cc
```
