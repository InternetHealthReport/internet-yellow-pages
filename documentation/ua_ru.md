# Examples  for

## Find all nodes that have the word 'crimea' in their name
match (x)--(n:NAME) WHERE toLower(n.name) contains 'crimea' RETURN  x, n;

## Crimean neighbors network dependencies
match (crimea:NAME)--(:AS)-[:PEERS_WITH]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:NAME) where toLower(crimea.name) contains 'crimea' and toFloat(r.hegemony)>0.2 return transit_as, collect(distinct transit_name.name), count(distinct r) as nb_links order by nb_links desc;

## Crimean IXP members dependencies
match (crimea:NAME)--(:IXP)-[:MEMBER_OF]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:NAME) where toLower(crimea.name) contains 'crimea' and toFloat(r.hegemony)>0.2 return transit_as, collect(distinct transit_name.name), count(distinct r) as nb_links order by nb_links desc;

## who is at Crimea ixp but not any other ixp


# Rostelecom

## Top domain names that are hosted by Rostelecom
```
match (n:AS {asn:12389})-[:ORIGINATE]-(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<1000 return n,p,i,d
```

## Top domain names that depends on Rostelecom
```
match (n:AS {asn:12389})--(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<1000 return n,p,i,d
```

## all ases assigned to same org


## which top domain names map to this ASes
```
match (oid:OPAQUE_ID)--(net:AS)--(pfx:PREFIX)--(ip:IP)--(dname:DOMAIN_NAME)-[r]-(:RANKING), (net)-[{reference_org:'RIPE NCC'}]-(asname:NAME) where (oid:OPAQUE_ID)--(:AS {asn:12389}) and r.rank < 10000 and net.asn<>12389 return net, pfx, ip, dname, asname
```

## which prefixes map to other counties
```
match (:AS {asn:12389})--(oid:OPAQUE_ID)--(net:AS)--(pfx:PREFIX)--(cc:COUNTRY), (net)-[{reference_org:'RIPE NCC'}]-(asname:NAME) where net.asn<>12389 and cc.country_code <> 'RU' return net, pfx, asname, cc, oid
```

## which IXPs they are member of:
```
match (oid:OPAQUE_ID)--(n:AS)--(ix:IXP) where (oid:OPAQUE_ID)--(:AS {asn:12389}) return n, ix
```

## which IXPs are they operating to?
```
match (n:AS {asn:12389})--(ix:IXP)--(cc:COUNTRY) return n,ix,cc
```

## prefixes assigned to this org and ASes announcing it

!
## Country code of ASes hosting top domain names
match (:RANKING)-[r:RANK]-(domain:DOMAIN_NAME)--(:IP)--(:PREFIX)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:COUNTRY) where r.rank<10000 and domain.name ends with '.ru' return cc.country_code, count(distinct domain.name) as dm_count ORDER BY dm_count DESC
match (:RANKING)-[r:RANK]-(domain:DOMAIN_NAME)--(:IP)--(:PREFIX)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:COUNTRY) where r.rank<10000 and domain.name ends with '.jp' return cc.country_code, count(distinct domain.name) as dm_count ORDER BY dm_count DESC
match (:RANKING)-[r:RANK]-(domain:DOMAIN_NAME)--(:IP)--(:PREFIX)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:COUNTRY) where r.rank<10000 and domain.name ends with '.jp' return cc.country_code, count(distinct domain.name) as dm_count ORDER BY dm_count DESC

# Interesting queries:
## Orange presence
```
match (oid)--(n:AS)--(ix:IXP)--(cc:COUNTRY) where (oid:OPAQUE_ID)--(:AS {asn:5511}) return n,ix,cc
```
