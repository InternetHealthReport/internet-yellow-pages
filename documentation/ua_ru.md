# Examples  for

## Which country an AS maps to
### Registration country code (administrative)

### Presence at IXP (biased by peering db)

### Country code of its peers

### Geoloc of prefixes


## Find all nodes that have the word 'crimea' in their name
match (x)--(n:Name) WHERE toLower(n.name) contains 'crimea' RETURN  x, n;

## Crimean neighbors network dependencies
match (crimea:Name)--(:AS)-[:PEERS_WITH]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:Name) where toLower(crimea.name) contains 'crimea' and toFloat(r.hegemony)>0.2 return transit_as, collect(distinct transit_name.name), count(distinct r) as nb_links order by nb_links desc;

## Crimean IXP members dependencies
match (crimea:Name)--(:IXP)-[:MEMBER_OF]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:Name) where toLower(crimea.name) contains 'crimea' and toFloat(r.hegemony)>0.2 return transit_as, collect(distinct transit_name.name), count(distinct r) as nb_links order by nb_links desc;

## who is at Crimea ixp but not any other ixp


# Rostelecom

## Top domain names that are hosted by Rostelecom
```
match (n:AS {asn:12389})-[:ORIGINATE]-(p:Prefix)--(i:IP)--(d:DomainName)-[r:RANK]-(:Ranking) where r.rank<1000 return n,p,i,d
```

## Top domain names that depends on Rostelecom
```
match (n:AS {asn:12389})--(p:Prefix)--(i:IP)--(d:DomainName)-[r:RANK]-(:Ranking) where r.rank<1000 return n,p,i,d
```

## all ases assigned to same org


## which top domain names map to this ASes
```
match (oid:OpaqueID)--(net:AS)--(pfx:Prefix)--(ip:IP)--(dname:DomainName)-[r]-(:Ranking), (net)-[{reference_org:'RIPE NCC'}]-(asname:Name) where (oid:OpaqueID)--(:AS {asn:12389}) and r.rank < 10000 and net.asn<>12389 return net, pfx, ip, dname, asname
```

## which prefixes map to other counties
```
match (:AS {asn:12389})--(oid:OpaqueID)--(net:AS)--(pfx:Prefix)--(cc:Country), (net)-[{reference_org:'RIPE NCC'}]-(asname:Name) where net.asn<>12389 and cc.country_code <> 'RU' return net, pfx, asname, cc, oid
```

## which IXPs they are member of:
```
match (oid:OpaqueID)--(n:AS)--(ix:IXP) where (oid:OpaqueID)--(:AS {asn:12389}) return n, ix
```

## which IXPs are they operating to?
```
match (n:AS {asn:12389})--(ix:IXP)--(cc:Country) return n,ix,cc
```

## prefixes assigned to this org and ASes announcing it

!
## Country code of ASes hosting top domain names
match (:Ranking)-[r:RANK]-(domain:DomainName)--(:IP)--(:Prefix)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country) where r.rank<10000 and domain.name ends with '.ru' return cc.country_code, count(distinct domain.name) as dm_count ORDER BY dm_count DESC
match (:Ranking)-[r:RANK]-(domain:DomainName)--(:IP)--(:Prefix)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country) where r.rank<10000 and domain.name ends with '.jp' return cc.country_code, count(distinct domain.name) as dm_count ORDER BY dm_count DESC
match (:Ranking)-[r:RANK]-(domain:DomainName)--(:IP)--(:Prefix)-[:ORIGINATE]-(:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country) where r.rank<10000 and domain.name ends with '.jp' return cc.country_code, count(distinct domain.name) as dm_count ORDER BY dm_count DESC

# Interesting queries:
## Orange presence
```
match (oid)--(n:AS)--(ix:IXP)--(cc:Country) where (oid:OpaqueID)--(:AS {asn:5511}) return n,ix,cc
```
