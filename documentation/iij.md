# Looking into one AS: AS2497, IIJ

## IIJ's geographical footprint
#### IXP where IIJ is present
```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:COUNTRY) return iij, ix, cc
```

#### Geolocation of prefixes announced by IIJ
```cypher
MATCH (iij:AS {asn:2497})-[:ORIGINATE]-(pfx:PREFIX)--(cc:COUNTRY) return iij, pfx, cc
```

#### Geolocation of prefixes announced by IIJ's customer
```cypher
MATCH (iij:AS {asn:2497})<-[:DEPENDS_ON]-(customer:AS)-[:ORIGINATE]-(pfx:PREFIX)--(cc:COUNTRY) return iij, customer, pfx, cc
```

## IIJ's logical (DNS) footprint
### All domains that map to prefixes originated from AS2497 
```cypher
match (n:AS {asn:2497})-[:ORIGINATE]-(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<10000 return n,p,i,d
```

### All domains that are related to AS2497 (including domains that map to prefixes orginated by AS2497 and prefixes that depends on AS2497)
```cypher
match (n:AS {asn:2497})--(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<1000 return n,p,i,d
```

### All Japanese domains
Graph:
```cypher
MATCH (:RANKING)-[r:RANK]-(dn:DOMAIN_NAME)--(ip:IP)--(pfx:PREFIX)-[:ORIGINATE]-(net:AS)
WHERE dn.name ends with '.jp' and r.rank<10000
RETURN dn, ip, pfx, net
```

Table:
```cypher
MATCH (:RANKING)-[r:RANK]-(dn:DOMAIN_NAME)--(ip:IP)--(pfx:PREFIX)-[:ORIGINATE]-(net:AS)
WHERE dn.name ends with '.jp' and r.rank<100000
RETURN net.asn,  count(distinct dn) as nb_domain_name order by nb_domain_name desc
```
