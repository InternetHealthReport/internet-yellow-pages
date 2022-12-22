# Looking into one AS: AS2497, IIJ

## IIJ's geographical footprint
#### IXP where IIJ is present
```cypher
MATCH (iij:AS {asn:2497})-[:MEMBER_OF]-(ix:IXP)--(cc:COUNTRY) return iij, ix, cc
```

### Facilities where IIJ is present
```cypher
MATCH (iij:AS {asn:2497})--(ix:FACILITY)--(cc:COUNTRY) return iij, ix, cc
```

#### Geolocation of prefixes announced by IIJ
```cypher
MATCH (iij:AS {asn:2497})-[:ORIGINATE]-(pfx:PREFIX)--(cc:COUNTRY) return iij, pfx, cc
```

#### Geolocation of prefixes announced by IIJ's customer
graph:
```cypher
MATCH (iij:AS {asn:2497})<-[:DEPENDS_ON]-(customer:AS)-[:ORIGINATE]-(pfx:PREFIX)--(cc:COUNTRY) return iij, customer, pfx, cc
```
table:
```cypher
MATCH (iij:AS {asn:2497})<-[dep:DEPENDS_ON]-(customer:AS)-[:ORIGINATE]-(pfx:PREFIX)--(cc:COUNTRY) 
RETURN cc.country_code, count(distinct customer) as nb_dep order by nb_dep desc
```

## IIJ's logical (DNS) footprint
### Top domain names that map to prefixes originated from AS2497 
```cypher
match (n:AS {asn:2497})-[:ORIGINATE]-(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<10000 return n,p,i,d
```

### Top domain names that are related to AS2497 (including domains that map to prefixes orginated by AS2497 and prefixes that depends on AS2497)
```cypher
match (n:AS {asn:2497})--(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<10000 return n,p,i,d
```

## IIJ's main competitors
```cypher
match (comp:AS)-[:PEERS_WITH {rel:1}]->(customer:AS)<-[:PEERS_WITH {rel:1}]-(iij:AS {asn:2497})
WITH comp, customer OPTIONAL MATCH (comp)-[:NAME {reference_org:'RIPE NCC'}]-(comp_name:NAME)
return comp, comp_name, count(distinct customer) as nb_customer order by nb_customer desc
```

## Top Japanese domains
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
WITH net, dn  OPTIONAL MATCH (net:AS)-[:NAME {reference_org:'RIPE NCC'}]-(net_name:NAME)
RETURN net.asn, net_name.name, count(distinct dn) as nb_domain_name order by nb_domain_name desc
```
