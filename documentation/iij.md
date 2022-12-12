# Looking into one AS

## What are the top domain names that map to AS2497
### All domains that map to prefixes originated from AS2497 
```
match (n:AS {asn:2497})-[:ORIGINATE]-(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<10000 return n,p,i,d
```

### All domains that are related to AS2497 (including domains that map to prefixes orginated by AS2497 and prefixes that depends on AS2497)
```
match (n:AS {asn:2497})--(p:PREFIX)--(i:IP)--(d:DOMAIN_NAME)-[r:RANK]-(:RANKING) where r.rank<1000 return n,p,i,d
```

