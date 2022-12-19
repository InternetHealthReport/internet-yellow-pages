# IYP Gallery

Below are examples queries that you can run in Neo4j browser. 

## Simple queries

### Names for AS2497

```
MATCH (a:AS {asn:2497})--(n:NAME) RETURN a,n
```
![Names for AS2497](/documentation/assets/gallery/as2497names.svg)

### All nodes related to 8.8.8.0/24

```
MATCH (gdns:PREFIX {prefix:'8.8.8.0/24'})--(neighbor) RETURN gdns, neighbor
```

![All nodes related to 8.8.8.0/24](/documentation/assets/gallery/prefixAllRelated.png)
