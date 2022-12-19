# CAIDA -- https://caida.org

AS rank in terms of customer cone size, meaning that large transit providers are
higher ranked.

## Graph representation

### Ranking
Connect ASes nodes to a single ranking node corresponding to ASRank. The rank is
given as a link attribute.
For example:
```
(:AS  {asn:2497})-[:RANK {rank:87}]-(:RANKING {name:'CAIDA ASRank'})
```

### Country
Connect AS to country nodes, meaning that the AS is registered in that country.

```
(:AS)-[:COUNTRY]-(:COUNTRY)
```

### AS name
Connect AS to names nodes, providing the name of an AS.
For example:
```
(:AS {asn:2497})-[:NAME]-(:NAME {name:'IIJ'})
```

## Dependence

This crawler is not depending on other crawlers.
