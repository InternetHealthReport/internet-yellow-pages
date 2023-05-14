# BGP.Tools -- https://bgp.tools

Data collected by BGP.Tools, including:
- AS names
- AS tags
- IPV4, and IPV6 prefixes


## Graph representation

### AS names
Connect AS to names nodes, providing the name of an AS.
For example:
```
(:AS {asn:2497})-[:NAME]-(:Name {name:'IIJ'})
```

### AS tags
Connect AS to tag nodes meaning that an AS has been categorized according to the
given tag.
```
(:AS {asn:2497})-[:CATEGORIZED]-(:Tag {label: 'Internet Critical Infra'})
```

### IPV4 and IPV6 prefixes
Connect Prefix to tag node meaning that an AS has been categorized according to the tag with a label `Anycast`.
```
MATCH (p:Prefix {prefix: '43.249.213.0/24'})-[r:CATEGORIZED]-(t:Tag {label: 'Anycast'}) RETURN p, t
```

## Dependence

This crawler is not depending on other crawlers.
