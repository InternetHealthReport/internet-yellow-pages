# BGP.Tools -- https://bgp.tools

Data collected by BGP.Tools, including:
- AS names
- AS tags
- Anycast IPv4, and IPv6 prefixes


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

### Anycast IPv4 and IPv6 prefixes
Connect Prefix to tag node meaning that an prefix has been categorized according to the TAG with a label `Anycast`.
```
(:Prefix {prefix: '43.249.213.0/24'})-[:CATEGORIZED]-(:Tag {label: 'Anycast'})
```

## Dependence

This crawler is not depending on other crawlers.
