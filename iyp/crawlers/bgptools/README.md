# BGP.Tools -- https://bgp.tools

Data collected by BGP.Tools, including:
- AS names
- AS tags


## Graph representation

### AS name
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


## Dependence

This crawler is not depending on other crawlers.
