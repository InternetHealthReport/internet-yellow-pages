# bgp.tools -- https://bgp.tools

Data collected by bgp.tools, including:

- AS names
- AS tags
- Anycast IPv4 and IPv6 prefixes

## Graph representation

### AS names

Connect AS to names nodes, providing the name of an AS. Names from bgp.tools can include
corrections made by users of the website.

```cypher
(:AS {asn:2497})-[:NAME]->(:Name {name:'IIJ'})
```

### AS tags

Connect AS to tag nodes meaning that an AS has been categorized according to the
given tag.

```cypher
(:AS {asn:2497})-[:CATEGORIZED]->(:Tag {label: 'Internet Critical Infra'})
```

### Anycast IPv4 and IPv6 prefixes

Connect Prefix to Tag node indicating that the prefix has been categorized as an Anycast
prefix.

```cypher
(:BGPPrefix {prefix: '8.8.8.0/24'})-[:CATEGORIZED]->(:Tag {label: 'Anycast'})
```

## Dependence

This crawler is not depending on other crawlers.
