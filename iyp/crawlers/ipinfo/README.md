# IPinfo

Data collected by IPinfo, including:

- IP geolocation

## Graph representation

### IP geolocation

Connect prefixes to country nodes, providing the IP geolocation of the prefix.
For example:

```cypher
(:Prefix)-[:COUNTRY]-(:Country)
```

## Dependence

This crawler is not depending on other crawlers.
