# LACeS Anycast Census -- https://github.com/ut-dacs/anycast-census

LACeS (Longitudinal Anycast Census System) is a system to detect anycast prefixes and
estimate the geolocation of their sites. Results are produced daily for IPv4 and IPv6.

## Graph representation

Because the site geolocation is a geographic property, we model prefixes both as
`BGPPrefix` and `GeoPrefix`. Since the site information is attached to usually
more-specific prefixes than what is visible in BGP, we use the `backing_prefix` (aka BGP
prefix) for `BGPPrefix` nodes and the `prefix` for `GeoPrefix` nodes.

In addition, since multiple prefixes can belong to the same BGP prefix, the detailed
properties of the dataset are only attached to the `GeoPrefix` relationships.

```cypher
(:BGPPrefix {prefix: '1.1.1.0/24'})-[:CATEGORIZED]->(:Tag {label: 'Anycast'})
(:GeoPrefix {prefix: '1.1.1.0/24'})-[:LOCATED_IN {city: 'Tokyo'}]->(:Point)
(:GeoPrefix {prefix: '1.1.1.0/24'})-[:LOCATED_IN {country_code: 'JP'}]->(:Point)
(:GeoPrefix {prefix: '1.1.1.0/24'})-[:COUNTRY]->(:Country {country_code: 'JP'})
```

All `GeoPrefix` nodes created by this crawler are connected to a WGS84 point. Most (but
not all) of the `LOCATED_IN` relationships have a `city` and `country_code` property
(from the LACeS dataset) that is useful to filter on the location. If a `country_code`
exist, we also create a `COUNTRY` relationship to the respective `Country` node.

To find all anycast sites for a specific prefix, you need to match both node types:

```cypher
MATCH p = (:Tag {label: 'Anycast'})<-[:CATEGORIZED]-
          (:BGPPrefix {prefix: '1.1.1.0/24'})-[:PART_OF]->
          (:GeoPrefix)-[:LOCATED_IN]->(:Point)
RETURN p
```

## Dependence

This crawler is not depending on other crawlers.
