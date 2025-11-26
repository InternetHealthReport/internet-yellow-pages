# LACeS Anycast Census -- https://github.com/ut-dacs/anycast-census

LACeS (Longitudinal Anycast Census System) is a system to detect anycast prefixes and
estimate the geolocation of their sites. Results are produced daily for IPv4 and IPv6.

## Graph representation

Because the site geolocation is a geographic property, we model prefixes both as
`AnycastPrefix` and `GeoPrefix`.

```cypher
(:AnycastPrefix {prefix: '1.1.1.0/24'})-[:CATEGORIZED]->(:Tag {label: 'Anycast'})
```

The `CATEGORIZED` relationship contains the properties related to this dataset and can
be used to filter on the `reference_name`.

```cypher
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
MATCH p = (:AnycastPrefix {prefix: '1.1.1.0/24'})-[:PART_OF]->(:GeoPrefix)-[:LOCATED_IN]->(:Point)
RETURN p
```

## Dependence

This crawler is not depending on other crawlers.
