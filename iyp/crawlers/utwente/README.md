# LACeS Anycast Census -- https://github.com/ut-dacs/anycast-census

LACeS (Longitudinal Anycast Census System) is a system to detect anycast prefixes and
estimate the geolocation of their sites. Results are produced daily for IPv4 and IPv6.

## Graph representation

The original dataset contains information about partial anycast prefixes. Basically, not
all parts of a BGP prefix have to be anycasted. However, for consistent modeling, we tag
the corresponding `BGPPrefix` node as Anycast. Users interested in the partial anycast
property should either use the original dataset, or run the query described below.

Because the site geolocation is a geographic property, we model the exact prefixes as
`GeoPrefix` nodes. These do not have to correspond to BGP prefixes, which is consistent
with existing modeling.

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

To find which parts of a BGP prefix are actually inferred as anycast by LACeS, query for
`GeoPrefix` nodes created by this dataset. Note that this only works for prefixes tagged
by LACeS, not other anycast datasets.

For example, `1.204.0.0/14` is a large BGP prefix that contains only a few inferred
anycast prefixes:

```cypher
MATCH (:BGPPrefix {prefix: '1.204.0.0/14'})<-[:PART_OF]-
      (p:GeoPrefix)-[:LOCATED_IN {reference_name: 'utwente.laces_v4'}]->
      (:Point)
RETURN DISTINCT(p.prefix) AS anycast_prefix
```

Although we maybe do not need the geographic information, we need to ensure we only
include `GeoPrefix` nodes created by this dataset and not others.

## Dependence

This crawler is not depending on other crawlers.
