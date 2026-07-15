# MaxMind -- https://www.maxmind.com/

MaxMind is an IP geolocation service that provides different kinds of IP
databases, including a [free
tier](https://www.maxmind.com/en/geolite-free-ip-geolocation-data) that maps IP
prefixes to countries. We import the free database into IYP.

## Graph representation

A prefix can also be just a single IP, resulting in /32 or /128 prefixes, which
is intended. We also import the auxiliary country data provided by MaxMind as
relationship properties.

```cypher
(pfx:GeoPrefix {prefix: '202.208.0.0/12'})-[:COUNTRY {continent_name: 'Asia', is_in_european_union: 0}]->(:Country {country_code: 'JP'})
```

## Dependence

This crawler is not depending on other crawlers.
