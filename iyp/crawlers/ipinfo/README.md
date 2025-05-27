# IPinfo -- https://ipinfo.io/

IPinfo is an IP geolocation service, that provides different kinds of IP databases,
including a [free tier](https://ipinfo.io/products/free-ip-database) that maps IP ranges
to countries. We import the free database into IYP.

## Graph representation

Since the IP ranges are not necessarily CIDR aligned, we decompose unaligned ranges
into their CIDR-equivalent blocks. The original range is retained in the `start_ip` and
`end_ip` properties of the `COUNTRY` relationship.

A range can also be just a single IP, resulting in /32 or /128 prefixes, which is
intended.

```cypher
(:GeoPrefix {prefix: '203.180.224.0/19'})-[:COUNTRY {start_ip: '203.180.204.28', end_ip: '203.181.102.41'}]->(:Country {country_code: 'JP'})
```

## Dependence

This crawler is not depending on other crawlers.
