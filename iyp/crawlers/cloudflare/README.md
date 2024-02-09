# Cloudflare Radar -- https://radar.cloudflare.com/

Cloudflare uses aggregated and anonymized DNS queries to their `1.1.1.1` public resolver service to
provide various datasets, including:

- [Ordered top 100 domain
  names](https://developers.cloudflare.com/radar/investigate/domain-ranking-datasets/): The 100
  domains with the most DNS queries, including assigned ranks.
- [Unordered top 1,000 to 1,000,000
  domains](https://developers.cloudflare.com/radar/investigate/domain-ranking-datasets/): Same as
  above, but there are no ranks assigned. Fetched set sizes are 1,000, 2,000, 5,000, 10,000, 20,000,
  50,000, 100,000, 200,000, 500,000, and 1,000,000.
- [Top 100 countries querying each of the 10,000 highest ranked domain
  names](https://developers.cloudflare.com/radar/investigate/dns/#top-locations): For each domain
  that is in the top 10,000 of *any* ranking included in IYP, fetch the top 100 countries with the
  most DNS queries.
- [Top 100 ASes querying each of the 10,000 highest ranked domain
  names](https://developers.cloudflare.com/api/operations/radar-get-dns-top-ases): Same as above, but
  fetch AS numbers instead.

All rankings are based on one week of data.
Cloudflare radar's top location and ASes is available for both domain names
and host names. Results are likely accounting for all NS, A, AAAA queries made to
Cloudflare's resolver. Since NS queries for host names make no sense IYP links these
results to `DomainName` nodes.

## Graph representation

### Ordered top 100 domain names - `top100.py`

Connect DomainName nodes to a single Ranking node corresponding to the ordered Cloudflare top 100
ranking. The `rank` is given as a relationship property.

```Cypher
(:DomainName {name: 'google.com'})-[:RANK {rank: 1}]->(:Ranking {name: 'Cloudflare top 100 domains'})
```

### Unordered top *n* domain names - `ranking_bucket.py`

Connect DomainName nodes to a single Ranking node corresponding to the unordered Cloudflare top *n*
ranking. There is no rank assigned to the domain name, but *n* is specified in the `top` property of
the Ranking node.

```Cypher
(:DomainName {name: 'google.com'})-[:RANK]->(:Ranking {name: 'Cloudflare Top 1000 ranking domains', top: 1000})
```

### Top countries - `dns_top_locations.py`

Connect each DomainName node to up to 100 Country nodes representing the countries from which the
domain was queried from the most. The `value` property of the QUERIED_FROM relationship describes
the percentage of all queries (within one week) originating from the country.

```Cypher
(:DomainName {name: 'google.com'})-[:QUERIED_FROM {value: 37.05}]->(:Country {country_code: 'US'})
```

### Top ASes - `dns_top_ases.py`

Connect each DomainName node to up to 100 AS nodes  from which the domain was queried from the most.
The `value` property of the QUERIED_FROM relationship describes the percentage of all queries
(within one week) originating from the AS.

```Cypher
(:DomainName {name: 'google.com'}-[:QUERIED_FROM {value: 3.51}]->(:AS {asn: 714}))
```

## Dependence

The `dns_top_locations` and `dns_top_ases` crawlers should be run after all crawlers that produce
`(:DomainName)-[:RANK {rank: n}]->(:Ranking)` relationships:

- `cloudflare.top100`
- `tranco.top1m`

## Notes

This crawler requires an application key to access the radar's API.
