# M-Lab -- https://www.measurementlab.net/

M-Lab is an open source project that provides a platform for global network
performance measurements. We import [aggregated monthly
statistics](https://www.measurementlab.net/data/stats/) that provide
distributions for some performance metrics (download/upload speed, latency, and
loss) at different granularities.

## `stats_country.py`

This crawler imports country-level statistics, which aggregate speed tests
performed by *clients* from a specific country. Note that the destination server
of the test can be in a different country.

### Graph representation

```cypher
(:Country {country_code: 'JP'})-[:SPEED_TEST]->(:Estimate {name: 'M-Lab Stats All Country'})
```

The distributions are specified as selected percentiles (p1, p5, p10, p25, p50,
p75, p90, p95, and p99). Thus, for each metric there are nine properties (e.g.,
`download_p50`). Download/upload speeds are specified as megabits per second,
latency is milliseconds, loss rate is a ratio (in range 0-1).

Other important properties are the `[download|upload]_sample_count` properties,
which specify the number of aggregated tests. You should use these to filter out
outliers caused by results with too few samples. `download_sample_count` is
relevant for download, latency, and loss results, `upload_sample_count` is only
for the upload speed metric.

## `stats_country_asn.py`

This crawler imports country/AS-level statistics, which aggregate speed tests
performed by clients located in a certain AS *and* a specific country. An AS can
span multiple countries so clients are geolocated based on their specific IP
(using MaxMind).

### Graph representation

```cypher
(:AS {asn: 2497})-[:SPEED_TEST {country_code: 'JP'}]->(:Estimate {name: 'M-Lab Stats JP'})
```

We create one `Estimate` node per country. The above representation shows
results for clients from AS2497 that are located in Japan. Note that we could
for example target `{name: 'M-Lab Stats US'}` for clients from AS2497 located in
the US.

The `country_code` property is added for convenience. The remaining properties
are explained above.

## Dependence

These crawlers are not depending on other crawlers.
