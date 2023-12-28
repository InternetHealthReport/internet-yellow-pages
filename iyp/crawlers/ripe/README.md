# RIPE NCC -- https://www.ripe.net/

The RIPE Network Coordination Centre (RIPE NCC) is the Regional Internet Registry (RIR) for Europe,
the Middle East and parts of Central Asia.

IYP does not use RIPE-specific information, but fetches data from RIPE‘s convenient [FTP
server](https://ftp.ripe.net/).

## Graph representation

### AS names - `as_names.py`

RIPE NCC provides a simple [list of AS names](https://ftp.ripe.net/ripe/asnames/) (also containing a
country code) which is the base of many AS nodes in IYP.

```Cypher
(:AS {asn: 2497})-[:NAME]->(:Name {name: 'IIJ Internet Initiative Japan Inc.'})
(:AS {asn: 2497})-[:COUNTRY]->(:Country {country_code: 'JP'})
```

### Route Origin Authorizations - `roa.py`

A Route Origin Authorization (ROA) is a cryptographically signed object that states which AS is
authorized to originate a particular IP address prefix or set of prefixes.

IYP uses RIPE NCC‘s [mirror of Trust Anchor Locators](https://ftp.ripe.net/rpki/) of the five RIRs
to extract ROA information. The max length specification of the ROA is added as the `maxLength`
property on the relationship.

```Cypher
(:AS {asn: 2497})-[:ROUTE_ORIGIN_AUTHORIZATION {maxLength: 18}]->(:Prefix {prefix: '49.239.64.0/18'})
```

### Atlas Probes - `atlas_probes.py`

[RIPE Atlas](https://atlas.ripe.net/) is an Internet measurement platform maintained by
the RIPE NCC. RIPE Atlas employs a global network of probes that measure Internet
connectivity and reachability.

We fetch the [list of probes](https://atlas.ripe.net/docs/apis/rest-api-manual/probes/)
to model the location of Atlas probes in the graph topology based on the probe's IP(s),
ASN(s), and country.

```Cypher
(:AtlasProbe {id: 6425})-[:LOCATED_IN]->(:AS {asn: 2497})
(:AtlasProbe {id: 6425})-[:COUNTRY]->(:Country {country_code: 'JP'})
(:IP {ip: '202.214.97.16'})-[:ASSIGNED]->(:AtlasProbe {id: 6425})
```

### Atlas Measurements - `atlas_measurements.py`

We fetch the [list of probe measurements](https://atlas.ripe.net/api/v2/measurements) to obtain the measurement data of the `AtlasProbe`. This data is based on the probe's ID, connected through relationships defined by `PART_OF` and `TARGET`. The `TARGET` relationship encompass associations with both `DOMAIN` and `IP`.

The Cypher query for these relationships appears as follows:

```Cypher
(:AtlasProbe)-[:PART_OF]->(:AtlasMeasurement)-[:TARGET]->(:DomainName)
(:AtlasProbe)-[:PART_OF]->(:AtlasMeasurement)-[:TARGET]->(:IP)
```

This query is designed to identify `AtlasProbes` linked via the `PART_OF` relationship to `AtlasMeasurements`, which, in turn, are linked through `TARGET` to either a `DomainName` or an `IP`.

## Dependence

This crawler is not depending on other crawlers.
