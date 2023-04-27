# Number Resource Organization -- https://www.nro.net/

The Number Resource Organization (NRO) is the coordinating body for the world’s Regional Internet
Registries (RIRs). The RIRs manage the distribution of Internet number resources (IP address space
and Autonomous System Numbers) within their respective regions.

As part of a joint RIR project to provide consistent and accessible Internet number resource
statistics the NRO publishes [*Extended Allocation and Assignment
Reports*](https://www.nro.net/about/rirs/statistics/) (also called *delegated stats*) that contain
information about assigned IP address ranges and AS numbers.

Each line of the report is a record that either represents an IP address range or an AS number. The
record has a status and maps to an *opaque ID* that uniquely identifies a single organization.
Finally, the record contains a country code to which the organization belongs.

**Note:** If the record is not assigned, the country code is `ZZ`, which will still be inserted into
IYP.

## Graph representation

```Cypher
(:AS {asn: 7494})-[:AVAILABLE {registry: 'apnic'}]->(:OpaqueID {id: 'apnic'})
(:AS {asn: 2497})-[:ASSIGNED {registry: 'apnic'}]->(:OpaqueID {id: 'A91A7381'})
(:AS {asn: 608})-[:RESERVED {registry: 'arin'}]->(:OpaqueID {id: 'arin'})
(:AS {asn: 2497})-[:COUNTRY]->(:Country {country_code: 'JP'})

(:Prefix {prefix: '2a03:1dc0::/27'})-[:AVAILABLE {registry: 'ripencc'}]->(:OpaqueID {id: 'ripencc'})
(:Prefix {prefix: '202.0.65.0/24'})-[:ASSIGNED {registry: 'apnic'}]->(:OpaqueID {id: 'A91A7381'})
(:Prefix {prefix: '196.20.32.0/19}')-[:RESERVED {registry: 'afrinic'}]->(:OpaqueID {id: 'afrinic'})
(:Prefix {prefix: '196.20.32.0/19}')-[:COUNTRY]->(:Country {country_code: 'ZZ'})
```

The report also contains `allocated` records that would result in a `ALLOCATED` relationship.
However, this crawler does not add ASes, so if the AS node was not created by another crawler, which
should not happen for `allocated` ASes, the relationship is not created.

The IPv4 address ranges in the report are not necessarily aligned with CIDR ranges (prefixes are
represented by the first IP and a *count of hosts*). However, the crawler rounds down to the next
CIDR range.

## Dependence

This crawler does not create new AS nodes and should be run after crawlers that push many AS nodes
(e.g., `ripe.as_names`).
