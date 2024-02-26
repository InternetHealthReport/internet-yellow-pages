# rDNS RIR data -- https://rir-data.org/

"Lowering the Barriers to Working with Public RIR-Level Data" is a joint project of
SimulaMet and the University of Twente with the goal of making WHOIS, route object
delegation, and reverse DNS (rDNS) zone files published by Regional Internet Registries
(RIRs) more accessible.

IYP imports the rDNS files in a simplified format to indicate which authoritative name
servers are responsible for a prefix. We do not model PTR records and the corresponding
hierarchy but instead add a simple MANAGED_BY link.

## Graph representation

```cypher
(:Prefix {prefix: '103.2.57.0/24'})-[:MANAGED_BY {source: 'APNIC', ttl: 172800}]->(:AuthoritativeNameServer {name: 'dns0.iij.ad.jp'})
```

The `source` property indicates from which RIR the information was obtained, the `ttl`
property refers to the time-to-live of the associated SOA record.

## Dependence

This crawler is not depending on other crawlers.
