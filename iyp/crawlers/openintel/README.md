# OpenINTEL -- [https://www.openintel.nl/](https://www.openintel.nl/)

The OpenINTEL measurement platform captures daily snapshots of the state of large parts of the global Domain Name System (DNS) by running a number of forward and reverse DNS measurements.

## Overview

OpenINTEL performs DNS measurements for various domain names. Currently, **IYP only fetches data for the [Tranco top 1 million list](https://tranco-list.eu/) and the [CISCO Umbrella top 1 million list](https://umbrella.cisco.com/)** since it merges rankings. Additionally, IYP retrieves authoritative name servers observed by OpenINTEL.

A **mail server crawler exists but is currently not in use**, as it generates a very large number of links and has not been requested by anyone.

## Graph Representation

### IP Resolution for Popular Host Names

```cypher
(:HostName {name: 'google.com'})-[:RESOLVES_TO]->(:IP {ip: '142.250.179.142'})

```

IP resolution of authoritative name servers:

```Cypher
(:HostName:AuthoritativeNameServer {name: 'ns1.google.com'})-[:RESOLVES_TO]->(:IP {ip: '216.239.32.10'})
(:IP {ip: '216.239.32.10'})-[:SERVE]->(:Service {name: 'DNS'})
```

Domain names managed by name servers:

```Cypher
(:DomainName {name: 'google.com'})-[:MANAGED_BY]->(:HostName:AuthoritativeNameServer {name: 'ns1.google.com'})
```

## Dependence

This crawler is not depending on other crawlers.
