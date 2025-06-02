# OpenINTEL -- https://www.openintel.nl/

The OpenINTEL project captures daily snapshots of the state of large parts of the global
Domain Name System (DNS) by running a number of forward and reverse DNS measurements.

Currently, IYP imports three open [toplist-based
datasets](https://openintel.nl/data/forward-dns/top-lists/) and one closed
[infrastructure dataset](https://openintel.nl/data/forward-dns/infrastructure/) with
kind permission:

- Google CRuX: `crux.py` (see also
  [google.crux_top1m_country](https://github.com/InternetHealthReport/internet-yellow-pages/blob/main/iyp/crawlers/google))
- Tranco Top 1M: `tranco1m.py` (see also
  [tranco.top1m](https://github.com/InternetHealthReport/internet-yellow-pages/tree/main/iyp/crawlers/tranco))
- Cisco Umbrella Top 1M: `umbrella1m.py` (see also
  [cisco.umbrella_top1m](https://github.com/InternetHealthReport/internet-yellow-pages/tree/main/iyp/crawlers/cisco))
- Infrastructure: `infra_ns.py`

The list-based datasets contain results for 12 different types of DNS queries, however
we only import the results of A, AAAA, and NS queries. The toplist datasets yield the
name resolution for popular domains as well as the responsible authoritative name
servers. The infrastructure dataset performs additional A/AAAA queries for the names of
authoritative name servers observed on the list-based measurements. We also model CNAME
records that are retrieved as part of the name resolution.

A crawler that imports MX records and their corresponding resolution is implemented as
well, but currently not in use (`infra_mx.py`).

IYP also imports three datasets of the related [DNS Dependency
Graph](https://dnsgraph.dacs.utwente.nl) measurement. This measurement performs name
resolution of various host names but records the entire DNS tree, starting at the root,
that is required to resolve the name.

The datasets resolve different host names from different vantage points:

- `dnsgraph_crux.py`: Google CRuX hosts measured from Japan.
- `dnsgraph_jp.py` (inactive): Tranco Top 1M hosts from Japan.
- `dnsgraph_nl.py`: Tranco Top 1M hosts from the Netherlands.
- `dnsgraph_rdns.py`: Authoritative name servers managing rDNS zones from the
  Netherlands (see also
  [simulamet.rirdata_rdns](https://github.com/InternetHealthReport/internet-yellow-pages/blob/main/iyp/crawlers/simulamet)).

## Graph representation

**IP resolution for host names:**

```Cypher
(:HostName {name: 'www.youtube.com'})-[:RESOLVES_TO]->(:IP {ip: '142.250.179.174'})
```

The RESOLVES_TO relationship of these crawlers also contains a `source` property (`A`,
`AAAA`, or `CNAME`) that indicates the source of the name resolution. The example above
is actually not a direct A record, but resolved via a CNAME, thus its `source` property
is `CNAME`.

**The CNAME redirections are modelled using the ALIAS_OF relationship:**

```Cypher
(:HostName {name: 'www.youtube.com'})-[:ALIAS_OF]->(:HostName {name: 'youtube-ui.l.google.com'})-[:RESOLVES_TO {source: 'A'}]->(:IP {ip: '142.250.179.174'})
```

Note that the `source` property is `A` this time, indicating an A record. **WARNING: The
`source` property is only present for the OpenINTEL crawlers so including it in the
query will ignore other datasets.**

**Authoritative name servers managing domains:**

```Cypher
(:DomainName {name: 'youtube.com'})-[:MANAGED_BY]->(:HostName:AuthoritativeNameServer {name: 'ns1.google.com'})-[:RESOLVES_TO]->(:IP {ip: '216.239.32.10'})
```

Authoritative name servers are HostName nodes with the additional
AuthoritativeNameServer label. Name resolution data for these comes from the closed
infrastructure measurement.

Note that **domain names** are managed by name servers, as opposed to the **host names**
above. For cases where the domain name (managed by an authoritative name server) and the
host name (resolving to an IP) are the same, this crawler also adds a PART_OF
relationship:

```Cypher
(:HostName {name: 'youtube.com'})-[:PART_OF]->(:DomainName {name: 'youtube.com'})
```

More complex domain name relationships are included by the DNS Graph crawler, which
infers PART_OF relationships for cases where the host and domain name are not the same
as well as PARENT relationships between domain names.

**Parent relationships between domain names:**

```Cypher
(:DomainName {name: 'youtube.com'})-[:PARENT]->(:DomainName {name: 'com'})
```

## Dependence

The `crux.py` crawler only fetches data for existing countries and thus should run after
crawlers that create `Country` nodes.