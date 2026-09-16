# Hagezi DNS blocklists -- https://github.com/hagezi/dns-blocklists

[Hagezi's DNS blocklists](https://github.com/hagezi/dns-blocklists) are curated lists of
domain names grouped by the kind of content or behavior they are associated with (fake
shops, pop-up ads, threats, newly registered domains, DoH resolvers, dynamic DNS, URL
shorteners, piracy, gambling, social networks, NSFW).

IHR resolves hostnames in these lists (https://github.com/InternetHealthReport/hagezi-blocklists-forward-dns)
and publishes the A/AAAA records, the zone of each domain name, and the 
authoritative name servers.

## Graph representation

**Blocklist membership:**

```Cypher
(:HostName {name: 'bit.ly'})-[:CATEGORIZED]->(:Tag {label: 'url-shortener'})
```

The Tag label is the name of the Hagezi list the name was taken from.
A name can be part of several lists.

List membership is Hagezi's data, so these relationships carry
`reference_org: 'Hagezi'` with a `reference_url_info` pointing to the [blocklist
repository](https://github.com/hagezi/dns-blocklists). All other relationships describe
the DNS measurement performed by IHR and carry `reference_org: 'IHR'`.

For CATEGORIZED relationships `reference_time_modification` is the date of the Hagezi
commit the names were fetched from (the `source.commit_date` field of the result file).

**IP resolution for hostnames:**

```Cypher
(:HostName {name: 'bit.ly'})-[:RESOLVES_TO]->(:IP {ip: '67.199.248.10'})
```

**Zone of a hostname:**

```Cypher
(:HostName {name: 'www.example.com'})-[:PART_OF]->(:DomainName {name: 'example.com'})
```

**Authoritative name servers managing zones:**

```Cypher
(:DomainName {name: 'example.com'})-[:MANAGED_BY]->(:HostName:AuthoritativeNameServer {name: 'a.iana-servers.net'})
```

Name servers are HostName nodes with the additional AuthoritativeNameServer label, and
their IPs are attached with RESOLVES_TO relationships as above.

## Dependence

This crawler is not depending on other crawlers.

## Notes

The crawler uses the GitHub API to find the most recent scan directory, since scans are
not published on a fixed schedule. It fails if the most recent scan is older than 30
days, which indicates that the measurement pipeline stopped working.
