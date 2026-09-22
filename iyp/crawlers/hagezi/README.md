# Hagezi DNS blocklists -- https://github.com/hagezi/dns-blocklists

[Hagezi's DNS blocklists](https://github.com/hagezi/dns-blocklists) are curated lists of
domain names grouped by the kind of content or behavior they are associated with (fake
shops, pop-up ads, threats, newly registered domains, DoH resolvers, dynamic DNS, URL
shorteners, piracy, gambling, social networks, NSFW).

[IHR resolves
hostnames](https://github.com/InternetHealthReport/hagezi-blocklists-forward-dns)
in these lists and publishes the A/AAAA records, the zone of each domain name,
and the authoritative name servers.

The resulting tags are:

* `fake`: This blocklist targets fake stores, fake streaming sites, rip-offs,
  subscription traps, and similar scams.
* `popup-ads`: Targets pop-up ads that range from annoying to outright malicious.
* `threat-med`: (medium version) This blocklist targets malware, cryptojacking,
  scams, spam, and phishing. It blocks domains known for spreading malware,
  running phishing attacks, and hosting command-and-control servers.
* `nrd-dga`: Newly registered domains (NRDs) are a favorite tool for threat
  actors running phishing, malware, and command-and-control operations, since
  these domains are easy to throw away and help dodge detection.
* `encrypted-dns-resolver`: Encrypted DNS servers
* `dynamic-dns`: Blocks dynamic DNS services that get abused for phishing
  campaigns and other shady activity.
* `url-shortener`: Blocks every known URL/link shortener out there.
* `piracy`: Blocks sites and services mainly used for illegally distributing
  copyrighted content.
* `gambling`: Blocks gambling-related sites.
* `social-networks`: Blocks social networks like Facebook, Instagram, TikTok, X
  (formerly Twitter), Snapchat, and others.
* `nsfw`: Blocks adult content.

## Graph representation

Blocklist membership is attributed to Hagezi, so these relationships carry
`reference_org: 'Hagezi'` with a `reference_url_info` pointing to the blocklist
repository. All other relationships describe
the DNS measurement performed by IHR and carry `reference_org: 'IHR'`.

For CATEGORIZED relationships `reference_time_modification` is the date of the Hagezi
commit the names were fetched from (the `source.commit_date` field of the result file).
For other relationships, the modification time refers to the time of the IHR scan.

**Blocklist membership:**

```Cypher
(:HostName {name: 'bit.ly'})-[:CATEGORIZED]->(:Tag {label: 'url-shortener'})
```

**IP resolution for hostnames & name servers:**

```Cypher
(:HostName {name: 'bit.ly'})-[:RESOLVES_TO]->(:IP {ip: '67.199.248.10'})
(:AuthoritativeNameServer {name: 'ns-cloud-c2.googledomains.com'})-[:RESOLVES_TO]->(:IP {ip: '216.239.34.108'})
```

**Zone of a hostname:**

```Cypher
(:HostName {name: 'bit.ly'})-[:PART_OF]->(:DomainName {name: 'bit.ly'})
```

**Authoritative name servers managing zones:**

```Cypher
(:DomainName {name: 'bit.ly'})-[:MANAGED_BY]->(:AuthoritativeNameServer {name: 'ns-cloud-c2.googledomains.com'})
```

## Dependence

This crawler is not depending on other crawlers.
