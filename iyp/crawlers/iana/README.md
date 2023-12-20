# IANA -- https://www.iana.org/

The Internet Assigned Numbers Authority (IANA) is responsible for the global
coordination of the DNS Root, IP addressing, and other Internet protocol resources.

Datasets used by IYP:

- DNS [root zone file](https://www.iana.org/domains/root/files) to retrieve information
  about authoritative name servers of the top-level domains as well as their IP
  addresses.

## Graph representation

### Root zone file -  `root_zone.py`

IYP imports `NS`, `A`, and `AAAA` records from the root zone file.

```Cypher
// NS record
(:DomainName {name: 'jp'})-[:MANAGED_BY]->(:DomainName:AuthoritativeNameServer {name: 'a.dns.jp'})
// A record
(:DomainName:AuthoritativeNameServer {name: 'a.dns.jp'})-[:RESOLVES_TO]->(:IP {ip: '203.119.1.1'})
// AAAA record
(:DomainName:AuthoritativeNameServer {name: 'a.dns.jp'})-[:RESOLVES_TO]->(:IP {ip: '2001:dc4::1'})
```
