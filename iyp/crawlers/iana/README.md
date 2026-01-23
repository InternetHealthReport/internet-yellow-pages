# IANA -- https://www.iana.org/

The Internet Assigned Numbers Authority (IANA) is responsible for the global
coordination of the DNS Root, IP addressing, and other Internet protocol resources.

Datasets used by IYP:

- DNS [root zone file](https://www.iana.org/domains/root/files) to retrieve information
  about authoritative name servers of the top-level domains as well as their IP
  addresses.
- [IPv4 Address Space Registry](https://www.iana.org/assignments/ipv4-address-space/)
  and [IPv6 Unicast Address Assignments](https://www.iana.org/assignments/ipv6-unicast-address-assignments/)
  for IANA-level address space allocations to Regional Internet Registries (RIRs).
- [IPv4](https://www.iana.org/assignments/iana-ipv4-special-registry/) and
  [IPv6](https://www.iana.org/assignments/iana-ipv6-special-registry/) Special-Purpose
  Address Registries for reserved address blocks.

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

### Address Space Registries - `address_space.py`

IYP imports IANA-level IPv4 and IPv6 address space allocations and special-purpose
reservations. This provides the top-level view of global IP address management.

```cypher
// RIR allocation
(:IANAPrefix {prefix: '1.0.0.0/8'})-[:ALLOCATED]->(:Organization {name: 'APNIC'})

// Legacy allocation
(:IANAPrefix {prefix: '17.0.0.0/8'})-[:LEGACY]->(:Organization {name: 'Apple Computer Inc.'})

// Special-purpose reservation
(:IANAPrefix {prefix: '192.168.0.0/16'})-[:RESERVED {Name: 'Private-Use'}]->(:Organization {name: 'IANA'})
```
