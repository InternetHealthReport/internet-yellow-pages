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

IYP imports IANA-level IPv4 and IPv6 address space allocations and special-purpose reservations. This provides the top-level view of global IP address management.
```cypher
// RIR allocation
(:IANAPrefix {prefix: '1.0.0.0/8'})-[:ALLOCATED {designation: 'APNIC', date: '2010-01', whois: 'whois.apnic.net', status: 'ALLOCATED'}]->(:Organization {name: 'APNIC'})

// Legacy allocation
(:IANAPrefix {prefix: '3.0.0.0/8'})-[:LEGACY {designation: 'General Electric Company', date: '1994-05', whois: 'whois.arin.net', status: 'LEGACY'}]->(:Organization {name: 'ARIN'})

// Special-purpose reservation
(:IANAPrefix {prefix: '10.0.0.0/8'})-[:RESERVED {designation: 'Private-Use', allocation_date: '1996-02', reserved: True, rfcs: ['RFC1918']}]->(:Organization {name: 'IANA'})
```

#### Node Labels

**IANAPrefix**: Represents an IP prefix (IPv4 or IPv6) allocated or reserved by IANA.
- `prefix`: IP prefix in CIDR notation (e.g., "1.0.0.0/8", "2001:200::/23")

**Organization**: Organizations managing address space (RIRs, IANA, or legacy holders).
- `name`: Normalized organization name (e.g., "APNIC", "ARIN", "RIPE NCC", "LACNIC", "AFRINIC", "IANA")

#### Relationship Types

**ALLOCATED**: Address space allocated by IANA to a Regional Internet Registry.
- Properties: `designation`, `date`, `whois`, `status`

**RESERVED**: Address space reserved for special purposes or by protocol specification.
- Properties: `designation`, `allocation_date`, `reserved`, `rfcs`

**LEGACY**: Legacy allocations made before RIRs existed, now administered by RIRs.
- Properties: `designation`, `date`, `whois`, `status`

#### Organization Name Normalization

Organization names are normalized from IANA's "Designation" field:
- `"IANA - xxx"` → `"IANA"`
- `"Administered by xxx"` → `"xxx"`
- `"Multicast"` → `"IANA"`
- `"Future use"` → `"IANA"`
- Company/agency names preserved as-is

The original designation is kept in relationship properties for full data provenance.