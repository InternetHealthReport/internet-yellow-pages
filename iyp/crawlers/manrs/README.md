# MANRS -- https://www.manrs.org/

Mutually Agreed Norms for Routing Security (MANRS) is an initiative to improve the security and
resilience of the Internet’s global routing system. It does this by encouraging those running BGP to
implement well-established industry best practices and technological solutions that can address the
most common threats.

A network operator can become a MANRS member by implementing *Actions* that are further described
[here](https://www.manrs.org/netops/network-operator-actions/). Currently there are four actions:

1. Filtering: Prevent propagation of incorrect routing information
1. Anti-spoofing: Prevent traffic with spoofed source IP addresses
1. Coordination: Facilitate global operational communication and coordination
1. Global Validation: Facilitate routing information on a global scale

IYP contains information about the membership status of networks (in form of AS nodes) and which
actions are implemented by each member. The country assignment provided by MANRS is also used to
enhance the existing AS-to-Country mappings.

## Graph representation

```Cypher
(:AS {asn: 2497})-[:MEMBER_OF]->(:Organization {name: 'MANRS'})
(:AS {asn: 2497})-[:IMPLEMENT]->(:ManrsAction {label: 'MANRS Action 1: Filtering'})
(:AS {asn: 2497})-[:COUNTRY]->(:Country {country_code: 'JP'})
```

Possible labels for ManrsAction nodes:

- `MANRS Action 1: Filtering`
- `MANRS Action 2: Anti-spoofing`
- `MANRS Action 3: Coordination`
- `MANRS Action 4: Global Validation`

## Dependence

This crawler is not depending on other crawlers.
