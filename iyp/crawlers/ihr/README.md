# Internet Health Report -- https://ihr.iijlab.net/

Data inferred by IIJ's Internet Health Report, including:

- AS inter-dependency
- country's AS dependency
- prefixes' RPKI and IRR status

## Graph representation

### AS inter-dependency - `local_hegemony.py`

Connect ASes that are depending on each other, meaning that an AS is commonly found on routes
towards an origin AS. The strength of the dependence is given by the `hege` link attribute (AS
Hegemony value) which range from 0 to 1. Strongest dependencies being equal to 1.

```Cypher
(:AS {asn: 2907})-[:DEPENDS_ON {hege: 0.82}]-(:AS {asn: 2497})
```

### Country's AS dependency - `country_dependency.py`

Connect ASes to ranking nodes which are also connected to a country.  A Country AS dependency is
computed in two different ways, emphasizing either the distribution of the country's population
(a.k.a. Total eyeball) or the country ASes (a.k.a. Total AS), for example:

```Cypher
(:AS  {asn: 2497})-[:RANK {rank: 1, hege: 0.19}]->
(:Ranking {name: 'IHR country ranking: Total AS (JP)'})-[:COUNTRY]->
(:Country {country_code: 'JP'})
```

means that Japan ASes depends strongly (AS Hegemony equals 0.19) on AS2497.

### Prefixes' RPKI and IRR status - `rov.py`

Connect prefixes to their origin AS, their AS dependencies, their RPKI/IRR status, and their country
(provided by Maxmind).

```Cypher
(:Prefix {prefix: '8.8.8.0/24'})<-[:ORIGINATE]-(:AS {asn: 15169})
(:Prefix {prefix: '8.8.8.0/24'})-[:DEPENDS_ON]->(:AS {asn: 15169})
(:Prefix {prefix: '8.8.8.0/24'})-[:CATEGORIZED]->(:Tag {label: 'RPKI Valid'})
(:Prefix {prefix: '8.8.8.0/24'})-[:COUNTRY]->(:Country {country_code: 'US'})
```

Tag labels (possibly) added by this crawler:

- `RPKI Valid`
- `RPKI Invalid`
- `RPKI Invalid,more-specific`
- `RPKI NotFound`
- `IRR Valid`
- `IRR Invalid`
- `IRR Invalid,more-specific`
- `IRR NotFound`

## Dependence

`rov.py` assumes ASes and prefixes are already registered in the database, it becomes very slow if
this is not the case. Running `bgpkit.pfx2asn` before makes it much faster.
