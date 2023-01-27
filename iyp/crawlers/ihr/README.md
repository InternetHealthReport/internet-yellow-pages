# Internet Health Report -- https://ihr.iijlab.net/

Data inferred by IIJ's Internet Health Report, including:
- AS inter-dependency
- country's AS dependency
- prefixes' RPKI and IRR status


## Graph representation

### AS inter-dependency
Connect ASes that are depending on each other, meaning that an AS is commonly 
found on routes towards an origin AS. The strength of the dependence is given
by the 'hege' link attribute (AS Hegemony value) which range from 0 to 1.
Strongest dependencies being equal to 1.

```
(:AS {asn:2907})-[:DEPENDS_ON hege:0.82]-(:AS {asn:2497})
```

### Country's AS dependency
Connect ASes to ranking nodes which are also connected to a country.
A Country AS dependency is computed in two different ways, emphasizing 
either the distribution of the country's population (a.k.a. Total eyeball) or 
the country ASes (a.k.a. Total AS), for example:
```
(:AS  {asn:2497})-[:RANK {rank:1, hege:0.19}]-(:Ranking {name:'IHR country ranking: Total AS (JP)'})--(:Country {country_code:'JP'})
```

means that Japan ASes depends strongly (AS Hegemony equals 0.19) on AS2497.

### Prefixes' RPKI and IRR status
Connect prefixes to their origin AS, their AS dependencies, their RPKI/IRR 
status, and their country (provided by Maxmind).

```
(:Prefix {prefix:'8.8.8.0/24'})-[:ORIGINATE]-(:AS {asn:15169})
(:Prefix {prefix:'8.8.8.0/24'})-[:DEPENDS_ON]-(:AS {asn:15169})
(:Prefix {prefix:'8.8.8.0/24'})-[:CATEGORIZED]-(:Tag {label: 'RPKI Valid'})
(:Prefix {prefix:'8.8.8.0/24'})-[:COUNTRY]-(:Country {country_code:'US'})
```

## Dependence

This crawler assumes ASes and prefixes are already registered in the database,
it become very slow if it is not the case. Running bgpkit.pfx2asn before make
it much faster. 
