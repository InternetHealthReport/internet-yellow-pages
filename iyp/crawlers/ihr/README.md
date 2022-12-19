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
TODO
Connect ASes that are depending on each other, meaning that an AS is commonly 
found on routes towards an origin AS. The strength of the dependence is given
by the 'hege' link attribute (AS Hegemony value) which range from 0 to 1.
Strongest dependencies being equal to 1.

```
(:AS {asn:2907})-[:DEPENDS_ON hege:0.82]-(:AS {asn:2497})
```

## Dependence

This crawler is not depending on other crawlers.
