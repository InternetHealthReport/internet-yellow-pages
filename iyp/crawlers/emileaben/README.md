# EmileAben's AS Names -- https://github.com/emileaben/asnames

Data collected by EmileAben's AS Names, including:

- AS numbers
- AS names

## Graph representation

### AS names

Connect AS to names nodes, providing the name of an AS.
For example:

```Cypher
(:AS {asn: 2497})-[:NAME]-(:Name {name: 'IIJ'})
```

## Dependence

This crawler is not depending on other crawlers.
