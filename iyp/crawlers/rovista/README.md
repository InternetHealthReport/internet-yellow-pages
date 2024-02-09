# RoVista -- https://rovista.netsecurelab.org/

> RoVista aims to determine the Routing Origin Validation (ROV) status of network
> operators.
>
> RoV Scores are determined based on the number of RPKI-invalid prefixes reachable by an
> Autonomous System (AS). Consequently, a higher ROV score suggests that the AS can
> effectively filter more RPKI-invalid prefixes. However, it is important to note that
> the RoV score does not conclusively indicate whether an AS has actually implemented
> ROV or not, partly due to limitations in [the] framework and other contributing
> factors.

IYP converts these scores (or ratios) to two Tags:

- ASes with a ratio greater than 0.5 are categorized as `Validating RPKI ROV`
- ASes with a ratio of less or equal 0.5 are categorized as `Not Validating RPKI ROV`

## Graph representation

```cypher
(:AS {asn: 2497})-[:CATEGORIZED {ratio: 1.0}]->(:Tag {label: 'Validating RPKI ROV'})
(:AS {asn: 6762})-[:CATEGORIZED {ratio: 0}]->(:Tag {label: 'Not Validating RPKI ROV'})
```

## Dependence

This crawler is not depending on other crawlers.
