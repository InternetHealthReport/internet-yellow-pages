# Internet Intelligence Lab - Dataset: AS to Organization mapping -- https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping

The dataset contains historical and current versions of the AS to Organization 
mapping datasets. A mapping will be created between AS to its sibling ASes.

## Graph representation

### Sibling ASes
Connect ASes that are managed by the same organization.
```cypher
(a:AS {asn: 2497})-[:SIBLING_OF]->(b:AS)
```

### Sibling organizations
```cypher
(a:Organization {name: 'NTT Communications Corporation'})-[:SIBLING_OF]->(b:Organization {name: 'NTT Communications (N-BONE)'})
```

## Dependence

This crawler assumes PeeringDB organizations are already present.
