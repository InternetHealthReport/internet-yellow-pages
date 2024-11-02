# Example Crawler

This is an example of crawler where nodes/relationships are pushed in batches. It is not
a working example but can be used as a template. For a simple working example see
[here](../bgpkit/pfx2asn.py).

While there are methods to get/create individual nodes they should only be used in rare
cases, as batch creation is almost always faster.

The first paragraph of this readme should be a description of the dataset and give an
overview of the parts we push to IYP.

## Graph representation

Connect AS nodes to EXAMPLE_NODE_LABEL with EXAMPLE_RELATIONSHIP_LABEL relationship.

```cypher
(:AS)-[:EXAMPLE_RELATIONSHIP_LABEL]-(:EXAMPLE_NODE_LABEL)
```

## Dependence

This crawler is not depending on other crawlers.
