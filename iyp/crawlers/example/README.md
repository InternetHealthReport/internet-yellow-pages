# Example Crawler

This is an example of crawler where nodes/links are pushed one by one (no 
batching). It is not a working example but can be used as a template. For a 
simple working example see iyp/crawlers/manrs/member.py.

When pushing a large number of nodes/links to the database, it is recommended to
use IYP's batch methods. See iyp/crawlers/ripe/as_names.py for a simple example
using batching.


## Graph representation

Connect AS nodes to EXAMPLE_NODE_LABEL with EXAMPLE_RELATIONSHIP_LABEL 
relationship.

```
(:AS)-[:EXAMPLE_RELATIONSHIP_LABEL]-(:EXAMPLE_NODE_LABEL)
```

## Dependence

This crawler is not depending on other crawlers.
