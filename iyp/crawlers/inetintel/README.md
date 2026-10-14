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

# Internet Intelligence Lab - Dataset: AS2Biz -- https://github.com/InetIntel/Dataset-AS2Biz

AS2Biz classifies ASes by the business sectors of their operating organizations. The
classification is primarily derived from the organization's official website (scraped
and classified by an LLM) and augmented by inheriting labels from sibling ASes.
ASes that cannot be classified this way fall back to Wikipedia-based and AI web
search-based classification. The taxonomy is a multi-label taxonomy derived from
[ASdb's NAICSlite](https://asdb.stanford.edu/) with a finer-grained *Agriculture,
Mining, and Refineries* sector.

The dataset is published as semiannual snapshots in the `data/` directory of the
repository (`data/YYYY-MM/as2biz.YYYY-MM.json`). The crawler always imports the latest
snapshot and sets `reference_time_modification` to the snapshot month.

Each label is of the form `Sector - Subcategory`. Following the `stanford.asdb` crawler,
the label is split into two layers so that the Tag nodes are shared with ASdb: the
sector becomes a layer 1 Tag and the subcategory a layer 2 Tag. The generic `Other`
subcategory (e.g., `Service - Other`) does not create a Tag; only the sector is stored
in this case.

## Graph representation

### AS tags

Connect AS to Tag nodes meaning that the AS has been categorized according to the given
tag. The `layer` property indicates the taxonomy level (1 = sector, 2 = subcategory)
and the `source` property contains the provenance of the label as given in the dataset
(`Direct - Website`, `Inherit from AS...`, `Direct - Wikipedia`,
`Direct - Fallback Web Search AI`, `Reuse from YYYY-MM`).

```Cypher
(:AS {asn: 13079})-[:CATEGORIZED {layer: 1, source: 'Direct - Website'}]->(:Tag {label: 'Computer and Information Technology'})
(:AS {asn: 13079})-[:CATEGORIZED {layer: 2, source: 'Direct - Website'}]->(:Tag {label: 'Satellite Communication'})
```

An AS with several subcategories from the same sector has a single layer 1 relationship
to the sector per distinct `source` value, i.e., there can be multiple layer 1
relationships between the same AS and Tag if the subcategories have different
sources. The `source` property is only set by this crawler; filtering on it excludes
other datasets.

### Tag hierarchy

Connect subcategory Tags to their sector Tag.

```Cypher
(:Tag {label: 'Satellite Communication'})-[:PART_OF]->(:Tag {label: 'Computer and Information Technology'})
```

## Dependence

This crawler is not depending on other crawlers.
