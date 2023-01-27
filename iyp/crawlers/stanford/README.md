# Stanford's ASdb -- https://asdb.stanford.edu/

ASdb is a research dataset that maps public autonomous systems (identified by 
ASN) to organizations and up to three industry types using data from business 
intelligence databases, website classifiers, and a machine learning algorithm. 

## Graph representation

### AS tags
Connect AS to tag nodes meaning that an AS has been categorized according to the
given tag.
```
(:AS {asn:32})-[:CATEGORIZED]-(:Tag {label: 'Colleges, Universities, and Professional Schools'})
```

## Dependence

This crawler is not depending on other crawlers.
