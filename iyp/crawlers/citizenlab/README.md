# Citizen Lab's Test lists -- https://github.com/citizenlab/test-lists/blob/master/lists/

Citizen lab's test lists are URL testing lists intended to help in testing URL censorship, divided by country codes. In addition to these local lists, the global list consists of a wide range of internationally relevant and popular websites, including sites with content that is perceived to be provocative or objectionable. The dataset tha maps URL to Category.

## Graph representation

### URL tags
Connect URL to tag nodes meaning that an URL has been categorized according to the
given tag.
```
(u:URL {url: "https://www.flipkart.com/"})-[:CATEGORIZED]->(t:Tag {label: 'COMM'})
```

## Dependence

This crawler is not depending on other crawlers.