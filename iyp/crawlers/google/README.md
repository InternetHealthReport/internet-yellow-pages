# Google CrUX

The Chrome User Experience Report (CrUX for short) is a dataset collected by
Google that reflects how real-world Chrome users experience popular 
destinations on the web.

CrUX data is collected from real browsers around the world, based on certain 
browser options which determine user eligibility. A set of dimensions and metrics 
are collected which allow site owners to determine how users experience their sites.

IYP fetches CrUX's [top 1M popular websites per country](https://github.com/InternetHealthReport/crux-top-lists-country).
Unlike others, CrUX rankings are buketed by rank magnitude order, not by
specific rank. For example, rank are 1000, 10k, 100k, or 1M.

In addition, CrUX ranks *origins* (e.g. https://www.google.com), not domain
or host names. In IYP we extract the hostname part of the origin and model this
dataset using the hostname.

## Graph representation

```cypher
(:HostName {name:'www.iij.ad.jp'})-[:RANK {rank: 50000, origin:'https://www.iij.ad.jp'}]-(r:Ranking {name:'CrUX top 1M (JP)'})-[:COUNTRY]-(:Country {country_code:'JP'})
```

The `RANK` relationship contains the property `origin` to recover the origin
given in the original dataset.

## Dependence

This crawler is not depending on other crawlers.
