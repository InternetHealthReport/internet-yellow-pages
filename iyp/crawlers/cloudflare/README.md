# Cloudflare Radar -- https://radar.cloudflare.com/ 

Data provided by Cloudflare's radar API, including:
- Top 100 domain names.

## Graph representation

### Ranking
Connect domain name nodes to a single ranking node corresponding to Cloudflare
top 100 ranking. The rank is given as a link attribute.
For example:
```
(:DomainName  {name:'google.com'})-[:RANK {rank:1}]-(:Ranking {name:'Cloudflare top 100 domains'})
```

## Dependence

This crawler is not depending on other crawlers.

## Notes

This crawler requires an application key to access the radar's API.
