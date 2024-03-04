# World Bank's country population -- https://www.worldbank.org/en/home

> The World Bank Group works in every major area of development. We provide a wide array of financial products and technical assistance, and we help countries share and apply innovative knowledge and solutions to the challenges they face.

> The World Bank is like a cooperative, made up of 189 member countries. These member countries, or shareholders, are represented by a Board of Governors, who are the ultimate policymakers at the World Bank. Generally, the governors are member countries' ministers of finance or ministers of development. They meet once a year at the Annual Meetings of the Boards of Governors of the World Bank Group and the International Monetary Fund.

## Graph representation

### Country Estimate
Connect `Country` to an `Estimate` node meaning that a country has an estimated population of `value`.
```
(:Country)-[:POPULATION {value: 123}]->(:Estimate {name: 'World Bank Population Estimate'})
```

## Dependence
This crawler depends on crawlers setting the country codes.