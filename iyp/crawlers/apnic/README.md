# APNIC population estimates -- https://labs.apnic.net/

Population per AS estimated using an advertisement on Youtube. This dataset 
provides:
- the percentage of the population per country/AS
- AS ranking in terms of population
- AS names


## Graph representation

### Population
Connect AS to country nodes with a 'population' relationship representing the
percentage of the country's population hosted by the AS.

```
(:AS {asn:2516})-[:POPULATION {percent:19.3}]-(:COUNTRY {country_code:'JP'})
```


### Country
Connect AS to country nodes, meaning that the AS serves people in that country.

```
(:AS)-[:COUNTRY]-(:COUNTRY)
```

### Ranking
Connect ASes to ranking nodes which are also connected to a country. Meaning 
that an AS is ranked for a certain country in terms of population.
For example:
```
(:AS  {asn:2516})-[:RANK {rank:1}]-(:RANKING)--(:COUNTRY {country_code:'JP'})
```

### AS name
Connect AS to names nodes, providing the name of ranked ASes. 
For example:
```
(:AS {asn:2497})-[:NAME]-(:NAME {name:'IIJ'})
```


## Dependence

This crawler is not depending on other crawlers.
