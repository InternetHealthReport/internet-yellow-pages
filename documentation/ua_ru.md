# Examples  for

## Find all nodes that have the word 'crimea' in their name
match (x)--(n:NAME) WHERE toLower(n.name) contains 'crimea' RETURN  x, n;

## Crimean neighbors network dependencies
match (crimea:NAME)--(:AS)-[:PEERS_WITH]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:NAME) where toLower(crimea.name) contains 'crimea' and toFloat(r.hegemony)>0.2 return transit_as, collect(distinct transit_name.name), count(distinct r) as nb_links order by nb_links desc;

## Crimean IXP members dependencies
match (crimea:NAME)--(:IXP)-[:MEMBER_OF]-(neighbors:AS)-[r:DEPENDS_ON]-(transit_as:AS)--(transit_name:NAME) where toLower(crimea.name) contains 'crimea' and toFloat(r.hegemony)>0.2 return transit_as, collect(distinct transit_name.name), count(distinct r) as nb_links order by nb_links desc;

## who is at Crimea ixp but not any other ixp


# Rostelecom

## all ases assigned to same org
## which IXPs are they operating to?

## prefixes assigned to this org and ASes announcing it
