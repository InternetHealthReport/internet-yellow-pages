# A look into the Chinese Internet

ASNs:
- China telecom: 4134, 23764 (CTGNet replacing 4809 overseas)
- China telecom 5G network?: 131285

Notes: 
- some international ASes have HK country code (e.g. telstra)


Interesting links:
- China Telecom map: https://www.chinatelecomasiapacific.com/jp/wp-content/uploads/2021/03/2021-CTG-Infrastructure-Map-8M.pdf
- China Telecom peering policy: https://2021v.peeringasia.com/files/NewCTPeeringPolicy.pdf


## General stats:
Number ASNs registered in China/HK:
```
MATCH (a:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country)
WHERE cc.country_code = 'CN' OR cc.country_code = 'HK'
RETURN cc.country_code, count(DISTINCT a)
```
╒═════════════════╤═══════════════════╕
│"cc.country_code"│"count(DISTINCT a)"│
╞═════════════════╪═══════════════════╡
│"CN"             │6508               │
├─────────────────┼───────────────────┤
│"HK"             │1198               │
└─────────────────┴───────────────────┘

Number chinese/HK ASNs that are active:
```
MATCH (a:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:Country), (a)-[:ORIGINATE]-(:Prefix)
WHERE cc.country_code = 'CN' OR cc.country_code = 'HK'
RETURN cc, count(DISTINCT a)
```
╒═════════════════════╤═══════════════════╕
│"cc"                 │"count(DISTINCT a)"│
╞═════════════════════╪═══════════════════╡
│{"country_code":"CN"}│5071               │
├─────────────────────┼───────────────────┤
│{"country_code":"HK"}│683                │
└─────────────────────┴───────────────────┘

Facilities for CT (4134, 23764):
```
MATCH (a:AS)--(fac:Facility)
WHERE a.asn IN [4134, 23764]
RETURN a, fac
```

Country codes for these facilities:
```
MATCH (a:AS)--(fac:Facility)--(cc:Country)
WHERE a.asn IN [4134, 23764]
RETURN DISTINCT cc.country_code
```

Country codes for all AS registered for the opaque ID:
```
MATCH (a:AS)-[:ASSIGNED]-(oid:OpaqueID)
WHERE a.asn IN [4134, 23764]
WITH oid
MATCH (oid)--(other:AS)--(fac:Facility)--(cc:Country)
RETURN DISTINCT cc.country_code, collect(DISTINCT other.asn)
```
╒═════════════════╤═════════════════════════════╕
│"cc.country_code"│"collect(DISTINCT other.asn)"│
╞═════════════════╪═════════════════════════════╡
│"SG"             │[131285,23764]               │
├─────────────────┼─────────────────────────────┤
│"ID"             │[131285]                     │
├─────────────────┼─────────────────────────────┤
│"KE"             │[4809]                       │
├─────────────────┼─────────────────────────────┤
│"BR"             │[4809,4134]                  │
├─────────────────┼─────────────────────────────┤
│"AE"             │[4809]                       │
├─────────────────┼─────────────────────────────┤
│"HK"             │[4809,23764]                 │
├─────────────────┼─────────────────────────────┤
│"ZA"             │[4809,23764]                 │
├─────────────────┼─────────────────────────────┤
│"US"             │[4134]                       │
├─────────────────┼─────────────────────────────┤
│"DE"             │[4134,23764]                 │
├─────────────────┼─────────────────────────────┤
│"NL"             │[4134]                       │
├─────────────────┼─────────────────────────────┤
│"GB"             │[4134,23764]                 │
├─────────────────┼─────────────────────────────┤
│"JP"             │[23764]                      │
├─────────────────┼─────────────────────────────┤
│"FR"             │[23764]                      │
└─────────────────┴─────────────────────────────┘

## Co-location facilities 
Facilities in china/hong-kong:
```
MATCH (c:Country)--(f:Facility)--(a:AS)
WHERE c.country_code = 'CN' OR c.country_code = 'HK'
RETURN f, count(DISTINCT a) AS nb_as ORDER BY nb_as DESC
```

Facilities where Chinese ASes are (28):
```
MATCH (net_country:Country)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:LOCATED_IN]-(fac:Facility)--(fac_country:Country)
WHERE net_country.country_code = 'CN'
RETURN fac_country.country_code, count(DISTINCT net) AS nb_AS ORDER BY nb_AS DESC
```

ASes present at the largest number of facilities:
```
MATCH (net_country:Country)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:LOCATED_IN]-(fac:Facility)--(fac_country:Country)
WHERE net_country.country_code = 'HK' OR net_country.country_code = 'CN'
RETURN net.asn, net_country.country_code, count(DISTINCT fac_country) AS nb_fac ORDER BY nb_fac DESC
```

## Prefix geolocation
Geolocation of prefixes announced by Chinese ASes (54):
```
MATCH (net_country:Country)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:ORIGINATE]-(pfx:Prefix)-[:COUNTRY {reference_org:'Internet Health Report'}]-(pfx_country:Country)
WHERE net_country.country_code = 'CN'
RETURN pfx_country.country_code, count(DISTINCT net) AS nb_AS ORDER BY nb_AS DESC
```

Geolocation of prefixes announced by HK ASes (81):
```
MATCH (net_country:Country)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:ORIGINATE]-(pfx:Prefix)-[:COUNTRY {reference_org:'Internet Health Report'}]-(pfx_country:Country)
WHERE net_country.country_code = 'HK'
RETURN pfx_country.country_code, count(DISTINCT net) AS nb_AS ORDER BY nb_AS DESC
```

ASes with the largest footprint:
```
MATCH (net_country:Country)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:ORIGINATE]-(pfx:Prefix)--(pfx_country:Country)
WHERE net_country.country_code = 'HK' OR net_country.country_code = 'CN'
RETURN net.asn, net_country.country_code, count(DISTINCT pfx_country) AS nb_pfx ORDER BY nb_pfx DESC
```

## Tier-1 comparison


## Domain name distribution
Graph (top10k):
```
MATCH (:Ranking)-[r:RANK]-(dn:DomainName)--(ip:IP)--(pfx:Prefix)-[:ORIGINATE]-(net:AS)
WHERE dn.name ENDS WITH '.cn' AND r.rank<10000
RETURN dn, ip, pfx, net
```

Table (top1M):
```
MATCH (:Ranking)-[r:RANK]-(dn:DomainName)--(ip:IP)--(pfx:Prefix)-[:ORIGINATE]-(net:AS)
WHERE dn.name ENDS WITH '.cn' AND r.rank<100000
RETURN net.asn,  count(DISTINCT dn) AS nb_domain_name ORDER BY nb_domain_name DESC
```
