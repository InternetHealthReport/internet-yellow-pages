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
match (a:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:COUNTRY)
where cc.country_code = 'CN' or cc.country_code = 'HK'
return cc.country_code, count(distinct a)
```
╒═════════════════╤═══════════════════╕
│"cc.country_code"│"count(distinct a)"│
╞═════════════════╪═══════════════════╡
│"CN"             │6508               │
├─────────────────┼───────────────────┤
│"HK"             │1198               │
└─────────────────┴───────────────────┘

Number chinese/HK ASNs that are active:
```
match (a:AS)-[:COUNTRY {reference_org:'NRO'}]-(cc:COUNTRY), (a)-[:ORIGINATE]-(:PREFIX)
where cc.country_code = 'CN' or cc.country_code = 'HK'
return cc, count(distinct a)
```
╒═════════════════════╤═══════════════════╕
│"cc"                 │"count(distinct a)"│
╞═════════════════════╪═══════════════════╡
│{"country_code":"CN"}│5071               │
├─────────────────────┼───────────────────┤
│{"country_code":"HK"}│683                │
└─────────────────────┴───────────────────┘

Facilities for CT (4134, 23764):
```
match (a:AS)--(fac:FACILITY) 
where a.asn in [4134, 23764]
return a, fac
```

Country codes for these facilities:
```
match (a:AS)--(fac:FACILITY)--(cc:COUNTRY) 
where a.asn in [4134, 23764]
return distinct cc.country_code
```

Country codes for all AS registered for the opaque ID:
```
match (a:AS)-[:ASSIGNED]-(oid:OPAQUE_ID)
where a.asn in [4134, 23764]
with oid
match (oid)--(other:AS)--(fac:FACILITY)--(cc:COUNTRY) 
return distinct cc.country_code, collect(distinct other.asn)
```
╒═════════════════╤═════════════════════════════╕
│"cc.country_code"│"collect(distinct other.asn)"│
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
match (c:COUNTRY)--(f:FACILITY)--(a:AS) 
where c.country_code = 'CN' or c.country_code = 'HK'
return f, count(distinct a) as nb_as order by nb_as desc
```

Facilities where Chinese ASes are (28):
```
MATCH (net_country:COUNTRY)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:LOCATED_IN]-(fac:FACILITY)--(fac_country:COUNTRY)
WHERE net_country.country_code = 'CN'
RETURN fac_country.country_code, count(distinct net) as nb_AS order by nb_AS desc
```

ASes present at the largest number of facilities:
```
MATCH (net_country:COUNTRY)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:LOCATED_IN]-(fac:FACILITY)--(fac_country:COUNTRY)
WHERE net_country.country_code = 'HK' or net_country.country_code = 'CN'
RETURN net.asn, net_country.country_code, count(distinct fac_country) as nb_fac order by nb_fac desc
```

## Prefix geolocation
Geolocation of prefixes announced by Chinese ASes (54):
```
MATCH (net_country:COUNTRY)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:ORIGINATE]-(pfx:PREFIX)-[:COUNTRY {reference_org:'Internet Health Report'}]-(pfx_country:COUNTRY)
WHERE net_country.country_code = 'CN'
RETURN pfx_country.country_code, count(distinct net) as nb_AS order by nb_AS desc
```

Geolocation of prefixes announced by HK ASes (81):
```
MATCH (net_country:COUNTRY)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:ORIGINATE]-(pfx:PREFIX)-[:COUNTRY {reference_org:'Internet Health Report'}]-(pfx_country:COUNTRY)
WHERE net_country.country_code = 'HK'
RETURN pfx_country.country_code, count(distinct net) as nb_AS order by nb_AS desc
```

ASes with the largest footprint:
```
MATCH (net_country:COUNTRY)-[:COUNTRY {reference_org:'NRO'}]-(net:AS)-[:ORIGINATE]-(pfx:PREFIX)--(pfx_country:COUNTRY)
WHERE net_country.country_code = 'HK' or net_country.country_code = 'CN'
RETURN net.asn, net_country.country_code, count(distinct pfx_country) as nb_pfx order by nb_pfx desc
```

## Tier-1 comparison


## Domain name distribution
Graph (top10k):
```
MATCH (:RANKING)-[r:RANK]-(dn:DOMAIN_NAME)--(ip:IP)--(pfx:PREFIX)-[:ORIGINATE]-(net:AS)
WHERE dn.name ends with '.cn' and r.rank<10000
RETURN dn, ip, pfx, net
```

Table (top1M):
```
MATCH (:RANKING)-[r:RANK]-(dn:DOMAIN_NAME)--(ip:IP)--(pfx:PREFIX)-[:ORIGINATE]-(net:AS)
WHERE dn.name ends with '.cn' and r.rank<100000
RETURN net.asn,  count(distinct dn) as nb_domain_name order by nb_domain_name desc
```
