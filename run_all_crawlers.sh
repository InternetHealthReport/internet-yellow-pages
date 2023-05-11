echo "Starting all crawlers"
date

# MANRS
python3 -m iyp.crawlers.manrs.members                                                                                                                                                                                                                                                                                                            

# AS Names
python3 -m iyp.crawlers.ripe.as_names                                                                                                                                                                                                                                                                                                            
python3 -m iyp.crawlers.bgptools.as_names

# AS Peers
python3 -m iyp.crawlers.inetintel.siblings_asdb

# Rankings
python3 -m iyp.crawlers.apnic.eyeball
python3 -m iyp.crawlers.caida.asrank                                                                                                                                                                                                                                                                                                            
python3 -m iyp.crawlers.ihr.country_dependency                                                                                                                                                                                                                                                                                                            

# BGP data
python3 -m iyp.crawlers.bgpkit.pfx2asn
python3 -m iyp.crawlers.bgpkit.as2rel   
python3 -m iyp.crawlers.bgpkit.peerstats   
python3 -m iyp.crawlers.ripe.roa                                                                                                                                                                                                                                                                                                            

# IHR
python3 -m iyp.crawlers.ihr.local_hegemony
python3 -m iyp.crawlers.ihr.rov

# DNS
python3 -m iyp.crawlers.tranco.top1M
python3 -m iyp.crawlers.cloudflare.top100

#BGP.Tools tags
python3 -m iyp.crawlers.bgptools.tags

#PeeringDB
python3 -m iyp.crawlers.peeringdb.org
python3 -m iyp.crawlers.peeringdb.ix

# Delegated files
python3 -m iyp.crawlers.nro.delegated_stats

# URL data
python3 -m iyp.crawlers.citizenlab.urldb

echo "All crawlers finished"
date
