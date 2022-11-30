echo "Starting all crawlers"
date

# MANRS
python -m iyp.crawlers.manrs.members                                                                                                                                                                                                                                                                                                            

# AS Names
python -m iyp.crawlers.ripe.as_names                                                                                                                                                                                                                                                                                                            
python -m iyp.crawlers.bgptools.as_names

# Rankings
python -m iyp.crawlers.apnic.eyeball
python -m iyp.crawlers.caida.asrank                                                                                                                                                                                                                                                                                                            
python -m iyp.crawlers.ihr.country_dependency                                                                                                                                                                                                                                                                                                            

# BGP data
python -m iyp.crawlers.bgpkit.pfx2asns
python -m iyp.crawlers.bgpkit.as2rel   
python -m iyp.crawlers.bgpkit.peerstats   
python -m iyp.crawlers.ripe.roa                                                                                                                                                                                                                                                                                                            

# IHR
python -m iyp.crawlers.ihr.local_hegemony
python -m iyp.crawlers.ihr.rov

# DNS
python -m iyp.crawlers.tranco.top1M
python -m iyp.crawlers.cloudflare.top100

#BGP.Tools tags
python -m iyp.crawlers.bgptools.tags

#PeeringDB
python -m iyp.crawlers.peeringdb.org
python -m iyp.crawlers.peeringdb.ix

# Delegated files
python -m iyp.crawlers.ripe.delegated_stats

echo "All crawlers finished"
date
