echo "Starting all crawlers"
date

# AS Names
python -m iyp.crawlers.manrs.members                                                                                                                                                                                                                                                                                                            

# MANRS
python -m iyp.crawlers.ripe.as_names                                                                                                                                                                                                                                                                                                            

# Rankings
python -m iyp.crawlers.apnic.eyeball
python -m iyp.crawlers.caida.asrank                                                                                                                                                                                                                                                                                                            
python -m iyp.crawlers.ihr.country_dependency                                                                                                                                                                                                                                                                                                            

# BGP data
python -m iyp.crawlers.bgpkit.pfx2asns
python -m iyp.crawlers.bgpkit.as2rel   
python -m iyp.crawlers.ripe.roa                                                                                                                                                                                                                                                                                                            

# DNS
python -m iyp.crawlers.tranco.top1M

echo "All crawlers finished"
date
