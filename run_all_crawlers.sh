echo "Starting all crawlers"
date

# MANRS
python3 -m iyp.crawlers.manrs.members                                                                                                                                                                                                                                                                                                            

# AS Names
python3 -m iyp.crawlers.ripe.as_names                                                                                                                                                                                                                                                                                                            
python3 -m iyp.crawlers.bgptools.as_names
python3 -m iyp.crawlers.emileaben.as_names

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

#BGP.Tools tags, and anycast prefixes
python3 -m iyp.crawlers.bgptools.tags
python3 -m iyp.crawlers.bgptools.anycast_prefixes

#PeeringDB
python3 -m iyp.crawlers.peeringdb.org
python3 -m iyp.crawlers.peeringdb.ix

# Delegated files
python3 -m iyp.crawlers.nro.delegated_stats

# URL data
python3 -m iyp.crawlers.citizenlab.urldb

# OONI
python3 -m iyp.crawlers.ooni.webconnectivity
python3 -m iyp.crawlers.ooni.facebookmessenger
python3 -m iyp.crawlers.ooni.signal
python3 -m iyp.crawlers.ooni.telegram
python3 -m iyp.crawlers.ooni.whatsapp
python3 -m iyp.crawlers.ooni.httpheaderfieldmanipulation
python3 -m iyp.crawlers.ooni.httpinvalidrequestline
python3 -m iyp.crawlers.ooni.psiphon
python3 -m iyp.crawlers.ooni.riseupvpn
python3 -m iyp.crawlers.ooni.stunreachability
python3 -m iyp.crawlers.ooni.tor
python3 -m iyp.crawlers.ooni.torsf
python3 -m iyp.crawlers.ooni.vanillator

echo "All crawlers finished"
date
