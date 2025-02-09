
# Node types available in IYP

| Node types              | Description                                                                                                                       |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| AS                      | Autonomous System, uniquely identified with the **asn** property.                                                                 |
| AtlasMeasurement        | RIPE Atlas Measurement, uniquely identified with the **id** property.                                                             |
| AtlasProbe              | RIPE Atlas probe, uniquely identified with the **id** property.                                                                   |
| AuthoritativeNameServer | Authoritative DNS nameserver for a set of domain names, uniquely identified with the **name** property.                           |
| BGPCollector            | A RIPE RIS or RouteViews BGP collector, uniquely identified with the **name** property.                                           |
| BGPPrefix               | An IP prefix announced in BGP, this is a subtype of Prefix.                                                                       |
| CaidaIXID               | Unique identifier for IXPs from CAIDA's IXP dataset.                                                                              |
| Country                 | Represent an economy, uniquely identified by either its two or three character code (properties **country_code** and **alpha3**). |
| DomainName              | Any DNS domain name that is not a FQDN (see HostName), uniquely identified by the **name** property.                              |
| Estimate                | Represent a report that approximate a quantity, for example the World Bank population estimate.                                   |
| Facility                | Co-location facility for IXPs and ASes, uniquely identified by the **name** property.                                             |
| HostName                | A fully qualified domain name uniquely identified by the **name** property.                                                       |
| IP                      | An IPv4 or IPv6 address uniquely identified by the **ip** property. The **af** property (address family) provides the IP version of the prefix.|
| IXP                     | An Internet Exchange Point, loosely identified by the **name** property or using related IDs (see the EXTERNAL_ID relationship).  |
| Name                    | Represent a name that could be associated to a network resource (e.g. an AS), uniquely identified by the **name** property.       |
| OpaqueID                | Represent the opaque-id value found in RIR's delegated files. Resources related to the same opaque-id are registered to the same resource holder. Uniquely identified by the **id** property.|
| Organization            | Represent an organization and is loosely identified by the **name** property or using related IDs (see the EXTERNAL_ID relationship).|
| PeeringdbFacID          | Unique identifier for a Facility as assigned by PeeringDB.                                                                        |
| PeeringdbIXID           | Unique identifier for an IXP as assigned by PeeringDB.                                                                            |
| PeeringdbNetID          | Unique identifier for an AS as assigned by PeeringDB.                                                                             |
| PeeringdbOrgID          | Unique identifier for an Organization as assigned by PeeringDB.                                                                   |
| PeeringLAN              | An IP prefix used by an IXP for its peering LAN, this is a subtype of Prefix.                                                     |
| Prefix                  | An IPv4 or IPv6 prefix uniquely identified by the **prefix** property. The **af** property (address family) provides the IP version of the prefix.|
| Ranking                 | Represent a specific ranking of Internet resources (e.g. CAIDA's ASRank or Tranco ranking). The rank value for each resource is given by the RANK relationship. |
| Resolver                | An additional label added to IP nodes if they are a DNS resolver.                                                                                               |
| RIRPrefix               | An IP prefix assigned by of the five RIRs' (delegated files), this is a subtype of Prefix.                                                                      |
| RPKIPrefix              | An IP prefix registered in RPKI, this is a subtype of Prefix.                                                                                                   |
| Tag                     | The output of a classification. A tag can be the result of a manual or automated classification. Uniquely identified by the **label** property.|
| URL                     | The full URL for an Internet resource, uniquely identified by the **url** property.                                               |

