# PeeringDB crawlers -- https://www.peeringdb.com/

[PeeringDB](https://www.peeringdb.com/) is a freely available, user-maintained, database of networks.
The database contains information about Internet Exchange Points (IXPs) and data centers, including
peering LAN and membership information.

IYP retrieves information about organizations, interconnection facilities, IXPs, their peering LANs,
and members.

## Dependencies

Run crawlers in this order:

1. org
1. fac
1. ix

## `org.py`

Information about organizations that own networks, IXPs, and facilities.

### Graph representation

Nodes:

- `(:Country {country_code})`: Country code
- `(:Name {name})`: Name
- `(:Organization {name})`: Name
- `(:PeeringdbOrgID {id})`: ID
- `(:URL {url})`: Website

Relationships:

```Cypher
(:Organization)-[:COUNTRY]->(:Country)
(:Organization)-[:EXTERNAL_ID]->(:PeeringdbOrgID)
(:Organization)-[:NAME]->(:Name)
(:Organization)-[:WEBSITE]->(:URL)
```

The `EXTERNAL_ID` relationship contains the raw organization data from PeeringDB [as defined in the
API.](https://tutorial.peeringdb.com/apidocs/#tag/api/operation/list%20org)

## `fac.py`

Information about co-location facilities.

### Graph representation

Nodes:

- `(:Country {country_code})`: Country code
- `(:Facility {name})`: Name
- `(:Name {name})`: Name
- `(:PeeringdbFacID {id})`: ID
- `(:URL {url})`: Website

Relationships:

```Cypher
(:Facility)-[:COUNTRY]->(:Country)
(:Facility)-[:EXTERNAL_ID]->(:PeeringdbFacID)
(:Facility)-[:MANAGED_BY]->(:Organization)
(:Facility)-[:NAME]->(:Name)
(:Facility)-[:WEBSITE]->(:URL)
```

The `EXTERNAL_ID` relationship contains the raw facility data from PeeringDB [as defined in the
API.](https://tutorial.peeringdb.com/apidocs/#tag/api/operation/list%20fac)

## `ix.py`

Information about IXPs, peering LANs, and IXP member networks.

### Graph representation

Nodes:

- `(:AS {asn})`: ASN of IXP member
- `(:IXP {name})`: Name
- `(:Name {name})`: Names of IXPs and networks
- `(:PeeringdbIXID {id})`: ID of the IXP
- `(:PeeringdbNetID {id})`: ID of the network
- `(:Prefix {prefix})`: Prefix of IXP peering LAN
- `(:URL {url})`: Websites of IXPs and networks

Relationships:

```Cypher
(:IXP)-[:COUNTRY]->(:Country)
(:IXP)-[:EXTERNAL_ID]->(:PeeringdbIXID)
(:IXP)-[:LOCATED_IN]->(:Facility)
(:IXP)-[:MANAGED_BY]->(:Organization)
(:IXP)-[:NAME]->(:Name)
(:IXP)-[:WEBSITE]->(:URL)

(:AS)-[:EXTERNAL_ID]->(:PeeringdbNetID)
(:AS)-[:LOCATED_IN]->(:Facility)
(:AS)-[:MANAGED_BY]->(:Organization)
(:AS)-[:MEMBER_OF]->(:IXP)
(:AS)-[:NAME]->(:Name)
(:AS)-[:WEBSITE]->(:URL)

(:Prefix)-[:MANAGED_BY]->(:IXP)
```

Raw data attached to relationships:

- [`net`](https://tutorial.peeringdb.com/apidocs/#tag/api/operation/list%20net):
  - `(:AS)-[:EXTERNAL_ID]->(:PeeringdbNetID)`
  - `(:AS)-[:MANAGED_BY]->(:Organization)`
  - `(:AS)-[:MEMBER_OF]->(:IXP)`
  - `(:AS)-[:NAME]->(:Name)`
  - `(:AS)-[:WEBSITE]->(:URL)`
- [`netfac`](https://tutorial.peeringdb.com/apidocs/#tag/api/operation/list%20netfac):
  `(:AS)-[:LOCATED_IN]->(:Facility)`

## Example

Run the (long) query below to get an Example that contains all nodes and relationships created by
the crawlers.

```Cypher
MATCH (iij:AS {asn: 2497})-[r0:EXTERNAL_ID]->(n0:PeeringdbNetID)
MATCH (iij)-[r1:LOCATED_IN]->(n1:Facility {name: 'IIJ Ikebukuro DC'})
MATCH (iij)-[r2:MANAGED_BY]->(n2:Organization)
MATCH (iij)-[r3:MEMBER_OF]->(ix:IXP {name: 'DE-CIX Frankfurt'})
MATCH (iij)-[r4:NAME {reference_org: 'PeeringDB'}]->(n3:Name)
MATCH (iij)-[r5:WEBSITE]->(n4:URL)
MATCH (pfx:Prefix {af: 4})-[r6:MANAGED_BY]->(ix)
MATCH (ix)-[r7:COUNTRY]->(n5:Country)
MATCH (ix)-[r8:EXTERNAL_ID]->(n6:PeeringdbIXID)
MATCH (ix)-[r9:LOCATED_IN]->(n7:Facility {name: 'Global Switch Frankfurt'})
MATCH (ix)-[r10:MANAGED_BY]->(n8:Organization)
MATCH (ix)-[r11:NAME]->(n9:Name)
MATCH (ix)-[r12:WEBSITE]->(n10)
RETURN iij,ix,pfx,n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,r0,r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12
```

![Nodes and relationships created by the PeeringDB
crawler](/documentation/assets/gallery/peeringdbAll.svg)
