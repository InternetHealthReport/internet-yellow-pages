# Acknowledgments

The Internet Yellow Pages could not exist without all the awesome prior research and
data sources. We list all of them here, if possible with their corresponding licenses,
to which you will need to conform if you use the public instance or create a dump that
includes these data sources.

Please refer to the READMEs in the respective crawler directories for more information.

## Alice-LG

We retrieve route server looking glass snapshots from the following IXPs.

|     Name     |            URL             |
|--------------|----------------------------|
| AMS-IX       | https://lg.ams-ix.net/     |
| BCIX         | https://lg.bcix.de/        |
| DD-IX        | https://lg.dd-ix.net       |
| DE-CIX       | https://lg.de-cix.net/     |
| IX.br        | https://lg.ix.br/          |
| LINX         | https://alice-rs.linx.net/ |
| Megaport     | https://lg.megaport.com/   |
| Netnod       | https://lg.netnod.se/      |
| IX Australia | https://lg.ix.asn.au       |
| NZIX         | https://lg.ix.nz           |
| PIX          | https://glass.gigapix.pt   |
| SFMIX        | https://alice.sfmix.org    |
| Stuttgart-IX | https://lg.s-ix.de         |
| TOP-IX       | https://lg.top-ix.org      |

## Amazon

We use the [IP address ranges](https://ip-ranges.amazonaws.com/ip-ranges.json) provided by
[Amazon Web Services](https://aws.amazon.com/).

## APNIC

We use [APNIC](https://labs.apnic.net/)'s [AS population
estimate](https://labs.apnic.net/index.php/2014/10/02/how-big-is-that-network/).

## BGPKIT

We use the as2rel, peer-stats, and pfx2as [datasets](https://data.bgpkit.com/) from
[BGPKIT](https://bgpkit.com/).

Use of this data is authorized under their [Acceptable Use
Agreement](https://bgpkit.com/aua).

## bgp.tools

We use [AS names, AS tags](https://bgp.tools/kb/api), and [anycast prefix
tags](https://github.com/bgptools/anycast-prefixes) provided by
[bgp.tools](https://bgp.tools/).

## CAIDA

We use three datasets from [CAIDA](https://www.caida.org/) which use is authorized
under their [Acceptable Use Agreement](https://www.caida.org/about/legal/aua/).

> AS Rank https://doi.org/10.21986/CAIDA.DATA.AS-RANK.

and

> Internet eXchange Points Dataset,
> https://doi.org/10.21986/CAIDA.DATA.IXPS

and

> AS Relationships (serial-1), https://catalog.caida.org/dataset/as_relationships_serial_1

and

> AS to organization mappings, https://catalog.caida.org/dataset/as_organizations/

## Cisco

We use the [Cisco Umbrella Popularity
List](https://s3-us-west-1.amazonaws.com/umbrella-static/index.html).

## Citizen Lab

We use URL testing lists from [The Citizen Lab](https://citizenlab.ca/).

> Citizen Lab and Others. 2014. URL Testing Lists Intended for Discovering Website
> Censorship. https://github.com/citizenlab/test-lists.

This data is licensed under [CC BY-NC-SA
4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). No changes were made to the data.

## Cloudflare

We use the `radar/dns/top/ases`, `radar/dns/top/locations`, `radar/ranking/top`, and
`radar/datasets` endpoints of the [Clouflare Radar](https://radar.cloudflare.com/) API.

This data is licensed under [CC BY-NC
4.0](https://creativecommons.org/licenses/by-nc/4.0/). No changes were made to the data.

## Emile Aben

We use [AS names](https://github.com/emileaben/asnames) provided by Emile Aben and
others with permission (Hi Emile!).

## Google

We use the top 1M websites per country from the Google [Chrome User Experience Report
(CrUX)](https://developer.chrome.com/docs/crux).

This data is licensed under  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
No changes were made to the data.

## Internet Health Report

We use three datasets from the [Internet Health Report](https://ihr.iijlab.net/) (that's
us!): Country Dependency, AS Hegemony, and Route Origin Validation.

This data is licensed under [CC BY-NC-SA
4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). No changes were made to the
data.

## Internet Intelligence Lab

We use the AS to organization mapping from the [Internet Intelligence Lab at Georgia
Tech](https://inetintel.notion.site/Internet-Intelligence-Research-Lab-d186184563d345bab51901129d812ed6).

> Z. Chen, Z. Bischof, C. Testart, A. Dainotti, "AS to Organization Mapping",
> Internet Intelligence Lab at Georgia Tech,
> https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping

Use of this data is authorized under their [Acceptable Use
Agreement](https://raw.githubusercontent.com/InetIntel/Dataset-AS-to-Organization-Mapping/master/LICENSE).

## IPinfo

We use the free [IP-to-country](https://ipinfo.io/products/free-ip-database) mapping
provided by [IPinfo](https://ipinfo.io) and released the under [Creative Commons
Attribution-ShareAlike 4.0 International
License](https://creativecommons.org/licenses/by-sa/4.0/).

## Number Resource Organization

We use the [extended allocation and assignment
reports](https://www.nro.net/about/rirs/statistics/) provided by the [Number Resource
Organization](https://www.nro.net/).

## Open Observatory of Network Interference

We use [Internet censorship measurements](https://explorer.ooni.org/) provided by the
[Open Observatory of Network Interference](https://ooni.org/).

This data is licensed under [CC BY-NC-SA
4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). The data is aggregated for
display in the graph.

## OpenINTEL

We use several datasets from [OpenINTEL](https://www.openintel.nl/), a joint project of
the University of Twente, SURF, SIDN Labs and NLnet Labs.

The `tranco1m`, `umbrella1m`, and `crux`
[datasets](https://openintel.nl/data/forward-dns/top-lists/) are licensed under [CC
BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). No changes were made
to the data. In addition, there are [Terms of Use](https://openintel.nl/download/terms/)
for this data.

The [DNS Dependency Graph tool](https://dnsgraph.dacs.utwente.nl/) is a joint project of
the University of Twente and IIJ Research Laboratory.

Other datasets are used with permission from OpenINTEL.

## Packet Clearing House

We use the [daily routing snapshots](https://www.pch.net/resources/Routing_Data/) from
[Packet Clearing House](https://www.pch.net/).

This data is licensed under [CC BY-NC-SA
3.0](https://creativecommons.org/licenses/by-nc-sa/3.0/). No changes were made to the
data.

## PeeringDB

We use the `fac`, `ix`, `ixlan`, `netfac`, and `org` endpoints of the
[PeeringDB](https://www.peeringdb.com/) API.

Use of this data is authorized under their [Acceptable Use
Policy](https://www.peeringdb.com/aup).

## RIPE NCC

We use AS names, Atlas measurement information, and RPKI data from the [RIPE
NCC](https://www.ripe.net/) and [RIPE Atlas](https://atlas.ripe.net/).

## SimulaMet

We use rDNS data from [RIR-data.org](https://rir-data.org/), a joint project of
SimulaMet and the University of Twente.

> Alfred Arouna, Ioana Livadariu, and Mattijs Jonker. "[Lowering the Barriers to Working
> with Public RIR-Level Data.](https://dl.acm.org/doi/10.1145/3606464.3606473)"
> Proceedings of the 2023 Workshop on Applied Networking Research (ANRW '23).

## Stanford

We use the [Stanford ASdb dataset](https://asdb.stanford.edu/) provided by the [Stanford
Empirical Security Research Group](https://esrg.stanford.edu/).

> [ASdb: A System for Classifying Owners of Autonomous
> Systems](https://zakird.com/papers/asdb.pdf).
> Maya Ziv, Liz Izhikevich, Kimberly Ruth, Katherine Izhikevich, and Zakir Durumeric.
> ACM Internet Measurement Conference (IMC), November 2021.

## Tranco

We use the [Tranco list](https://tranco-list.eu/) provided by the [DistriNet Research
Unit KU Leuven](https://distrinet.cs.kuleuven.be/), [TU Delft](https://www.tudelft.nl/),
and [LIG](https://www.liglab.fr/).

The Tranco list combines lists from five providers:

1. [Cisco
Umbrella](https://umbrella-static.s3-us-west-1.amazonaws.com/index.html)
1. [Majestic](https://majestic.com/reports/majestic-million) (available under a [CC BY
   3.0](https://creativecommons.org/licenses/by/3.0/) license)
1. [Farsight](https://www.domaintools.com/resources/blog/mirror-mirror-on-the-wall-whos-the-fairest-website-of-them-all)
1. [Chrome User Experience Report (CrUX)](https://developer.chrome.com/docs/crux/)
   ([available](https://research.google/resources/datasets/chrome-user-experience-report/)
   under a [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) license)
1. [Cloudflare Radar](https://radar.cloudflare.com/domains)
   ([available](https://radar.cloudflare.com/about) under a [CC BY-NC
   4.0](https://creativecommons.org/licenses/by-nc/4.0/) license).

## University of Twente

We use the [LACeS Anycast Census](https://github.com/ut-dacs/anycast-census) dataset
provided by the Design and Analysis of Communication Systems group at the University of
Twente.

> [LACeS: an Open, Fast, Responsible and Efficient Longitudinal Anycast Census
> System](https://arxiv.org/pdf/2503.20554). Remi Hendriks, Matthew Luckie, Mattijs
> Jonker, Raffaele Sommese, and Roland van Rijswijk-Deij.
> ACM Internet Measurement Conference (IMC), November 2025.

## Virginia Tech

We use the [RoVista](https://rovista.netsecurelab.org/) dataset provided by the
NetSecLab group at Virginia Tech.

> RoVista: Measuring and Understanding the Route Origin Validation (ROV) in RPKI.
> Weitong Li, Zhexiao Lin, Md. Ishtiaq Ashiq, Emile Aben, Romain Fontugne,
> Amreesh Phokeer, and Taejoong Chung.
> ACM Internet Measurement Conference (IMC), October 2023.

## World Bank
We use the country population indicator `SP.POP.TOTL.` from the
[Indicators API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation)
dataset provided by the
[World Bank](https://www.worldbank.org/en/home).
