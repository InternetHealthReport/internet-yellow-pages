# OONI -- https://ooni.org/

The [Open Observatory of Network Interference](https://ooni.org/) (OONI) is a non-profit
free software project that aims to empower decentralized efforts in documenting internet
censorship around the world.  OONI runs a number of tests from devices provided by
volunteers, and we import a subset of these into IYP.

Since most of these crawlers create the same graph representation, we first briefly
describe the function of all tests and link to their detailed test specification. Then
we give one combined description of the graph representation at the end.

## Crawlers

### Facebook Messenger (facebookmessenger.py)

Specification:
[ts-019-facebook-messenger.md](https://github.com/ooni/spec/blob/master/nettests/ts-019-facebook-messenger.md)

This test verifies if a set of Facebook Messenger endpoints resolve to consistent IPs
and if it is possible to establish a TCP connection to them on port 443.

### Header Field Manipulation Test (httpheaderfieldmanipulation.py)

Specification:
[ts-006-header-field-manipulation.md](https://github.com/ooni/spec/blob/master/nettests/ts-006-header-field-manipulation.md)

This test performs HTTP requests with request headers that vary capitalization towards a
backend. If the headers reported by the server differ from the ones that were sent, then
tampering is detected.

### Signal (osignal.py)

Specification:
[ts-029-signal.md](https://github.com/ooni/spec/blob/master/nettests/ts-029-signal.md)

This test checks if it is possible to establish a TLS connection with the Signal server
backend and perform an HTTP GET request.

### Psiphon (psiphon.py)

Specification:
[ts-015-psiphon.md](https://github.com/ooni/spec/blob/master/nettests/ts-015-psiphon.md)

This test creates a Psiphon tunnel and then uses it to fetch the
https://www.google.com/humans.txt webpage.

### RiseupVPN (riseupvpn.py)

Specification:
[ts-026-riseupvpn.md](https://github.com/ooni/spec/blob/master/nettests/ts-026-riseupvpn.md)

This test checks if a LEAP-platform-based VPN service like RiseupVPN is working as
expected. It first performs a HTTP GET request to the RiseupVPN API service, followed by
a TCP connection to the VPN gateways.

### STUN reachability (stunreachability.py)

Specification:
[ts-025-stun-reachability.md](https://github.com/ooni/spec/blob/master/nettests/ts-025-stun-reachability.md)

For each STUN input URL, this test sends a binding request to the given URL's endpoint
and receives the corresponding response. If a valid response is received, then the test
is successful, otherwise it failed.

### Telegram (telegram.py)

Specification:
[ts-020-telegram.md](https://github.com/ooni/spec/blob/master/nettests/ts-020-telegram.md)

This test checks if two services are working as they should:

1. The Telegram access points (the addresses used by the Telegram desktop client)
1. The Telegram web version

### Tor (tor.py)

Specification:
[ts-023-tor.md](https://github.com/ooni/spec/blob/master/nettests/ts-023-tor.md)

This test loops through the list of  measurement targets. The measurement action depends
on the target type:

- for dir_port targets, the test will GET the /tor/status-vote/current/consensus.z
  resource using the HTTP protocol;
- for or_port and or_port_dirauth targets, the test will connect to the address and
  perform a TLS handshake;
- for obfs4 targets, the test will connect to the address and perform an OBFS4
  handshake;
- otherwise, the test will TCP connect to the address.

### Tor using snowflake (torsf.py)

Specification:
[ts-030-torsf.md](https://github.com/ooni/spec/blob/master/nettests/ts-030-torsf.md)

This test detects detect if tor bootstraps using the Snowflake pluggable transport
(PT) within a reasonable timeout.

### Vanilla Tor (vanillator.py)

Specification:
[ts-016-vanilla-tor.md](https://github.com/ooni/spec/blob/master/nettests/ts-016-vanilla-tor.md)

This test runs the Tor executable and collect logs. The bootstrap will either succeed
or eventually time out.

### Web Connectivity (webconnectivity.py)

Specification:
[ts-017-web-connectivity.md](https://github.com/ooni/spec/blob/master/nettests/ts-017-web-connectivity.md)

This test checks if a website is censored using a sequence of steps. For more details,
please check the specification.

### WhatsApp (whatsapp.py)

Specification:
[ts-018-whatsapp.md](https://github.com/ooni/spec/blob/master/nettests/ts-018-whatsapp.md)

This test checks if three services are working as they should:

1. The WhatsApp endpoints used by the WhatsApp mobile app;
1. The registration service, i.e. the service used to register a new account;
1. The WhatsApp web interface.

## Graph Representation

All crawlers create `CENSORED` relationships from `AS` nodes to either a `Tag`, `URL` or
`IP` node, indicating that there exists a censorship test result from at least one probe
in this AS.

We aggregate test results on an AS-country basis, i.e., if an AS contains probes from
multiple countries, we create one `CENSORED` relationship per country. This results in
multiple `CENSORED` relationships between the same AS and target, which can be
distinguished by using the `country_code` property of the relationship.

The result categories differ per test and are described in more detail below. However
all relationships contain the following two properties:

- `total_count`: The total number of aggregated test results. Note that this is
  different from the `count_total` field present for some crawlers.
- `country_code`: The country code of the country for which results were aggregated

For each result category we create two properties:

- `count_*`: The number of results in this category
- `percentage_*`: The relative size in percent of this category

For many tests the result is derived from a combination of fields. In order to aggregate
the results we group them into categories and chose a name that should be recognizable
when looking at the OONI documentation as well.

### `(:AS)-[:CENSORED]->(:Tag)` Crawlers

As mentioned above most crawlers create `(:AS)-[:CENSORED]->(:Tag)` relationships. The
`Tag` node represents a specific OONI test (e.g.,
[WhatsApp](https://github.com/ooni/spec/blob/master/nettests/ts-018-whatsapp.md)) and
the `CENSORED` relationship represents aggregated results. For brevity we only discuss
the result categories for each crawler here.

If a result category is binary, it has a counterpart prefixed with `no_*` indicating a
negative result.

#### facebookmessenger.py

- `unblocked`: No blocking
- `dns_blocking`: Endpoints are DNS blocked
- `tcp_blocking`: Endpoints are TCP blocked
- `both_blocked`: Endpoints are blocked via both DNS & TCP

#### httpheaderfieldmanipulation.py

This test performs multiple measurements at once, which is why we introduce a meta
category.

- `[no_]total`: Meta category indicating that any of the following results was positive
- `[no_]request_line_capitalization`: Request line was manipulated
- `[no_]header_name_capitalization`: Header field names were manipulated
- `[no_]header_field_value`: Header field values were manipulated
- `[no_]header_field_number`: Number of headers was manipulated

#### httpinvalidrequestline.py

- `[no_]tampering`: Tampering detected

#### osignal.py

- `ok`: Connection succeeded
- `blocked`: Connection failed

#### psiphon.py

- `bootstrapping_error`: Error in bootstrapping Psiphon
- `usage_error`: Error in using Psiphon
- `working`: Bootstrap worked
- `invalid`: Invalid (should not happen)

#### riseupvpn.py

- `ok`: VPN API is functional and reachable
- `failure`: Connection to VPN API failed

#### telegram.py

- `total_[ok|blocked]`: Meta category indicating that any of the following results was
  blocked (`total_blocked`) or all are ok (`total_ok`)
- `web_[ok|blocked|none]`: Telegram web version is blocked. `web_none` should not really
  happen but is kept for completeness.
- `http_[ok|blocked]`: Telegram access point blocked at HTTP level
- `tcp_[ok|blocked]`: Telegram access point blocked at TCP level

#### torsf.py

- `ok`: Bootstrap succeeded
- `failure`: Bootstrap failed

#### vanillator.py

- `ok`: Bootstrap succeeded
- `failure`: Bootstrap failed

#### whatsapp.py

- `total_[ok|blocked]`: Meta category indicating that any of the following results was
  blocked (`total_blocked`) or all are ok (`total_ok`)
- `endpoint_[ok|blocked]`: Failed to connect to any endpoint
- `registration_server_[ok|blocked]`: Cannot connect to registration service
- `web_[ok|blocked]`: WhatsApp web is blocked

### stunreachability.py

This crawler connects `AS` with `URL` nodes and also adds hostnames and the IPs they
resolve to for the URL if available. The URL will be connected to the hostname by the
[`url2hostname`](../../post/url2hostname.py) postprocessing script.

```Cypher
(:AS {asn: 2497})-[:CENSORED {country_code: 'JP'}]->(:URL {url: 'stun://stun.l.google.com:19302'})
(:HostName {name: 'stun.l.google.com'})-[:RESOLVES_TO]->(:IP {ip: '198.18.5.122'})
```

Result categories:

- `ok`: STUN is working
- `failure`: STUN is not working

### tor.py

This crawler connects `AS` with `IP` nodes and tags IPs as Tor directories or bridges.

```Cypher
(:AS {asn: 2497})-[:CENSORED {country_code: 'JP'}]->(:IP {ip: '192.95.36.142'})-[:CATEGORIZED]->(:Tag {label: 'OONI Probe Tor Tag obfs4'})
```

Result categories:

- `ok`: Target reachable
- `failure`: Target not reachable

Tag names:

- `OONI Probe Tor Tag dir_port`
- `OONI Probe Tor Tag obfs4`
- `OONI Probe Tor Tag or_port`
- `OONI Probe Tor Tag or_port_dirauth`

### webconnectivity.py

This crawler connects `AS` with `URL` nodes and also adds hostnames and the IPs they
resolve to for the URL if available. The URL will be connected to the hostname by the
[`url2hostname`](../../post/url2hostname.py) postprocessing script.

Since this test sometimes targets URLs which contain an IP instead of a normal hostname,
it also adds a `PART_OF` relationship between `IP` and `URL` nodes in rare cases.

```Cypher
(:AS {asn: 2497})-[:CENSORED {country_code: 'JP'}]->(:URL {url: 'https://www.reddit.com/'})
(:HostName {name: 'www.reddit.com'})-[:RESOLVES_TO]->(:IP {ip: '199.232.73.140'})
(:IP {ip: '180.215.14.121'})-[:PART_OF]->(:URL {url: 'http://180.215.14.121/'})
```

Result categories

- `ok`: Website reachable
- `confirmed`: Confirmed censorship by some form of blocking
- `failure`: Failed to reach website, but could be caused by normal connectivity issues
- `anomaly`: Default if no other case matches

The webconnectivity crawler is also responsible for adding AS-to-country mapping and
`Resolver` nodes to the graph. Since this information is based on probes it does make
sense to add it from multiple crawlers. In addition, the webconnectivity test is
excecuted the most.

```Cypher
(:AS {asn: 2497})-[:COUNTRY {reference_name: 'ooni.webconnectivity'}]->(:Country)
(:IP & Resolver {ip: '210.138.77.93'})
```

## Implemented Tests

| Test Name                                | Implementation Tracker       | GitHub URL                                                                                                    |
|------------------------------------------|------------------------------|---------------------------------------------------------------------------------------------------------------|
| Dash (Video Performance Test)            | X - Won’t Fix                | [Dash Test](https://github.com/ooni/spec/blob/master/nettests/ts-016-dash.md)                                  |
| DNS Check (new DNS Test)                 | ? - All Results Fail,        | [DNS Check](https://github.com/ooni/spec/blob/master/nettests/ts-020-dns-check.md)                             |
|                                          | Results Questionable         |                                                                                                               |
| Facebook Messenger                       | O - Done                     | [Facebook Messenger](https://github.com/ooni/spec/blob/master/nettests/ts-023-facebook-messenger.md)           |
| HTTP Header Field Manipulation           | O - Done                     | [HTTP Header Field Manipulation](https://github.com/ooni/spec/blob/master/nettests/ts-012-http-header-field.md)|
| HTTP Invalid Requestline                 | O - Done                     | [HTTP Invalid Requestline](https://github.com/ooni/spec/blob/master/nettests/ts-011-http-invalid-requestline.md)|
| NDT (Speed Test)                         | X - Won’t Fix                | [NDT Test](https://github.com/ooni/spec/blob/master/nettests/ts-022-ndt.md)                                    |
| Psiphon (Censorship Circumvention VPN)   | O - Done                     | [Psiphon Test](https://github.com/ooni/spec/blob/master/nettests/ts-007-psiphon.md)                            |
| RiseUp VPN                               | O - Done                     | [RiseUp VPN](https://github.com/ooni/spec/blob/master/nettests/ts-019-riseup-vpn.md)                           |
| Run                                      | ???                          | [OONI Run](https://github.com/ooni/run)                                                                        |
| Signal                                   | O - Done                     | [Signal Test](https://github.com/ooni/spec/blob/master/nettests/ts-018-signal.md)                              |
| STUN Reachability                        | O - Done                     | [STUN Reachability](https://github.com/ooni/spec/blob/master/nettests/ts-021-stun-reachability.md)             |
| Telegram                                 | O - Done                     | [Telegram Test](https://github.com/ooni/spec/blob/master/nettests/ts-009-telegram.md)                          |
| TOR                                      | O - Done                     | [TOR Test](https://github.com/ooni/spec/blob/master/nettests/ts-001-tor.md)                                    |
| TORSF                                    | O - Done                     | [TORSF Test](https://github.com/ooni/spec/blob/master/nettests/ts-014-torsf.md)                                |
| Vanilla TOR                              | O - Done                     | [Vanilla TOR](https://github.com/ooni/spec/blob/master/nettests/ts-002-vanilla-tor.md)                         |
| Webconnectivity                          | O - Done                     | [Webconnectivity](https://github.com/ooni/spec/blob/master/nettests/ts-017-web-connectivity.md)                |
| Whatsapp                                 | O - Done                     | [WhatsApp Test](https://github.com/ooni/spec/blob/master/nettests/ts-010-whatsapp.md)                          |
