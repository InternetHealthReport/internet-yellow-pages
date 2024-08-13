# IYP OONI Implementation Tracker
This Crawler pulls the censorship data provided by the [Open
Observatory of Network Interference (OONI)](https://ooni.org/) into
the IYP. OONI runs a number of tests on devices provided by
volunteers, each test has their own crawler and they are specified
below.

As for the implementation:

The OoniCrawler baseclass, which extends the BaseCrawler, is defined
in the init.py. Each crawler then extends the base class with their
unique attributes. Common among all crawlers are the attributes reference, repo,
dataset, all_asns, all_countries, all_results, all_percentages, 
all_dns_resolvers and unique_links.

- reference and repo are set to OONI to identify the crawler.
- dataset needs to be set to the dataset that specific crawler is pulling, e.g. whatsapp.
- all_asns tracks all asns in the dataset and is added to by the
  process_one_line() function
- all_countries tracks all countries in the dataset and is added to by the
process_one_line() function
- all_results contains all results the process_one_line() function
  produces, but as there are crawler-specific attributes, the
  process_one_line() function is extended in each crawler and also
  modifies this variable. To do that, we first run the base function
  and then acess the last result in the extended crawler class.
  Therefore, if we choose not to proceed with a given result in the
  process_one_line() class for any reason, e.g. invalid parameters,
  one has to be careful to pop() the last result in all_results or it
  will contain an invalid result.
- all_percentages is calculated by each crawler-specific
  calculate_percentages() function, which highly depend on the OONI
  test implementation. See each tests' github page for that
  implementation.
- all_dns_resolvers is handled in the base OoniCrawler class to track
  dns resolvers and add them to the IYP. No changes need be made in
  extended crawlers.
- unique_links is a dictionary of currently the following sets: 
            'COUNTRY': set(),
            'CENSORED': set(),
            'RESOLVES_TO': set(),
            'PART_OF': set(),
            'CATEGORIZED': set(),
  if you are adding a new link, make sure to add it to this
  dictionary. This is done to prevent link duplication stemming from
  the same crawler, e.g. if multiple result files add the same PART_OF
  relationship, the link would be duplicated if we do not track
  existing links. Whenever you create a link in the extended
  batch_add_to_iyp() class, make sure you add it to the corresponding
  unique_links set, and before you create a link, check the set for
  the existence of the link.

Functions:

- Each run starts by calling the download_and_extract() function of the grabber class.
  This function is shared amongst all OONI crawlers, and takes the
  repo, a directory and the dataset as the input. If implementing a
  new crawler, only set the dataset correctly to the same name OONI
  uses and you do not need to interact with this class.
- Then, each line in the downloaded and extracted results files is
  processed in process_one_line(). This needs to be done in both the
  base and the extended class, as there are test specific attributes
  the extended class needs to process. See above, all_results(), and
  comments in the init.py code for implementation specifics.
- calculate_percentages() calculates the link percentages based on
  test-specific attributes. This is entirely done in the extended
  crawler and needs to be implemented by you if you're adding a new
  crawler.
- Finally, batch_add_to_iyp() is called to add the results to the IYP.


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
