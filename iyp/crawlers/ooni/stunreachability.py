import argparse
import logging
import os
import sys
import tempfile
import json
import tldextract
import ipaddress
from collections import defaultdict

from .utils import grabber

from iyp import BaseCrawler

ORG = "OONI"
URL = "s3://ooni-data-eu-fra/raw/"
NAME = "ooni.stunreachability"


class Crawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.repo = "ooni-data-eu-fra"
        self.reference["reference_url_info"] = "https://ooni.org/post/mining-ooni-data"
        self.unique_links = {"COUNTRY": set(), "CENSORED": set()}

    def run(self):
        """Fetch data and push to IYP."""

        self.all_asns = set()
        self.all_urls = set()
        self.all_countries = set()
        self.all_results = list()
        self.all_percentages = list()
        self.all_hostnames = set()
        self.all_dns_resolvers = set()
        self.all_ips = set()

        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Fetch data
        grabber.download_and_extract(self.repo, tmpdir, "stunreachability")
        logging.info("Successfully downloaded and extracted all files")
        # Now that we have downloaded the jsonl files for the test we want, we can extract the data we want
        testdir = os.path.join(tmpdir, "stunreachability")
        for file_name in os.listdir(testdir):
            file_path = os.path.join(testdir, file_name)
            if os.path.isfile(file_path) and file_path.endswith(".jsonl"):
                with open(file_path, "r") as file:
                    for i, line in enumerate(file):
                        data = json.loads(line)
                        self.process_one_line(data)
                        logging.info(f"\rProcessed {i+1} lines")
        logging.info("\nProcessed lines, now calculating percentages\n")
        self.calculate_percentages()
        logging.info("\nCalculated percentages, now adding entries to IYP\n")
        self.batch_add_to_iyp()
        logging.info("\nSuccessfully added all entries to IYP\n")

    # Process a single line from the jsonl file and store the results locally
    def process_one_line(self, one_line):
        """Add the entry to IYP if it's not already there and update its properties."""

        probe_asn = (
            int(one_line.get("probe_asn")[2:])
            if one_line.get("probe_asn") and one_line.get("probe_asn").startswith("AS")
            else None
        )
        # Add the DNS resolver to the set, unless it's not a valid IP address
        try:
            self.all_dns_resolvers.add(
                ipaddress.ip_address(one_line.get("resolver_ip"))
            )
        except ValueError:
            pass
        probe_cc = one_line.get("probe_cc")
        stun_endpoint = one_line.get("input")
        test_keys = one_line.get("test_keys", {})
        failure = test_keys.get("failure")
        result = "Success" if failure is None else "Failure"

        if stun_endpoint:
            # Extract the hostname from the STUN endpoint URL if it's not an IP address
            hostname = None
            stun_url = stun_endpoint.split("//")[-1]
            stun_ip_port = stun_url.split(":")
            stun_ip = stun_ip_port[0]

            try:
                ipaddress.ip_address(stun_ip)
            except ValueError:
                hostname = tldextract.extract(stun_url).fqdn

            # Handle "queries" section to get IP addresses and map them to the hostname
            ip_addresses = []
            for query in test_keys.get("queries", []):
                if query and query.get("answers"):
                    for answer in query.get("answers", []):
                        if "ipv4" in answer:
                            ip_addresses.append(answer["ipv4"])
                        elif "ipv6" in answer:
                            ip_addresses.append(answer["ipv6"])

            self.all_ips.update(ip_addresses)

            # Ensure all required fields are present
            if probe_asn and probe_cc and stun_endpoint:
                # Append the results to the list
                self.all_asns.add(probe_asn)
                self.all_countries.add(probe_cc)
                self.all_urls.add(stun_endpoint)
                if hostname:
                    self.all_hostnames.add(hostname)
                self.all_results.append(
                    (probe_asn, probe_cc, stun_endpoint, result, hostname, ip_addresses)
                )

    def batch_add_to_iyp(self):
        # First, add the nodes and store their IDs directly as returned dictionaries
        self.node_ids = {
            "asn": self.iyp.batch_get_nodes_by_single_prop("AS", "asn", self.all_asns),
            "country": self.iyp.batch_get_nodes_by_single_prop(
                "Country", "country_code", self.all_countries
            ),
            "url": self.iyp.batch_get_nodes_by_single_prop("URL", "url", self.all_urls),
            "hostname": self.iyp.batch_get_nodes_by_single_prop(
                "HostName", "name", self.all_hostnames
            ),
            "dns_resolver": self.iyp.batch_get_nodes_by_single_prop(
                "IP", "ip", self.all_dns_resolvers, all=False
            ),
        }

        country_links = []
        stun_links = []
        resolves_to_links = []

        # Fetch all IP nodes in one batch
        if self.all_ips:
            ip_id_map = self.iyp.batch_get_nodes_by_single_prop(
                "IP", "ip", list(self.all_ips)
            )
        else:
            ip_id_map = {}

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(lambda: 0))

        # Ensure all IDs are present and process results
        for (
            asn,
            country,
            stun_endpoint,
            result,
            hostname,
            ip_addresses,
        ) in self.all_results:
            asn_id = self.node_ids["asn"].get(asn)
            url_id = self.node_ids["url"].get(stun_endpoint)
            country_id = self.node_ids["country"].get(country)
            hostname_id = self.node_ids["hostname"].get(hostname)

            if asn_id and url_id:
                props = self.reference.copy()
                if (asn, country, stun_endpoint) in self.all_percentages:
                    percentages = self.all_percentages[
                        (asn, country, stun_endpoint)
                    ].get("percentages", {})
                    counts = self.all_percentages[(asn, country, stun_endpoint)].get(
                        "category_counts", {}
                    )
                    total_count = self.all_percentages[
                        (asn, country, stun_endpoint)
                    ].get("total_count", 0)

                    for category in ["Success", "Failure"]:
                        props[f"percentage_{category}"] = percentages.get(category, 0)
                        props[f"count_{category}"] = counts.get(category, 0)
                    props["total_count"] = total_count

                # Accumulate properties
                link_properties[(asn_id, url_id)] = props

            if asn_id and country_id:
                if (
                    asn_id
                    and country_id
                    and (asn_id, country_id) not in self.unique_links["COUNTRY"]
                ):
                    self.unique_links["COUNTRY"].add((asn_id, country_id))
                    country_links.append(
                        {
                            "src_id": asn_id,
                            "dst_id": country_id,
                            "props": [self.reference],
                        }
                    )

            if result == "Success" and hostname_id:
                for ip in ip_addresses:
                    ip_id = ip_id_map.get(ip)
                    if ip_id:
                        resolves_to_links.append(
                            {
                                "src_id": hostname_id,
                                "dst_id": ip_id,
                                "props": [self.reference],
                            }
                        )

        for (asn_id, url_id), props in link_properties.items():
            if (asn_id, url_id) not in self.unique_links["CENSORED"]:
                self.unique_links["CENSORED"].add((asn_id, url_id))
                stun_links.append(
                    {"src_id": asn_id, "dst_id": url_id, "props": [props]}
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links("CENSORED", stun_links)
        self.iyp.batch_add_links("COUNTRY", country_links)
        self.iyp.batch_add_links("RESOLVES_TO", resolves_to_links)

        # Batch add node labels
        self.iyp.batch_add_node_label(
            list(self.node_ids["dns_resolver"].values()), "Resolver"
        )

    # Calculate the percentages of the results
    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, stun_endpoint, result, hostname, ip_addresses = entry
            target_dict[(asn, country, stun_endpoint)][result] += 1

        self.all_percentages = {}

        # Define all possible result categories to ensure they are included
        possible_results = ["Success", "Failure"]

        for (asn, country, stun_endpoint), counts in target_dict.items():
            total_count = sum(counts.values())

            # Initialize counts for all possible results to ensure they are included
            for result in possible_results:
                counts[result] = counts.get(result, 0)

            percentages = {
                category: (
                    (counts[category] / total_count) * 100 if total_count > 0 else 0
                )
                for category in possible_results
            }

            result_dict = {
                "total_count": total_count,
                "category_counts": dict(counts),
                "percentages": percentages,
            }
            self.all_percentages[(asn, country, stun_endpoint)] = result_dict


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--unit-test", action="store_true")
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace("/", "_")[0:-3]
    FORMAT = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        format=FORMAT,
        filename="log/" + scriptname + ".log",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"Started: {sys.argv}")

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f"Finished: {sys.argv}")


if __name__ == "__main__":
    main()
    sys.exit(0)
