import argparse
import logging
import os
import sys
import tempfile
import json
import ipaddress
from collections import defaultdict

from .utils import grabber
from iyp import BaseCrawler

ORG = "OONI"
URL = "s3://ooni-data-eu-fra/raw/"
NAME = "ooni.tor"


class Crawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.repo = "ooni-data-eu-fra"
        self.reference["reference_url_info"] = "https://ooni.org/post/mining-ooni-data"
        self.unique_links = {"COUNTRY": set(), "CENSORED": set(), "CATEGORIZED": set()}

    def run(self):
        """Fetch data and push to IYP."""
        self.all_asns = set()
        self.all_countries = set()
        self.all_results = list()
        self.all_percentages = {}
        self.all_ips = set()
        self.all_dns_resolvers = set()
        self.all_tags = {"or_port_dirauth", "dir_port", "obfs4", "or_port"}
        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Fetch data
        grabber.download_and_extract(self.repo, tmpdir, "tor")
        logging.info("Successfully downloaded and extracted all files")

        # Process each JSONL file
        testdir = os.path.join(tmpdir, "tor")
        for file_name in os.listdir(testdir):
            file_path = os.path.join(testdir, file_name)
            if os.path.isfile(file_path) and file_path.endswith(".jsonl"):
                with open(file_path, "r") as file:
                    for i, line in enumerate(file):
                        data = json.loads(line)
                        self.process_one_line(data)
                        logging.info(f"Processed {i+1} lines")
        logging.info("Processed lines, now calculating percentages")
        self.calculate_percentages()
        logging.info("Calculated percentages, now adding entries to IYP")
        self.batch_add_to_iyp()
        logging.info("Successfully added all entries to IYP")

    def process_one_line(self, one_line):
        """Process a single line of the JSONL file."""

        probe_asn = (
            int(one_line.get("probe_asn")[2:])
            if one_line.get("probe_asn") and one_line.get("probe_asn").startswith("AS")
            else None
        )
        probe_cc = one_line.get("probe_cc")
        test_keys = one_line.get("test_keys", {})

        self.all_asns.add(probe_asn)
        self.all_countries.add(probe_cc)

        # Add the DNS resolver to the set, unless it's not a valid IP address
        try:
            self.all_dns_resolvers.add(
                ipaddress.ip_address(one_line.get("resolver_ip"))
            )
        except ValueError:
            pass

        if not test_keys:
            return
        # Check each target in the test_keys
        targets = test_keys.get("targets", {})
        for _, target_data in targets.items():
            ip = ipaddress.ip_address(
                target_data.get("target_address").rsplit(":", 1)[0].strip("[]")
            )
            self.all_ips.add(ip)
            result = target_data.get("failure")
            target_protocol = target_data.get("target_protocol")
            if target_protocol not in self.all_tags:
                continue
            self.all_results.append((probe_asn, probe_cc, ip, target_protocol, result))

    def batch_add_to_iyp(self):
        # Prepend "OONI Probe Tor Tag" to all tag labels
        prepended_tags = {f"OONI Probe Tor Tag {tag}" for tag in self.all_tags}
        self.node_ids = {
            "asn": self.iyp.batch_get_nodes_by_single_prop("AS", "asn", self.all_asns),
            "country": self.iyp.batch_get_nodes_by_single_prop(
                "Country", "country_code", self.all_countries
            ),
            "ip": self.iyp.batch_get_nodes_by_single_prop(
                "IP", "ip", [str(ip) for ip in self.all_ips]
            ),
            "tag": self.iyp.batch_get_nodes_by_single_prop(
                "Tag", "label", prepended_tags
            ),
            "dns_resolver": self.iyp.batch_get_nodes_by_single_prop(
                "IP", "ip", self.all_dns_resolvers, all=False
            ),
        }

        country_links = []
        censored_links = []
        categorized_links = []

        link_properties = defaultdict(lambda: defaultdict(lambda: 0))

        for asn, country, ip, tor_type, _ in self.all_results:
            asn_id = self.node_ids["asn"].get(asn)
            country_id = self.node_ids["country"].get(country)
            ip_id = self.node_ids["ip"].get(str(ip))
            tag_id = self.node_ids["tag"].get(f"OONI Probe Tor Tag {tor_type}")

            if asn_id and ip_id:
                props = self.reference.copy()
                if (asn, ip) in self.all_percentages:
                    percentages = self.all_percentages[(asn, ip)].get("percentages", {})
                    counts = self.all_percentages[(asn, ip)].get("category_counts", {})
                    total_count = self.all_percentages[(asn, ip)].get("total_count", 0)

                    for category in ["Failure", "Success"]:
                        props[f"percentage_{category}"] = percentages.get(category, 0)
                        props[f"count_{category}"] = counts.get(category, 0)
                    props["total_count"] = total_count
                link_properties[(asn_id, ip_id)] = props

            if (
                asn_id
                and country_id
                and (asn_id, country_id) not in self.unique_links["COUNTRY"]
            ):
                self.unique_links["COUNTRY"].add((asn_id, country_id))
                country_links.append(
                    {"src_id": asn_id, "dst_id": country_id, "props": [self.reference]}
                )
            if (
                ip_id
                and tag_id
                and (ip_id, tag_id) not in self.unique_links["CATEGORIZED"]
            ):
                self.unique_links["CATEGORIZED"].add((ip_id, tag_id))
                categorized_links.append(
                    {"src_id": ip_id, "dst_id": tag_id, "props": [self.reference]}
                )

        for (asn_id, ip_id), props in link_properties.items():
            if (asn_id, ip_id) not in self.unique_links["CENSORED"]:
                self.unique_links["CENSORED"].add((asn_id, ip_id))
                censored_links.append(
                    {"src_id": asn_id, "dst_id": ip_id, "props": [props]}
                )

        self.iyp.batch_add_links("CENSORED", censored_links)
        self.iyp.batch_add_links("COUNTRY", country_links)
        self.iyp.batch_add_links("CATEGORIZED", categorized_links)

        # Batch add node labels
        self.iyp.batch_add_node_label(
            list(self.node_ids["dns_resolver"].values()), "Resolver"
        )

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))
        categories = ["Failure", "Success"]
        for entry in self.all_results:
            asn, country, ip, tor_type, result = entry
            if result is not None:
                target_dict[(asn, ip)]["Failure"] += 1
            else:
                target_dict[(asn, ip)]["Success"] += 1

        self.all_percentages = {}

        for (asn, ip), counts in target_dict.items():
            total_count = sum(counts.values())
            for category in categories:
                counts[category] = counts.get(category, 0)

            percentages = {
                category: (
                    (counts[category] / total_count) * 100 if total_count > 0 else 0
                )
                for category in categories
            }

            result_dict = {
                "total_count": total_count,
                "category_counts": dict(counts),
                "percentages": percentages,
            }
            self.all_percentages[(asn, ip)] = result_dict


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--unit-test", action="store_true")
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace("/", "_")[0:-3]
    FORMAT = "%(asctime)s %(levellevelname)s %(message)s"
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
