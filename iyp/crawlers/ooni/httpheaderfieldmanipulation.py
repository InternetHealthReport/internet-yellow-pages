import argparse
import logging
import os
import sys
import tempfile
import json
from collections import defaultdict

from .utils import grabber

from iyp import BaseCrawler

ORG = "OONI"
URL = "s3://ooni-data-eu-fra/raw/"
NAME = "ooni.httpheaderfieldmanipulation"

label = "OONI HTTP Header Field Manipulation Test"


class Crawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.repo = "ooni-data-eu-fra"
        self.reference["reference_url_info"] = "https://ooni.org/post/mining-ooni-data"
        self.unique_links = {"COUNTRY": set(), "CENSORED": set()}

    def run(self):
        """Fetch data and push to IYP."""

        self.all_asns = set()
        self.all_countries = set()
        self.all_results = list()
        self.all_percentages = {}
        self.all_dns_resolvers = set()

        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Fetch data
        grabber.download_and_extract(self.repo, tmpdir, "httpheaderfieldmanipulation")
        logging.info("Successfully downloaded and extracted all files")
        # Now that we have downloaded the jsonl files for the test we want, we can extract the data we want
        testdir = os.path.join(tmpdir, "httpheaderfieldmanipulation")
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
        probe_cc = one_line.get("probe_cc")
        test_keys = one_line.get("test_keys", {}).get("tampering", {})

        total = "total" if test_keys.get("total", False) else "no_total"
        request_line_capitalization = (
            "request_line_capitalization"
            if test_keys.get("request_line_capitalization", False)
            else "no_request_line_capitalization"
        )
        header_name_capitalization = (
            "header_name_capitalization"
            if test_keys.get("header_name_capitalization", False)
            else "no_header_name_capitalization"
        )
        header_field_value = (
            "header_field_value"
            if test_keys.get("header_field_value", False)
            else "no_header_field_value"
        )
        header_field_number = (
            "header_field_number"
            if test_keys.get("header_field_number", False)
            else "no_header_field_number"
        )

        # Append the results to the list
        self.all_asns.add(probe_asn)
        self.all_countries.add(probe_cc)
        self.all_results.append(
            (
                probe_asn,
                probe_cc,
                total,
                request_line_capitalization,
                header_name_capitalization,
                header_field_value,
                header_field_number,
            )
        )

    def batch_add_to_iyp(self):
        # First, add the nodes and store their IDs directly as returned dictionaries
        self.node_ids = {
            "asn": self.iyp.batch_get_nodes_by_single_prop("AS", "asn", self.all_asns),
            "country": self.iyp.batch_get_nodes_by_single_prop(
                "Country", "country_code", self.all_countries
            ),
            "dns_resolver": self.iyp.batch_get_nodes_by_single_prop(
                "IP", "ip", self.all_dns_resolvers, all=False
            ),
        }

        httpheader_id = self.iyp.batch_get_nodes_by_single_prop(
            "Tag", "label", {label}
        ).get(label)

        country_links = []
        censored_links = []

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(lambda: 0))

        # Ensure all IDs are present and process results
        for (
            asn,
            country,
            total,
            request_line_capitalization,
            header_name_capitalization,
            header_field_value,
            header_field_number,
        ) in self.all_results:
            asn_id = self.node_ids["asn"].get(asn)
            country_id = self.node_ids["country"].get(country)

            if asn_id and country_id:
                props = self.reference.copy()
                if (asn, country) in self.all_percentages:
                    percentages = self.all_percentages[(asn, country)].get(
                        "percentages", {}
                    )
                    counts = self.all_percentages[(asn, country)].get(
                        "category_counts", {}
                    )
                    total_count = self.all_percentages[(asn, country)].get(
                        "total_count", 0
                    )

                    for category in [
                        "total",
                        "no_total",
                        "request_line_capitalization",
                        "no_request_line_capitalization",
                        "header_name_capitalization",
                        "no_header_name_capitalization",
                        "header_field_value",
                        "no_header_field_value",
                        "header_field_number",
                        "no_header_field_number",
                    ]:
                        props[f"percentage_{category}"] = percentages.get(category, 0)
                        props[f"count_{category}"] = counts.get(category, 0)
                    props["total_count"] = total_count

                # Accumulate properties
                link_properties[(asn_id, httpheader_id)] = props

                if (asn_id, country_id) not in self.unique_links["COUNTRY"]:
                    self.unique_links["COUNTRY"].add((asn_id, country_id))
                    country_links.append(
                        {
                            "src_id": asn_id,
                            "dst_id": country_id,
                            "props": [self.reference],
                        }
                    )

        for (asn_id, httpheader_id), props in link_properties.items():
            if (asn_id, httpheader_id) not in self.unique_links["CENSORED"]:
                self.unique_links["CENSORED"].add((asn_id, httpheader_id))
                censored_links.append(
                    {"src_id": asn_id, "dst_id": httpheader_id, "props": [props]}
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links("CENSORED", censored_links)
        self.iyp.batch_add_links("COUNTRY", country_links)

        # Batch add node labels
        self.iyp.batch_add_node_label(
            list(self.node_ids["dns_resolver"].values()), "Resolver"
        )

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Initialize counts for all categories
        categories = [
            "total",
            "no_total",
            "request_line_capitalization",
            "no_request_line_capitalization",
            "header_name_capitalization",
            "no_header_name_capitalization",
            "header_field_value",
            "no_header_field_value",
            "header_field_number",
            "no_header_field_number",
        ]

        # Populate the target_dict with counts
        for entry in self.all_results:
            (
                asn,
                country,
                total,
                request_line_capitalization,
                header_name_capitalization,
                header_field_value,
                header_field_number,
            ) = entry
            target_dict[(asn, country)][total] += 1
            target_dict[(asn, country)][request_line_capitalization] += 1
            target_dict[(asn, country)][header_name_capitalization] += 1
            target_dict[(asn, country)][header_field_value] += 1
            target_dict[(asn, country)][header_field_number] += 1

        self.all_percentages = {}

        for (asn, country), counts in target_dict.items():
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
            self.all_percentages[(asn, country)] = result_dict


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
