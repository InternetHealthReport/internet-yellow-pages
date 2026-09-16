import argparse
import gzip
import io
import logging
import sys
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address

import ijson
import requests
from github import Github

from iyp import BaseCrawler, DataNotAvailableError, MissingKeyError

ORG = 'IHR'
URL = 'https://github.com/InternetHealthReport/hagezi-blocklists-forward-dns'
URL_INFO = 'https://github.com/InternetHealthReport/hagezi-blocklists-forward-dns#readme'
NAME = 'hagezi.blocklists_forward_dns'

GITHUB_REPO = 'InternetHealthReport/hagezi-blocklists-forward-dns'
RESULTS_DIR = 'results'
# The blocklists themselves are maintained by Hagezi, IHR only resolves them. The list
# membership is therefore attributed to Hagezi, the DNS data to IHR.
HAGEZI_ORG = 'Hagezi'
HAGEZI_URL_INFO = 'https://github.com/hagezi/dns-blocklists#readme'
# Scans are performed roughly once a week. Refuse to import data that is much older
# than that, since it would mean the measurement pipeline stopped working.
MAX_AGE_IN_DAYS = 30


def get_latest_scan(github_repo: str, results_dir: str):
    """Get the files of the most recent scan in the given repository.

    Scan directories are named after the scan date (YYYY-MM-DD), so sorting them
    lexicographically also sorts them chronologically.

    Return a tuple (scan date, list of download URLs).
    """
    repo = Github().get_repo(github_repo)
    scan_dirs = sorted(entry.path for entry in repo.get_contents(results_dir) if entry.type == 'dir')
    if not scan_dirs:
        logging.error(f'No scan directory found in {github_repo}/{results_dir}')
        raise DataNotAvailableError('Failed to find any scan directory.')

    latest_dir = scan_dirs[-1]
    scan_date = datetime.strptime(latest_dir.split('/')[-1], '%Y-%m-%d').replace(tzinfo=timezone.utc)
    if scan_date < datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_IN_DAYS):
        logging.error(f'Latest scan is from {scan_date.date()}, which is too old.')
        raise DataNotAvailableError('Failed to find a recent scan.')

    urls = [entry.download_url for entry in repo.get_contents(latest_dir) if entry.path.endswith('.json.gz')]
    if not urls:
        logging.error(f'No result file found in {latest_dir}')
        raise DataNotAvailableError('Failed to find any result file.')

    return scan_date, sorted(urls)


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        # The DNS data is produced by IHR. Blocklist membership is attributed to Hagezi
        # instead, see list_reference below.
        self.reference['reference_url_info'] = URL_INFO

        # Nodes.
        self.host_names = set()
        self.domain_names = set()
        self.name_servers = set()
        self.ips = set()
        # Blocklist membership: list name -> set of host names. The lists are Hagezi's
        # data, so these relationships reference the blocklist repository instead of the
        # resolution results.
        self.list_hosts = dict()
        self.list_reference = dict()
        # Relationships. The DNS data describes one measurement and is independent of
        # the list a name was taken from, so these are deduplicated globally and use the
        # crawler-wide reference.
        self.part_of = set()     # (host name, domain name)
        self.managed_by = set()  # (domain name, name server)
        # Record type -> set of (host name, IP). Kept separate instead of in a single
        # dict to save memory, since this is by far the largest structure.
        self.resolves_to = {'A': set(), 'AAAA': set()}

    @staticmethod
    def normalize_name(name: str):
        """Strip the root label and lowercase a DNS name.

        DNS names are case-insensitive and the data contains name server names in mixed
        case, which would create duplicate nodes. Interned to save memory, since names
        are repeated a lot across records.
        """
        if name != '.':
            name = name.rstrip('.')
        return sys.intern(name.lower())

    @staticmethod
    def read_header(stream):
        """Read the metadata preceding the record array of a result file.

        Nested fields are flattened, i.e., the commit date of the blocklist is available
        as "source.commit_date". Stops before the records are parsed, so this is cheap
        even for large files.
        """
        header = dict()
        for prefix, event, value in ijson.parse(stream):
            if prefix == str() and event == 'map_key' and value == 'records':
                break
            if prefix and event in ('string', 'number', 'null'):
                header[prefix] = value
        return header

    def get_list_modification_time(self, header: dict, url: str, generated_at: datetime):
        """Get the time at which the blocklist was published upstream.

        The result files contain the date of the Hagezi commit the names were fetched
        from, which is the actual modification time of the list. It is absent in older
        files and null if the commit could not be resolved at scan time, in which case
        we fall back to the time at which the list was fetched and resolved.
        """
        commit_date = header.get('source.commit_date')
        if not commit_date:
            logging.warning(f'No commit date in {url}, using the resolution time instead.')
            return generated_at
        try:
            # Dates are UTC, but expressed with a "Z" suffix.
            return datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
        except ValueError as e:
            logging.warning(f'Ignoring malformed commit date "{commit_date}" in {url}: {e}')
            return generated_at

    def process_list(self, url: str):
        """Fetch and process the results of a single blocklist."""
        logging.info(f'Fetching {url}')
        req = requests.get(url)
        req.raise_for_status()
        # These files contain hundreds of thousands of records, so they are parsed
        # incrementally instead of being loaded into memory as a whole.
        compressed_data = io.BytesIO(req.content)
        del req

        header = self.read_header(gzip.GzipFile(fileobj=compressed_data))
        for key in ('list', 'generated_at'):
            if key not in header:
                logging.error(f'Missing "{key}" field in {url}')
                raise MissingKeyError(f'Missing "{key}" field in result file.')

        generated_at = datetime.fromisoformat(header['generated_at'])

        list_name = header['list']
        # The names of a list are Hagezi's data, but the version of the list we describe
        # here is the one contained in the result file, so the data URL points to that
        # file and the info URL to the blocklist repository.
        list_reference = self.reference.copy()
        list_reference['reference_org'] = HAGEZI_ORG
        list_reference['reference_url_data'] = url
        list_reference['reference_url_info'] = HAGEZI_URL_INFO
        list_reference['reference_time_modification'] = self.get_list_modification_time(header, url, generated_at)
        self.list_reference[list_name] = list_reference
        list_hosts = self.list_hosts.setdefault(list_name, set())

        records = 0
        failed_resolutions = 0
        missing_zones = 0

        compressed_data.seek(0)
        for record in ijson.items(gzip.GzipFile(fileobj=compressed_data), 'records.item'):
            records += 1
            host_name = self.normalize_name(record['hostname'])
            if not host_name:
                logging.warning(f'Ignoring empty host name in list "{list_name}"')
                continue
            self.host_names.add(host_name)
            list_hosts.add(host_name)

            # Names that could not be resolved are kept, since they are part of the
            # blocklist, they just have no DNS data attached.
            if 'error' in record:
                failed_resolutions += 1
                continue

            # The zone of the name as observed during the iterative resolution.
            zone = record['zone']
            if zone:
                zone = self.normalize_name(zone)
                self.domain_names.add(zone)
                self.part_of.add((host_name, zone))
            else:
                missing_zones += 1

            for record_type, addresses in (('A', record['a']), ('AAAA', record['aaaa'])):
                for address in addresses:
                    ip = self.normalize_ip(address, host_name)
                    if ip is None:
                        continue
                    expected_type = 'AAAA' if ':' in ip else 'A'
                    if expected_type != record_type:
                        # Keep the data as reported, but this should not happen.
                        logging.warning(f'{record_type} record of "{host_name}" contains the '
                                        f'{expected_type} address "{ip}"')
                    self.resolves_to[record_type].add((host_name, ip))

            for name_server in record['nameserver_ips']:
                ns_name = self.normalize_name(name_server['name'])
                if not ns_name:
                    logging.warning(f'Ignoring empty name server name for "{host_name}"')
                    continue
                self.host_names.add(ns_name)
                self.name_servers.add(ns_name)
                if zone:
                    self.managed_by.add((zone, ns_name))

                # The data gives the IPs of the name servers, but not the record type
                # they were obtained with, so infer it from the address family.
                ip = self.normalize_ip(name_server['ip'], ns_name)
                if ip is None:
                    continue
                self.resolves_to['AAAA' if ':' in ip else 'A'].add((ns_name, ip))

        logging.info(f'Processed list "{list_name}": {records} records, '
                     f'{failed_resolutions} failed resolutions, {missing_zones} records without zone')
        if records == 0:
            logging.warning(f'List "{list_name}" contains no record.')
        elif failed_resolutions == records:
            logging.warning(f'No name of list "{list_name}" was resolved successfully.')

    def normalize_ip(self, address: str, name: str):
        """Return the compressed form of an IP address or None if it is malformed."""
        try:
            return sys.intern(ip_address(address).compressed)
        except ValueError as e:
            logging.warning(f'Ignoring malformed IP address "{address}" of "{name}": {e}')
            return None

    def categorized_link_generator(self, host_id: dict, tag_id: dict):
        for list_name, host_names in self.list_hosts.items():
            reference = self.list_reference[list_name]
            tag_qid = tag_id[list_name]
            for host_name in host_names:
                yield {'src_id': host_id[host_name], 'dst_id': tag_qid, 'props': [reference]}

    def name_link_generator(self, pairs, src_id: dict, dst_id: dict):
        for src, dst in pairs:
            yield {'src_id': src_id[src], 'dst_id': dst_id[dst], 'props': [self.reference]}

    def resolves_to_link_generator(self, host_id: dict, ip_id: dict):
        for record_type, pairs in self.resolves_to.items():
            for host_name, ip in pairs:
                yield {'src_id': host_id[host_name],
                       'dst_id': ip_id[ip],
                       'props': [self.reference]}

    def run(self):
        """Fetch the latest scan of all blocklists and push the results to IYP."""
        scan_date, urls = get_latest_scan(GITHUB_REPO, RESULTS_DIR)
        logging.info(f'Processing scan of {scan_date.date()} containing {len(urls)} lists.')
        # This crawler combines the results of multiple lists, so point to the directory
        # containing all of them. Individual lists have a precise URL in their own
        # reference.
        self.reference['reference_url_data'] = f'{URL}/tree/main/{RESULTS_DIR}/{scan_date.date()}'
        self.reference['reference_time_modification'] = scan_date

        for url in urls:
            self.process_list(url)

        # Collect the IP nodes only now, to avoid keeping a separate set while parsing.
        for pairs in self.resolves_to.values():
            for _, ip in pairs:
                self.ips.add(ip)

        # Get/create nodes. 
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', self.host_names,
                                                          all=False, batch_size=100000)
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', self.domain_names,
                                                            all=False, batch_size=100000)
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', self.ips, all=False, batch_size=100000)
        tag_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', set(self.list_hosts), all=False)
        self.iyp.batch_add_node_label([host_id[ns] for ns in self.name_servers], 'AuthoritativeNameServer')

        # Push all links to IYP.
        self.iyp.batch_add_links('CATEGORIZED', self.categorized_link_generator(host_id, tag_id))
        self.iyp.batch_add_links('PART_OF', self.name_link_generator(self.part_of, host_id, domain_id))
        self.iyp.batch_add_links('MANAGED_BY', self.name_link_generator(self.managed_by, domain_id, host_id))
        self.iyp.batch_add_links('RESOLVES_TO', self.resolves_to_link_generator(host_id, ip_id))

    def unit_test(self):
        return super().unit_test(['CATEGORIZED', 'PART_OF', 'MANAGED_BY', 'RESOLVES_TO'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
