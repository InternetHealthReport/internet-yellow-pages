import logging
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network

import pandas as pd

from iyp import BaseCrawler, DataNotAvailableError, get_commit_datetime

DATA_URL_FMT = 'https://raw.githubusercontent.com/bgptools/anycast-prefixes/refs/heads/master/anycatch-v{ip_version}-prefixes.csv'  # noqa: E501


class AnycastPrefixesCrawler(BaseCrawler):
    def __init__(self, organization: str, url: str, name: str, ip_version: int):
        super().__init__(organization, url, name)
        self.ip_version = ip_version
        self.repo = 'bgptools/anycast-prefixes'
        self.latest_file = f'anycatch-v{ip_version}-prefixes.csv'
        self.reference['reference_url_info'] = 'https://bgp.tools/kb/anycatch'

    def run(self):
        prefixes_url = DATA_URL_FMT.format(ip_version=self.ip_version)
        # Overriding the reference_url_data according to prefixes
        modification_time = get_commit_datetime(self.repo, self.latest_file)
        if modification_time < datetime.now(tz=timezone.utc) - timedelta(days=7):
            raise DataNotAvailableError(f'No recent IPv{self.ip_version} data available.')
        self.reference['reference_time_modification'] = modification_time
        self.reference['reference_url_data'] = prefixes_url

        anycast_tag_qid = self.iyp.get_node('Tag', {'label': 'Anycast'})

        bgp_prefixes = set()
        categorized_links = list()
        df = pd.read_csv(prefixes_url, names=['prefix', 'probed_ip'])
        for row in df.itertuples():
            try:
                prefix = ip_network(row.prefix).compressed
            except ValueError as e:
                logging.warning(f'Ignoring malformed prefix: "{row.prefix}": {e}')
                continue
            bgp_prefixes.add(prefix)
            categorized_links.append({
                'src_id': prefix,
                'dst_id': anycast_tag_qid,
                'props': [
                    self.reference,
                    {'probed_ip': row.probed_ip}
                ]
            })

        bgp_prefix_id = self.iyp.batch_get_nodes_by_single_prop('BGPPrefix', 'prefix', bgp_prefixes, all=False)
        self.iyp.batch_add_node_label(list(bgp_prefix_id.values()), 'Prefix')

        for link in categorized_links:
            link['src_id'] = bgp_prefix_id[link['src_id']]

        self.iyp.batch_add_links('CATEGORIZED', categorized_links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED'])
