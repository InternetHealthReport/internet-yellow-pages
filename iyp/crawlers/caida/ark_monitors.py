import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Tuple

import requests
from iso3166 import countries_by_alpha2
from neo4j.spatial import WGS84Point

from iyp import BaseCrawler

URL = 'https://api.arkmon.caida.org/public/locations'
ORG = 'CAIDA'
NAME = 'caida.ark_monitors'


class Crawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.caida.org/projects/ark/'

    @staticmethod
    def parse_duration(duration_str: str) -> timedelta | None:
        """Parse a duration from a string to a timedelta.

        Durations can be specified as
          x day[s] y hr z min
        where both the day and hour part are optional.

        Return None if the duration string is empty (instead of raising an exception),
        since sometimes there are empty strings.

        Args:
            duration_str (str): Duration as string.

        Raises:
            ValueError: Raised if string is not empty but in an unexpected form.

        Returns:
            timedelta | None: Duration as timedelta or None if string is empty.
        """
        split = duration_str.strip().split()
        match len(split):
            case 0:
                # Empty duration string. Can sometimes happen for last_seen values for
                # some reason. Emit error message in calling function.
                return None
            case 2:
                if split[1] != 'min':
                    raise ValueError(f'Failed to parse duration "{duration_str}": Expected value of form "x min".')
                return timedelta(minutes=int(split[0]))
            case 4:
                if split[1] != 'hr' or split[3] != 'min':
                    raise ValueError(f'Failed to parse duration "{duration_str}": Expected value of form "x hr y min".')
                return timedelta(hours=int(split[0]), minutes=int(split[2]))
            case 6:
                if (split[1] != 'day' and split[1] != 'days') or split[3] != 'hr' or split[5] != 'min':
                    raise ValueError(f'Failed to parse duration "{duration_str}": Expected value '
                                     'of form "x days y hr z min".')
                return timedelta(days=int(split[0]), hours=int(split[2]), minutes=int(split[4]))
            case _:
                raise ValueError(f'Failed to parse duration "{duration_str}": Unknown format.')

    @staticmethod
    def get_last_seen_and_uptime(ping_str: str) -> Tuple[timedelta | None, timedelta]:
        """Get the last_seen time and the update duration from a ping string.

        The ping string has the form
          last sign of life [duration] ago; at that time uptime was [duration]
        Sometimes the life sign contains no duration, resulting in a value of None
        instead of a timedelta.

        Args:
            ping_str (str): The ping string

        Returns:
            Tuple[timedelta | None, timedelta]: Tuple of last_seen (if it exists) and
                uptime.
        """
        l, r = ping_str.split(';')
        last_seen_timedelta = Crawler.parse_duration(l.removeprefix('last sign of life').removesuffix('ago'))
        uptime = Crawler.parse_duration(r.removeprefix(' at that time uptime was'))
        return last_seen_timedelta, uptime

    @staticmethod
    def unwrap_organization(org_name: str) -> Tuple[str, str]:
        """Unwrap the organization name from a surrounding HTML hyperlink.

        Supports plain organization names without surrounding link.

        Remove the link if it points to www.caida.org, but the organization is not
        CAIDA (this link was used as default value in the past).

        Args:
            org_name (str): The organization string.

        Returns:
            Tuple[str, str]: Organization name and URL (can be empty string).
        """
        if not org_name.startswith('<a href'):
            return org_name, str()
        link, org_name_proper = org_name.split('>', maxsplit=1)
        org_name_proper = org_name_proper.removesuffix('</a>')
        url = link.removeprefix('<a href=').strip('"')
        # Some varation of http://www.caida.org is used as default URL for some
        # organizations, which is misleading.
        if 'caida' in url and org_name_proper != 'CAIDA':
            return org_name_proper, str()
        return org_name_proper, url

    @staticmethod
    def extract_country_code(city_str: str) -> str:
        """Extract the country code from the city string.

        City string has form
          [city], [country-specific region identifier], [country code]
        Extract the country code and also handle CAIDA's use of UK instead of GB.


        Args:
            city_str (str): The city string.

        Returns:
            str: ISO 3166-1 alpha-2 country code. Can be empty string in case of error.
        """
        _, country_code = city_str.rsplit(',', maxsplit=1)
        country_code = country_code.strip()
        # CAIDA uses UK code.
        if country_code == 'UK':
            country_code = 'GB'
        if country_code not in countries_by_alpha2:
            logging.warning(f'Invalid country code "{country_code}" in city string "{city_str}"')
            return str()
        return country_code

    def __transform_monitor_data(self, mon: dict):
        """Parse some fields from the monitor dict into more suitable datatypes.

        The specified monitor dict will be updates in-place.

        Args:
            mon (dict): The monitor dict.
        """
        country_code = self.extract_country_code(mon['city'])
        if country_code:
            mon['country_code'] = country_code
        mon['longitude'] = float(mon['longitude'])
        mon['latitude'] = float(mon['latitude'])
        mon['activation'] = datetime.strptime(mon['activation'], '%Y-%m-%d').date()
        # No null values in neo4j and there is at least one instance of a monitor
        # without specified ASN.
        asn = mon.pop('as_number')
        if asn:
            mon['as_number'] = int(asn)
        # HTML code to be displayed on the website. Not useful as a property.
        mon.pop('html')
        mon['Activity'] = mon['Activity'].split(',')
        # "ping" is a string of form
        #   last sign of life x min  ago; at that time uptime was x days y hr z min
        # for display on the website. Transform to more useable properties instead.
        last_seen, uptime = self.get_last_seen_and_uptime(mon.pop('ping'))
        if last_seen is None:
            logging.warning(f'Failed to set last_seen time for monitor {mon["name"]}: Empty duration value.')
        else:
            mon['last_seen'] = self.fetch_time - last_seen
        mon['uptime'] = uptime
        org_name, org_url = self.unwrap_organization(mon.pop('org_name'))
        mon['org_name'] = org_name
        if org_url:
            mon['org_url'] = org_url

    @staticmethod
    def __replace_link_values(links: set, l_id: dict = dict(), r_id: dict = dict()) -> set:
        ret = set()
        for l, r in links:
            if l_id:
                l = l_id[l]
            if r_id:
                r = r_id[r]
            ret.add((l, r))
        return ret

    def run(self):
        self.fetch_time = datetime.now(tz=timezone.utc)
        r = requests.get(URL).json()
        monitors = list()
        most_recent_last_seen = None
        for mon in r['mons'].values():
            self.__transform_monitor_data(mon)
            if 'last_seen' in mon and (most_recent_last_seen is None or mon['last_seen'] > most_recent_last_seen):
                # This is not precisely the last modification time, but the best
                # signal we have for freshness of the API data.
                most_recent_last_seen = mon['last_seen']
            monitors.append(mon)
        if most_recent_last_seen is None:
            logging.warning('Failed to set modification time: No monitor with valid last_seen value found.')
        else:
            self.reference['reference_time_modification'] = most_recent_last_seen

        ases = set()
        points = set()
        countries = set()
        located_in_as_links = set()
        located_in_point_links = set()
        country_links = set()

        for mon in monitors:
            monitor_name = mon['name']

            if asn := mon.get('as_number'):
                ases.add(asn)
                located_in_as_links.add((monitor_name, asn))

            point = WGS84Point((mon['longitude'], mon['latitude']))
            points.add(point)
            located_in_point_links.add((monitor_name, point))

            country_code = mon['country_code']
            countries.add(country_code)
            country_links.add((monitor_name, country_code))

        logging.info(f'Creating {len(monitors)} ArkMonitor nodes.')
        ark_monitor_id = self.iyp.batch_get_nodes('ArkMonitor', monitors, ['name'])
        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases, all=False)
        point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)

        self.iyp.batch_add_links('LOCATED_IN',
                                 self.link_generator(
                                     self.__replace_link_values(located_in_as_links, ark_monitor_id, as_id)
                                 ))
        self.iyp.batch_add_links('LOCATED_IN',
                                 self.link_generator(
                                     self.__replace_link_values(located_in_point_links, ark_monitor_id, point_id)
                                 ))
        self.iyp.batch_add_links('COUNTRY',
                                 self.link_generator(
                                     self.__replace_link_values(country_links, ark_monitor_id, country_id)
                                 ))

    def unit_test(self):
        return super().unit_test(['COUNTRY', 'LOCATED_IN'])


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
