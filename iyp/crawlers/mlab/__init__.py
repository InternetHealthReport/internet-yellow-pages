import logging
from datetime import UTC, datetime

import pandas as pd
import requests

from iyp import BaseCrawler, DataNotAvailableError

URL_FMT = 'https://storage.googleapis.com/mlab-sandbox-iqb-us-central1/cache/v1/{start_ts}/{end_ts}/{direction}_by_{aggregate}/data.parquet'
TS_FMT = '%Y%m%dT000000Z'
MAX_LOOKBACK_INTERVAL_MONTHS = 6


class MLabStatsCrawler(BaseCrawler):
    def __init__(self, organization, url, name, aggregate: str):
        super().__init__(organization, url, name)
        self.aggregate = aggregate
        self.reference['reference_url_info'] = 'https://www.measurementlab.net/data/stats/'
        self.start_ts = None
        self.end_ts = None
        self.parquet_files = dict()

    @staticmethod
    def decrement_moth(date: datetime) -> datetime:
        if date.month == 1:
            return date.replace(year=date.year - 1, month=12)
        return date.replace(month=date.month - 1)

    def __probe_available_data(self):
        self.end_ts = datetime.now(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        self.start_ts = self.decrement_moth(self.end_ts)
        lookback = 0
        while lookback < MAX_LOOKBACK_INTERVAL_MONTHS:
            url = URL_FMT.format(
                start_ts=self.start_ts.strftime(TS_FMT),
                end_ts=self.end_ts.strftime(TS_FMT),
                direction='downloads',
                aggregate=self.aggregate,
            )
            logging.info(f'Probing url: {url}')
            r = requests.head(url)
            if r.status_code == 200:
                break
            self.start_ts = self.decrement_moth(self.start_ts)
            self.end_ts = self.decrement_moth(self.end_ts)
            lookback += 1
        else:
            raise DataNotAvailableError(
                'Failed to find data within the specified lookback interval'
            )
        # Technically we use both downloads/uploads URLs, but we cannot link to both.
        self.reference['reference_url_data'] = url
        self.reference['reference_time_modification'] = self.end_ts

    def fetch(self):
        for direction in ['downloads', 'uploads']:
            self.parquet_files[direction] = pd.read_parquet(
                URL_FMT.format(
                    start_ts=self.start_ts.strftime(TS_FMT),
                    end_ts=self.end_ts.strftime(TS_FMT),
                    direction=direction,
                    aggregate=self.aggregate,
                )
            )

    def process_data(self):
        raise NotImplementedError()

    def run(self):
        self.__probe_available_data()
        self.fetch()
        self.process_data()

    def unit_test(self):
        return super().unit_test(['SPEED_TEST'])
