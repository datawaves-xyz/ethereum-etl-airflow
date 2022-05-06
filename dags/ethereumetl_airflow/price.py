import copy
import csv
from datetime import datetime
from typing import List

import pendulum as pdl
import requests
from ethereumetl.progress_logger import ProgressLogger

from ethereumetl_airflow.token import (
    TokenProvider,
    Token,
    DuneTokenProvider
)

iso_format = '%Y-%m-%dT%H:%M:%SZ'
minutes_format = '%Y-%m-%d %H:%M'
day_format = '%Y-%m-%d'


class PriceRecord:
    attrs = ['minute', 'price', 'decimals', 'contract_address', 'symbol', 'dt']

    def __init__(
            self,
            minute: str,
            price: float,
            decimals: int,
            contract_address: str,
            symbol: str,
            dt: str
    ) -> None:
        self.minute = minute
        self.price = price
        self.decimals = decimals
        self.contract_address = contract_address
        self.symbol = symbol
        self.dt = dt

    def copy_it_with_datetime(self, time: pdl.datetime) -> 'PriceRecord':
        other = copy.copy(self)
        other.minute = time.strftime(minutes_format)
        other.dt = time.strftime(day_format)
        return other


class PriceProvider:
    def __init__(self, token_provider: TokenProvider, auth_key: str):
        self.token_provider = token_provider
        self.progress_logger = ProgressLogger()
        self.auth_key = auth_key

    def get_single_token_daily_price(
            self, token: Token, start: int, end: int,
    ) -> List[PriceRecord]:
        raise NotImplementedError()

    def create_temp_csv(self, output_path: str, start: int, end: int) -> None:
        tokens = self.token_provider.get_tokens()
        self.progress_logger.start(total_items=len(tokens))

        with open(output_path, 'w') as csvfile:
            spam_writer = csv.DictWriter(csvfile, fieldnames=PriceRecord.attrs)
            spam_writer.writeheader()

            for token in tokens:
                if token.end is not None:
                    end_at = int(token.end.timestamp())
                    if end_at < end:
                        continue

                spam_writer.writerows([i.__dict__ for i in self.get_single_token_daily_price(token, start, end)])
                self.progress_logger.track()

        self.progress_logger.finish()


class CoinpaprikaPriceProvider(PriceProvider):
    host = "https://api-pro.coinpaprika.com"

    def __init__(self, auth_key: str):
        super().__init__(token_provider=DuneTokenProvider(), auth_key=auth_key)

    @staticmethod
    def _copy_record_across_interval(
            record: PriceRecord, interval: int
    ) -> List[PriceRecord]:
        start = pdl.from_format(record.minute, minutes_format)
        end = start.add(minutes=interval)
        records = []

        while start < end:
            records.append(record.copy_it_with_datetime(start))
            start = start.add(minutes=1)

        return records

    def get_single_token_daily_price(
            self, token: Token, start: int, end: int
    ) -> List[PriceRecord]:
        uri = f'{self.host}/v1/tickers/{token.id}/historical'
        res = requests.get(
            url=uri,
            params={
                'start': start,
                'end': end,
                'limit': 5000,
                'interval': '5m'
            },
            headers={
                'Accept': 'application/json',
                'User-Agent': 'coinpaprika/python',
                'Authorization': self.auth_key
            }
        )

        if not str(res.status_code).startswith('2'):
            raise Exception(f'Coinpaprika API failed: {res.text}, {res.url}')

        records = []
        for item in res.json():
            time = pdl.instance(datetime.strptime(item['timestamp'], iso_format))
            records += self._copy_record_across_interval(
                record=PriceRecord(
                    minute=time.strftime(minutes_format),
                    price=item['price'],
                    decimals=token.decimals,
                    contract_address=token.address,
                    symbol=token.symbol,
                    dt=time.strftime(day_format)
                ),
                interval=5
            )

        return records
