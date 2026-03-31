from datetime import date, datetime
from typing import Union, overload

import pandas as pd

from .enums import Freq
from .jsonrpc_client import JsonRpcClient
from .params import Params


class Store:
    def __init__(self, endpoint: str = "http://localhost:5993/rpc"):
        self.client = JsonRpcClient(endpoint)

    @overload
    def get(
        self,
        symbols: list[str],
        freq: Freq = Freq.day,
        start_dt: pd.Timestamp | datetime | date | str | int | None = None,
        end_dt: pd.Timestamp | datetime | date | str | int | None = None,
        limit: int | None = None,
    ) -> dict[str, pd.DataFrame]: ...

    @overload
    def get(
        self,
        symbol: str,
        freq: Freq = Freq.day,
        start_dt: pd.Timestamp | datetime | date | str | int | None = None,
        end_dt: pd.Timestamp | datetime | date | str | int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame | None: ...

    def get(
        self,
        symbols: list[str] | str,
        freq: Freq = Freq.day,
        start_dt: pd.Timestamp | datetime | date | str | int | None = None,
        end_dt: pd.Timestamp | datetime | date | str | int | None = None,
        limit: int | None = None,
    ) -> Union[dict[str, pd.DataFrame], pd.DataFrame | None]:
        many = True
        if isinstance(symbols, str):
            many = False
            symbols = [symbols]

        symbols = [symbol.upper() for symbol in symbols]

        p = Params(
            symbols=symbols,
            timeframe=freq.value,
            attrgroup="OHLCV",
            start=start_dt,
            end=end_dt,
            limit=limit,
        )

        try:
            d = {ds.symbol: ds.df() for symbol, ds in self.client.query(p).all().items()}
        except Exception as e:
            if "no results returned from query" in str(e):
                return {} if many else None
            raise e

        return d if many else d[symbols[0]]

    def get_latest_dt(self, symbol: str, freq: Freq) -> pd.Timestamp | None:
        df = self.get(symbol, freq, limit=1)
        if df is None or df.empty:
            return None
        return df.index[-1]

    def has(self, symbol: str, freq: Freq = Freq.day) -> bool:
        """
        Returns true if the store has data for the given symbol and frequency.
        """
        return bool(self.get_latest_dt(symbol, freq))

    def get_symbols(
        self,
        freq: Freq = Freq.day,
        dt: pd.Timestamp | datetime | date | str | int | None = None,
    ) -> list[str]:
        """
        Get a list of all ticker symbols in the store.
        """
        return self.client.list_symbols(
            timeframe=freq.value,
            date=pd.Timestamp(dt) if dt is not None else None,
        )

    def write(self, symbol: str, freq: Freq, bars: pd.DataFrame) -> None:
        """
        Write or append bars to the store for a given symbol and frequency.

        :param symbol: The ticker symbol (e.g., "AAPL")
        :param freq: The frequency/timeframe for the data
        :param bars: DataFrame with OHLCV data indexed by datetime
        """
        tbk = f"{symbol.upper()}/{freq.value}/OHLCV"
        self.client.write(bars, tbk)
