from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from pymarketstore.utils import (
    get_timestamp,
    is_iterable,
    parse_date_to_string,
    timeseries_data_to_write_request,
)

from .test_results import btc_array, btc_bytes, btc_df


class TestParseDateToString:
    def test_none(self):
        assert parse_date_to_string(None) is None

    def test_int(self):
        # 1705276800 is 2024-01-15 00:00:00 UTC
        assert parse_date_to_string(1705276800) == "2024-01-15"

    def test_string_iso_format(self):
        assert parse_date_to_string("2024-01-15") == "2024-01-15"

    def test_string_epoch(self):
        # Epoch seconds as string should be converted to YYYY-MM-DD
        assert parse_date_to_string("1705276800") == "2024-01-15"

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 12, 30, 0)
        assert parse_date_to_string(dt) == "2024-01-15"

    def test_date(self):
        d = date(2024, 1, 15)
        assert parse_date_to_string(d) == "2024-01-15"

    def test_invalid_type(self):
        with pytest.raises(TypeError):
            parse_date_to_string([2024, 1, 15])

    def test_invalid_string_format(self):
        with pytest.raises(TypeError):
            parse_date_to_string("not-a-date")

    def test_pd_timestamp(self):
        ts = pd.Timestamp("2024-01-15 12:30:00", tz="UTC")
        assert parse_date_to_string(ts) == "2024-01-15"


class TestTimeseriesDataToWriteRequest:
    def test_np_array(self):
        assert timeseries_data_to_write_request(btc_array, "BTC/1Min/OHLCV") == dict(
            column_data=btc_bytes,
            column_names=["Epoch", "Open", "High", "Low", "Close", "Volume"],
            column_types=["i8", "f8", "f8", "f8", "f8", "f8"],
            length=5,
        )

    def test_pd_series_indexed_by_timestamp(self):
        series = pd.Series(btc_df.Open, index=btc_df.index)
        assert timeseries_data_to_write_request(series, "BTC/1Min/Open") == dict(
            column_data=[btc_bytes[0], btc_bytes[1]],
            column_names=["Epoch", "Open"],
            column_types=["i8", "f8"],
            length=5,
        )

    def test_pd_series_row_from_df(self):
        series = btc_df.iloc[0]
        expected_epoch = bytes(
            memoryview(series.name.to_numpy().astype(dtype="i8") // 10**9)
        )
        assert timeseries_data_to_write_request(series, "BTC/1Min/OHLCV") == dict(
            column_data=[expected_epoch]
            + [bytes(memoryview(val)) for val in series.array],
            column_names=["Epoch", "Open", "High", "Low", "Close", "Volume"],
            column_types=["i8", "f8", "f8", "f8", "f8", "f8"],
            length=1,
        )

    def test_pd_dataframe(self):
        assert timeseries_data_to_write_request(btc_df, "BTC/1Min/OHLCV") == dict(
            column_data=btc_bytes,
            column_names=["Epoch", "Open", "High", "Low", "Close", "Volume"],
            column_types=["i8", "f8", "f8", "f8", "f8", "f8"],
            length=5,
        )

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError, match="data must be"):
            timeseries_data_to_write_request("not_valid_data", "BTC/1Min/OHLCV")

    def test_np_array_without_names_raises(self):
        data = np.array([1.0, 2.0, 3.0])
        with pytest.raises(TypeError, match="named column dtypes"):
            timeseries_data_to_write_request(data, "BTC/1Min/OHLCV")


# ---------------------------------------------------------------------------
# is_iterable()
# ---------------------------------------------------------------------------


class TestIsIterable:
    def test_list(self):
        assert is_iterable([1, 2, 3]) is True

    def test_tuple(self):
        assert is_iterable((1, 2)) is True

    def test_set(self):
        assert is_iterable({1, 2}) is True

    def test_string_is_not_iterable(self):
        assert is_iterable("hello") is False

    def test_int_is_not_iterable(self):
        assert is_iterable(42) is False

    def test_none_is_not_iterable(self):
        assert is_iterable(None) is False

    def test_dict_is_not_iterable(self):
        """Dicts are iterable in Python but is_iterable should return False."""
        assert is_iterable({"a": 1}) is False


# ---------------------------------------------------------------------------
# get_timestamp()
# ---------------------------------------------------------------------------


class TestGetTimestamp:
    def test_none_returns_none(self):
        assert get_timestamp(None) is None

    def test_int_returns_timestamp(self):
        result = get_timestamp(1500000000)
        assert isinstance(result, pd.Timestamp)
        assert result == pd.Timestamp(1500000000, unit="s")

    def test_np_integer_returns_timestamp(self):
        result = get_timestamp(np.int64(1500000000))
        assert isinstance(result, pd.Timestamp)

    def test_string_returns_timestamp(self):
        result = get_timestamp("2024-01-15")
        assert isinstance(result, pd.Timestamp)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_pd_timestamp_returns_same(self):
        ts = pd.Timestamp("2024-01-15")
        result = get_timestamp(ts)
        assert result is ts
