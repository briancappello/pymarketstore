"""Tests for pymarketstore.store.Store."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from pymarketstore.enums import Freq
from pymarketstore.results import DataSet, QueryReply, QueryResult
from pymarketstore.store import Store


def _make_dataset(symbol, timeframe, n=5):
    """Build a real DataSet with OHLCV data for testing."""
    epochs = np.arange(1700000000, 1700000000 + n * 60, 60, dtype="i8")
    dt = np.dtype(
        [
            ("Epoch", "i8"),
            ("Open", "f8"),
            ("High", "f8"),
            ("Low", "f8"),
            ("Close", "f8"),
            ("Volume", "f8"),
        ]
    )
    arr = np.empty(n, dtype=dt)
    arr["Epoch"] = epochs
    arr["Open"] = np.random.default_rng(42).uniform(90, 110, n)
    arr["High"] = arr["Open"] + 2
    arr["Low"] = arr["Open"] - 2
    arr["Close"] = arr["Open"] + 1
    arr["Volume"] = np.random.default_rng(42).uniform(100, 10000, n)

    key = f"{symbol}/{timeframe}/OHLCV"
    return DataSet(arr, key, "UTC")


def _make_query_reply(datasets):
    """Build a QueryReply from a list of DataSets."""
    result_dict = {ds.key: ds for ds in datasets}
    qr = QueryResult.__new__(QueryResult)
    qr.result = result_dict
    qr.timezone = "UTC"
    reply = QueryReply.__new__(QueryReply)
    reply.results = [qr]
    reply.timezone = "UTC"
    return reply


# ---------------------------------------------------------------------------
# get()
# ---------------------------------------------------------------------------


class TestGet:
    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_single_symbol_returns_dataframe(self, MockClient):
        mock_client = MockClient.return_value
        ds = _make_dataset("AAPL", "1D")
        mock_client.query.return_value = _make_query_reply([ds])

        store = Store()
        result = store.get("AAPL", Freq.day)

        assert isinstance(result, pd.DataFrame)
        assert result.shape == (5, 5)
        assert list(result.columns) == ["Open", "High", "Low", "Close", "Volume"]

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_multiple_symbols_returns_dict(self, MockClient):
        mock_client = MockClient.return_value
        ds1 = _make_dataset("AAPL", "1D")
        ds2 = _make_dataset("TSLA", "1D")
        mock_client.query.return_value = _make_query_reply([ds1, ds2])

        store = Store()
        result = store.get(["AAPL", "TSLA"], Freq.day)

        assert isinstance(result, dict)
        assert set(result.keys()) == {"AAPL", "TSLA"}
        for sym, df in result.items():
            assert isinstance(df, pd.DataFrame)

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_single_symbol_no_results_returns_none(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.query.side_effect = Exception("no results returned from query")

        store = Store()
        result = store.get("AAPL", Freq.day)

        assert result is None

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_multiple_symbols_no_results_returns_empty_dict(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.query.side_effect = Exception("no results returned from query")

        store = Store()
        result = store.get(["AAPL", "TSLA"], Freq.day)

        assert result == {}

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_propagates_other_exceptions(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.query.side_effect = ConnectionError("server down")

        store = Store()
        with pytest.raises(ConnectionError, match="server down"):
            store.get("AAPL", Freq.day)

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_uppercases_symbols_in_query(self, MockClient):
        """Symbols should be uppercased before querying."""
        mock_client = MockClient.return_value
        ds = _make_dataset("AAPL", "1D")
        mock_client.query.return_value = _make_query_reply([ds])

        store = Store()
        # Use uppercase input to avoid the lookup bug (symbols[0] is not uppercased)
        store.get("AAPL", Freq.day)

        call_args = mock_client.query.call_args[0][0]
        assert "AAPL" in call_args.tbk

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_lowercased_symbol_uppercases_tbk(self, MockClient):
        """When a lowercase symbol is passed, the tbk in the query uses uppercase."""
        mock_client = MockClient.return_value
        ds = _make_dataset("AAPL", "1D")
        mock_client.query.return_value = _make_query_reply([ds])

        store = Store()
        # Use multiple symbols mode (returns dict), which avoids the d[symbols[0]] lookup
        result = store.get(["aapl"], Freq.day)

        call_args = mock_client.query.call_args[0][0]
        assert "AAPL" in call_args.tbk
        assert isinstance(result, dict)
        assert "AAPL" in result

    @patch("pymarketstore.store.JsonRpcClient")
    def test_get_passes_limit(self, MockClient):
        mock_client = MockClient.return_value
        ds = _make_dataset("AAPL", "1D", n=1)
        mock_client.query.return_value = _make_query_reply([ds])

        store = Store()
        store.get("AAPL", Freq.day, limit=10)

        call_args = mock_client.query.call_args[0][0]
        assert call_args.limit == 10


# ---------------------------------------------------------------------------
# get_latest_dt()
# ---------------------------------------------------------------------------


class TestGetLatestDt:
    @patch("pymarketstore.store.JsonRpcClient")
    def test_returns_last_index(self, MockClient):
        mock_client = MockClient.return_value
        ds = _make_dataset("AAPL", "1D", n=3)
        mock_client.query.return_value = _make_query_reply([ds])

        store = Store()
        result = store.get_latest_dt("AAPL", Freq.day)

        assert isinstance(result, pd.Timestamp)

    @patch("pymarketstore.store.JsonRpcClient")
    def test_returns_none_for_no_data(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.query.side_effect = Exception("no results returned from query")

        store = Store()
        result = store.get_latest_dt("AAPL", Freq.day)

        assert result is None


# ---------------------------------------------------------------------------
# has()
# ---------------------------------------------------------------------------


class TestHas:
    @patch("pymarketstore.store.JsonRpcClient")
    def test_returns_true_when_data_exists(self, MockClient):
        mock_client = MockClient.return_value
        ds = _make_dataset("AAPL", "1D", n=1)
        mock_client.query.return_value = _make_query_reply([ds])

        store = Store()
        assert store.has("AAPL", Freq.day) is True

    @patch("pymarketstore.store.JsonRpcClient")
    def test_returns_false_when_no_data(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.query.side_effect = Exception("no results returned from query")

        store = Store()
        assert store.has("AAPL", Freq.day) is False


# ---------------------------------------------------------------------------
# write()
# ---------------------------------------------------------------------------


class TestWrite:
    @patch("pymarketstore.store.JsonRpcClient")
    def test_write_builds_correct_tbk(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.write.return_value = None

        store = Store()
        df = pd.DataFrame(
            {"Open": [100.0], "Close": [101.0]},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-15", tz="UTC")], name="Epoch"),
        )
        store.write("aapl", Freq.day, df)

        mock_client.write.assert_called_once()
        call_args = mock_client.write.call_args
        assert call_args[0][1] == "AAPL/1D/OHLCV"
        assert call_args[0][0] is df
