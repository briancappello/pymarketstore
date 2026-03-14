from ast import literal_eval
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from pymarketstore import results
from pymarketstore.results import (
    DataSet,
    QueryReply,
    QueryResult,
    decode,
    decode_grpc_responses,
)


testdata1 = literal_eval(r"""
{'responses': [{'result': {'data': [b'\xf4\xe8^Z\x00\x00\x00\x000\xe9^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00',
     b'{\x14\xaeG\x01\xf4\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@',
     b'{\x14\xaeG\x01\xf4\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\xfe\xc5@',
     b'\x85\xebQ\xb8^\xe0\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@{\x14\xaeG\x01\xfa\xc5@',
     b'H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\xfe\xfd\xc5@',
     b'iL\xd2F\xbf\xaf\n@\xfe\xe6\xff49\xfd\x0b@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xd7\xd2\x8a\x0c\xfe\x00\xf9?'],
    'length': 5,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'UTC',
 'version': 'dev'}
""")  # noqa: E501

testdata2 = literal_eval(r"""
{'responses': [{'result': {'data': [b'l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00 \xea^Z\x00\x00\x00\x00\\\xea^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00 \xea^Z\x00\x00\x00\x00\\\xea^Z\x00\x00\x00\x00',
     b'\x00\x00\x00\x00\x00\x88\x8f@)\\\x8f\xc2\xf5\x90\x8f@\xa4p=\n\xd7\x8f\x8f@\xcd\xcc\xcc\xcc\xcc\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x00\x00\x00\x00 \x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\x00\x00\x00\x00\x00\xb0\x8f@fffff\xa2\x8f@\x00\x00\x00\x00\x00\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\x00\x00\x00\x00\x00\x88\x8f@{\x14\xaeG\xe1\x84\x8f@\xa4p=\n\xd7\x8f\x8f@\xf6(\\\x8f\xc2\xa7\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@\\\x8f\xc2\xf5h\xf0\xc5@\x00\x00\x00\x00 \x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\xd7\xa3p=\n\x99\x8f@\xa4p=\n\xd7\x8f\x8f@\x00\x00\x00\x00\x00\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\x1e\x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'f\r\x83\x9erg8@j\xa8\xcd\x0f\x8e\xdf<@\x7f\xc7\xa6Ku\xbcP@AG\xe5\x05\xdc\x1aU@\xdc\xb1d\xd0\x012+@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xa2\x9a\xa3\xd8\x1bb\x19@s!\xc1\x1a\x88\xa9/@\xbaI\x0c\x02+\x87\xf4?'],
    'length': 10,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5,
     'ETH/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5,
     'ETH/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'America/New_York',
 'version': 'dev'}
""")  # noqa: E501

btc_array = results.decode_responses(testdata1["responses"])[0]["BTC/1Min/OHLCV"]
btc_bytes = testdata1["responses"][0]["result"]["data"]
btc_df = pd.DataFrame(btc_array).set_index("Epoch")
btc_df.index = pd.DatetimeIndex(btc_df.index * 10**9, tz="UTC")


def test_results():
    reply = results.QueryReply.from_response(testdata1)
    assert reply.timezone == "UTC"
    assert (
        str(reply)
        == """QueryReply(QueryResult(DataSet(key=BTC/1Min/OHLCV, shape=(5,), dtype=[('Epoch', '<i8'), ('Open', '<f8'), ('High', '<f8'), ('Low', '<f8'), ('Close', '<f8'), ('Volume', '<f8')])))"""
    )  # noqa
    assert reply.first().timezone == "UTC"
    assert reply.first().symbol == "BTC"
    assert reply.first().timeframe == "1Min"
    assert reply.first().attribute_group == "OHLCV"
    assert reply.first().df().shape == (5, 5)
    assert list(reply.by_symbols().keys()) == ["BTC"]
    assert reply.keys() == ["BTC/1Min/OHLCV"]
    assert reply.symbols() == ["BTC"]
    assert reply.timeframes() == ["1Min"]

    reply = results.QueryReply.from_response(testdata2)
    assert str(reply.first().df().index.tzinfo) == "America/New_York"


# ---------------------------------------------------------------------------
# decode() unit tests
# ---------------------------------------------------------------------------


class TestDecode:
    def test_basic_decode(self):
        """decode() correctly builds a structured numpy array from column data."""
        epochs = np.array([1000, 2000, 3000], dtype="i8")
        prices = np.array([100.0, 200.0, 300.0], dtype="f8")

        arr = decode(
            column_names=["Epoch", "Price"],
            column_types=["i8", "f8"],
            column_data=[bytes(memoryview(epochs)), bytes(memoryview(prices))],
            data_length=3,
        )

        assert arr.dtype.names == ("Epoch", "Price")
        assert list(arr["Epoch"]) == [1000, 2000, 3000]
        assert list(arr["Price"]) == [100.0, 200.0, 300.0]

    def test_decode_single_column(self):
        data = np.array([42], dtype="i8")
        arr = decode(
            column_names=["Val"],
            column_types=["i8"],
            column_data=[bytes(memoryview(data))],
            data_length=1,
        )
        assert arr["Val"][0] == 42


# ---------------------------------------------------------------------------
# decode_grpc_responses()
# ---------------------------------------------------------------------------


class TestDecodeGrpcResponses:
    def test_single_symbol(self):
        """Decode a gRPC response with one symbol."""
        # Build the same data as testdata1 but in gRPC object form
        epochs = np.array([1518048500, 1518048560, 1518048620], dtype="i8")
        opens = np.array([100.0, 101.0, 102.0], dtype="f8")

        data_ns = SimpleNamespace(
            column_names=["Epoch", "Open"],
            column_types=["i8", "f8"],
            column_data=[bytes(memoryview(epochs)), bytes(memoryview(opens))],
            length=3,
        )
        packed_ns = SimpleNamespace(
            data=data_ns,
            start_index={"BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup": 0},
            lengths={"BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup": 3},
        )
        response = SimpleNamespace(result=packed_ns)

        decoded = decode_grpc_responses([response])

        assert len(decoded) == 1
        assert "BTC/1Min/OHLCV" in decoded[0]
        arr = decoded[0]["BTC/1Min/OHLCV"]
        assert len(arr) == 3
        assert list(arr["Open"]) == [100.0, 101.0, 102.0]

    def test_from_grpc_response(self):
        """QueryReply.from_grpc_response() builds a proper QueryReply."""
        epochs = np.array([1518048500], dtype="i8")
        opens = np.array([100.0], dtype="f8")

        data_ns = SimpleNamespace(
            column_names=["Epoch", "Open"],
            column_types=["i8", "f8"],
            column_data=[bytes(memoryview(epochs)), bytes(memoryview(opens))],
            length=1,
        )
        packed_ns = SimpleNamespace(
            data=data_ns,
            start_index={"BTC/1D/OHLCV:Symbol/Timeframe/AttributeGroup": 0},
            lengths={"BTC/1D/OHLCV:Symbol/Timeframe/AttributeGroup": 1},
        )
        grpc_resp = SimpleNamespace(
            responses=[SimpleNamespace(result=packed_ns)],
            timezone="UTC",
        )

        reply = QueryReply.from_grpc_response(grpc_resp)

        assert reply.timezone == "UTC"
        assert reply.first().symbol == "BTC"
        assert reply.first().timeframe == "1D"
        assert reply.first().attribute_group == "OHLCV"
        assert reply.first().df().shape == (1, 1)


# ---------------------------------------------------------------------------
# DataSet
# ---------------------------------------------------------------------------


class TestDataSet:
    def test_repr(self):
        arr = np.array([(1, 100.0)], dtype=[("Epoch", "i8"), ("Open", "f8")])
        ds = DataSet(arr, "BTC/1Min/OHLCV", "UTC")
        r = repr(ds)
        assert "BTC/1Min/OHLCV" in r
        assert "(1,)" in r

    def test_df_utc_timezone(self):
        arr = np.array([(1518048500, 100.0)], dtype=[("Epoch", "i8"), ("Open", "f8")])
        ds = DataSet(arr, "BTC/1Min/OHLCV", "UTC")
        df = ds.df()
        assert df.shape == (1, 1)
        assert str(df.index.tz) == "UTC"

    def test_df_non_utc_timezone(self):
        arr = np.array([(1518048500, 100.0)], dtype=[("Epoch", "i8"), ("Open", "f8")])
        ds = DataSet(arr, "BTC/1Min/OHLCV", "America/New_York")
        df = ds.df()
        assert str(df.index.tz) == "America/New_York"


# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------


class TestQueryResult:
    def test_keys_and_first(self):
        arr = np.array([(1, 100.0)], dtype=[("Epoch", "i8"), ("Open", "f8")])
        qr = QueryResult(
            {"BTC/1Min/OHLCV": arr, "ETH/1Min/OHLCV": arr},
            timezone="UTC",
        )
        assert set(qr.keys()) == {"BTC/1Min/OHLCV", "ETH/1Min/OHLCV"}
        assert qr.first().key in {"BTC/1Min/OHLCV", "ETH/1Min/OHLCV"}

    def test_all_returns_all_datasets(self):
        arr = np.array([(1, 100.0)], dtype=[("Epoch", "i8"), ("Open", "f8")])
        qr = QueryResult({"BTC/1Min/OHLCV": arr}, timezone="UTC")
        all_ds = qr.all()
        assert "BTC/1Min/OHLCV" in all_ds
        assert isinstance(all_ds["BTC/1Min/OHLCV"], DataSet)

    def test_repr(self):
        arr = np.array([(1, 100.0)], dtype=[("Epoch", "i8"), ("Open", "f8")])
        qr = QueryResult({"BTC/1Min/OHLCV": arr}, timezone="UTC")
        r = repr(qr)
        assert "QueryResult" in r
        assert "BTC/1Min/OHLCV" in r
