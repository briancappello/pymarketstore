import pytest

import pymarketstore as pymkts

from pymarketstore.params import DataShape, DataType, Params


def test_init():
    p = pymkts.Params("TSLA", "1Min", "OHLCV", 1500000000, 4294967296)
    tbk = "TSLA/1Min/OHLCV"
    assert p.tbk == tbk


def test_init_single_symbol_is_wrapped():
    """A single symbol string should produce the same tbk as a one-element list."""
    p = Params("AAPL", "1D", "OHLCV")
    assert p.tbk == "AAPL/1D/OHLCV"


def test_to_query_request():
    p = pymkts.Params("TSLA", "1Min", "OHLCV", 1500000000, 4294967296)
    assert p.to_query_request() == {
        "destination": "TSLA/1Min/OHLCV",
        "epoch_start": 1500000000,
        "epoch_end": 4294967296,
    }

    p2 = pymkts.Params(
        symbols=["FORD", "TSLA"],
        timeframe="5Min",
        attrgroup="OHLCV",
        start=1000000000,
        end=4294967296,
        limit=200,
        limit_from_start=False,
    )
    assert p2.to_query_request() == {
        "destination": "FORD,TSLA/5Min/OHLCV",
        "epoch_start": 1000000000,
        "epoch_end": 4294967296,
        "limit_record_count": 200,
        "limit_from_start": False,
    }


def test_to_query_request_minimal():
    """Query without start/end/limit only contains destination."""
    p = Params("BTC", "1Min", "OHLCV")
    result = p.to_query_request()
    assert result == {"destination": "BTC/1Min/OHLCV"}
    assert "epoch_start" not in result
    assert "epoch_end" not in result
    assert "limit_record_count" not in result


def test_set():
    """Params.set() should update the attribute and return self for chaining."""
    p = Params("BTC", "1Min", "OHLCV")
    result = p.set("limit", 500)
    assert p.limit == 500
    assert result is p  # returns self

    p.set("start", 1500000000)
    assert p.start is not None


def test_set_invalid_key_raises():
    p = Params("BTC", "1Min", "OHLCV")
    with pytest.raises(AttributeError):
        p.set("nonexistent_key", "value")


def test_repr():
    p = Params("BTC", "1Min", "OHLCV")
    r = repr(p)
    assert "Params(" in r
    assert "BTC/1Min/OHLCV" in r


# ---------------------------------------------------------------------------
# DataShape
# ---------------------------------------------------------------------------


class TestDataShape:
    def test_init_from_list(self):
        ds = DataShape([("Epoch", "i8"), ("Open", "f8")])
        cols = list(ds)
        assert len(cols) == 2
        assert cols[0] == ("Epoch", DataType.i8)
        assert cols[1] == ("Open", DataType.f8)

    def test_add(self):
        ds = DataShape()
        ds.add("Epoch", "i8")
        ds.add("Price", DataType.float64)
        cols = list(ds)
        assert len(cols) == 2
        assert cols[0] == ("Epoch", DataType.i8)
        assert cols[1] == ("Price", DataType.float64)

    def test_add_duplicate_raises(self):
        ds = DataShape()
        ds.add("Epoch", "i8")
        with pytest.raises(ValueError, match="Epoch"):
            ds.add("Epoch", "i8")

    def test_empty(self):
        ds = DataShape()
        assert list(ds) == []


# ---------------------------------------------------------------------------
# DataType
# ---------------------------------------------------------------------------


class TestDataType:
    def test_aliases(self):
        """i8 and int64 should map to the same value."""
        assert DataType.i8.value == DataType.int64.value == "INT64"
        assert DataType.f4.value == DataType.float32.value == "FLOAT32"
        assert DataType.f8.value == DataType.float64.value == "FLOAT64"
