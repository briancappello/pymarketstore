from datetime import date, datetime
from unittest.mock import MagicMock, patch

import numpy as np

import pymarketstore as pymkts

from pymarketstore.results import QueryReply

from tests.utils import parametrize


@parametrize(
    dict(
        endpoint="http://127.0.0.1:5994/rpc",
        grpc=False,
        expect_endpoint="http://127.0.0.1:5994/rpc",
        instance=pymkts.jsonrpc_client.JsonRpcClient,
    ),
    dict(
        endpoint="http://192.168.1.10:5993/rpc",
        grpc=True,
        expect_endpoint="192.168.1.10:5995",
        instance=pymkts.grpc_client.GRPCClient,
    ),
    dict(
        endpoint="localhost:5996",
        grpc=True,
        expect_endpoint="localhost:5996",
        instance=pymkts.grpc_client.GRPCClient,
    ),
)
def test_client_init(endpoint, grpc, expect_endpoint, instance):
    # --- when ---
    c = pymkts.Client(endpoint, grpc)

    # --- then ---
    assert expect_endpoint == c.endpoint
    assert instance == c.client.__class__


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_query(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    # Simulate a raw server response that decode_responses can process
    mock_rpc.call.return_value = {
        "timezone": "UTC",
        "responses": [
            {
                "result": {
                    "data": [
                        b"\xf4\xe8^Z\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00Y@",
                        b"\x00\x00\x00\x00\x00@Y@",
                        b"\x00\x00\x00\x00\x00\x00X@",
                        b"\x00\x00\x00\x00\x00 Y@",
                        b"\x00\x00\x00\x00\x00\x88\xc3@",
                    ],
                    "length": 1,
                    "lengths": {"BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup": 1},
                    "names": ["Epoch", "Open", "High", "Low", "Close", "Volume"],
                    "startindex": {"BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup": 0},
                    "types": ["i8", "f8", "f8", "f8", "f8", "f8"],
                }
            }
        ],
    }

    c = pymkts.Client()
    p = pymkts.Params("BTC", "1Min", "OHLCV")
    result = c.query(p)

    # Verify the correct RPC method and arguments were used
    mock_rpc.call.assert_called_once_with(
        "DataService.Query",
        requests=[p.to_query_request()],
    )
    # Verify the result is a properly decoded QueryReply
    assert isinstance(result, QueryReply)
    assert result.timezone == "UTC"
    assert result.first().symbol == "BTC"


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_create(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"responses": []}

    c = pymkts.Client()
    dtype = [("Epoch", "i8"), ("Bid", "f4"), ("Ask", "f4")]
    tbk = "TEST/1Min/TICK"
    result = c.create(tbk=tbk, data_shape=pymkts.DataShape(dtype))

    mock_rpc.call.assert_called_once_with(
        "DataService.Create",
        requests=[
            {
                "key": "TEST/1Min/TICK",
                "data_shapes": "Epoch/int64:Bid/float32:Ask/float32",
                "row_type": "fixed",
            }
        ],
    )
    assert result == {"responses": []}


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_write(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"responses": []}

    c = pymkts.Client()
    data = np.array([(1, 0)], dtype=[("Epoch", "i8"), ("Ask", "f4")])
    tbk = "TEST/1Min/TICK"
    result = c.write(data, tbk)

    # Verify the call was made with correct method and data structure
    mock_rpc.call.assert_called_once()
    call_args = mock_rpc.call.call_args
    assert call_args[0][0] == "DataService.Write"
    req = call_args[1]["requests"][0]
    assert req["is_variable_length"] is False
    assert req["dataset"]["names"] == ["Epoch", "Ask"]
    assert req["dataset"]["types"] == ["i8", "f4"]
    assert req["dataset"]["startindex"] == {"TEST/1Min/TICK": 0}
    assert req["dataset"]["lengths"] == {"TEST/1Min/TICK": 1}
    assert result == {"responses": []}


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["AAPL", "TSLA", "BTC"]}

    c = pymkts.Client()
    result = c.list_symbols()

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="symbol",
    )
    assert result == ["AAPL", "TSLA", "BTC"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_with_timeframe(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["AAPL", "TSLA"]}

    c = pymkts.Client()
    result = c.list_symbols(timeframe="1Min")

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="symbol",
        timeframe="1Min",
    )
    assert result == ["AAPL", "TSLA"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_with_date_int(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["BTC"]}

    c = pymkts.Client()
    result = c.list_symbols(date=1705276800)  # 2024-01-15 00:00:00 UTC

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="symbol",
        date="2024-01-15",
    )
    assert result == ["BTC"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_with_date_string(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["BTC"]}

    c = pymkts.Client()
    result = c.list_symbols(date="2024-01-15")

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="symbol",
        date="2024-01-15",
    )
    assert result == ["BTC"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_with_date_datetime(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["ETH"]}

    c = pymkts.Client()
    result = c.list_symbols(date=datetime(2024, 1, 15))

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="symbol",
        date="2024-01-15",
    )
    assert result == ["ETH"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_with_timeframe_and_date(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["AAPL"]}

    c = pymkts.Client()
    result = c.list_symbols(timeframe="1Min", date="2024-01-15")

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="symbol",
        timeframe="1Min",
        date="2024-01-15",
    )
    assert result == ["AAPL"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_empty_response(MockRpcClient):
    """list_symbols should return empty list when server returns no Results."""
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {}

    c = pymkts.Client()
    result = c.list_symbols()

    assert result == []


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_list_symbols_tbk_format(MockRpcClient):
    """list_symbols with TBK format should pass format='tbk'."""
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"Results": ["AAPL/1Min/OHLCV", "TSLA/1D/OHLCV"]}

    c = pymkts.Client()
    result = c.list_symbols(fmt=pymkts.ListSymbolsFormat.TBK)

    mock_rpc.call.assert_called_once_with(
        "DataService.ListSymbols",
        format="tbk",
    )
    assert result == ["AAPL/1Min/OHLCV", "TSLA/1D/OHLCV"]


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_destroy(MockRpcClient):
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"responses": []}

    c = pymkts.Client()
    tbk = "TEST/1Min/TICK"
    result = c.destroy(tbk)

    mock_rpc.call.assert_called_once_with(
        "DataService.Destroy",
        requests=[{"key": "TEST/1Min/TICK"}],
    )
    assert result == {"responses": []}


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_server_version(MockRpcClient):
    """Client.server_version() delegates to the underlying client."""
    mock_rpc = MockRpcClient.return_value

    # JsonRpcClient.server_version does a requests.head, not rpc.call
    # So we need to mock at the requests level
    with patch("pymarketstore.jsonrpc_client.requests") as mock_requests:
        mock_requests.Session.return_value = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"Marketstore-Version": "1.2.3"}
        mock_requests.head.return_value = mock_resp

        c = pymkts.Client()
        result = c.server_version()

        mock_requests.head.assert_called_once_with("http://localhost:5993/rpc")
        assert result == "1.2.3"


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_create_with_list_datashape(MockRpcClient):
    """Client.create() should accept a list of tuples and convert to DataShape."""
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"responses": []}

    c = pymkts.Client()
    dtype = [("Epoch", "i8"), ("Price", "f8")]
    result = c.create(tbk="SYM/1D/OHLCV", data_shape=dtype)

    mock_rpc.call.assert_called_once()
    call_args = mock_rpc.call.call_args
    assert call_args[0][0] == "DataService.Create"
    req = call_args[1]["requests"][0]
    assert req["key"] == "SYM/1D/OHLCV"
    assert req["data_shapes"] == "Epoch/int64:Price/float64"
    assert req["row_type"] == "fixed"
    assert result == {"responses": []}


@patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
def test_write_with_variable_length(MockRpcClient):
    """Client.write() with is_variable_length=True should forward the flag."""
    mock_rpc = MockRpcClient.return_value
    mock_rpc.call.return_value = {"responses": []}

    c = pymkts.Client()
    data = np.array([(1, 0)], dtype=[("Epoch", "i8"), ("Ask", "f4")])
    c.write(data, "TEST/1Min/TICK", is_variable_length=True)

    call_args = mock_rpc.call.call_args
    req = call_args[1]["requests"][0]
    assert req["is_variable_length"] is True
