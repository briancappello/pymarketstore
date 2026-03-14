from unittest.mock import MagicMock, patch

import numpy as np

import pymarketstore as pymkts

from pymarketstore import grpc_client
from pymarketstore.proto import marketstore_pb2 as proto
from pymarketstore.proto.marketstore_pb2 import MultiQueryRequest, QueryRequest


def test_grpc_client_init():
    c = pymkts.GRPCClient("127.0.0.1:5995")
    assert c.endpoint == "127.0.0.1:5995"
    assert isinstance(c.stub, grpc_client.MarketstoreStub)


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_query(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    p = pymkts.Params("BTC", "1Min", "OHLCV")

    mock_response = MagicMock()
    mock_response.timezone = "UTC"
    mock_response.responses = []
    c.stub.Query.return_value = mock_response

    # --- when ---
    result = c.query(p)

    # --- then ---
    c.stub.Query.assert_called_once()
    call_arg = c.stub.Query.call_args[0][0]
    assert isinstance(call_arg, MultiQueryRequest)
    assert len(call_arg.requests) == 1
    assert call_arg.requests[0].destination == "BTC/1Min/OHLCV"
    assert isinstance(result, pymkts.results.QueryReply)
    assert result.timezone == "UTC"


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_query_with_multiple_params(stub):
    """query() with a list of Params should build a MultiQueryRequest with all of them."""
    c = pymkts.GRPCClient()
    p1 = pymkts.Params("BTC", "1Min", "OHLCV")
    p2 = pymkts.Params("ETH", "1D", "OHLCV", 1000000000, 2000000000)

    mock_response = MagicMock()
    mock_response.timezone = "UTC"
    mock_response.responses = []
    c.stub.Query.return_value = mock_response

    c.query([p1, p2])

    call_arg = c.stub.Query.call_args[0][0]
    assert len(call_arg.requests) == 2
    assert call_arg.requests[0].destination == "BTC/1Min/OHLCV"
    assert call_arg.requests[1].destination == "ETH/1D/OHLCV"
    assert call_arg.requests[1].epoch_start == 1000000000
    assert call_arg.requests[1].epoch_end == 2000000000


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_create(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    dtype = [("Epoch", "i8"), ("Bid", "f4"), ("Ask", "f4")]
    tbk = "TEST/1Min/TICK"

    mock_response = MagicMock()
    c.stub.Create.return_value = mock_response

    # --- when ---
    result = c.create(tbk=tbk, data_shape=pymkts.DataShape(dtype))

    # --- then ---
    c.stub.Create.assert_called_once()
    call_arg = c.stub.Create.call_args[0][0]
    assert isinstance(call_arg, proto.MultiCreateRequest)
    assert len(call_arg.requests) == 1
    req = call_arg.requests[0]
    assert req.key == "TEST/1Min/TICK"
    assert req.row_type == "fixed"
    # Verify data shapes were correctly built
    shapes = {ds.name: ds.type for ds in req.data_shapes}
    assert shapes == {"Epoch": "INT64", "Bid": "FLOAT32", "Ask": "FLOAT32"}
    assert result is mock_response


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_write(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    data = np.array([(1, 0)], dtype=[("Epoch", "i8"), ("Ask", "f4")])
    tbk = "TEST/1Min/TICK"

    mock_response = MagicMock()
    c.stub.Write.return_value = mock_response

    # --- when ---
    result = c.write(data, tbk)

    # --- then ---
    c.stub.Write.assert_called_once()
    call_arg = c.stub.Write.call_args[0][0]
    assert isinstance(call_arg, proto.MultiWriteRequest)
    assert len(call_arg.requests) == 1
    req = call_arg.requests[0]
    assert req.is_variable_length is False
    assert req.data.start_index == {"TEST/1Min/TICK": 0}
    assert req.data.lengths == {"TEST/1Min/TICK": 1}
    # Verify the data payload was serialized
    assert req.data.data.column_names == ["Epoch", "Ask"]
    assert req.data.data.column_types == ["i8", "f4"]
    assert result is mock_response


def test_build_query():
    # --- given ---
    c = pymkts.GRPCClient(endpoint="127.0.0.1:5995")
    p = pymkts.Params("TSLA", "1Min", "OHLCV", 1500000000, 4294967296)

    # --- when ---
    query = c._build_query([p])

    # --- then ---
    assert query == MultiQueryRequest(
        requests=[
            QueryRequest(
                destination="TSLA/1Min/OHLCV",
                epoch_start=1500000000,
                epoch_end=4294967296,
            )
        ]
    )


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_list_symbols_default(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    mock_response = MagicMock()
    mock_response.results = ["AAPL", "TSLA"]
    c.stub.ListSymbols.return_value = mock_response

    # --- when ---
    result = c.list_symbols()

    # --- then ---
    c.stub.ListSymbols.assert_called_once()
    call_arg = c.stub.ListSymbols.call_args[0][0]
    assert isinstance(call_arg, proto.ListSymbolsRequest)
    assert call_arg.format == proto.ListSymbolsRequest.Format.SYMBOL
    assert result == ["AAPL", "TSLA"]


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_list_symbols_tbk_format(stub):
    """list_symbols with TBK format should send Format.TIME_BUCKET_KEY."""
    c = pymkts.GRPCClient()
    mock_response = MagicMock()
    mock_response.results = ["AAPL/1Min/OHLCV"]
    c.stub.ListSymbols.return_value = mock_response

    result = c.list_symbols(fmt=pymkts.ListSymbolsFormat.TBK)

    call_arg = c.stub.ListSymbols.call_args[0][0]
    assert call_arg.format == proto.ListSymbolsRequest.Format.TIME_BUCKET_KEY
    assert result == ["AAPL/1Min/OHLCV"]


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_list_symbols_with_timeframe_and_date(stub):
    """list_symbols should forward timeframe and date to the protobuf request."""
    c = pymkts.GRPCClient()
    mock_response = MagicMock()
    mock_response.results = ["BTC"]
    c.stub.ListSymbols.return_value = mock_response

    result = c.list_symbols(timeframe="1Min", date="2024-01-15")

    call_arg = c.stub.ListSymbols.call_args[0][0]
    assert call_arg.timeframe == "1Min"
    assert call_arg.date == "2024-01-15"
    assert result == ["BTC"]


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_list_symbols_empty_response(stub):
    """list_symbols should return empty list when server returns falsy response."""
    c = pymkts.GRPCClient()
    c.stub.ListSymbols.return_value = None

    result = c.list_symbols()

    assert result == []


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_destroy(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    tbk = "TEST/1Min/TICK"
    mock_response = MagicMock()
    c.stub.Destroy.return_value = mock_response

    # --- when ---
    result = c.destroy(tbk)

    # --- then ---
    c.stub.Destroy.assert_called_once()
    call_arg = c.stub.Destroy.call_args[0][0]
    assert isinstance(call_arg, proto.MultiKeyRequest)
    assert len(call_arg.requests) == 1
    assert call_arg.requests[0].key == "TEST/1Min/TICK"
    assert result is mock_response


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_server_version(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    mock_response = MagicMock()
    mock_response.version = "2.0.0"
    c.stub.ServerVersion.return_value = mock_response

    # --- when ---
    result = c.server_version()

    # --- then ---
    c.stub.ServerVersion.assert_called_once()
    call_arg = c.stub.ServerVersion.call_args[0][0]
    assert isinstance(call_arg, proto.ServerVersionRequest)
    assert result == "2.0.0"


@patch("pymarketstore.grpc_client.MarketstoreStub")
def test_repr(stub):
    c = pymkts.GRPCClient("myhost:5995")
    assert repr(c) == 'GRPCClient("myhost:5995")'
