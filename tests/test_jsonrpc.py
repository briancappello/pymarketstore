from unittest.mock import MagicMock, patch

import pytest
import requests as real_requests

from pymarketstore import jsonrpc_client
from pymarketstore.jsonrpc_client import JsonRpcClient, MsgpackRpcClient


# ---------------------------------------------------------------------------
# MsgpackRpcClient
# ---------------------------------------------------------------------------


class TestMsgpackRpcClient:
    @patch.object(jsonrpc_client, "requests")
    def test_rpc_request_returns_response(self, mock_requests):
        mock_requests.Session().post.return_value = "dummy_data"

        cli = MsgpackRpcClient("http://localhost:5993/rpc")
        result = cli._rpc_request("DataService.Query", a=1)

        assert result == "dummy_data"

    def test_rpc_response_returns_result(self):
        cli = MsgpackRpcClient("http://localhost:5993/rpc")
        resp = {"jsonrpc": "2.0", "id": 1, "result": {"ok": True}}

        result = cli._rpc_response(resp)

        assert result == {"ok": True}

    def test_rpc_response_raises_on_error(self):
        cli = MsgpackRpcClient("http://localhost:5993/rpc")
        resp = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"message": "Error", "data": "something"},
        }

        with pytest.raises(Exception, match="Error: something"):
            cli._rpc_response(resp)

    def test_rpc_response_raises_on_missing_keys(self):
        """Response without 'result' or 'error' should raise."""
        cli = MsgpackRpcClient("http://localhost:5993/rpc")
        resp = {"jsonrpc": "2.0", "id": 1}

        with pytest.raises(Exception, match="invalid JSON-RPC protocol"):
            cli._rpc_response(resp)

    def test_init_requires_endpoint(self):
        with pytest.raises(ValueError, match="endpoint"):
            MsgpackRpcClient("")


# ---------------------------------------------------------------------------
# JsonRpcClient — error handling
# ---------------------------------------------------------------------------


class TestJsonRpcClientErrorHandling:
    @patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
    def test_connection_error_raises_with_message(self, MockRpcClient):
        mock_rpc = MockRpcClient.return_value
        mock_rpc.call.side_effect = real_requests.exceptions.ConnectionError(
            "connection refused"
        )

        client = JsonRpcClient("http://localhost:5993/rpc")

        with pytest.raises(Exception, match="Could not connect to marketstore"):
            client._request("DataService.Query", requests=[])

    @patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
    def test_http_error_is_reraised(self, MockRpcClient):
        mock_rpc = MockRpcClient.return_value
        mock_rpc.call.side_effect = real_requests.exceptions.HTTPError("500 Server Error")

        client = JsonRpcClient("http://localhost:5993/rpc")

        with pytest.raises(real_requests.exceptions.HTTPError, match="500 Server Error"):
            client._request("DataService.Query", requests=[])

    @patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
    def test_create_invalid_row_type_raises(self, MockRpcClient):
        from pymarketstore.params import DataShape

        client = JsonRpcClient("http://localhost:5993/rpc")

        with pytest.raises(TypeError, match="row_type"):
            client.create("TEST/1Min/TICK", DataShape([("Epoch", "i8")]), row_type="bad")

    @patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
    def test_repr(self, MockRpcClient):
        client = JsonRpcClient("http://myhost:5993/rpc")
        assert repr(client) == 'MsgPackRPCClient("http://myhost:5993/rpc")'

    @patch("pymarketstore.jsonrpc_client.MsgpackRpcClient")
    def test_stream_returns_stream_conn(self, MockRpcClient):
        from pymarketstore.stream import StreamConn

        client = JsonRpcClient("http://localhost:5993/rpc")
        conn = client.stream()

        assert isinstance(conn, StreamConn)
        assert conn.endpoint == "ws://localhost:5993/ws"
