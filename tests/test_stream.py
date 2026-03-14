"""Tests for pymarketstore.stream.StreamConn."""

import re
from unittest.mock import MagicMock, call, patch

import msgpack
import pytest

from pymarketstore.stream import StreamConn

from websocket import ABNF


# ---------------------------------------------------------------------------
# Registration / deregistration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_register_with_string_compiles_pattern(self):
        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()

        conn.register(r"^BTC/", handler)

        patterns = list(conn._handlers.keys())
        assert len(patterns) == 1
        assert isinstance(patterns[0], re.Pattern)
        assert patterns[0].pattern == r"^BTC/"
        assert conn._handlers[patterns[0]] is handler

    def test_register_with_compiled_pattern(self):
        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        pat = re.compile(r"^ETH/")

        conn.register(pat, handler)

        assert pat in conn._handlers
        assert conn._handlers[pat] is handler

    def test_on_decorator_registers_handler(self):
        conn = StreamConn("ws://localhost:5993/ws")

        @conn.on(r"^BTC/")
        def handler(conn, msg):
            pass

        assert any(pat.pattern == r"^BTC/" for pat in conn._handlers)
        # The original function is returned (not wrapped)
        assert handler.__name__ == "handler"

    def test_deregister_removes_handler(self):
        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^BTC/", handler)

        conn.deregister(r"^BTC/")

        assert len(conn._handlers) == 0

    def test_deregister_nonexistent_raises(self):
        """Unlike AsyncStreamConn, sync StreamConn uses del which raises KeyError."""
        conn = StreamConn("ws://localhost:5993/ws")

        with pytest.raises(KeyError):
            conn.deregister(r"^NONEXISTENT/")


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_matching_handler_is_called(self):
        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^BTC/", handler)

        msg = {"key": "BTC/1Min/OHLCV", "Open": 100.0}
        conn._dispatch("BTC/1Min/OHLCV", msg)

        handler.assert_called_once_with(conn, msg)

    def test_non_matching_handler_not_called(self):
        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^ETH/", handler)

        conn._dispatch("BTC/1Min/OHLCV", {"key": "BTC/1Min/OHLCV"})

        handler.assert_not_called()

    def test_multiple_handlers_all_called(self):
        conn = StreamConn("ws://localhost:5993/ws")
        handler_a = MagicMock()
        handler_b = MagicMock()
        conn.register(r"^BTC/", handler_a)
        conn.register(r".*1Min.*", handler_b)

        msg = {"key": "BTC/1Min/OHLCV"}
        conn._dispatch("BTC/1Min/OHLCV", msg)

        handler_a.assert_called_once_with(conn, msg)
        handler_b.assert_called_once_with(conn, msg)


# ---------------------------------------------------------------------------
# Connect and subscribe
# ---------------------------------------------------------------------------


class TestConnectAndSubscribe:
    @patch("pymarketstore.stream.websocket.WebSocket")
    def test_connect_calls_endpoint(self, MockWebSocket):
        mock_ws = MockWebSocket.return_value
        conn = StreamConn("ws://localhost:5993/ws")

        ws = conn._connect()

        mock_ws.connect.assert_called_once_with("ws://localhost:5993/ws")
        assert ws is mock_ws

    @patch("pymarketstore.stream.websocket.WebSocket")
    def test_subscribe_sends_msgpack_binary(self, MockWebSocket):
        mock_ws = MockWebSocket.return_value
        conn = StreamConn("ws://localhost:5993/ws")

        conn._subscribe(mock_ws, ["BTC/*/*", "ETH/*/*"])

        expected = msgpack.dumps({"streams": ["BTC/*/*", "ETH/*/*"]})
        mock_ws.send.assert_called_once_with(expected, opcode=ABNF.OPCODE_BINARY)


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


class TestRun:
    @patch("pymarketstore.stream.websocket.WebSocket")
    def test_run_dispatches_messages(self, MockWebSocket):
        mock_ws = MockWebSocket.return_value

        msg1 = {"key": "BTC/1Min/OHLCV", "Open": 100.0}
        msg2 = {"key": "ETH/1Min/OHLCV", "Open": 200.0}

        # recv returns msgpack bytes, then raises to exit the loop
        mock_ws.recv.side_effect = [
            msgpack.dumps(msg1),
            msgpack.dumps(msg2),
            KeyboardInterrupt,
        ]

        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r".*", handler)

        with pytest.raises(KeyboardInterrupt):
            conn.run(["BTC/*/*"])

        assert handler.call_count == 2
        handler.assert_any_call(conn, msg1)
        handler.assert_any_call(conn, msg2)
        mock_ws.close.assert_called_once()

    @patch("pymarketstore.stream.websocket.WebSocket")
    def test_run_ignores_messages_without_key(self, MockWebSocket):
        mock_ws = MockWebSocket.return_value

        msg_no_key = {"data": "something"}
        mock_ws.recv.side_effect = [
            msgpack.dumps(msg_no_key),
            KeyboardInterrupt,
        ]

        conn = StreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r".*", handler)

        with pytest.raises(KeyboardInterrupt):
            conn.run(["BTC/*/*"])

        handler.assert_not_called()
        mock_ws.close.assert_called_once()

    @patch("pymarketstore.stream.websocket.WebSocket")
    def test_run_closes_ws_on_error(self, MockWebSocket):
        """ws.close() is called even if recv raises."""
        mock_ws = MockWebSocket.return_value
        mock_ws.recv.side_effect = ConnectionError("dropped")

        conn = StreamConn("ws://localhost:5993/ws")

        with pytest.raises(ConnectionError):
            conn.run(["BTC/*/*"])

        mock_ws.close.assert_called_once()
