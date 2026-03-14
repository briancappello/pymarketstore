"""
Tests for pymarketstore.async_stream.AsyncStreamConn.

The WebSocket layer is mocked with AsyncMock so no real network connections
are made.  Each test that exercises run() drives it by controlling what the
mock WebSocket yields and then cancelling the task (or letting stop() exit it).
"""

from __future__ import annotations

import asyncio
import re

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import msgpack
import pytest

from pymarketstore.async_stream import AsyncStreamConn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pack_confirm(streams=None, error=None):
    """Build a msgpack-encoded subscription confirmation message."""
    if error is not None:
        return msgpack.packb({"error": error})
    return msgpack.packb({"streams": streams or []})


def _pack_msg(key, data):
    """Build a msgpack-encoded data message."""
    return msgpack.packb({"key": key, "data": data})


def _make_ws_connect(recv_messages, *, on_enter=None):
    """
    Return a mock suitable for patching ``ws_connect``.

    ``ws_connect`` is used as ``async with ws_connect(endpoint) as ws``, so
    it must be a callable that returns an async context manager.

    *recv_messages*: list where the first element is the subscription
    confirmation bytes and the rest are data message bytes yielded by the
    async-for loop.

    *on_enter*: optional coroutine called when the context manager is entered,
    e.g. to call ``conn.stop()`` from inside a test.
    """
    ws = MagicMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock(
        return_value=recv_messages[0] if recv_messages else _pack_confirm()
    )
    ws.close = AsyncMock()

    data_msgs = recv_messages[1:]

    async def _aiter():
        for msg in data_msgs:
            yield msg

    ws.__aiter__ = lambda self_: _aiter()

    @asynccontextmanager
    async def _ctx(endpoint):
        if on_enter:
            await on_enter()
        yield ws

    return _ctx


# ---------------------------------------------------------------------------
# Registration / handler management
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_on_decorator_registers_handler(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()

        conn.on(r"^BTC/")(handler)

        assert any(pat.pattern == r"^BTC/" for pat in conn._handlers)

    def test_register_with_string_compiles_pattern(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()

        conn.register(r"^ETH/", handler)

        patterns = list(conn._handlers.keys())
        assert len(patterns) == 1
        assert isinstance(patterns[0], re.Pattern)
        assert patterns[0].pattern == r"^ETH/"

    def test_register_with_compiled_pattern(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        pat = re.compile(r"^SOL/")

        conn.register(pat, handler)

        assert pat in conn._handlers
        assert conn._handlers[pat] is handler

    def test_deregister_removes_handler(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^BTC/", handler)

        conn.deregister(r"^BTC/")

        assert len(conn._handlers) == 0

    def test_deregister_missing_pattern_does_not_raise(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")

        # Should not raise
        conn.deregister(r"^NONEXISTENT/")


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_matching_handler_is_called(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^BTC/", handler)

        conn._dispatch("BTC/1Min/OHLCV", {"Open": 100.0})

        handler.assert_called_once_with("BTC/1Min/OHLCV", {"Open": 100.0})

    def test_non_matching_handler_is_not_called(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^ETH/", handler)

        conn._dispatch("BTC/1Min/OHLCV", {"Open": 100.0})

        handler.assert_not_called()

    def test_multiple_matching_handlers_all_called(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler_a = MagicMock()
        handler_b = MagicMock()
        conn.register(r"^BTC/", handler_a)
        conn.register(r".*1Min.*", handler_b)

        conn._dispatch("BTC/1Min/OHLCV", {"Open": 100.0})

        handler_a.assert_called_once_with("BTC/1Min/OHLCV", {"Open": 100.0})
        handler_b.assert_called_once_with("BTC/1Min/OHLCV", {"Open": 100.0})

    def test_handler_exception_is_caught_and_logged(self, caplog):
        conn = AsyncStreamConn("ws://localhost:5993/ws")

        def bad_handler(key, data):
            raise RuntimeError("boom")

        conn.register(r"^BTC/", bad_handler)

        import logging

        with caplog.at_level(logging.ERROR, logger="pymarketstore.async_stream"):
            conn._dispatch("BTC/1Min/OHLCV", {})

        assert "boom" in caplog.text


# ---------------------------------------------------------------------------
# run() — happy path
# ---------------------------------------------------------------------------


class TestRunHappyPath:
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_sends_subscription_message(self, mock_ws_connect):
        conn = AsyncStreamConn("ws://localhost:5993/ws")

        async def stop_conn():
            await conn.stop()

        mock_ws_connect.side_effect = _make_ws_connect(
            [_pack_confirm(streams=["BTC/*/*"])], on_enter=stop_conn
        )

        # Grab a reference to the inner ws mock for assertions
        captured_ws = []
        real_side = mock_ws_connect.side_effect

        @asynccontextmanager
        async def capturing_ctx(endpoint):
            async with real_side(endpoint) as ws:
                captured_ws.append(ws)
                yield ws

        mock_ws_connect.side_effect = capturing_ctx

        await conn.run(["BTC/*/*"])

        expected = msgpack.packb({"streams": ["BTC/*/*"]})
        captured_ws[0].send.assert_called_once_with(expected)

    @patch("pymarketstore.async_stream.ws_connect")
    async def test_received_messages_dispatched_to_handler(self, mock_ws_connect):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        handler = MagicMock()
        conn.register(r"^BTC/", handler)

        data = {"Open": 4370.0, "Close": 4371.74}

        async def _empty_aiter():
            return
            yield

        @asynccontextmanager
        async def fake_ctx(endpoint):
            ws = MagicMock()
            ws.send = AsyncMock()
            ws.close = AsyncMock()
            ws.recv = AsyncMock(return_value=_pack_confirm(streams=["BTC/*/*"]))

            async def _data_then_stop():
                yield _pack_msg("BTC/1Min/OHLCV", data)
                await conn.stop()

            ws.__aiter__ = lambda self_: _data_then_stop()
            yield ws

        mock_ws_connect.side_effect = fake_ctx

        await conn.run(["BTC/*/*"])

        handler.assert_called_once_with("BTC/1Min/OHLCV", data)

    @patch("pymarketstore.async_stream.ws_connect")
    async def test_subscription_failures_reset_on_success(self, mock_ws_connect):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        conn._subscription_failures = 2  # simulate prior failures

        async def stop_conn():
            await conn.stop()

        mock_ws_connect.side_effect = _make_ws_connect(
            [_pack_confirm(streams=["BTC/*/*"])], on_enter=stop_conn
        )

        await conn.run(["BTC/*/*"])

        assert conn._subscription_failures == 0


# ---------------------------------------------------------------------------
# run() — reconnection on network error
# ---------------------------------------------------------------------------


class TestRunReconnection:
    @patch("pymarketstore.async_stream.asyncio.sleep", new_callable=AsyncMock)
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_network_error_triggers_reconnect(self, mock_ws_connect, mock_sleep):
        conn = AsyncStreamConn("ws://localhost:5993/ws", reconnect_delay=3.0)
        connect_count = 0

        @asynccontextmanager
        async def fake_connect(endpoint):
            nonlocal connect_count
            connect_count += 1
            if connect_count == 1:
                raise OSError("connection refused")
            await conn.stop()
            yield _make_ws_connect([_pack_confirm()])(endpoint)

        # Use a proper context manager per call
        attempt = 0
        ws_obj = MagicMock()
        ws_obj.send = AsyncMock()
        ws_obj.recv = AsyncMock(return_value=_pack_confirm())
        ws_obj.close = AsyncMock()

        async def _empty_aiter():
            return
            yield

        ws_obj.__aiter__ = lambda self_: _empty_aiter()

        @asynccontextmanager
        async def fake_ctx(endpoint):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise OSError("connection refused")
            await conn.stop()
            yield ws_obj

        mock_ws_connect.side_effect = fake_ctx

        await conn.run(["BTC/*/*"])

        assert attempt == 2
        mock_sleep.assert_any_call(3.0)

    @patch("pymarketstore.async_stream.asyncio.sleep", new_callable=AsyncMock)
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_stop_during_reconnect_delay_exits_cleanly(
        self, mock_ws_connect, mock_sleep
    ):
        conn = AsyncStreamConn("ws://localhost:5993/ws", reconnect_delay=5.0)

        async def fake_sleep(delay):
            conn._running = False

        mock_sleep.side_effect = fake_sleep

        @asynccontextmanager
        async def fake_ctx(endpoint):
            raise OSError("dropped")
            yield  # make it a generator

        mock_ws_connect.side_effect = fake_ctx

        await conn.run(["BTC/*/*"])

        assert mock_ws_connect.call_count == 1


# ---------------------------------------------------------------------------
# run() — subscription error handling
# ---------------------------------------------------------------------------


class TestRunSubscriptionErrors:
    @patch("pymarketstore.async_stream.asyncio.sleep", new_callable=AsyncMock)
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_single_subscription_rejection_retries(
        self, mock_ws_connect, mock_sleep
    ):
        conn = AsyncStreamConn(
            "ws://localhost:5993/ws",
            reconnect_delay=1.0,
            max_subscription_retries=3,
        )
        attempt = 0

        async def _empty_aiter():
            return
            yield

        @asynccontextmanager
        async def fake_ctx(endpoint):
            nonlocal attempt
            attempt += 1
            ws = MagicMock()
            ws.send = AsyncMock()
            ws.close = AsyncMock()
            ws.__aiter__ = lambda self_: _empty_aiter()
            if attempt == 1:
                ws.recv = AsyncMock(return_value=_pack_confirm(error="bad stream"))
            else:
                ws.recv = AsyncMock(return_value=_pack_confirm(streams=["BTC/*/*"]))
                await conn.stop()
            yield ws

        mock_ws_connect.side_effect = fake_ctx

        await conn.run(["BTC/*/*"])

        assert attempt == 2
        mock_sleep.assert_any_call(1.0)

    @patch("pymarketstore.async_stream.asyncio.sleep", new_callable=AsyncMock)
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_max_subscription_retries_raises(self, mock_ws_connect, mock_sleep):
        conn = AsyncStreamConn(
            "ws://localhost:5993/ws",
            reconnect_delay=1.0,
            max_subscription_retries=3,
        )

        async def _empty_aiter():
            return
            yield

        @asynccontextmanager
        async def fake_ctx(endpoint):
            ws = MagicMock()
            ws.send = AsyncMock()
            ws.close = AsyncMock()
            ws.recv = AsyncMock(return_value=_pack_confirm(error="bad stream"))
            ws.__aiter__ = lambda self_: _empty_aiter()
            yield ws

        mock_ws_connect.side_effect = fake_ctx

        with pytest.raises(ConnectionError, match="bad stream"):
            await conn.run(["BTC/*/*"])

        assert conn._subscription_failures == conn.max_subscription_retries

    @patch("pymarketstore.async_stream.asyncio.sleep", new_callable=AsyncMock)
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_subscription_failure_count_resets_after_success(
        self, mock_ws_connect, mock_sleep
    ):
        conn = AsyncStreamConn(
            "ws://localhost:5993/ws",
            reconnect_delay=1.0,
            max_subscription_retries=3,
        )
        attempt = 0

        async def _empty_aiter():
            return
            yield

        @asynccontextmanager
        async def fake_ctx(endpoint):
            nonlocal attempt
            attempt += 1
            ws = MagicMock()
            ws.send = AsyncMock()
            ws.close = AsyncMock()
            ws.__aiter__ = lambda self_: _empty_aiter()
            if attempt <= 2:
                ws.recv = AsyncMock(return_value=_pack_confirm(error="bad stream"))
            else:
                ws.recv = AsyncMock(return_value=_pack_confirm(streams=["BTC/*/*"]))
                await conn.stop()
            yield ws

        mock_ws_connect.side_effect = fake_ctx

        await conn.run(["BTC/*/*"])

        assert conn._subscription_failures == 0

    @patch("pymarketstore.async_stream.asyncio.sleep", new_callable=AsyncMock)
    @patch("pymarketstore.async_stream.ws_connect")
    async def test_network_errors_do_not_count_toward_subscription_retries(
        self, mock_ws_connect, mock_sleep
    ):
        """Network errors must not increment _subscription_failures."""
        conn = AsyncStreamConn(
            "ws://localhost:5993/ws",
            reconnect_delay=1.0,
            max_subscription_retries=2,
        )
        attempt = 0

        async def _empty_aiter():
            return
            yield

        @asynccontextmanager
        async def fake_ctx(endpoint):
            nonlocal attempt
            attempt += 1
            if attempt <= 3:
                raise OSError("network drop")
            ws = MagicMock()
            ws.send = AsyncMock()
            ws.close = AsyncMock()
            ws.recv = AsyncMock(return_value=_pack_confirm(streams=["BTC/*/*"]))
            ws.__aiter__ = lambda self_: _empty_aiter()
            await conn.stop()
            yield ws

        mock_ws_connect.side_effect = fake_ctx

        # Should NOT raise even though we had 3 network errors (> max_subscription_retries=2)
        await conn.run(["BTC/*/*"])

        assert conn._subscription_failures == 0


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


class TestStop:
    async def test_stop_sets_running_false_and_closes_ws(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        mock_ws = AsyncMock()
        conn._ws = mock_ws
        conn._running = True

        await conn.stop()

        assert conn._running is False
        mock_ws.close.assert_called_once()
        assert conn._ws is None

    async def test_stop_without_active_ws_does_not_raise(self):
        conn = AsyncStreamConn("ws://localhost:5993/ws")
        conn._ws = None

        await conn.stop()

        assert conn._running is False
