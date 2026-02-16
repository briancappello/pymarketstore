"""
Async WebSocket streaming client for MarketStore.

Uses the ``websockets`` library (async) instead of ``websocket-client`` (sync),
making it compatible with asyncio event loops such as NautilusTrader's.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from typing import Any

import msgpack

try:
    import websockets
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    raise ImportError(
        "The 'websockets' package is required for async streaming. "
        "Install it with: pip install websockets"
    )

logger = logging.getLogger(__name__)


class AsyncStreamConn:
    """
    Async WebSocket streaming client for MarketStore.

    Connects to MarketStore's ``/ws`` endpoint, subscribes to stream
    patterns, and dispatches incoming data payloads to registered handlers.

    Parameters
    ----------
    endpoint : str
        The WebSocket endpoint URL (e.g., ``"ws://localhost:5993/ws"``).
    reconnect_delay : float, default 3.0
        Seconds to wait before attempting reconnection after a disconnect.

    """

    def __init__(
        self,
        endpoint: str,
        reconnect_delay: float = 3.0,
    ) -> None:
        self.endpoint = endpoint
        self.reconnect_delay = reconnect_delay
        self._handlers: dict[re.Pattern, Callable] = {}
        self._ws: Any | None = None
        self._running = False
        self._streams: list[str] = []

    def on(self, stream_pat: str) -> Callable:
        """
        Decorator to register a handler for streams matching a regex pattern.

        Parameters
        ----------
        stream_pat : str
            A regex pattern matched against the stream key (e.g., ``r"^BTC"``).

        """
        def decorator(func: Callable) -> Callable:
            self.register(stream_pat, func)
            return func
        return decorator

    def register(self, stream_pat: str | re.Pattern, func: Callable) -> None:
        """
        Register a handler for streams matching the given pattern.

        The handler signature should be ``handler(key: str, data: dict)``.

        Parameters
        ----------
        stream_pat : str or re.Pattern
            Regex pattern to match against the message key.
        func : Callable
            Handler function called with ``(key, data)`` for each matching message.

        """
        if isinstance(stream_pat, str):
            stream_pat = re.compile(stream_pat)
        self._handlers[stream_pat] = func

    def deregister(self, stream_pat: str | re.Pattern) -> None:
        """Remove a previously registered handler."""
        if isinstance(stream_pat, str):
            stream_pat = re.compile(stream_pat)
        self._handlers.pop(stream_pat, None)

    async def run(self, streams: list[str]) -> None:
        """
        Connect, subscribe, and receive messages in a loop.

        This coroutine runs until ``stop()`` is called or the task is cancelled.
        It will automatically reconnect on connection loss.

        Parameters
        ----------
        streams : list[str]
            MarketStore stream patterns to subscribe to
            (e.g., ``["BTCUSDT/1Min/OHLCV", "*/1D/OHLCV"]``).

        """
        self._streams = streams
        self._running = True

        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("AsyncStreamConn cancelled")
                break
            except Exception as e:
                if not self._running:
                    break
                logger.warning(
                    "MarketStore WebSocket disconnected: %s. "
                    "Reconnecting in %.1f seconds...",
                    e,
                    self.reconnect_delay,
                )
                await asyncio.sleep(self.reconnect_delay)

    async def _connect_and_listen(self) -> None:
        """Establish connection, subscribe, and enter receive loop."""
        async with ws_connect(self.endpoint) as ws:
            self._ws = ws
            logger.info("Connected to MarketStore WebSocket: %s", self.endpoint)

            # Subscribe
            subscribe_msg = msgpack.packb({"streams": self._streams})
            await ws.send(subscribe_msg)

            # Read subscription confirmation
            raw = await ws.recv()
            confirm = msgpack.unpackb(raw, raw=False)
            if "error" in confirm:
                raise ConnectionError(
                    f"MarketStore subscription error: {confirm['error']}"
                )
            logger.info(
                "Subscribed to MarketStore streams: %s",
                confirm.get("streams", self._streams),
            )

            # Receive loop
            async for raw_msg in ws:
                msg = msgpack.unpackb(raw_msg, raw=False)
                key = msg.get("key")
                if key is not None:
                    data = msg.get("data", {})
                    self._dispatch(key, data)

    def _dispatch(self, key: str, data: dict) -> None:
        """Dispatch a received message to all matching handlers."""
        for pat, handler in self._handlers.items():
            if pat.match(key):
                try:
                    handler(key, data)
                except Exception:
                    logger.exception(
                        "Error in stream handler for key '%s'", key
                    )

    async def stop(self) -> None:
        """Stop the streaming connection."""
        self._running = False
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        logger.info("AsyncStreamConn stopped")
