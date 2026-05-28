"""JSON-RPC over a persistent WebSocket to the upstream node.

This module owns no subscription state. It exposes:
  - `request(method, params)` — sends a JSON-RPC request and awaits the matching
    response by id correlation.
  - `on_notification(callable)` — callback invoked for unsolicited JSON-RPC
    notifications (i.e. messages with no `id`, `method == "eth_subscription"`).

Reconnect & backoff are layered on in `_run_forever`. Callers do not see
reconnects directly; instead, `SubscriptionManager` is told via the
`on_reconnect` callback (see Task 2).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
from aiohttp import ClientSession, WSMsgType

logger = logging.getLogger("exec_rest_api.upstream_ws")

NotificationCallback = Callable[[dict[str, Any]], None]
ReconnectCallback = Callable[[], Awaitable[None]]


class UpstreamWsClosed(Exception):
    """Raised on in-flight requests when the WS closes."""


class UpstreamWsJsonRpcError(Exception):
    """JSON-RPC `error` returned for a request over the WS."""

    def __init__(self, *, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"jsonrpc error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class UpstreamWebSocket:
    """Persistent JSON-RPC client over a WebSocket.

    Lifecycle:
        ws = UpstreamWebSocket(session=..., url=..., on_notification=cb)
        await ws.start()      # establish connection, spawn read loop
        result = await ws.request("eth_subscribe", ["newHeads"])
        ...
        await ws.stop()       # cancel read loop and close socket
    """

    def __init__(
        self,
        *,
        session: ClientSession,
        url: str,
        on_notification: NotificationCallback,
        on_reconnect: ReconnectCallback | None = None,
        reconnect: bool = True,
        backoff_schedule: tuple[float, ...] = (1.0, 2.0, 5.0, 30.0),
    ) -> None:
        self._session = session
        self._url = url
        self.on_notification = on_notification
        self.on_reconnect = on_reconnect
        self._reconnect = reconnect
        self._backoff = backoff_schedule
        self._id_counter = itertools.count(1)
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task[None] | None = None
        self._connected_event = asyncio.Event()
        self._stopping = False
        self._first_connect_error: Exception | None = None

    async def start(self) -> None:
        """Start the read loop and wait for the first successful connect.

        Raises UpstreamWsClosed if the first connect attempt fails and reconnect=False.
        """
        self._task = asyncio.create_task(self._run_forever(), name="upstream_ws")
        await self._connected_event.wait()
        if self._first_connect_error is not None:
            raise UpstreamWsClosed(f"initial connect failed: {self._first_connect_error!r}")

    async def stop(self) -> None:
        self._stopping = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._fail_pending(UpstreamWsClosed("ws stopped"))

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    async def request(
        self,
        method: str,
        params: list[Any] | None = None,
        *,
        timeout_seconds: float = 30.0,
    ) -> Any:
        if not self.connected:
            raise UpstreamWsClosed("ws not connected")
        ws = self._ws
        assert ws is not None
        rid = next(self._id_counter)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[rid] = future
        try:
            await ws.send_str(
                json.dumps(
                    {"jsonrpc": "2.0", "id": rid, "method": method, "params": params or []}
                )
            )
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending.pop(rid, None)

    async def _run_forever(self) -> None:
        attempt = 0
        while not self._stopping:
            try:
                async with self._session.ws_connect(self._url, heartbeat=20.0) as ws:
                    self._ws = ws
                    attempt = 0
                    is_reconnect = self._connected_event.is_set()
                    self._connected_event.set()
                    if is_reconnect and self.on_reconnect is not None:
                        await self.on_reconnect()
                    await self._read_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("upstream WS error: %r", exc)
                if not self._connected_event.is_set():
                    self._first_connect_error = exc
            finally:
                self._ws = None
                self._fail_pending(UpstreamWsClosed("ws disconnected"))

            if not self._reconnect or self._stopping:
                self._connected_event.set()  # unblock start() either way
                return

            delay = self._backoff[min(attempt, len(self._backoff) - 1)]
            attempt += 1
            logger.info("reconnecting to upstream WS in %.1fs", delay)
            await asyncio.sleep(delay)

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                self._dispatch(msg.data)
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                return

    def _dispatch(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except ValueError:
            logger.warning("upstream WS sent non-JSON: %r", raw[:200])
            return
        if not isinstance(payload, dict):
            return
        if "method" in payload and "id" not in payload:
            try:
                self.on_notification(payload)
            except Exception:
                logger.exception("notification handler raised")
            return
        rid = payload.get("id")
        if not isinstance(rid, int):
            return
        future = self._pending.get(rid)
        if future is None or future.done():
            return
        if "error" in payload:
            err = payload["error"]
            future.set_exception(
                UpstreamWsJsonRpcError(
                    code=int(err.get("code", -32603)),
                    message=str(err.get("message", "")),
                    data=err.get("data"),
                )
            )
        else:
            future.set_result(payload.get("result"))

    def _fail_pending(self, exc: Exception) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()
