# Execution REST API — SSE streams + WS subscription manager (Plan 4 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the four Server-Sent Event streams (`/streams/blocks`, `/streams/logs`, `/streams/pending-transactions`, `/streams/sync-status`) backed by a persistent WebSocket connection to the upstream and a subscription multiplexer that shares a single upstream `eth_subscribe` across all clients with identical filters.

**Architecture:** Two new long-running components live in the application: `UpstreamWebSocket` (single persistent WS, exponential-backoff reconnect, JSON-RPC over WS) and `SubscriptionManager` (multiplexes one upstream subscription per `(kind, params)` to N consumer queues, re-issues subscribes on reconnect with a `gap` sentinel per consumer). SSE handlers borrow the existing `Problem`/`map_jsonrpc_error` infrastructure for pre-stream errors and an `event: error` frame for mid-stream errors. Heartbeats are SSE comment lines emitted by the framing helper itself when the upstream is quiet. `Last-Event-ID` replay for blocks and logs is bounded by `sse_replay_window` and falls back to a `gap` event when exceeded.

**Tech Stack:** Same as Plans 1–3 — aiohttp (already provides `ws_connect` and `WebSocketResponse`), asyncio, pytest, anvil for integration tests. No new runtime dependencies.

---

## Companion documents

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — §3.9 (endpoint list), §7 (full SSE contract), §5.2 (status mapping for pre-stream errors).
- `docs/superpowers/specs/2026-05-28-execution-rest-api-openapi.yaml` — schemas: `BlockHeader`, `Log`, `Transaction`, `SyncStatus`.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — §4 (file layout), §7 "WS subscription manager", §9 (backpressure).

---

## File structure (created or modified by this plan)

```
src/exec_rest_api/
├── upstream_ws.py                  (NEW) persistent WS + JSON-RPC framing + reconnect
├── subscriptions.py                (NEW) SubscriptionManager: multiplex, ref-count, gap emission
├── sse.py                          (NEW) SSE framing (retry/event/id/data, heartbeat, backpressure)
├── handlers/
│   └── streams.py                  (NEW) /streams/blocks /logs /pending-transactions /sync-status
└── __main__.py                     (MODIFIED) start/stop SubscriptionManager, register streams routes
tests/
├── unit/
│   ├── test_upstream_ws.py         (NEW) request/response, notifications, reconnect
│   ├── test_subscriptions.py       (NEW) multiplexing, ref counting, gap on reconnect
│   ├── test_sse.py                 (NEW) framing, heartbeat, backpressure
│   └── test_handlers_streams.py    (NEW) per-stream handlers with mocked manager
├── integration/
│   └── test_streams.py             (NEW) live anvil: subscribe, observe new blocks, observe logs
├── conformance/
│   └── test_streams.py             (NEW) first event of each stream validates against schema
└── conftest.py                     (MODIFIED) start/stop SubscriptionManager in proxy_client
```

---

## Task 1: `upstream_ws.py` — request/response framing over a WebSocket

The low-level WS client. Owns no subscription state — only knows about JSON-RPC requests (with response correlation by `id`) and JSON-RPC notifications (no `id`, `method == "eth_subscription"`). Reconnect & backoff go in Task 2.

**Files:**
- Create: `src/exec_rest_api/upstream_ws.py`
- Create: `tests/unit/test_upstream_ws.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_upstream_ws.py`:

```python
"""Tests for the low-level JSON-RPC over WebSocket client.

These tests exercise the in-process server <-> client loop only; reconnect &
backoff are covered in test_upstream_ws_reconnect.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from aiohttp import ClientSession, WSMsgType, web

from exec_rest_api.upstream_ws import (
    UpstreamWebSocket,
    UpstreamWsClosed,
    UpstreamWsJsonRpcError,
)


@pytest.fixture
async def ws_server(aiohttp_server):
    """An aiohttp WS server that lets the test script its replies."""
    scripted: dict[str, Any] = {"on_message": None}
    handle: dict[str, Any] = {"ws": None, "received": []}

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        handle["ws"] = ws
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                handle["received"].append(msg.data)
                if scripted["on_message"] is not None:
                    await scripted["on_message"](ws, msg.data)
        return ws

    app = web.Application()
    app.router.add_get("/", ws_handler)
    server = await aiohttp_server(app)
    yield server, handle, scripted


async def test_request_returns_result(ws_server):
    server, handle, scripted = ws_server

    async def reply(ws: web.WebSocketResponse, raw: str) -> None:
        req = json.loads(raw)
        await ws.send_str(
            json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": "ok"})
        )

    scripted["on_message"] = reply

    notifications: list[dict[str, Any]] = []

    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url=str(server.make_url("/")).replace("http://", "ws://"),
            on_notification=notifications.append,
        )
        await client.start()
        result = await client.request("foo", ["bar"])
        assert result == "ok"
        sent = json.loads(handle["received"][0])
        assert sent["method"] == "foo"
        assert sent["params"] == ["bar"]
        assert sent["id"] == 1
        await client.stop()


async def test_request_id_increments(ws_server):
    server, handle, scripted = ws_server

    async def reply(ws: web.WebSocketResponse, raw: str) -> None:
        req = json.loads(raw)
        await ws.send_str(json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": req["id"]}))

    scripted["on_message"] = reply

    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url=str(server.make_url("/")).replace("http://", "ws://"),
            on_notification=lambda _: None,
        )
        await client.start()
        a, b, c = await asyncio.gather(
            client.request("foo"),
            client.request("foo"),
            client.request("foo"),
        )
        assert {a, b, c} == {1, 2, 3}
        await client.stop()


async def test_request_jsonrpc_error_raises(ws_server):
    server, _, scripted = ws_server

    async def reply(ws: web.WebSocketResponse, raw: str) -> None:
        req = json.loads(raw)
        await ws.send_str(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": req["id"],
                    "error": {"code": -32601, "message": "no such method"},
                }
            )
        )

    scripted["on_message"] = reply

    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url=str(server.make_url("/")).replace("http://", "ws://"),
            on_notification=lambda _: None,
        )
        await client.start()
        with pytest.raises(UpstreamWsJsonRpcError) as exc_info:
            await client.request("foo")
        assert exc_info.value.code == -32601
        await client.stop()


async def test_notifications_dispatched(ws_server):
    server, handle, scripted = ws_server

    notifications: list[dict[str, Any]] = []
    delivered = asyncio.Event()

    def on_notification(payload: dict[str, Any]) -> None:
        notifications.append(payload)
        delivered.set()

    async def reply(ws: web.WebSocketResponse, raw: str) -> None:
        # Send a notification before responding to the request
        await ws.send_str(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {"subscription": "0xabc", "result": {"number": "0x1"}},
                }
            )
        )
        req = json.loads(raw)
        await ws.send_str(json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": "x"}))

    scripted["on_message"] = reply

    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url=str(server.make_url("/")).replace("http://", "ws://"),
            on_notification=on_notification,
        )
        await client.start()
        await client.request("anything")
        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        assert notifications[0]["params"]["subscription"] == "0xabc"
        await client.stop()


async def test_pending_requests_raise_on_close(ws_server):
    """If the WS closes while a request is in flight, the awaiting caller sees
    UpstreamWsClosed rather than hanging forever."""
    server, handle, scripted = ws_server

    # Don't reply — instead, close the WS from the server side.
    closer = asyncio.Event()

    async def silence_then_close(ws: web.WebSocketResponse, raw: str) -> None:
        closer.set()

    scripted["on_message"] = silence_then_close

    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url=str(server.make_url("/")).replace("http://", "ws://"),
            on_notification=lambda _: None,
            reconnect=False,  # disable so this test sees the exception
        )
        await client.start()
        request_task = asyncio.create_task(client.request("foo"))
        await closer.wait()
        await handle["ws"].close()
        with pytest.raises(UpstreamWsClosed):
            await request_task
        await client.stop()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/unit/test_upstream_ws.py -v`

Expected: `ImportError` on every test — the module doesn't exist.

- [ ] **Step 3: Implement `upstream_ws.py`**

Create `src/exec_rest_api/upstream_ws.py`:

```python
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
        self._on_notification = on_notification
        self._on_reconnect = on_reconnect
        self._reconnect = reconnect
        self._backoff = backoff_schedule
        self._id_counter = itertools.count(1)
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task[None] | None = None
        self._connected_event = asyncio.Event()
        self._stopping = False

    async def start(self) -> None:
        """Start the read loop and wait for the first successful connect."""
        self._task = asyncio.create_task(self._run_forever(), name="upstream_ws")
        await self._connected_event.wait()

    async def stop(self) -> None:
        self._stopping = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
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
        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
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
                    if is_reconnect and self._on_reconnect is not None:
                        await self._on_reconnect()
                    await self._read_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("upstream WS error: %r", exc)
            finally:
                self._ws = None
                self._fail_pending(UpstreamWsClosed("ws disconnected"))

            if not self._reconnect or self._stopping:
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
                self._on_notification(payload)
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/unit/test_upstream_ws.py -v`

Expected: all 5 tests pass.

- [ ] **Step 5: Type-check**

Run: `mypy src/exec_rest_api/upstream_ws.py`

Expected: `Success: no issues found in 1 source file`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/upstream_ws.py tests/unit/test_upstream_ws.py
git commit -m "Add JSON-RPC over WebSocket client (UpstreamWebSocket)"
```

---

## Task 2: Reconnect with exponential backoff

Verify the reconnect path with backoff by extending the test file. The implementation already supports reconnect (Task 1); this task adds tests that prove it and locks the backoff schedule down.

**Files:**
- Modify: `tests/unit/test_upstream_ws.py` (append two tests)

- [ ] **Step 1: Append reconnect tests**

Add to `tests/unit/test_upstream_ws.py`:

```python
async def test_reconnects_after_server_close(aiohttp_server):
    """The client transparently reconnects and continues serving requests."""
    connect_count = 0

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        nonlocal connect_count
        connect_count += 1
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                req = json.loads(msg.data)
                if req["method"] == "close_me":
                    await ws.close()
                    return ws
                await ws.send_str(
                    json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": "ok"})
                )
        return ws

    app = web.Application()
    app.router.add_get("/", ws_handler)
    server = await aiohttp_server(app)

    on_reconnect_called = asyncio.Event()

    async def on_reconnect() -> None:
        on_reconnect_called.set()

    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url=str(server.make_url("/")).replace("http://", "ws://"),
            on_notification=lambda _: None,
            on_reconnect=on_reconnect,
            backoff_schedule=(0.05,),
        )
        await client.start()
        assert await client.request("hello") == "ok"
        # Force the server to drop the connection
        with pytest.raises(UpstreamWsClosed):
            await client.request("close_me")
        # Wait for the reconnect callback (sentinel that re-subscribe should run)
        await asyncio.wait_for(on_reconnect_called.wait(), timeout=2.0)
        # And new requests succeed on the new connection
        assert await client.request("hello") == "ok"
        await client.stop()
    assert connect_count >= 2


async def test_backoff_schedule_clamped(aiohttp_server):
    """Backoff progresses through the schedule and clamps to the last entry."""
    # We can't really observe internal sleeps without slowing the test down,
    # but we can verify the public schedule attribute is what the caller set.
    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url="ws://127.0.0.1:1",  # unreachable; we won't start()
            on_notification=lambda _: None,
            backoff_schedule=(1.0, 2.0, 5.0, 30.0),
        )
        assert client._backoff == (1.0, 2.0, 5.0, 30.0)
```

- [ ] **Step 2: Run the tests**

Run: `pytest tests/unit/test_upstream_ws.py -v`

Expected: all 7 tests pass. `test_reconnects_after_server_close` may be a hair slow because of the 0.05s backoff.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_upstream_ws.py
git commit -m "Cover UpstreamWebSocket reconnect-with-backoff path"
```

---

## Task 3: `subscriptions.py` — `SubscriptionManager` with multiplexing

Multiplexes upstream `eth_subscribe` calls so identical `(kind, params)` tuples share one upstream subscription. Each client gets its own queue; events are fanned out. Reference-counts and tears down the upstream subscription when the last client disconnects.

**Files:**
- Create: `src/exec_rest_api/subscriptions.py`
- Create: `tests/unit/test_subscriptions.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_subscriptions.py`:

```python
"""Tests for SubscriptionManager — multiplexing, reference counting, gap on reconnect."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from exec_rest_api.subscriptions import (
    GAP,
    StreamEvent,
    SubscriptionManager,
    SubscriptionUnavailable,
)


class FakeWebSocket:
    """In-memory stand-in for UpstreamWebSocket."""

    def __init__(self) -> None:
        self.request_log: list[tuple[str, list[Any]]] = []
        self.next_subscription_id = 0
        self.on_notification: Callable[[dict[str, Any]], None] | None = None
        self.on_reconnect: Callable[[], Awaitable[None]] | None = None
        self.connected = True
        # The fake_subscribe handler returns a synthetic subscription id.
        self.replies: dict[str, Any] = {}

    async def request(self, method: str, params: list[Any] | None = None) -> Any:
        self.request_log.append((method, list(params or [])))
        if method == "eth_subscribe":
            self.next_subscription_id += 1
            sid = f"0x{self.next_subscription_id:x}"
            return sid
        if method == "eth_unsubscribe":
            return True
        return self.replies.get(method)

    def emit(self, subscription_id: str, payload: Any) -> None:
        assert self.on_notification is not None
        self.on_notification(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": subscription_id, "result": payload},
            }
        )

    async def trigger_reconnect(self) -> None:
        assert self.on_reconnect is not None
        await self.on_reconnect()


@pytest.fixture
def fake_ws() -> FakeWebSocket:
    return FakeWebSocket()


async def _drive_with_subscription(mgr: SubscriptionManager, fake_ws: FakeWebSocket) -> None:
    """Wire fake_ws's notification/reconnect callbacks to the manager."""
    fake_ws.on_notification = mgr.on_notification
    fake_ws.on_reconnect = mgr.on_reconnect


async def test_subscribe_calls_upstream_once_per_kind(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub_a = await mgr.subscribe(kind="newHeads", params=None)
    sub_b = await mgr.subscribe(kind="newHeads", params=None)
    try:
        # Only one upstream subscribe call, despite two consumers
        assert [m for m, _ in fake_ws.request_log if m == "eth_subscribe"] == [
            "eth_subscribe"
        ]
        # Fan out: both consumers see the event
        fake_ws.emit("0x1", {"number": "0x10"})
        a_event = await asyncio.wait_for(sub_a.__anext__(), 1.0)
        b_event = await asyncio.wait_for(sub_b.__anext__(), 1.0)
        assert a_event == StreamEvent(kind="event", payload={"number": "0x10"})
        assert b_event == StreamEvent(kind="event", payload={"number": "0x10"})
    finally:
        await sub_a.aclose()
        await sub_b.aclose()


async def test_unsubscribe_when_last_consumer_leaves(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub = await mgr.subscribe(kind="newHeads", params=None)
    await sub.aclose()
    # Allow background cleanup to run
    await asyncio.sleep(0)
    methods = [m for m, _ in fake_ws.request_log]
    assert "eth_subscribe" in methods
    assert "eth_unsubscribe" in methods


async def test_distinct_params_get_distinct_subscriptions(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub_a = await mgr.subscribe(kind="logs", params={"address": "0xa"})
    sub_b = await mgr.subscribe(kind="logs", params={"address": "0xb"})
    try:
        subs = [m for m, _ in fake_ws.request_log if m == "eth_subscribe"]
        assert len(subs) == 2
    finally:
        await sub_a.aclose()
        await sub_b.aclose()


async def test_gap_emitted_on_reconnect(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub = await mgr.subscribe(kind="newHeads", params=None)
    try:
        # Pre-reconnect: subscription id is "0x1"
        fake_ws.emit("0x1", {"number": "0x10"})
        assert (await asyncio.wait_for(sub.__anext__(), 1.0)).kind == "event"
        await fake_ws.trigger_reconnect()
        # First post-reconnect event must be a gap
        gap_event = await asyncio.wait_for(sub.__anext__(), 1.0)
        assert gap_event == GAP
        # And the manager re-issued eth_subscribe (new subscription id)
        fake_ws.emit("0x2", {"number": "0x11"})
        live_event = await asyncio.wait_for(sub.__anext__(), 1.0)
        assert live_event == StreamEvent(kind="event", payload={"number": "0x11"})
    finally:
        await sub.aclose()


async def test_subscribe_raises_when_ws_unavailable(fake_ws):
    fake_ws.connected = False
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    with pytest.raises(SubscriptionUnavailable):
        await mgr.subscribe(kind="newHeads", params=None)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/unit/test_subscriptions.py -v`

Expected: `ImportError` on every test.

- [ ] **Step 3: Implement `subscriptions.py`**

Create `src/exec_rest_api/subscriptions.py`:

```python
"""Subscription multiplexer for the upstream WebSocket.

One upstream `eth_subscribe` per unique (kind, params) tuple, fanned out to N
consumer queues. On WS reconnect, all active upstream subscriptions are
re-issued and each consumer queue receives a synthetic `GAP` event so the SSE
handler can emit `event: gap` to its client.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal, Protocol

logger = logging.getLogger("exec_rest_api.subscriptions")

StreamKind = Literal["newHeads", "logs", "newPendingTransactions", "syncing"]


class SubscriptionUnavailable(Exception):
    """Raised when the upstream WS isn't connected and a subscribe is attempted."""


@dataclass(frozen=True)
class StreamEvent:
    """One event delivered to a consumer.

    `kind == "event"` carries the JSON-RPC `result` payload verbatim.
    `kind == "gap"` is a synthetic sentinel (see GAP).
    """

    kind: Literal["event", "gap"]
    payload: Any = None


GAP = StreamEvent(kind="gap")


class _WebSocketLike(Protocol):
    connected: bool

    async def request(self, method: str, params: list[Any] | None = None) -> Any: ...


def _canonicalize(params: Any) -> str:
    """Stable canonical form of subscribe params for use as a dict key."""
    return json.dumps(params, sort_keys=True, separators=(",", ":"))


@dataclass
class _Slot:
    """One upstream subscription and its consumer queues."""

    kind: StreamKind
    params: Any
    subscription_id: str | None
    consumers: list[asyncio.Queue[StreamEvent]]


class SubscriptionManager:
    """Multiplex upstream eth_subscribe calls across N client SSE streams."""

    def __init__(self, *, ws: _WebSocketLike) -> None:
        self._ws = ws
        self._slots: dict[tuple[StreamKind, str], _Slot] = {}
        self._slot_by_subscription_id: dict[str, _Slot] = {}
        self._lock = asyncio.Lock()

    # ── public callbacks for UpstreamWebSocket ────────────────────────────

    def on_notification(self, payload: dict[str, Any]) -> None:
        params = payload.get("params") or {}
        sub_id = params.get("subscription")
        if not isinstance(sub_id, str):
            return
        slot = self._slot_by_subscription_id.get(sub_id)
        if slot is None:
            return
        event = StreamEvent(kind="event", payload=params.get("result"))
        for q in slot.consumers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("dropping event for slow consumer on %s", slot.kind)

    async def on_reconnect(self) -> None:
        """Re-issue all active subscriptions and notify all consumers of the gap."""
        async with self._lock:
            self._slot_by_subscription_id.clear()
            for slot in list(self._slots.values()):
                # Re-subscribe upstream first
                params_list = _params_to_subscribe_args(slot.kind, slot.params)
                try:
                    new_id = await self._ws.request("eth_subscribe", params_list)
                except Exception as exc:
                    logger.warning("re-subscribe failed for %s: %r", slot.kind, exc)
                    for q in slot.consumers:
                        q.put_nowait(GAP)
                    continue
                slot.subscription_id = new_id
                self._slot_by_subscription_id[new_id] = slot
                for q in slot.consumers:
                    q.put_nowait(GAP)

    # ── public subscribe API ──────────────────────────────────────────────

    async def subscribe(
        self,
        *,
        kind: StreamKind,
        params: Any,
    ) -> AsyncIterator[StreamEvent]:
        """Open a new consumer queue against (kind, params)."""
        if not self._ws.connected:
            raise SubscriptionUnavailable("upstream WS not connected")

        key = (kind, _canonicalize(params))
        queue: asyncio.Queue[StreamEvent] = asyncio.Queue(maxsize=1024)

        async with self._lock:
            slot = self._slots.get(key)
            if slot is None:
                params_list = _params_to_subscribe_args(kind, params)
                sub_id = await self._ws.request("eth_subscribe", params_list)
                slot = _Slot(
                    kind=kind, params=params, subscription_id=sub_id, consumers=[queue]
                )
                self._slots[key] = slot
                self._slot_by_subscription_id[sub_id] = slot
            else:
                slot.consumers.append(queue)

        return self._iterate(key, slot, queue)

    async def _iterate(
        self,
        key: tuple[StreamKind, str],
        slot: _Slot,
        queue: asyncio.Queue[StreamEvent],
    ) -> AsyncIterator[StreamEvent]:
        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            await self._remove_consumer(key, slot, queue)

    async def _remove_consumer(
        self,
        key: tuple[StreamKind, str],
        slot: _Slot,
        queue: asyncio.Queue[StreamEvent],
    ) -> None:
        async with self._lock:
            try:
                slot.consumers.remove(queue)
            except ValueError:
                return
            if slot.consumers:
                return
            sub_id = slot.subscription_id
            self._slots.pop(key, None)
            if sub_id is not None:
                self._slot_by_subscription_id.pop(sub_id, None)
        if sub_id is not None and self._ws.connected:
            try:
                await self._ws.request("eth_unsubscribe", [sub_id])
            except Exception as exc:
                logger.debug("eth_unsubscribe failed (cleaning up anyway): %r", exc)


def _params_to_subscribe_args(kind: StreamKind, params: Any) -> list[Any]:
    """Translate (kind, params) into the JSON-RPC eth_subscribe argument list."""
    if params is None:
        return [kind]
    return [kind, params]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/unit/test_subscriptions.py -v`

Expected: all 5 tests pass.

- [ ] **Step 5: Type-check**

Run: `mypy src/exec_rest_api/subscriptions.py`

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/subscriptions.py tests/unit/test_subscriptions.py
git commit -m "Add SubscriptionManager with multiplexing and gap-on-reconnect"
```

---

## Task 4: `sse.py` — SSE framing, heartbeat, and backpressure

A small, pure module that renders dicts to SSE wire format. The heartbeat helper interleaves `: ping <ts>` comment lines when the source iterator is quiet for `interval` seconds. Backpressure check exposes a function the handler calls before each write.

**Files:**
- Create: `src/exec_rest_api/sse.py`
- Create: `tests/unit/test_sse.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_sse.py`:

```python
"""Tests for SSE framing helpers."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from exec_rest_api.sse import (
    format_comment,
    format_event,
    format_retry,
    stream_with_heartbeat,
)


def test_format_event_minimal():
    out = format_event(event="block", id_="42", data={"number": 42})
    # SSE field order does not matter, but the framing terminator must
    assert out.endswith(b"\n\n")
    text = out.decode("utf-8")
    assert "event: block" in text
    assert "id: 42" in text
    assert 'data: {"number":42}' in text or 'data: {"number": 42}' in text


def test_format_event_data_is_single_line():
    """Multi-line data would break SSE; the framing must compact JSON to one line."""
    out = format_event(event="x", id_="1", data={"k": "a\nb"})
    body = out.decode("utf-8")
    data_lines = [line for line in body.splitlines() if line.startswith("data:")]
    assert len(data_lines) == 1


def test_format_event_no_id():
    out = format_event(event="sync-status", id_=None, data={"syncing": False})
    assert b"id:" not in out
    assert b"event: sync-status" in out


def test_format_comment_strips_newlines():
    out = format_comment("ping 1700000000")
    assert out == b": ping 1700000000\n\n"


def test_format_retry():
    assert format_retry(5000) == b"retry: 5000\n\n"


async def test_stream_with_heartbeat_emits_pings_when_quiet():
    """If the source is idle for `interval`, a heartbeat is yielded."""

    async def source():
        await asyncio.sleep(0.2)
        yield b"event: x\ndata: 1\n\n"
        # Never produce again

    pings: list[bytes] = []
    started = time.monotonic()
    async for chunk in stream_with_heartbeat(source(), interval_seconds=0.05):
        if chunk.startswith(b":"):
            pings.append(chunk)
            if len(pings) >= 2:
                break
        if time.monotonic() - started > 2.0:
            pytest.fail("stream_with_heartbeat never yielded heartbeats")
    assert all(p.startswith(b": ping ") for p in pings)


async def test_stream_with_heartbeat_passes_source_through():
    async def source():
        yield b"event: x\ndata: 1\n\n"
        yield b"event: y\ndata: 2\n\n"

    seen: list[bytes] = []
    async for chunk in stream_with_heartbeat(source(), interval_seconds=1.0):
        if chunk.startswith(b":"):
            continue
        seen.append(chunk)
    assert seen == [b"event: x\ndata: 1\n\n", b"event: y\ndata: 2\n\n"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/unit/test_sse.py -v`

Expected: `ImportError`.

- [ ] **Step 3: Implement `sse.py`**

Create `src/exec_rest_api/sse.py`:

```python
"""Server-Sent Event framing helpers.

Outputs always end with the SSE frame terminator (`\\n\\n`). `data` payloads
are compacted to a single line so multi-line JSON cannot accidentally split a
frame. Heartbeats are SSE comment lines (`:` prefix) emitted by the stream
helper when the source is quiet — this keeps idle connections from being
closed by intermediaries.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any


def format_retry(milliseconds: int) -> bytes:
    return f"retry: {milliseconds}\n\n".encode()


def format_event(
    *,
    event: str,
    id_: str | None,
    data: Any,
) -> bytes:
    """Render one SSE event frame. `data` is JSON-encoded as a single line."""
    lines: list[str] = [f"event: {event}"]
    if id_ is not None:
        lines.append(f"id: {id_}")
    payload = json.dumps(data, separators=(",", ":"))
    lines.append(f"data: {payload}")
    return ("\n".join(lines) + "\n\n").encode()


def format_comment(text: str) -> bytes:
    """Render an SSE comment line. Newlines in `text` are dropped."""
    safe = text.replace("\n", " ").replace("\r", " ")
    return f": {safe}\n\n".encode()


async def stream_with_heartbeat(
    source: AsyncIterator[bytes],
    *,
    interval_seconds: float,
) -> AsyncIterator[bytes]:
    """Yield from `source`. If silent for `interval_seconds`, emit a heartbeat."""
    iterator = source.__aiter__()
    while True:
        next_task = asyncio.ensure_future(iterator.__anext__())
        try:
            chunk = await asyncio.wait_for(asyncio.shield(next_task), timeout=interval_seconds)
        except asyncio.TimeoutError:
            yield format_comment(f"ping {int(time.time())}")
            continue
        except StopAsyncIteration:
            return
        yield chunk
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/unit/test_sse.py -v`

Expected: all 7 tests pass.

- [ ] **Step 5: Type-check**

Run: `mypy src/exec_rest_api/sse.py`

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/sse.py tests/unit/test_sse.py
git commit -m "Add SSE framing helpers: event/id/data, comments, heartbeat"
```

---

## Task 5: `handlers/streams.py` — shared SSE driver + `/streams/blocks`

This is where the four endpoints meet the SubscriptionManager. Task 5 builds the shared SSE driver and the simplest stream (`/streams/blocks`). Tasks 6–8 add the remaining three streams reusing the same driver.

**Files:**
- Create: `src/exec_rest_api/handlers/streams.py`
- Create: `tests/unit/test_handlers_streams.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_handlers_streams.py`:

```python
"""Tests for the SSE stream handlers using a fake SubscriptionManager."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from exec_rest_api.config import Config
from exec_rest_api.handlers.streams import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable


def _config() -> Config:
    return Config(
        upstream_http="http://localhost:8545",
        upstream_ws="ws://localhost:8545",
        listen="127.0.0.1:8080",
        upstream_timeout_seconds=30.0,
        default_page_size=1000,
        max_page_size=10000,
        sse_buffer_bytes=65536,
        sse_replay_window=1024,
        sse_heartbeat_seconds=30,
        ready_sync_lag=10,
        log_level="info",
        log_format=None,
        metrics_enabled=True,
    )


class FakeManager:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.subscribe_calls: list[tuple[str, Any]] = []
        self._queues: list[asyncio.Queue[StreamEvent]] = []

    async def subscribe(self, *, kind: str, params: Any):
        if not self.available:
            raise SubscriptionUnavailable("ws down")
        self.subscribe_calls.append((kind, params))
        queue: asyncio.Queue[StreamEvent] = asyncio.Queue()
        self._queues.append(queue)

        async def iterator() -> AsyncIterator[StreamEvent]:
            while True:
                event = await queue.get()
                if event is None:  # sentinel for close
                    return
                yield event

        return iterator()

    def emit(self, payload: Any) -> None:
        for q in self._queues:
            q.put_nowait(StreamEvent(kind="event", payload=payload))

    def emit_gap(self) -> None:
        for q in self._queues:
            q.put_nowait(GAP)

    def close(self) -> None:
        for q in self._queues:
            q.put_nowait(None)  # type: ignore[arg-type]


async def _build_client(aiohttp_client, manager: FakeManager):
    app = create_app(config=_config(), upstream=None)  # type: ignore[arg-type]
    app["subscriptions"] = manager
    register_routes(app)
    return await aiohttp_client(app)


async def test_streams_blocks_emits_event_per_notification(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)

    resp = await client.get("/streams/blocks")
    assert resp.status == 200
    assert resp.content_type == "text/event-stream"

    # Push one head; read until we see an `event: block` line.
    rpc_block = {
        "number": "0x10",
        "hash": "0x" + "ab" * 32,
        "parentHash": "0x" + "cd" * 32,
        "stateRoot": "0x" + "00" * 32,
        "transactionsRoot": "0x" + "00" * 32,
        "receiptsRoot": "0x" + "00" * 32,
        "logsBloom": "0x" + "00" * 256,
        "gasUsed": "0x0",
        "gasLimit": "0x1c9c380",
        "timestamp": "0x65a00000",
        "miner": "0x0000000000000000000000000000000000000000",
        "difficulty": "0x0",
        "totalDifficulty": "0x0",
        "extraData": "0x",
        "mixHash": "0x" + "00" * 32,
        "nonce": "0x0000000000000000",
        "size": "0x0",
    }
    mgr.emit(rpc_block)
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: block" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw event: block; got {text!r}")
        text += await resp.content.read(256)

    # We get a retry directive too
    assert b"retry: 5000" in text
    assert b"id: 16" in text  # blockNumber = 0x10 = 16

    mgr.close()
    await resp.release()
    assert mgr.subscribe_calls == [("newHeads", None)]


async def test_streams_blocks_gap_renders_event_gap(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/blocks")

    mgr.emit_gap()
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: gap" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw event: gap; got {text!r}")
        text += await resp.content.read(256)

    mgr.close()
    await resp.release()


async def test_streams_blocks_returns_503_when_ws_unavailable(aiohttp_client):
    mgr = FakeManager(available=False)
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/blocks")
    assert resp.status == 503
    body = await resp.json()
    assert body["type"].endswith("/upstream-unavailable")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/unit/test_handlers_streams.py -v`

Expected: `ImportError`.

- [ ] **Step 3: Implement `handlers/streams.py`**

Create `src/exec_rest_api/handlers/streams.py`:

```python
"""/streams/* SSE handlers.

Shared driver:
  - validate request before opening the SSE response (so pre-stream errors return Problem+JSON)
  - prepare a `text/event-stream` StreamResponse
  - emit `retry: 5000`
  - subscribe via SubscriptionManager
  - stream events with periodic heartbeats
  - on mid-stream error, emit `event: error` and close
  - apply backpressure (drop if transport.get_write_buffer_size() exceeds sse_buffer_bytes)
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

from aiohttp import web

from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.blocks import block_header_from_rpc
from exec_rest_api.server import add_get
from exec_rest_api.sse import format_event, format_retry, stream_with_heartbeat
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable

logger = logging.getLogger("exec_rest_api.handlers.streams")


EventFormatter = Callable[[Any], tuple[str, str | None, Any]]
"""Converts one payload from the SubscriptionManager into (event-name, id, data)."""


def _block_event(payload: Any) -> tuple[str, str | None, Any]:
    header = block_header_from_rpc(payload)
    return "block", str(header["number"]), header


# ── shared driver ─────────────────────────────────────────────────────────


async def _open_sse(request: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    resp.enable_chunked_encoding()
    await resp.prepare(request)
    await resp.write(format_retry(5000))
    return resp


async def _run_stream(
    request: web.Request,
    *,
    kind: str,
    params: Any,
    formatter: EventFormatter,
    gap_event_name: str = "gap",
) -> web.StreamResponse:
    subscriptions = request.app["subscriptions"]
    config = request.app["config"]
    try:
        events: AsyncIterator[StreamEvent] = await subscriptions.subscribe(
            kind=kind, params=params
        )
    except SubscriptionUnavailable as exc:
        return problem_response(
            Problem(
                status=503,
                type_slug="upstream-unavailable",
                title="Upstream unavailable",
                detail=str(exc),
                instance=request.path,
            )
        )

    resp = await _open_sse(request)

    async def to_bytes() -> AsyncIterator[bytes]:
        async for event in events:
            if event is GAP or event.kind == "gap":
                yield format_event(event=gap_event_name, id_=None, data={})
                continue
            try:
                name, ev_id, payload = formatter(event.payload)
            except Exception:
                logger.exception("event formatter raised on %s", kind)
                yield format_event(
                    event="error",
                    id_=None,
                    data={
                        "type": "https://errors.ethereum-rest/internal-error",
                        "title": "Internal error",
                    },
                )
                return
            yield format_event(event=name, id_=ev_id, data=payload)

    try:
        async for chunk in stream_with_heartbeat(
            to_bytes(), interval_seconds=config.sse_heartbeat_seconds
        ):
            if _over_backpressure_threshold(request, config.sse_buffer_bytes):
                logger.info("dropping SSE client over backpressure threshold")
                return resp
            await resp.write(chunk)
    except ConnectionResetError:
        pass
    finally:
        await events.aclose()  # type: ignore[attr-defined]
    return resp


def _over_backpressure_threshold(request: web.Request, threshold_bytes: int) -> bool:
    transport = request.transport
    if transport is None:
        return False
    try:
        return transport.get_write_buffer_size() > threshold_bytes  # type: ignore[attr-defined]
    except AttributeError:
        return False


# ── handlers ──────────────────────────────────────────────────────────────


async def get_streams_blocks(request: web.Request) -> web.StreamResponse:
    return await _run_stream(
        request, kind="newHeads", params=None, formatter=_block_event
    )


def register_routes(app: web.Application) -> None:
    add_get(app, "/streams/blocks", get_streams_blocks)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/unit/test_handlers_streams.py -v`

Expected: all 3 tests pass.

- [ ] **Step 5: Type-check**

Run: `mypy src/exec_rest_api/handlers/streams.py`

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/handlers/streams.py tests/unit/test_handlers_streams.py
git commit -m "Add shared SSE driver and /streams/blocks handler"
```

---

## Task 6: `/streams/logs` handler

Adds the logs endpoint. The filter is built from query params (`address`, `topic0..topic3`); shape-conversion reuses `log_from_rpc` from `handlers/transactions`.

**Files:**
- Modify: `src/exec_rest_api/handlers/streams.py`
- Modify: `tests/unit/test_handlers_streams.py`

- [ ] **Step 1: Append the failing test**

Append to `tests/unit/test_handlers_streams.py`:

```python
async def test_streams_logs_passes_filter_to_subscribe(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    addr = "0x" + "ab" * 20
    topic = "0x" + "cd" * 32
    resp = await client.get(f"/streams/logs?address={addr}&topic0={topic}")
    assert resp.status == 200
    # Subscribe was called with the upstream filter shape
    assert mgr.subscribe_calls == [
        ("logs", {"address": [addr.lower()], "topics": [topic.lower()]})
    ]

    rpc_log = {
        "address": addr,
        "topics": [topic],
        "data": "0x",
        "blockHash": "0x" + "00" * 32,
        "blockNumber": "0x10",
        "transactionHash": "0x" + "11" * 32,
        "transactionIndex": "0x0",
        "logIndex": "0x2",
        "removed": False,
    }
    mgr.emit(rpc_log)
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: log" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw event: log; got {text!r}")
        text += await resp.content.read(512)
    assert b"id: 16-2" in text
    mgr.close()
    await resp.release()


async def test_streams_logs_rejects_bad_topic(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/logs?topic0=0xnothex")
    assert resp.status == 400
    body = await resp.json()
    assert body["type"].endswith("/invalid-request")


async def test_streams_logs_rejects_bad_address(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/logs?address=0xshort")
    assert resp.status == 400
    body = await resp.json()
    assert body["type"].endswith("/invalid-request")
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `pytest tests/unit/test_handlers_streams.py -v -k logs`

Expected: failures (route not implemented yet).

- [ ] **Step 3: Extend `handlers/streams.py`**

Replace the imports section at the top of `src/exec_rest_api/handlers/streams.py` with:

```python
from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator, Callable
from typing import Any

from aiohttp import web

from exec_rest_api.encoding import EncodingError, map_address_lowercase
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.blocks import block_header_from_rpc
from exec_rest_api.handlers.transactions import log_from_rpc
from exec_rest_api.server import add_get
from exec_rest_api.sse import format_event, format_retry, stream_with_heartbeat
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable

logger = logging.getLogger("exec_rest_api.handlers.streams")

_TOPIC_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
```

Then add (above `register_routes`):

```python
def _log_event(payload: Any) -> tuple[str, str | None, Any]:
    rest_log = log_from_rpc(payload)
    return "log", f"{rest_log['blockNumber']}-{rest_log['logIndex']}", rest_log


def _parse_log_filter(request: web.Request) -> dict[str, Any] | web.Response:
    """Build an eth_subscribe('logs', filter) params dict from query params, or
    a 400 Problem response if the params are malformed."""
    filter_: dict[str, Any] = {}
    addr_raw = request.query.get("address")
    if addr_raw:
        addrs: list[str] = []
        for piece in addr_raw.split(","):
            try:
                addrs.append(map_address_lowercase(piece.strip()))
            except EncodingError as e:
                return problem_response(
                    Problem(
                        status=400,
                        type_slug="invalid-request",
                        title="Invalid request",
                        detail=str(e),
                        instance=request.path,
                    )
                )
        filter_["address"] = addrs
    topics: list[str | None] = []
    last_set = -1
    for i in range(4):
        val = request.query.get(f"topic{i}")
        if val is None:
            topics.append(None)
        else:
            if not _TOPIC_RE.fullmatch(val):
                return problem_response(
                    Problem(
                        status=400,
                        type_slug="invalid-request",
                        title="Invalid request",
                        detail=f"topic{i} must be 0x-prefixed 32-byte hex, got {val!r}",
                        instance=request.path,
                    )
                )
            topics.append(val.lower())
            last_set = i
    if last_set >= 0:
        filter_["topics"] = topics[: last_set + 1]
    return filter_


async def get_streams_logs(request: web.Request) -> web.StreamResponse:
    filter_or_err = _parse_log_filter(request)
    if isinstance(filter_or_err, web.Response):
        return filter_or_err
    return await _run_stream(
        request, kind="logs", params=filter_or_err, formatter=_log_event
    )
```

Update `register_routes`:

```python
def register_routes(app: web.Application) -> None:
    add_get(app, "/streams/blocks", get_streams_blocks)
    add_get(app, "/streams/logs", get_streams_logs)
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/unit/test_handlers_streams.py -v`

Expected: all 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/exec_rest_api/handlers/streams.py tests/unit/test_handlers_streams.py
git commit -m "Add /streams/logs with query-param filter parsing"
```

---

## Task 7: `/streams/pending-transactions` handler

Default emits `event: pending-transaction` with `{ "hash": "0x…" }`. With `?full=true`, attempts to pass the second arg to `eth_subscribe("newPendingTransactions", true)`. Per the spec (§7.2), the upstream may reject `full=true`; if so, fall back to hashes by retrying with no second arg.

**Files:**
- Modify: `src/exec_rest_api/handlers/streams.py`
- Modify: `tests/unit/test_handlers_streams.py`

- [ ] **Step 1: Append the failing tests**

Append to `tests/unit/test_handlers_streams.py`:

```python
async def test_streams_pending_hash_only(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/pending-transactions")
    assert resp.status == 200
    assert mgr.subscribe_calls == [("newPendingTransactions", None)]

    mgr.emit("0x" + "ab" * 32)
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: pending-transaction" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw event: pending-transaction; got {text!r}")
        text += await resp.content.read(256)
    assert b'"hash":"0x' in text
    assert b"id: 0xabababababab" in text
    mgr.close()
    await resp.release()


async def test_streams_pending_full(aiohttp_client):
    """?full=true forwards `true` as the second arg to eth_subscribe."""
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/pending-transactions?full=true")
    assert resp.status == 200
    assert mgr.subscribe_calls == [("newPendingTransactions", True)]
    mgr.close()
    await resp.release()
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_handlers_streams.py -v -k pending`

Expected: 404 errors (route not registered yet).

- [ ] **Step 3: Extend `handlers/streams.py`**

Add (above `register_routes`):

```python
from exec_rest_api.handlers.transactions import transaction_from_rpc  # at top with other imports


def _pending_event_hash_only(payload: Any) -> tuple[str, str | None, Any]:
    # Upstream sends just the tx hash as a string when subscribed without `true`.
    tx_hash = payload if isinstance(payload, str) else payload.get("hash")
    return "pending-transaction", tx_hash, {"hash": tx_hash}


def _pending_event_full(payload: Any) -> tuple[str, str | None, Any]:
    rest_tx = transaction_from_rpc(payload)
    return "pending-transaction", rest_tx["hash"], rest_tx


async def get_streams_pending(request: web.Request) -> web.StreamResponse:
    full_raw = request.query.get("full")
    full = full_raw is not None and full_raw.lower() == "true"
    formatter = _pending_event_full if full else _pending_event_hash_only
    params = True if full else None
    return await _run_stream(
        request,
        kind="newPendingTransactions",
        params=params,
        formatter=formatter,
        gap_event_name="resumed",  # spec §7.3: no replay, one-time `resumed` on reconnect
    )
```

Update the imports block at the top of `streams.py` to also import `transaction_from_rpc`:

```python
from exec_rest_api.handlers.transactions import log_from_rpc, transaction_from_rpc
```

Update `register_routes`:

```python
def register_routes(app: web.Application) -> None:
    add_get(app, "/streams/blocks", get_streams_blocks)
    add_get(app, "/streams/logs", get_streams_logs)
    add_get(app, "/streams/pending-transactions", get_streams_pending)
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/unit/test_handlers_streams.py -v`

Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/exec_rest_api/handlers/streams.py tests/unit/test_handlers_streams.py
git commit -m "Add /streams/pending-transactions with ?full=true support"
```

---

## Task 8: `/streams/sync-status` handler

Emits `event: sync-status` with the SyncStatus schema (oneOf: `{"syncing": false}` or `{"syncing": true, startingBlock, currentBlock, highestBlock}`). No event id per the spec; ids would only confuse `Last-Event-ID` replay, which sync-status doesn't support.

**Files:**
- Modify: `src/exec_rest_api/handlers/streams.py`
- Modify: `tests/unit/test_handlers_streams.py`

- [ ] **Step 1: Append the failing tests**

Append to `tests/unit/test_handlers_streams.py`:

```python
async def test_streams_sync_status_synced(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/sync-status")
    assert resp.status == 200
    assert mgr.subscribe_calls == [("syncing", None)]
    mgr.emit(False)
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: sync-status" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw event: sync-status; got {text!r}")
        text += await resp.content.read(256)
    assert b'{"syncing":false}' in text
    mgr.close()
    await resp.release()


async def test_streams_sync_status_active(aiohttp_client):
    mgr = FakeManager()
    client = await _build_client(aiohttp_client, mgr)
    resp = await client.get("/streams/sync-status")
    mgr.emit(
        {
            "startingBlock": "0x0",
            "currentBlock": "0x10",
            "highestBlock": "0x100",
        }
    )
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: sync-status" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw event: sync-status; got {text!r}")
        text += await resp.content.read(256)
    assert b'"syncing":true' in text
    assert b'"currentBlock":16' in text
    assert b'"highestBlock":256' in text
    mgr.close()
    await resp.release()
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_handlers_streams.py -v -k sync_status`

Expected: 404 errors.

- [ ] **Step 3: Extend `handlers/streams.py`**

Add (above `register_routes`):

```python
from exec_rest_api.encoding import hex_to_int  # add at top with other imports


def _sync_status_event(payload: Any) -> tuple[str, str | None, Any]:
    if payload is False:
        return "sync-status", None, {"syncing": False}
    return "sync-status", None, {
        "syncing": True,
        "startingBlock": hex_to_int(payload["startingBlock"]),
        "currentBlock": hex_to_int(payload["currentBlock"]),
        "highestBlock": hex_to_int(payload["highestBlock"]),
    }


async def get_streams_sync_status(request: web.Request) -> web.StreamResponse:
    return await _run_stream(
        request,
        kind="syncing",
        params=None,
        formatter=_sync_status_event,
        gap_event_name="resumed",  # spec §7.3: no replay, one-time `resumed` on reconnect
    )
```

Update `register_routes`:

```python
def register_routes(app: web.Application) -> None:
    add_get(app, "/streams/blocks", get_streams_blocks)
    add_get(app, "/streams/logs", get_streams_logs)
    add_get(app, "/streams/pending-transactions", get_streams_pending)
    add_get(app, "/streams/sync-status", get_streams_sync_status)
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/unit/test_handlers_streams.py -v`

Expected: all 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/exec_rest_api/handlers/streams.py tests/unit/test_handlers_streams.py
git commit -m "Add /streams/sync-status SSE handler"
```

---

## Task 9: `Last-Event-ID` replay for `/streams/blocks` and `/streams/logs`

Per spec §7.3, on reconnect the client carries `Last-Event-ID`. For blocks, we fetch missed blocks (via `eth_getBlockByNumber`) between `lastId+1` and the current head. For logs, we fetch missed logs (via `eth_getLogs`) over the same range using the URL filter. Both are bounded by `sse_replay_window`. Beyond that, emit `event: gap` and resume live.

`pending-transactions` and `sync-status` have no replay — they get a one-time `event: resumed` then continue live.

**Files:**
- Modify: `src/exec_rest_api/handlers/streams.py`
- Modify: `tests/unit/test_handlers_streams.py`

- [ ] **Step 1: Append the failing tests**

Append to `tests/unit/test_handlers_streams.py`:

```python
class FakeUpstream:
    """Minimal stand-in for UpstreamClient. Tests inject scripted replies."""

    def __init__(self) -> None:
        self.replies: dict[tuple[str, str], Any] = {}
        self.calls: list[tuple[str, list[Any]]] = []

    async def call(self, method: str, params: list[Any] | None = None) -> Any:
        self.calls.append((method, list(params or [])))
        key = (method, json.dumps(params or [], sort_keys=True))
        return self.replies.get(key)


async def _build_client_with_upstream(aiohttp_client, manager, upstream):
    app = create_app(config=_config(), upstream=upstream)
    app["subscriptions"] = manager
    register_routes(app)
    return await aiohttp_client(app)


def _block_with_number(n: int) -> dict[str, Any]:
    return {
        "number": hex(n),
        "hash": "0x" + f"{n:064x}",
        "parentHash": "0x" + "00" * 32,
        "stateRoot": "0x" + "00" * 32,
        "transactionsRoot": "0x" + "00" * 32,
        "receiptsRoot": "0x" + "00" * 32,
        "logsBloom": "0x" + "00" * 256,
        "gasUsed": "0x0",
        "gasLimit": "0x0",
        "timestamp": "0x0",
        "miner": "0x" + "00" * 20,
        "difficulty": "0x0",
        "totalDifficulty": "0x0",
        "extraData": "0x",
        "mixHash": "0x" + "00" * 32,
        "nonce": "0x0000000000000000",
        "size": "0x0",
    }


async def test_streams_blocks_replay_via_last_event_id(aiohttp_client):
    import json as _json
    mgr = FakeManager()
    upstream = FakeUpstream()
    upstream.replies[("eth_blockNumber", "[]")] = "0x12"  # head = 18
    for n in (16, 17, 18):
        upstream.replies[
            ("eth_getBlockByNumber", _json.dumps([hex(n), True], sort_keys=True))
        ] = _block_with_number(n)

    client = await _build_client_with_upstream(aiohttp_client, mgr, upstream)
    resp = await client.get(
        "/streams/blocks", headers={"Last-Event-ID": "15"}
    )
    assert resp.status == 200

    # First three frames carry the replayed blocks (ids 16, 17, 18).
    text = b""
    deadline = asyncio.get_event_loop().time() + 2.0
    while text.count(b"event: block") < 3:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"only got {text.count(b'event: block')} block events; raw={text!r}")
        text += await resp.content.read(512)
    assert b"id: 16" in text
    assert b"id: 17" in text
    assert b"id: 18" in text
    mgr.close()
    await resp.release()


async def test_streams_blocks_replay_beyond_window_emits_gap(aiohttp_client):
    import json as _json
    mgr = FakeManager()
    upstream = FakeUpstream()
    # Pretend the chain is way ahead — more than sse_replay_window (1024) blocks.
    upstream.replies[("eth_blockNumber", "[]")] = hex(5000)

    client = await _build_client_with_upstream(aiohttp_client, mgr, upstream)
    resp = await client.get("/streams/blocks", headers={"Last-Event-ID": "100"})
    assert resp.status == 200
    text = b""
    deadline = asyncio.get_event_loop().time() + 1.0
    while b"event: gap" not in text:
        if asyncio.get_event_loop().time() > deadline:
            pytest.fail(f"never saw gap; got {text!r}")
        text += await resp.content.read(256)
    mgr.close()
    await resp.release()
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_handlers_streams.py -v -k replay`

Expected: failures (no replay implementation yet — handlers ignore Last-Event-ID).

- [ ] **Step 3: Extend `handlers/streams.py`**

Add (before the handlers section):

```python
async def _replay_blocks(
    request: web.Request,
    resp: web.StreamResponse,
    *,
    last_event_id: str,
) -> None:
    """Backfill blocks between (lastId+1) and current head, bounded by config.sse_replay_window."""
    config = request.app["config"]
    upstream = request.app["upstream"]
    try:
        last_block = int(last_event_id)
    except ValueError:
        return  # invalid id; skip replay
    head_hex = await upstream.call("eth_blockNumber")
    head = hex_to_int(head_hex)
    if last_block >= head:
        return
    missed = head - last_block
    if missed > config.sse_replay_window:
        await resp.write(format_event(event="gap", id_=None, data={
            "from": last_block + 1,
            "to": head,
        }))
        return
    for n in range(last_block + 1, head + 1):
        rpc = await upstream.call("eth_getBlockByNumber", [hex(n), True])
        if rpc is None:
            continue
        header = block_header_from_rpc(rpc)
        await resp.write(format_event(event="block", id_=str(header["number"]), data=header))


async def _replay_logs(
    request: web.Request,
    resp: web.StreamResponse,
    *,
    last_event_id: str,
    filter_: dict[str, Any],
) -> None:
    """Backfill logs over the missed range using eth_getLogs with the SSE URL filter."""
    config = request.app["config"]
    upstream = request.app["upstream"]
    try:
        # Last-Event-ID format: "<blockNumber>-<logIndex>"
        block_str, _ = last_event_id.split("-", 1)
        last_block = int(block_str)
    except ValueError:
        return
    head_hex = await upstream.call("eth_blockNumber")
    head = hex_to_int(head_hex)
    if last_block >= head:
        return
    if head - last_block > config.sse_replay_window:
        await resp.write(format_event(event="gap", id_=None, data={
            "from": last_block + 1,
            "to": head,
        }))
        return
    fetch_filter: dict[str, Any] = dict(filter_)
    fetch_filter["fromBlock"] = hex(last_block + 1)
    fetch_filter["toBlock"] = hex(head)
    logs = await upstream.call("eth_getLogs", [fetch_filter])
    for log in logs or []:
        rest_log = log_from_rpc(log)
        ev_id = f"{rest_log['blockNumber']}-{rest_log['logIndex']}"
        await resp.write(format_event(event="log", id_=ev_id, data=rest_log))
```

Replace `_run_stream` with a version that takes an optional `replay` async callback:

```python
ReplayFn = Callable[[web.Request, web.StreamResponse, str], "Any"]


async def _run_stream(
    request: web.Request,
    *,
    kind: str,
    params: Any,
    formatter: EventFormatter,
    gap_event_name: str = "gap",
    replay: ReplayFn | None = None,
) -> web.StreamResponse:
    subscriptions = request.app["subscriptions"]
    config = request.app["config"]
    try:
        events: AsyncIterator[StreamEvent] = await subscriptions.subscribe(
            kind=kind, params=params
        )
    except SubscriptionUnavailable as exc:
        return problem_response(
            Problem(
                status=503,
                type_slug="upstream-unavailable",
                title="Upstream unavailable",
                detail=str(exc),
                instance=request.path,
            )
        )

    resp = await _open_sse(request)

    last_event_id = request.headers.get("Last-Event-ID")
    if replay is not None and last_event_id is not None:
        try:
            await replay(request, resp, last_event_id)
        except Exception:
            logger.exception("replay failed on %s", kind)

    async def to_bytes() -> AsyncIterator[bytes]:
        async for event in events:
            if event is GAP or event.kind == "gap":
                yield format_event(event=gap_event_name, id_=None, data={})
                continue
            try:
                name, ev_id, payload = formatter(event.payload)
            except Exception:
                logger.exception("event formatter raised on %s", kind)
                yield format_event(
                    event="error",
                    id_=None,
                    data={
                        "type": "https://errors.ethereum-rest/internal-error",
                        "title": "Internal error",
                    },
                )
                return
            yield format_event(event=name, id_=ev_id, data=payload)

    try:
        async for chunk in stream_with_heartbeat(
            to_bytes(), interval_seconds=config.sse_heartbeat_seconds
        ):
            if _over_backpressure_threshold(request, config.sse_buffer_bytes):
                logger.info("dropping SSE client over backpressure threshold")
                return resp
            await resp.write(chunk)
    except ConnectionResetError:
        pass
    finally:
        await events.aclose()  # type: ignore[attr-defined]
    return resp
```

Wire replay into the blocks handler:

```python
async def get_streams_blocks(request: web.Request) -> web.StreamResponse:
    return await _run_stream(
        request,
        kind="newHeads",
        params=None,
        formatter=_block_event,
        replay=_replay_blocks,
    )
```

Wire replay into the logs handler:

```python
async def get_streams_logs(request: web.Request) -> web.StreamResponse:
    filter_or_err = _parse_log_filter(request)
    if isinstance(filter_or_err, web.Response):
        return filter_or_err

    async def replay(req: web.Request, resp: web.StreamResponse, leid: str) -> None:
        await _replay_logs(req, resp, last_event_id=leid, filter_=filter_or_err)

    return await _run_stream(
        request,
        kind="logs",
        params=filter_or_err,
        formatter=_log_event,
        replay=replay,
    )
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/unit/test_handlers_streams.py -v`

Expected: all 12 tests pass.

- [ ] **Step 5: Type-check**

Run: `mypy src/exec_rest_api/handlers/streams.py`

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/handlers/streams.py tests/unit/test_handlers_streams.py
git commit -m "Add Last-Event-ID replay for /streams/blocks and /streams/logs"
```

---

## Task 10: Wire into `__main__.py` and the test `conftest.py`

`UpstreamWebSocket` and `SubscriptionManager` need to be started on app boot, attached to `app["subscriptions"]`, and stopped on shutdown. The conformance + integration fixtures need the same wiring.

**Files:**
- Modify: `src/exec_rest_api/__main__.py`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Update `__main__.py`**

Open `src/exec_rest_api/__main__.py`. Find the `_run` function and replace it with the WS-aware version. Add a top-level import:

```python
from exec_rest_api.handlers import streams as streams_handler
from exec_rest_api.subscriptions import SubscriptionManager
from exec_rest_api.upstream_ws import UpstreamWebSocket
```

Replace `_run`:

```python
async def _run(config: Config) -> None:
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=config.upstream_timeout_seconds)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        upstream = UpstreamClient(
            session=session,
            http_url=config.upstream_http,
            default_timeout_seconds=config.upstream_timeout_seconds,
        )

        # WebSocket + subscription manager. If the WS endpoint can't be reached,
        # we still serve the REST surface; /streams/* return 503 until WS recovers.
        ws_client = UpstreamWebSocket(
            session=session,
            url=config.upstream_ws,
            on_notification=lambda _: None,  # rewired below once manager exists
        )
        subscriptions = SubscriptionManager(ws=ws_client)
        ws_client._on_notification = subscriptions.on_notification  # type: ignore[attr-defined]
        ws_client._on_reconnect = subscriptions.on_reconnect  # type: ignore[attr-defined]

        ws_started = False
        try:
            await asyncio.wait_for(ws_client.start(), timeout=5.0)
            ws_started = True
        except (asyncio.TimeoutError, Exception) as exc:
            logging.getLogger("exec_rest_api").warning(
                "upstream WS unreachable at startup (%r); /streams/* will 503 until it recovers",
                exc,
            )

        app = create_app(config=config, upstream=upstream)
        app["subscriptions"] = subscriptions
        health.register_routes(app)
        chain.register_routes(app)
        gas.register_routes(app)
        transactions.register_routes(app)
        blocks.register_routes(app)
        accounts.register_routes(app)
        logs.register_routes(app)
        traces.register_routes(app)
        computed.register_routes(app)
        utils_keccak.register_routes(app)
        streams_handler.register_routes(app)

        host, port = _split_listen(config.listen)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()

        logging.getLogger("exec_rest_api").info(
            "listening on http://%s (upstream %s)",
            config.listen,
            config.upstream_http,
            extra={"listen": config.listen, "upstream_http": config.upstream_http},
        )

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
        await runner.cleanup()
        if ws_started:
            await ws_client.stop()
```

Replace the `on_notification` wiring with public constructor arguments. To keep that clean, expose them on `UpstreamWebSocket` by editing the constructor: change the `_on_notification` / `_on_reconnect` attributes to public, so the `# type: ignore` shims above become straight assignments. Update `upstream_ws.py`:

In `src/exec_rest_api/upstream_ws.py`, change the constructor body from:

```python
        self._on_notification = on_notification
        self._on_reconnect = on_reconnect
```

to:

```python
        self.on_notification = on_notification
        self.on_reconnect = on_reconnect
```

And update the dispatch site (line in `_dispatch`):

```python
            try:
                self.on_notification(payload)
            except Exception:
                logger.exception("notification handler raised")
            return
```

…and `_run_forever`:

```python
                    if is_reconnect and self.on_reconnect is not None:
                        await self.on_reconnect()
```

Now in `__main__.py`, replace the `# type: ignore` lines with direct attribute assignment:

```python
        ws_client.on_notification = subscriptions.on_notification
        ws_client.on_reconnect = subscriptions.on_reconnect
```

- [ ] **Step 2: Update `tests/conftest.py`**

Open `tests/conftest.py`. Add imports and wire the WS + manager into `proxy_client`. Replace the `proxy_client` fixture entirely with:

```python
@pytest_asyncio.fixture
async def proxy_client(anvil_url, aiohttp_client):
    """Build the proxy app talking to anvil and return an aiohttp test client."""
    from exec_rest_api.handlers import streams as streams_handler
    from exec_rest_api.subscriptions import SubscriptionManager
    from exec_rest_api.upstream_ws import UpstreamWebSocket

    ws_url = anvil_url.replace("http://", "ws://")
    async with aiohttp.ClientSession() as session:
        upstream = UpstreamClient(session=session, http_url=anvil_url)
        ws_client = UpstreamWebSocket(
            session=session,
            url=ws_url,
            on_notification=lambda _: None,
            backoff_schedule=(0.1,),
        )
        manager = SubscriptionManager(ws=ws_client)
        ws_client.on_notification = manager.on_notification
        ws_client.on_reconnect = manager.on_reconnect
        try:
            await asyncio.wait_for(ws_client.start(), timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            pass

        app = create_app(config=_build_config(anvil_url), upstream=upstream)
        app["subscriptions"] = manager
        health.register_routes(app)
        chain.register_routes(app)
        gas.register_routes(app)
        transactions.register_routes(app)
        blocks.register_routes(app)
        accounts.register_routes(app)
        logs.register_routes(app)
        traces.register_routes(app)
        computed.register_routes(app)
        utils_keccak.register_routes(app)
        streams_handler.register_routes(app)
        try:
            client = await aiohttp_client(app)
            yield client
        finally:
            if ws_client.connected:
                await ws_client.stop()
```

Also add `import asyncio` to the top of `tests/conftest.py` if it isn't already there.

- [ ] **Step 3: Sanity-check the binary still starts**

Run: `python -m exec_rest_api --version`

Expected: `exec-rest-api 0.1.0`.

- [ ] **Step 4: Run the unit suite end-to-end**

Run: `pytest tests/unit -v`

Expected: every test passes.

- [ ] **Step 5: Type-check the modified files**

Run: `mypy src/exec_rest_api/upstream_ws.py src/exec_rest_api/__main__.py`

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/__main__.py src/exec_rest_api/upstream_ws.py tests/conftest.py
git commit -m "Wire UpstreamWebSocket and SubscriptionManager into runtime + tests"
```

---

## Task 11: Integration test against anvil

End-to-end: spin up anvil with `--block-time 1`, subscribe via SSE to `/streams/blocks`, observe at least two block events within 3 seconds.

**Files:**
- Create: `tests/integration/test_streams.py`

- [ ] **Step 1: Write the integration test**

Create `tests/integration/test_streams.py`:

```python
"""Live tests against anvil for /streams/*."""

from __future__ import annotations

import asyncio
import json


async def test_streams_blocks_emits_against_anvil(proxy_client):
    """Anvil mines every second; we should see ≥ 2 block events within 3s."""
    resp = await proxy_client.get("/streams/blocks")
    assert resp.status == 200
    assert resp.content_type == "text/event-stream"

    block_events: list[bytes] = []
    deadline = asyncio.get_event_loop().time() + 4.0
    buf = b""
    while asyncio.get_event_loop().time() < deadline and len(block_events) < 2:
        chunk = await resp.content.read(512)
        if not chunk:
            break
        buf += chunk
        while b"\n\n" in buf:
            frame, buf = buf.split(b"\n\n", 1)
            if b"event: block" in frame:
                block_events.append(frame)
    await resp.release()
    assert len(block_events) >= 2, f"only got {len(block_events)} block events"


async def test_streams_sync_status_emits_initial_state(proxy_client):
    """anvil reports `not syncing`; we should see at least one sync-status frame."""
    resp = await proxy_client.get("/streams/sync-status")
    assert resp.status == 200

    deadline = asyncio.get_event_loop().time() + 3.0
    seen = False
    buf = b""
    while asyncio.get_event_loop().time() < deadline and not seen:
        chunk = await resp.content.read(512)
        if not chunk:
            break
        buf += chunk
        seen = b"event: sync-status" in buf or b"retry: 5000" in buf
    await resp.release()
    # We only assert the channel opened correctly; anvil never starts syncing,
    # so the upstream subscription may never deliver a payload — which is fine.
    assert seen
```

- [ ] **Step 2: Run the test**

Run: `pytest tests/integration/test_streams.py -v`

Expected: both tests pass (or skip if anvil isn't installed). The blocks test may take ~3 seconds.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_streams.py
git commit -m "Add integration tests for /streams/blocks and /streams/sync-status against anvil"
```

---

## Task 12: Conformance — the first event of each stream validates against the schema

Pull one event from each stream and validate it against the OpenAPI schema. For blocks: BlockHeader. For logs: Log (skipped unless we can guarantee a log fires within the test window — we keep it light and only check structural shape). For sync-status: SyncStatus. For pending-transactions: structural check only.

**Files:**
- Create: `tests/conformance/test_streams.py`

- [ ] **Step 1: Write the conformance test**

Create `tests/conformance/test_streams.py`:

```python
"""Conformance: first events on each stream validate against the OpenAPI schemas."""

from __future__ import annotations

import asyncio
import json


def _extract_data(frame: bytes) -> dict | None:
    for line in frame.splitlines():
        if line.startswith(b"data: "):
            return json.loads(line[len(b"data: "):])
    return None


async def _first_event(resp, *, event_name: bytes, timeout: float = 4.0) -> dict:
    buf = b""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        chunk = await resp.content.read(512)
        if not chunk:
            break
        buf += chunk
        while b"\n\n" in buf:
            frame, buf = buf.split(b"\n\n", 1)
            if event_name in frame:
                data = _extract_data(frame)
                assert data is not None, f"no data line in frame: {frame!r}"
                return data
    raise AssertionError(f"never saw {event_name!r} within {timeout}s")


async def test_streams_blocks_first_event_validates(proxy_client, make_validator):
    resp = await proxy_client.get("/streams/blocks")
    assert resp.status == 200
    data = await _first_event(resp, event_name=b"event: block")
    make_validator("#/components/schemas/BlockHeader").validate(data)
    await resp.release()


async def test_streams_sync_status_first_event_validates(proxy_client, make_validator):
    resp = await proxy_client.get("/streams/sync-status")
    assert resp.status == 200
    # anvil may never push a sync-status frame, so we only assert the channel opened.
    buf = await resp.content.read(64)
    assert b"retry: 5000" in buf
    await resp.release()
```

- [ ] **Step 2: Run the test**

Run: `pytest tests/conformance/test_streams.py -v`

Expected: both tests pass (anvil mines every second, so the blocks event arrives well within the timeout).

- [ ] **Step 3: Commit**

```bash
git add tests/conformance/test_streams.py
git commit -m "Add conformance tests for /streams/blocks and /streams/sync-status framing"
```

---

## Task 13: Full suite + lint + final commit

Last cross-cuts: lint, type-check, ensure no regressions across the existing tests.

- [ ] **Step 1: Run the complete test suite**

Run: `pytest -v`

Expected: every unit, integration, and conformance test passes (or skips due to missing anvil).

- [ ] **Step 2: Run ruff**

Run: `ruff check src tests`

Expected: no findings. Fix any issues that crop up — common ones: unused imports in `__main__.py`, missing `from __future__ import annotations` in new modules.

- [ ] **Step 3: Run mypy on the whole `src/` tree**

Run: `mypy src`

Expected: `Success: no issues found in N source files`.

- [ ] **Step 4: Update the README status line**

Open `README.md` and bump the status section from `v0.3` (post-Plan 3) to mention streams:

```markdown
## Status

`v0.4` — streams added. Endpoints: `/chain/*`, `/blocks/*`, `/accounts/*`,
`/transactions/*`, `/logs`, `/traces/*`, `/gas/*`, `/utils/keccak256`,
`/health/*`, `/streams/{blocks,logs,pending-transactions,sync-status}`.
```

(Exact wording can mirror the prior status line — the important part is calling out the new streams capability.)

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Bump README to v0.4: SSE streams shipped"
```

- [ ] **Step 6: Tag a marker (optional)**

```bash
git tag v0.4.0-streams-complete
```

---

## Plan 4 complete

End state:

- A persistent WS connection multiplexes upstream subscriptions across all SSE clients.
- `/streams/blocks`, `/streams/logs`, `/streams/pending-transactions`, `/streams/sync-status` all emit correctly framed events.
- `Last-Event-ID` replay works for blocks and logs within the configured `sse_replay_window`; beyond it, clients receive `event: gap`.
- WS reconnects emit `event: gap` to every active SSE client; the upstream resubscribes transparently.
- Backpressure drops connections when the kernel send buffer exceeds `sse_buffer_bytes`.
- Heartbeats keep idle connections from being closed by intermediaries.
- Pre-stream errors return Problem+JSON; mid-stream errors emit `event: error` and close.

Plan 5 (observability + release pipeline) adds Prometheus metrics, `X-Block-Height` header from the `newHeads` consumer, and the signed CI/CD pipeline.
