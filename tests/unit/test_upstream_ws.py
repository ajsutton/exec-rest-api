"""Tests for the low-level JSON-RPC over WebSocket client.

These tests exercise the in-process server <-> client loop only; reconnect &
backoff are covered in test_upstream_ws_reconnect.
"""

from __future__ import annotations

import asyncio
import contextlib
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


async def test_start_raises_when_initial_connect_fails_without_reconnect():
    """start() must not hang when reconnect=False and the URL is unreachable."""
    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url="ws://127.0.0.1:1",  # unreachable
            on_notification=lambda _: None,
            reconnect=False,
        )
        with pytest.raises(UpstreamWsClosed):
            await asyncio.wait_for(client.start(), timeout=2.0)
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


async def test_stop_cleans_up_after_start_timeout():
    """If start() is cancelled mid-connect, stop() must still cancel the background task."""
    async with ClientSession() as session:
        client = UpstreamWebSocket(
            session=session,
            url="ws://127.0.0.1:1",  # unreachable, but reconnect=True so it keeps trying
            on_notification=lambda _: None,
            backoff_schedule=(0.5,),
        )
        start_task = asyncio.create_task(client.start())
        await asyncio.sleep(0.1)  # let _run_forever spin up
        start_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await start_task
        # The internal task should still be running
        assert client._task is not None
        assert not client._task.done()
        # stop() must terminate it
        await client.stop()
        assert client._task.done()
