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
