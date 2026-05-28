"""Tests for the SSE stream handlers using a fake SubscriptionManager."""

from __future__ import annotations

import asyncio
import json
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
