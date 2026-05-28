"""Tests for /traces handlers."""

from typing import Any
from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.cursor import TraceCursor, decode_trace_cursor, encode_trace_cursor
from exec_rest_api.handlers.traces import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError


def _config(default_page_size: int = 1000, max_page_size: int = 10000) -> Config:
    return Config(
        upstream_http="http://localhost:8545",
        upstream_ws="ws://localhost:8545",
        listen="127.0.0.1:8080",
        upstream_timeout_seconds=30.0,
        default_page_size=default_page_size,
        max_page_size=max_page_size,
        sse_buffer_bytes=65536,
        sse_replay_window=1024,
        sse_heartbeat_seconds=30,
        ready_sync_lag=10,
        log_level="info",
        log_format=None,
        metrics_enabled=True,
    )


async def _build_client(aiohttp_client, mock_upstream: UpstreamClient, **cfg_overrides):
    app = create_app(config=_config(**cfg_overrides), upstream=mock_upstream)
    register_routes(app)
    return await aiohttp_client(app)


def _trace_rpc(block: int) -> dict[str, Any]:
    return {
        "action": {"from": "0x" + "11" * 20, "to": "0x" + "22" * 20},
        "type": "call",
        "subtraces": 0,
        "traceAddress": [],
        "transactionHash": "0x" + "ee" * 32,
        "blockHash": "0x" + "cd" * 32,
        "blockNumber": f"0x{block:x}",
    }


# ─── /traces (filter) ─────────────────────────────────────────────────────


async def test_traces_filter_basic(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_blockNumber":
            return "0x100"
        if method == "trace_filter":
            captured.append(params[0])
            return [_trace_rpc(10), _trace_rpc(11)]
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    a = "0x" + "aa" * 20
    resp = await client.get(f"/traces?fromBlock=0&toBlock=100&fromAddress={a}")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 2
    assert body[0]["blockNumber"] == 10
    assert captured[0]["fromBlock"] == "0x0"
    assert captured[0]["toBlock"] == "0x64"
    assert captured[0]["fromAddress"] == [a]


async def test_traces_filter_to_address(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "trace_filter":
            captured.append(params[0])
            return []
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    a1 = "0x" + "aa" * 20
    a2 = "0x" + "bb" * 20
    resp = await client.get(f"/traces?fromBlock=0&toBlock=100&toAddress={a1},{a2}")
    assert resp.status == 200
    assert captured[0]["toAddress"] == [a1, a2]


async def test_traces_pagination_emits_next_cursor(aiohttp_client):
    """When upstream returns exactly `count` items, emit a next cursor."""
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "trace_filter":
            filt = params[0]
            count = filt.get("count", 10)
            return [_trace_rpc(10) for _ in range(count)]
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/traces?fromBlock=0&toBlock=100&limit=5")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 5
    link = resp.headers["Link"]
    assert 'rel="next"' in link
    import re
    m = re.search(r"cursor=([A-Za-z0-9_\-]+)", link)
    assert m is not None
    cursor = decode_trace_cursor(m.group(1))
    assert cursor.after == 5
    assert cursor.from_block == 0
    assert cursor.to_block == 100


async def test_traces_no_next_when_fewer_than_limit(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "trace_filter":
            return [_trace_rpc(10), _trace_rpc(11)]
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/traces?fromBlock=0&toBlock=100&limit=10")
    assert resp.status == 200
    assert "Link" not in resp.headers or 'rel="next"' not in resp.headers.get("Link", "")


async def test_traces_resume_via_cursor(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "trace_filter":
            captured.append(params[0])
            return []
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    cursor = encode_trace_cursor(
        TraceCursor(
            after=100,
            from_block=10,
            to_block=200,
            filter_={"fromAddress": ["0x" + "aa" * 20]},
        )
    )
    resp = await client.get(f"/traces?cursor={cursor}&fromBlock=99999")
    assert resp.status == 200
    # eth_blockNumber should NOT be called (range frozen)
    assert captured[0]["fromBlock"] == "0xa"
    assert captured[0]["toBlock"] == "0xc8"
    assert captured[0]["fromAddress"] == ["0x" + "aa" * 20]
    assert captured[0]["after"] == 100


async def test_traces_malformed_cursor_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/traces?cursor=garbage!!!")
    assert resp.status == 400


async def test_traces_from_gt_to_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/traces?fromBlock=200&toBlock=100")
    assert resp.status == 400


async def test_traces_filter_unsupported_upstream_501(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "trace_filter":
            raise UpstreamJsonRpcError(code=-32601, message="not found")
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/traces?fromBlock=0&toBlock=10")
    assert resp.status == 501


# ─── /traces/{txHash}/{traceAddress} ──────────────────────────────────────


async def test_trace_get_root(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _trace_rpc(10)
    client = await _build_client(aiohttp_client, mock)

    tx = "0x" + "ab" * 32
    resp = await client.get(f"/traces/{tx}/")
    assert resp.status == 200
    body = await resp.json()
    assert body["blockNumber"] == 10
    mock.call.assert_awaited_once_with("trace_get", [tx, []])


async def test_trace_get_nested(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _trace_rpc(10)
    client = await _build_client(aiohttp_client, mock)

    tx = "0x" + "ab" * 32
    resp = await client.get(f"/traces/{tx}/0,1,2")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("trace_get", [tx, [0, 1, 2]])


async def test_trace_get_not_found(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    tx = "0x" + "ff" * 32
    resp = await client.get(f"/traces/{tx}/0")
    assert resp.status == 404


async def test_trace_get_bad_hash_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/traces/0xnope/0,1")
    assert resp.status == 400


async def test_trace_get_bad_trace_address_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    tx = "0x" + "ab" * 32
    resp = await client.get(f"/traces/{tx}/not,a,number")
    assert resp.status == 400


# ─── POST /traces/call ────────────────────────────────────────────────────


async def test_trace_call_forwards_with_at_default(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"output": "0x", "trace": [], "stateDiff": None, "vmTrace": None}
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/traces/call",
        json={
            "call": {"to": "0x" + "ab" * 20, "data": "0x"},
            "tracers": ["trace"],
        },
    )
    assert resp.status == 200
    args, _ = mock.call.call_args
    method, params = args
    assert method == "trace_call"
    assert params[0]["to"] == "0x" + "ab" * 20
    assert params[1] == ["trace"]
    assert params[2] == "latest"


async def test_trace_call_explicit_at(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {}
    client = await _build_client(aiohttp_client, mock)
    await client.post(
        "/traces/call",
        json={
            "call": {"to": "0x" + "ab" * 20},
            "tracers": ["trace"],
            "at": "200",
        },
    )
    args, _ = mock.call.call_args
    _, params = args
    assert params[2] == "0xc8"


async def test_trace_call_missing_tracers_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/traces/call", json={"call": {"to": "0x" + "ab" * 20}})
    assert resp.status == 400


async def test_trace_call_many(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [{"output": "0x"}, {"output": "0x"}]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/traces/call-many",
        json={
            "calls": [{"to": "0x" + "11" * 20}, {"to": "0x" + "22" * 20}],
            "tracers": ["trace"],
            "at": "latest",
        },
    )
    assert resp.status == 200
    args, _ = mock.call.call_args
    method, params = args
    assert method == "trace_callMany"
    # Upstream expects [(call, tracers), (call, tracers), ...] and a block
    assert isinstance(params[0], list) and len(params[0]) == 2
    for call_with_tracers in params[0]:
        assert isinstance(call_with_tracers, list)
        assert call_with_tracers[1] == ["trace"]
    assert params[1] == "latest"


async def test_trace_raw_transaction(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"output": "0x", "trace": []}
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/traces/raw-transaction",
        json={"raw": "0xdeadbeef", "tracers": ["trace"]},
    )
    assert resp.status == 200
    mock.call.assert_awaited_once_with(
        "trace_rawTransaction", ["0xdeadbeef", ["trace"]]
    )


async def test_trace_raw_transaction_missing_raw_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/traces/raw-transaction", json={"tracers": ["trace"]}
    )
    assert resp.status == 400


async def test_post_traces_search_forwards_filter(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = [
        # eth_blockNumber for "latest" resolution
        "0x10",
    ]
    # then trace_filter
    mock.call.side_effect = [
        "0x10",
        [
            {
                "action": {},
                "type": "call",
                "subtraces": 0,
                "traceAddress": [],
                "transactionHash": "0x" + "aa" * 32,
                "blockHash": "0x" + "bb" * 32,
                "blockNumber": "0x10",
            }
        ],
    ]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/traces/search",
        json={
            "fromBlock": "0",
            "toBlock": "latest",
            "fromAddress": ["0x" + "11" * 20],
        },
    )
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list) and len(body) == 1
