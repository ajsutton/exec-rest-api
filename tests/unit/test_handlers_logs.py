"""Tests for /logs handler."""

from typing import Any
from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.cursor import Cursor, decode_cursor, encode_cursor
from exec_rest_api.handlers.logs import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


def _config(max_page_size: int = 10000, default_page_size: int = 1000) -> Config:
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


def _log_rpc(block: int, log_index: int, address: str = "0x" + "aa" * 20) -> dict[str, Any]:
    return {
        "address": address,
        "topics": [],
        "data": "0x",
        "blockHash": f"0x{block:064x}",
        "blockNumber": f"0x{block:x}",
        "transactionHash": "0x" + "ee" * 32,
        "transactionIndex": "0x0",
        "logIndex": f"0x{log_index:x}",
        "removed": False,
    }


def _block_rpc(number: int) -> dict[str, Any]:
    return {"number": f"0x{number:x}", "hash": f"0x{number:064x}"}


async def test_logs_basic_range(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getLogs":
            return [_log_rpc(100, 0), _log_rpc(101, 0)]
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/logs?fromBlock=100&toBlock=101")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 2
    assert body[0]["blockNumber"] == 100
    # No next-page link
    assert "Link" not in resp.headers or 'rel="next"' not in resp.headers.get("Link", "")


async def test_logs_with_topics_and_address(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getLogs":
            captured.append(params[0])
            return []
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    addr = "0x" + "aa" * 20
    t0 = "0x" + "11" * 32
    t1 = "0x" + "22" * 32
    resp = await client.get(
        f"/logs?fromBlock=100&toBlock=200&address={addr}&topic0={t0}&topic2={t1}"
    )
    assert resp.status == 200
    assert captured[0]["address"] == [addr]
    # topics array: topic0=t0, topic1=null, topic2=t1, topic3 omitted → trailing nulls stripped
    assert captured[0]["topics"] == [t0, None, t1]


async def test_logs_address_csv_list(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        captured.append(params[0])
        return []

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    a1 = "0x" + "aa" * 20
    a2 = "0x" + "bb" * 20
    resp = await client.get(f"/logs?fromBlock=100&toBlock=100&address={a1},{a2}")
    assert resp.status == 200
    assert captured[0]["address"] == [a1, a2]


async def test_logs_latest_resolves_to_block_number(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_blockNumber":
            return "0x200"  # 512
        if method == "eth_getLogs":
            captured.append(params[0])
            return []
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/logs?fromBlock=100&toBlock=latest")
    assert resp.status == 200
    # toBlock is the frozen head height
    assert captured[0]["toBlock"] == "0x200"


async def test_logs_from_gt_to_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/logs?fromBlock=200&toBlock=100")
    assert resp.status == 400
    body = await resp.json()
    assert body["type"].endswith("/invalid-request")


async def test_logs_invalid_topic_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/logs?fromBlock=0&toBlock=10&topic0=not-a-topic")
    assert resp.status == 400


async def test_logs_limit_emits_next_cursor(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getLogs":
            return [_log_rpc(100, 0), _log_rpc(100, 1), _log_rpc(100, 2)]
        if method == "eth_getBlockByNumber":
            return _block_rpc(100)
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/logs?fromBlock=100&toBlock=200&limit=2")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 2

    link = resp.headers["Link"]
    assert 'rel="next"' in link
    # Extract cursor from link header
    import re
    m = re.search(r"cursor=([A-Za-z0-9_\-]+)", link)
    assert m is not None
    cursor = decode_cursor(m.group(1))
    assert cursor.next_from_block == 100
    assert cursor.last_log_index == 1
    assert cursor.to_block == 200
    assert cursor.boundary_block_hash == f"0x{100:064x}"


async def test_logs_resume_via_cursor(aiohttp_client):
    """Following a cursor: filter params are ignored, cursor wins."""
    mock = AsyncMock(spec=UpstreamClient)
    captured: list[dict[str, Any]] = []

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getBlockByNumber":
            return _block_rpc(100)
        if method == "eth_getLogs":
            captured.append(params[0])
            return [_log_rpc(100, 2), _log_rpc(101, 0)]
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    cursor = encode_cursor(
        Cursor(
            next_from_block=100,
            last_log_index=1,
            to_block=200,
            boundary_block_hash=f"0x{100:064x}",
            filter_={"address": ["0x" + "aa" * 20]},
        )
    )
    resp = await client.get(f"/logs?cursor={cursor}&fromBlock=999999&address=0xignored")
    assert resp.status == 200
    body = await resp.json()
    # logIndex 0 and 1 on block 100 were already emitted; we get logIndex 2 + block 101 log 0
    assert (body[0]["blockNumber"], body[0]["logIndex"]) == (100, 2)
    assert (body[1]["blockNumber"], body[1]["logIndex"]) == (101, 0)
    # eth_getLogs was called with the cursor's filter, not the request's filter
    assert captured[0]["address"] == ["0x" + "aa" * 20]


async def test_logs_chain_reorg_during_pagination(aiohttp_client):
    """Cursor's boundary block hash no longer canonical → 409."""
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getBlockByNumber":
            # Block 100's hash has changed (reorg)
            return {"number": "0x64", "hash": "0x" + "ff" * 32}
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    cursor = encode_cursor(
        Cursor(
            next_from_block=100,
            last_log_index=1,
            to_block=200,
            boundary_block_hash=f"0x{100:064x}",
            filter_={},
        )
    )
    resp = await client.get(f"/logs?cursor={cursor}")
    assert resp.status == 409
    body = await resp.json()
    assert body["type"].endswith("/chain-reorged")


async def test_logs_chain_reorg_block_missing(aiohttp_client):
    """Cursor's boundary block no longer exists at all → 409."""
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getBlockByNumber":
            return None
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    cursor = encode_cursor(
        Cursor(
            next_from_block=100,
            last_log_index=1,
            to_block=200,
            boundary_block_hash=f"0x{100:064x}",
            filter_={},
        )
    )
    resp = await client.get(f"/logs?cursor={cursor}")
    assert resp.status == 409


async def test_logs_malformed_cursor_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/logs?cursor=not-a-real-cursor!!!")
    assert resp.status == 400


async def test_logs_limit_clamped_to_max(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        if method == "eth_getLogs":
            return []
        raise AssertionError(method)

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock, max_page_size=100)

    resp = await client.get("/logs?fromBlock=0&toBlock=10&limit=999999")
    assert resp.status == 200
    assert resp.headers["X-Page-Size"] == "100"


async def test_logs_x_page_size_header_default(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = []
    client = await _build_client(aiohttp_client, mock, default_page_size=500)

    resp = await client.get("/logs?fromBlock=0&toBlock=10")
    assert resp.status == 200
    assert resp.headers["X-Page-Size"] == "500"


async def test_post_logs_search_forwards_body_filter(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # Calls in order:
    #   eth_getBlockByNumber for fromBlock=earliest → returns block 0
    #   eth_blockNumber for toBlock=latest → "0x10"
    #   eth_getLogs → []
    mock.call.side_effect = ["0x10", []]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/logs/search",
        json={
            "fromBlock": "0",
            "toBlock": "latest",
            "address": ["0x" + "11" * 20, "0x" + "22" * 20],
            "topics": [
                "0x" + "ab" * 32,
                None,
                ["0x" + "cc" * 32, "0x" + "dd" * 32],
            ],
        },
    )
    assert resp.status == 200
    body = await resp.json()
    assert body == []
    # The eth_getLogs call should carry our filter
    last_call = mock.call.call_args_list[-1]
    args, _ = last_call
    method, params = args
    assert method == "eth_getLogs"
    rpc_filter = params[0]
    assert sorted(rpc_filter["address"]) == sorted(
        ["0x" + "11" * 20, "0x" + "22" * 20]
    )
    assert rpc_filter["topics"][0] == "0x" + "ab" * 32
    assert rpc_filter["topics"][1] is None
    assert rpc_filter["topics"][2] == ["0x" + "cc" * 32, "0x" + "dd" * 32]


async def test_post_logs_search_invalid_address_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/logs/search", json={"address": ["bad"]})
    assert resp.status == 400


async def test_post_logs_search_invalid_topic_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/logs/search",
        json={"topics": ["nope"]},
    )
    assert resp.status == 400
