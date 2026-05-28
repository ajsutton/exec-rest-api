"""Tests for POST /utils/keccak256."""

import re
from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.utils_keccak import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


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


async def _build_client(aiohttp_client, mock: UpstreamClient):
    app = create_app(config=_config(), upstream=mock)
    register_routes(app)
    return await aiohttp_client(app)


_HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")


async def test_keccak256_forwards_to_web3_sha3(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    expected_hash = "0x" + "ab" * 32
    mock.call.return_value = expected_hash
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/utils/keccak256", json={"data": "0xdeadbeef"})
    assert resp.status == 200
    body = await resp.json()
    assert body == {"hash": expected_hash}
    mock.call.assert_awaited_once_with("web3_sha3", ["0xdeadbeef"])


async def test_keccak256_lowercases_response(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x" + "AB" * 32
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/utils/keccak256", json={"data": "0x00"})
    body = await resp.json()
    assert _HASH_RE.fullmatch(body["hash"])


async def test_keccak256_missing_data_field_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/utils/keccak256", json={})
    assert resp.status == 400
    assert resp.headers["Content-Type"].startswith("application/problem+json")


async def test_keccak256_non_hex_data_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/utils/keccak256", json={"data": "notHex"})
    assert resp.status == 400


async def test_keccak256_garbled_json_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/utils/keccak256",
        data="not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400


async def test_keccak256_upstream_non_string_502(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/utils/keccak256", json={"data": "0x"})
    assert resp.status == 502
    assert resp.headers["Content-Type"].startswith("application/problem+json")
