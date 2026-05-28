"""Tests for /health and /health/ready handlers."""

from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.health import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient, UpstreamError


def _config(ready_sync_lag: int = 10) -> Config:
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
        ready_sync_lag=ready_sync_lag,
        log_level="info",
        log_format=None,
        metrics_enabled=True,
    )


async def _build_client(
    aiohttp_client, mock_upstream: UpstreamClient, config: Config | None = None
):
    app = create_app(config=config or _config(), upstream=mock_upstream)
    register_routes(app)
    return await aiohttp_client(app)


async def test_health_liveness_no_upstream_call(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health")
    assert resp.status == 200
    body = await resp.json()
    assert body == {"status": "ok"}
    mock.call.assert_not_called()


async def test_ready_upstream_reachable_in_sync(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # eth_syncing returns False (synced), eth_blockNumber returns hex 1000
    mock.call.side_effect = [False, "0x3e8"]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 200
    body = await resp.json()
    assert body == {
        "ready": True,
        "upstreamReachable": True,
        "syncing": False,
        "blockNumber": 1000,
    }


async def test_ready_when_actively_syncing_close_enough(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # Syncing 5 blocks behind, under the lag threshold of 10
    mock.call.side_effect = [
        {"startingBlock": "0x0", "currentBlock": "0x3e3", "highestBlock": "0x3e8"},
        "0x3e3",
    ]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 200
    body = await resp.json()
    assert body["ready"] is True
    assert body["syncing"] is True
    assert body["blockNumber"] == 0x3e3


async def test_not_ready_when_too_far_behind(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # 100 blocks behind, beyond ready_sync_lag=10
    mock.call.side_effect = [
        {"startingBlock": "0x0", "currentBlock": "0x384", "highestBlock": "0x3e8"},
        "0x384",
    ]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 503
    body = await resp.json()
    assert body["type"].endswith("/upstream-unavailable")


async def test_not_ready_when_upstream_unreachable(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamError("connection refused")
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 503
    body = await resp.json()
    assert body["type"].endswith("/upstream-unavailable")
