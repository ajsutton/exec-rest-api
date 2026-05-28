"""Tests for /chain/* handlers."""

import asyncio
from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.chain import register_routes
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


async def _build_client(aiohttp_client, mock_upstream: UpstreamClient):
    app = create_app(config=_config(), upstream=mock_upstream)
    register_routes(app)
    return await aiohttp_client(app)


async def test_chain_id(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x1"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/id")
    assert resp.status == 200
    assert await resp.json() == {"chainId": 1}
    mock.call.assert_awaited_once_with("eth_chainId")


async def test_chain_id_large(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x2a15c308d"  # 11297108109 (Palm)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/id")
    body = await resp.json()
    assert body == {"chainId": 11297108109}


async def test_chain_sync_status_when_synced(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = False
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/sync-status")
    assert await resp.json() == {"syncing": False}


async def test_chain_sync_status_when_syncing(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "startingBlock": "0x0",
        "currentBlock": "0x10",
        "highestBlock": "0x100",
    }
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/sync-status")
    assert await resp.json() == {
        "syncing": True,
        "startingBlock": 0,
        "currentBlock": 16,
        "highestBlock": 256,
    }


async def test_chain_client(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "anvil/v0.2.0"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/client")
    assert await resp.json() == {"client": "anvil/v0.2.0"}
    mock.call.assert_awaited_once_with("web3_clientVersion")


async def test_chain_peers(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # net_peerCount returns hex, net_listening returns bool
    mock.call.side_effect = ["0x23", True]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/peers")
    assert await resp.json() == {"peerCount": 35, "listening": True}


async def test_chain_composite(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method, params=None):
        return {
            "eth_chainId": "0x1",
            "net_version": "1",
            "web3_clientVersion": "anvil/v0.2.0",
            "eth_syncing": False,
            "eth_blockNumber": "0x100",
        }[method]

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain")
    body = await resp.json()
    assert body == {
        "chainId": 1,
        "networkId": "1",
        "client": "anvil/v0.2.0",
        "blockNumber": 256,
        "syncing": {"syncing": False},
    }


async def test_chain_composite_fans_out_in_parallel(aiohttp_client):
    """The composite endpoint must use asyncio.gather, not sequential awaits."""
    mock = AsyncMock(spec=UpstreamClient)
    call_order = []

    async def slow_call(method, params=None):
        call_order.append((method, "start"))
        await asyncio.sleep(0.05)
        call_order.append((method, "end"))
        return {
            "eth_chainId": "0x1",
            "net_version": "1",
            "web3_clientVersion": "anvil/v0.2.0",
            "eth_syncing": False,
            "eth_blockNumber": "0x0",
        }[method]

    mock.call.side_effect = slow_call
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain")
    assert resp.status == 200
    # All 5 calls must have started before any finished
    starts = [c for c in call_order if c[1] == "start"]
    ends = [c for c in call_order if c[1] == "end"]
    assert len(starts) == 5
    # The first "end" event must come after all "start" events
    first_end_index = call_order.index(ends[0])
    starts_before_first_end = [c for c in call_order[:first_end_index] if c[1] == "start"]
    assert len(starts_before_first_end) == 5
