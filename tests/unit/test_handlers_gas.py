"""Tests for /gas/* handlers."""

from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.gas import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError


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


async def test_gas_price(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x3b9aca00"  # 1_000_000_000 wei
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/price")
    assert resp.status == 200
    assert await resp.json() == {"wei": "1000000000"}
    mock.call.assert_awaited_once_with("eth_gasPrice")


async def test_priority_fee(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x59682f00"  # 1_500_000_000 wei
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/priority-fee")
    assert resp.status == 200
    assert await resp.json() == {"wei": "1500000000"}
    mock.call.assert_awaited_once_with("eth_maxPriorityFeePerGas")


async def test_blob_base_fee(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x1"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/blob-base-fee")
    assert resp.status == 200
    assert await resp.json() == {"wei": "1"}
    mock.call.assert_awaited_once_with("eth_blobBaseFee")


async def test_fee_history_basic(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "oldestBlock": "0x100",
        "baseFeePerGas": ["0x5", "0x6", "0x7"],
        "gasUsedRatio": [0.5, 0.6],
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/fee-history?blockCount=2&newest=latest")
    assert resp.status == 200
    body = await resp.json()
    assert body == {
        "oldestBlock": 0x100,
        "baseFeePerGas": ["5", "6", "7"],
        "gasUsedRatio": [0.5, 0.6],
    }
    mock.call.assert_awaited_once_with(
        "eth_feeHistory", ["0x2", "latest", []]
    )


async def test_fee_history_with_percentiles(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "oldestBlock": "0x100",
        "baseFeePerGas": ["0x5"],
        "gasUsedRatio": [0.5],
        "reward": [["0x1", "0x2"]],
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/fee-history?blockCount=1&newest=100&rewardPercentiles=25,75")
    assert resp.status == 200
    body = await resp.json()
    assert body == {
        "oldestBlock": 0x100,
        "baseFeePerGas": ["5"],
        "gasUsedRatio": [0.5],
        "reward": [["1", "2"]],
    }
    mock.call.assert_awaited_once_with(
        "eth_feeHistory", ["0x1", "0x64", [25.0, 75.0]]
    )


async def test_fee_history_blob_fields(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "oldestBlock": "0x100",
        "baseFeePerGas": ["0x5"],
        "gasUsedRatio": [0.5],
        "baseFeePerBlobGas": ["0x1", "0x2"],
        "blobGasUsedRatio": [0.1],
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/fee-history?blockCount=1&newest=latest")
    assert resp.status == 200
    body = await resp.json()
    assert body["baseFeePerBlobGas"] == ["1", "2"]
    assert body["blobGasUsedRatio"] == [0.1]


async def test_fee_history_missing_block_count_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/fee-history?newest=latest")
    assert resp.status == 400
    body = await resp.json()
    assert body["type"].endswith("/invalid-request")
    mock.call.assert_not_called()


async def test_fee_history_bad_block_count_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/fee-history?blockCount=not-a-number&newest=latest")
    assert resp.status == 400
    body = await resp.json()
    assert body["type"].endswith("/invalid-request")


async def test_fee_history_bad_block_id_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/fee-history?blockCount=2&newest=NOT_A_BLOCK")
    assert resp.status == 400


async def test_fee_history_bad_percentiles_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(
        "/gas/fee-history?blockCount=1&newest=latest&rewardPercentiles=oops"
    )
    assert resp.status == 400


async def test_blob_base_fee_method_not_supported(aiohttp_client):
    """Older clients without EIP-4844 support → upstream raises -32601 → 501."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(
        code=-32601, message="the method eth_blobBaseFee does not exist"
    )
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/blob-base-fee")
    assert resp.status == 501
    body = await resp.json()
    assert body["type"].endswith("/method-not-supported-by-upstream")


async def test_trailing_slash_tolerated(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x1"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/gas/price/")
    assert resp.status == 200
