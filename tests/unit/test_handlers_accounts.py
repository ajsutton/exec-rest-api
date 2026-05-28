"""Tests for /accounts/* handlers."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.accounts import register_routes
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


_ADDR = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
_LOWER = _ADDR.lower()


# ─── /accounts/{addr}/balance ─────────────────────────────────────────────


async def test_balance_default_latest(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0xde0b6b3a7640000"  # 1 ETH
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/balance")
    assert resp.status == 200
    assert await resp.json() == {"wei": "1000000000000000000"}
    mock.call.assert_awaited_once_with("eth_getBalance", [_LOWER, "latest"])


async def test_balance_at_specific_block_number(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x0"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/balance?at=100")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getBalance", [_LOWER, "0x64"])


async def test_balance_at_block_hash(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x0"
    client = await _build_client(aiohttp_client, mock)

    h = "0x" + "ab" * 32
    resp = await client.get(f"/accounts/{_ADDR}/balance?at={h}")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getBalance", [_LOWER, h])


async def test_balance_bad_address_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/accounts/0xnope/balance")
    assert resp.status == 400
    mock.call.assert_not_called()


async def test_balance_bad_at_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/balance?at=NOT_A_BLOCK")
    assert resp.status == 400
    mock.call.assert_not_called()


# ─── /accounts/{addr}/nonce ───────────────────────────────────────────────


async def test_nonce(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x2a"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/nonce")
    assert resp.status == 200
    assert await resp.json() == {"nonce": 42}
    mock.call.assert_awaited_once_with("eth_getTransactionCount", [_LOWER, "latest"])


# ─── /accounts/{addr}/code ────────────────────────────────────────────────


async def test_code(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x60806040"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/code")
    assert resp.status == 200
    assert await resp.json() == {"code": "0x60806040"}


async def test_code_empty(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/code")
    assert resp.status == 200
    assert await resp.json() == {"code": "0x"}


# ─── /accounts/{addr}/storage/{slot} ──────────────────────────────────────


async def test_storage_hex_slot(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x" + "00" * 31 + "01"
    client = await _build_client(aiohttp_client, mock)

    slot = "0x" + "0" * 63 + "5"
    resp = await client.get(f"/accounts/{_ADDR}/storage/{slot}")
    assert resp.status == 200
    body = await resp.json()
    assert body == {"value": "0x" + "00" * 31 + "01"}
    mock.call.assert_awaited_once_with("eth_getStorageAt", [_LOWER, slot, "latest"])


async def test_storage_decimal_slot_converts(aiohttp_client):
    """Decimal slot is converted to 0x-hex before forwarding."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x" + "00" * 32
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/storage/5")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getStorageAt", [_LOWER, "0x5", "latest"])


async def test_storage_bad_slot_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/storage/NOPE")
    assert resp.status == 400
    mock.call.assert_not_called()


# ─── /accounts/{addr}/proof ───────────────────────────────────────────────


async def test_proof_basic(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "address": _LOWER,
        "balance": "0x0",
        "codeHash": "0x" + "cc" * 32,
        "nonce": "0x0",
        "storageHash": "0x" + "11" * 32,
        "accountProof": ["0xaaaa", "0xbbbb"],
        "storageProof": [
            {"key": "0x" + "00" * 31 + "05", "value": "0x" + "00" * 31 + "01",
             "proof": ["0xcccc"]}
        ],
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/proof?slots=0x5")
    assert resp.status == 200
    body = await resp.json()
    assert body["address"] == _LOWER
    assert body["balance"] == "0"
    assert body["nonce"] == 0
    assert body["storageProof"][0]["value"] == "0x" + "00" * 31 + "01"
    # Slot is zero-padded to 32 bytes for eth_getProof
    mock.call.assert_awaited_once_with(
        "eth_getProof", [_LOWER, ["0x" + "0" * 63 + "5"], "latest"]
    )


async def test_proof_no_slots(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "address": _LOWER,
        "balance": "0x0",
        "codeHash": "0x" + "cc" * 32,
        "nonce": "0x0",
        "storageHash": "0x" + "11" * 32,
        "accountProof": [],
        "storageProof": [],
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/proof")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getProof", [_LOWER, [], "latest"])


async def test_proof_multiple_slots(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "address": _LOWER,
        "balance": "0x0",
        "codeHash": "0x" + "cc" * 32,
        "nonce": "0x0",
        "storageHash": "0x" + "11" * 32,
        "accountProof": [],
        "storageProof": [],
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/proof?slots=0x5,10,0x3")
    assert resp.status == 200
    args, _ = mock.call.call_args
    # All slots zero-padded to 32 bytes for eth_getProof
    assert args[1] == [
        _LOWER,
        ["0x" + "0" * 63 + "5", "0x" + "0" * 63 + "a", "0x" + "0" * 63 + "3"],
        "latest",
    ]


# ─── /accounts/{addr} composite ───────────────────────────────────────────


async def test_account_summary_eoa_no_code(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        return {
            "eth_getBalance": "0xde0b6b3a7640000",
            "eth_getTransactionCount": "0x5",
            "eth_getCode": "0x",
        }[method]

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}")
    assert resp.status == 200
    body = await resp.json()
    assert body == {
        "address": _LOWER,
        "balance": "1000000000000000000",
        "nonce": 5,
        "hasCode": False,
        "delegatedTo": None,
    }


async def test_account_summary_contract(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        return {
            "eth_getBalance": "0x0",
            "eth_getTransactionCount": "0x1",
            "eth_getCode": "0x60806040" + "11" * 100,
        }[method]

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}")
    assert resp.status == 200
    body = await resp.json()
    assert body["hasCode"] is True
    assert body["delegatedTo"] is None


async def test_account_summary_eip7702_delegated(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    delegate = "1234567890abcdef1234567890abcdef12345678"

    async def call(method: str, params: list[Any] | None = None) -> Any:
        return {
            "eth_getBalance": "0x0",
            "eth_getTransactionCount": "0x0",
            "eth_getCode": "0xef0100" + delegate,
        }[method]

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}")
    assert resp.status == 200
    body = await resp.json()
    assert body["hasCode"] is True
    assert body["delegatedTo"] == "0x" + delegate


async def test_account_summary_fans_out_in_parallel(aiohttp_client):
    """The three RPC calls must use asyncio.gather, not sequential awaits."""
    mock = AsyncMock(spec=UpstreamClient)
    call_order: list[tuple[str, str]] = []

    async def slow_call(method: str, params: list[Any] | None = None) -> Any:
        call_order.append((method, "start"))
        await asyncio.sleep(0.05)
        call_order.append((method, "end"))
        return {
            "eth_getBalance": "0x0",
            "eth_getTransactionCount": "0x0",
            "eth_getCode": "0x",
        }[method]

    mock.call.side_effect = slow_call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}")
    assert resp.status == 200
    starts = [c for c in call_order if c[1] == "start"]
    ends = [c for c in call_order if c[1] == "end"]
    assert len(starts) == 3
    first_end_index = call_order.index(ends[0])
    starts_before_first_end = [c for c in call_order[:first_end_index] if c[1] == "start"]
    assert len(starts_before_first_end) == 3


# ─── /accounts/{addr}/transaction-template ────────────────────────────────


async def test_transaction_template_pre_eip1559(aiohttp_client):
    """Upstream without eth_maxPriorityFeePerGas falls back to gasPrice only."""
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        return {
            "eth_getTransactionCount": "0x5",
            "eth_chainId": "0x1",
            "eth_gasPrice": "0x3b9aca00",
            "eth_maxPriorityFeePerGas": "0x77359400",  # may or may not be invoked
        }[method]

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/transaction-template")
    assert resp.status == 200
    body = await resp.json()
    assert body["nonce"] == 5
    assert body["chainId"] == 1
    assert body["gasPrice"] == "1000000000"
    assert body["maxPriorityFeePerGas"] == "2000000000"


# ─── trailing slash tolerance ─────────────────────────────────────────────


async def test_trailing_slash_balance(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x0"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(f"/accounts/{_ADDR}/balance/")
    assert resp.status == 200
