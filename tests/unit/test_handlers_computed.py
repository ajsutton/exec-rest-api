"""Tests for computed-read handlers + shared CallRequest conversion."""

from unittest.mock import AsyncMock

import pytest

from exec_rest_api.config import Config
from exec_rest_api.handlers.computed import call_request_to_rpc, register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError


def test_minimal_call_request():
    body = {"to": "0x" + "ab" * 20}
    rpc, at = call_request_to_rpc(body)
    assert rpc == {"to": "0x" + "ab" * 20}
    assert at == "latest"


def test_at_default_latest_can_be_overridden_with_block_number():
    body = {"to": "0x" + "ab" * 20, "at": "100"}
    rpc, at = call_request_to_rpc(body)
    assert at == "0x64"


def test_at_tag():
    body = {"to": "0x" + "ab" * 20, "at": "safe"}
    _, at = call_request_to_rpc(body)
    assert at == "safe"


def test_at_block_hash():
    h = "0x" + "ab" * 32
    body = {"to": "0x" + "ab" * 20, "at": h}
    _, at = call_request_to_rpc(body)
    assert at == h


def test_numeric_fields_converted_to_hex():
    body = {
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "gas": 21000,
        "gasPrice": "1000000000",
        "value": "5000000000000000000",
        "nonce": 7,
        "chainId": 1,
        "data": "0xdeadbeef",
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["from"] == "0x" + "11" * 20
    assert rpc["to"] == "0x" + "22" * 20
    assert rpc["gas"] == "0x5208"
    assert rpc["gasPrice"] == "0x3b9aca00"
    assert rpc["value"] == "0x4563918244f40000"
    assert rpc["nonce"] == "0x7"
    assert rpc["chainId"] == "0x1"
    assert rpc["data"] == "0xdeadbeef"


def test_eip1559_fields():
    body = {
        "maxFeePerGas": "2000000000",
        "maxPriorityFeePerGas": "1000000000",
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["maxFeePerGas"] == "0x77359400"
    assert rpc["maxPriorityFeePerGas"] == "0x3b9aca00"


def test_access_list_converted():
    body = {
        "accessList": [
            {
                "address": "0x" + "ab" * 20,
                "storageKeys": ["0x" + "11" * 32, "0x" + "22" * 32],
            }
        ]
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["accessList"] == [
        {
            "address": "0x" + "ab" * 20,
            "storageKeys": ["0x" + "11" * 32, "0x" + "22" * 32],
        }
    ]


def test_state_overrides_passthrough_with_numeric_fields_converted():
    body = {
        "stateOverrides": {
            "0x" + "11" * 20: {
                "balance": "1000000000000000000",
                "nonce": 5,
                "code": "0x60",
            }
        }
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["stateOverrides"]["0x" + "11" * 20] == {
        "balance": "0xde0b6b3a7640000",
        "nonce": "0x5",
        "code": "0x60",
    }


def test_block_overrides_numeric_fields_converted():
    body = {
        "blockOverrides": {
            "number": 18234567,
            "timestamp": 1700000000,
            "baseFeePerGas": "1000000000",
        }
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["blockOverrides"] == {
        "number": "0x1163cc7",
        "timestamp": "0x6553f100",
        "baseFeePerGas": "0x3b9aca00",
    }


def test_invalid_address_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc({"to": "not-an-address"})


def test_invalid_numeric_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc({"to": "0x" + "ab" * 20, "gas": "not a number"})


def test_invalid_at_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc({"to": "0x" + "ab" * 20, "at": "garbage"})


def test_access_list_entry_missing_address_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc(
            {"accessList": [{"storageKeys": ["0x" + "11" * 32]}]}
        )


def test_access_list_non_string_storage_key_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc(
            {"accessList": [{"address": "0x" + "ab" * 20, "storageKeys": [123]}]}
        )


def test_data_field_rejects_non_hex():
    with pytest.raises(ValueError):
        call_request_to_rpc({"data": "0xZZ"})


def test_data_field_rejects_odd_length():
    with pytest.raises(ValueError):
        call_request_to_rpc({"data": "0xabc"})


def test_data_field_accepts_empty_hex():
    rpc, _ = call_request_to_rpc({"data": "0x"})
    assert rpc["data"] == "0x"


# ── handler tests ──────────────────────────────────────────────────────────


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


# /call ─────────────────────────────────────────────────────────────────────


async def test_call_success(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x" + "00" * 31 + "2a"  # decimal 42 padded
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post(
        "/call",
        json={"to": "0x" + "ab" * 20, "data": "0x12345678"},
    )
    assert resp.status == 200
    body = await resp.json()
    assert body == {"data": "0x" + "00" * 31 + "2a"}
    mock.call.assert_awaited_once_with(
        "eth_call",
        [{"to": "0x" + "ab" * 20, "data": "0x12345678"}, "latest"],
    )


async def test_call_revert_returns_200_with_reverted_body(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # Error(string) "nope"
    sel = "08c379a0"
    offset = "0" * 62 + "20"
    length = "0" * 62 + "04"
    text = b"nope".hex() + "00" * (32 - 4)
    revert_data = "0x" + sel + offset + length + text
    mock.call.side_effect = UpstreamJsonRpcError(
        code=-32000, message="execution reverted: nope", data=revert_data
    )
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/call", json={"to": "0x" + "ab" * 20})
    assert resp.status == 200
    body = await resp.json()
    assert body["reverted"] is True
    assert body["reason"] == "nope"
    assert body["panicCode"] is None
    assert body["data"] == revert_data


async def test_call_non_revert_error_passes_to_middleware(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(code=-32602, message="bad params")
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/call", json={"to": "0x" + "ab" * 20})
    assert resp.status == 400  # mapped by middleware
    assert resp.headers["Content-Type"].startswith("application/problem+json")


async def test_call_malformed_body_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/call", json={"to": "not-an-address"})
    assert resp.status == 400


async def test_call_non_json_body_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/call", data="not json", headers={"Content-Type": "application/json"}
    )
    assert resp.status == 400


async def test_call_non_string_result_is_502(aiohttp_client):
    """Upstream returning a non-string for eth_call yields a 502 upstream-error."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"unexpected": "object"}
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/call", json={"to": "0x" + "ab" * 20})
    assert resp.status == 502
    body = await resp.json()
    assert body["title"] == "Upstream error"
    assert "eth_call" in body["detail"]


# /gas-estimate ────────────────────────────────────────────────────────────


async def test_gas_estimate_success(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x5208"  # 21000
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/gas-estimate", json={"to": "0x" + "ab" * 20})
    assert resp.status == 200
    body = await resp.json()
    assert body == {"gas": 21000}
    mock.call.assert_awaited_once_with(
        "eth_estimateGas",
        [{"to": "0x" + "ab" * 20}, "latest"],
    )


async def test_gas_estimate_revert_body(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(
        code=-32000, message="execution reverted", data="0x"
    )
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/gas-estimate", json={"to": "0x" + "ab" * 20})
    assert resp.status == 200
    body = await resp.json()
    assert body["reverted"] is True


# /access-list ─────────────────────────────────────────────────────────────


async def test_access_list_success(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "accessList": [
            {
                "address": "0x" + "11" * 20,
                "storageKeys": ["0x" + "00" * 32],
            }
        ],
        "gasUsed": "0x5208",
    }
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/access-list", json={"to": "0x" + "ab" * 20})
    assert resp.status == 200
    body = await resp.json()
    assert body == {
        "accessList": [
            {"address": "0x" + "11" * 20, "storageKeys": ["0x" + "00" * 32]}
        ],
        "gasUsed": 21000,
    }


async def test_access_list_error_field_preserved(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "accessList": [],
        "gasUsed": "0x0",
        "error": "execution reverted",
    }
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/access-list", json={"to": "0x" + "ab" * 20})
    body = await resp.json()
    assert body["error"] == "execution reverted"


async def test_gas_estimate_non_revert_error_passes_to_middleware(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(code=-32603, message="internal error")
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/gas-estimate", json={"to": "0x" + "ab" * 20})
    assert resp.status == 502
    assert resp.headers["Content-Type"].startswith("application/problem+json")


async def test_gas_estimate_non_string_result_is_502(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/gas-estimate", json={"to": "0x" + "ab" * 20})
    assert resp.status == 502


async def test_access_list_null_access_list_field(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"accessList": None, "gasUsed": "0x5208"}
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/access-list", json={"to": "0x" + "ab" * 20})
    assert resp.status == 200
    body = await resp.json()
    assert body == {"accessList": [], "gasUsed": 21000}
