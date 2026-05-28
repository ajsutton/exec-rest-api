"""Tests for /transactions/* handlers and the shape converters they expose."""

from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.transactions import (
    log_from_rpc,
    receipt_from_rpc,
    register_routes,
    trace_from_rpc,
    transaction_from_rpc,
)
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


# ─── shape converters ─────────────────────────────────────────────────────


def _legacy_tx_rpc() -> dict:
    return {
        "type": "0x0",
        "hash": "0x" + "ab" * 32,
        "blockHash": "0x" + "cd" * 32,
        "blockNumber": "0x100",
        "transactionIndex": "0x5",
        "from": "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa",
        "to": "0xBBBBBBBBbbbbbbbbBBBBBBBBbbbbbbbbBBBBBBBB",
        "value": "0xde0b6b3a7640000",  # 1 ether
        "nonce": "0xa",
        "gas": "0x5208",
        "gasPrice": "0x3b9aca00",
        "input": "0xabcd",
        "chainId": "0x1",
        "v": "0x1c",
        "r": "0x" + "11" * 32,
        "s": "0x" + "22" * 32,
    }


def test_transaction_from_rpc_legacy():
    out = transaction_from_rpc(_legacy_tx_rpc())
    assert out["type"] == "legacy"
    assert out["hash"] == "0x" + "ab" * 32
    assert out["blockHash"] == "0x" + "cd" * 32
    assert out["blockNumber"] == 0x100
    assert out["transactionIndex"] == 5
    assert out["from"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert out["to"] == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    assert out["value"] == "1000000000000000000"
    assert out["nonce"] == 10
    assert out["gas"] == 0x5208
    assert out["gasPrice"] == "1000000000"
    assert out["input"] == "0xabcd"
    assert out["chainId"] == 1
    assert out["v"] == "0x1c"
    assert out["r"] == "0x" + "11" * 32
    assert out["s"] == "0x" + "22" * 32
    # legacy txs have no yParity/accessList
    assert "yParity" not in out
    assert "accessList" not in out


def test_transaction_from_rpc_pending_has_null_block_fields():
    rpc = _legacy_tx_rpc()
    rpc["blockHash"] = None
    rpc["blockNumber"] = None
    rpc["transactionIndex"] = None
    out = transaction_from_rpc(rpc)
    assert out["blockHash"] is None
    assert out["blockNumber"] is None
    assert out["transactionIndex"] is None


def test_transaction_from_rpc_contract_creation_to_null():
    rpc = _legacy_tx_rpc()
    rpc["to"] = None
    out = transaction_from_rpc(rpc)
    assert out["to"] is None


def test_transaction_from_rpc_dynamic_fee():
    rpc = _legacy_tx_rpc()
    rpc["type"] = "0x2"
    del rpc["gasPrice"]
    rpc["maxFeePerGas"] = "0x4a817c800"
    rpc["maxPriorityFeePerGas"] = "0x77359400"
    rpc["accessList"] = [
        {"address": "0x" + "ab" * 20, "storageKeys": ["0x" + "cd" * 32]}
    ]
    rpc["yParity"] = "0x1"
    out = transaction_from_rpc(rpc)
    assert out["type"] == "dynamic-fee"
    assert out["maxFeePerGas"] == "20000000000"
    assert out["maxPriorityFeePerGas"] == "2000000000"
    assert out["accessList"] == [
        {"address": "0x" + "ab" * 20, "storageKeys": ["0x" + "cd" * 32]}
    ]
    assert out["yParity"] == 1
    assert "gasPrice" not in out


def test_transaction_from_rpc_blob():
    rpc = _legacy_tx_rpc()
    rpc["type"] = "0x3"
    rpc["maxFeePerBlobGas"] = "0x1"
    rpc["blobVersionedHashes"] = ["0x" + "ee" * 32]
    out = transaction_from_rpc(rpc)
    assert out["type"] == "blob"
    assert out["maxFeePerBlobGas"] == "1"
    assert out["blobVersionedHashes"] == ["0x" + "ee" * 32]


# ─── receipt converter ────────────────────────────────────────────────────


def _receipt_rpc() -> dict:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "transactionIndex": "0x3",
        "blockHash": "0x" + "cd" * 32,
        "blockNumber": "0x100",
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x3b9aca00",
        "contractAddress": None,
        "logs": [],
        "logsBloom": "0x" + "00" * 256,
        "status": "0x1",
        "type": "0x2",
    }


def test_receipt_from_rpc_success():
    out = receipt_from_rpc(_receipt_rpc())
    assert out["status"] == "success"
    assert out["type"] == "dynamic-fee"
    assert out["blockNumber"] == 0x100
    assert out["transactionIndex"] == 3
    assert out["effectiveGasPrice"] == "1000000000"
    assert out["contractAddress"] is None
    assert out["logs"] == []


def test_receipt_from_rpc_contract_creation():
    rpc = _receipt_rpc()
    rpc["to"] = None
    rpc["contractAddress"] = "0x" + "Cc" * 20
    out = receipt_from_rpc(rpc)
    assert out["to"] is None
    assert out["contractAddress"] == "0x" + "cc" * 20


def test_receipt_from_rpc_failed():
    rpc = _receipt_rpc()
    rpc["status"] = "0x0"
    out = receipt_from_rpc(rpc)
    assert out["status"] == "failed"


def test_receipt_from_rpc_blob_fields():
    rpc = _receipt_rpc()
    rpc["blobGasUsed"] = "0x20000"
    rpc["blobGasPrice"] = "0x1"
    out = receipt_from_rpc(rpc)
    assert out["blobGasUsed"] == 0x20000
    assert out["blobGasPrice"] == "1"


# ─── log converter ────────────────────────────────────────────────────────


def test_log_from_rpc():
    rpc = {
        "address": "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa",
        "topics": ["0x" + "11" * 32, "0x" + "22" * 32],
        "data": "0xabcd",
        "blockHash": "0x" + "cd" * 32,
        "blockNumber": "0x100",
        "transactionHash": "0x" + "ee" * 32,
        "transactionIndex": "0x3",
        "logIndex": "0x7",
        "removed": False,
    }
    out = log_from_rpc(rpc)
    assert out == {
        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topics": ["0x" + "11" * 32, "0x" + "22" * 32],
        "data": "0xabcd",
        "blockHash": "0x" + "cd" * 32,
        "blockNumber": 0x100,
        "transactionHash": "0x" + "ee" * 32,
        "transactionIndex": 3,
        "logIndex": 7,
        "removed": False,
    }


# ─── trace converter ──────────────────────────────────────────────────────


def _trace_rpc(block_number: object) -> dict[str, object]:
    """Build a parity-style trace RPC dict with the given blockNumber form."""
    return {
        "action": {"from": "0x" + "11" * 20, "to": "0x" + "22" * 20, "value": "0x0"},
        "type": "call",
        "subtraces": 0,
        "traceAddress": [],
        "transactionHash": "0x" + "AA" * 32,
        "blockHash": "0x" + "BB" * 32,
        "blockNumber": block_number,
    }


def test_trace_from_rpc_blocknumber_as_hex_string():
    out = trace_from_rpc(_trace_rpc("0x10"))
    assert out["blockNumber"] == 16


def test_trace_from_rpc_blocknumber_as_bare_int():
    """Newer anvil builds emit blockNumber as a JSON number for trace_filter."""
    out = trace_from_rpc(_trace_rpc(2))
    assert out["blockNumber"] == 2


def test_trace_from_rpc_blocknumber_as_decimal_string():
    out = trace_from_rpc(_trace_rpc("123"))
    assert out["blockNumber"] == 123


# ─── endpoints ────────────────────────────────────────────────────────────


async def test_get_transaction_by_hash(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _legacy_tx_rpc()
    client = await _build_client(aiohttp_client, mock)

    tx_hash = "0x" + "ab" * 32
    resp = await client.get(f"/transactions/{tx_hash}")
    assert resp.status == 200
    body = await resp.json()
    assert body["hash"] == tx_hash
    assert body["type"] == "legacy"
    mock.call.assert_awaited_once_with("eth_getTransactionByHash", [tx_hash])


async def test_get_transaction_not_found(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    tx_hash = "0x" + "ff" * 32
    resp = await client.get(f"/transactions/{tx_hash}")
    assert resp.status == 404
    body = await resp.json()
    assert body["type"].endswith("/not-found")


async def test_get_transaction_invalid_hash_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/transactions/0xnope")
    assert resp.status == 400
    body = await resp.json()
    assert body["type"].endswith("/invalid-request")
    mock.call.assert_not_called()


async def test_get_receipt(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _receipt_rpc()
    client = await _build_client(aiohttp_client, mock)

    tx_hash = "0x" + "ab" * 32
    resp = await client.get(f"/transactions/{tx_hash}/receipt")
    assert resp.status == 200
    body = await resp.json()
    assert body["status"] == "success"
    assert body["transactionIndex"] == 3
    mock.call.assert_awaited_once_with("eth_getTransactionReceipt", [tx_hash])


async def test_get_receipt_not_found(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    tx_hash = "0x" + "ff" * 32
    resp = await client.get(f"/transactions/{tx_hash}/receipt")
    assert resp.status == 404


async def test_get_trace(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [
        {
            "action": {"from": "0x" + "11" * 20, "to": "0x" + "22" * 20},
            "type": "call",
            "subtraces": 0,
            "traceAddress": [],
            "transactionHash": "0x" + "ab" * 32,
            "blockHash": "0x" + "cd" * 32,
            "blockNumber": "0x100",
        }
    ]
    client = await _build_client(aiohttp_client, mock)

    tx_hash = "0x" + "ab" * 32
    resp = await client.get(f"/transactions/{tx_hash}/trace")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["blockNumber"] == 0x100
    mock.call.assert_awaited_once_with("trace_transaction", [tx_hash])


async def test_get_trace_null_is_404(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    tx_hash = "0x" + "ff" * 32
    resp = await client.get(f"/transactions/{tx_hash}/trace")
    assert resp.status == 404


# ─── POST /transactions ───────────────────────────────────────────────────


from exec_rest_api.upstream import UpstreamJsonRpcError  # noqa: E402


async def test_post_transactions_json_body_returns_202(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    tx_hash = "0x" + "ab" * 32
    mock.call.return_value = tx_hash
    client = await _build_client(aiohttp_client, mock)

    resp = await client.post("/transactions", json={"raw": "0xdeadbeef"})
    assert resp.status == 202
    assert resp.headers["Location"] == f"/transactions/{tx_hash}"
    body = await resp.json()
    assert body == {"hash": tx_hash}
    mock.call.assert_awaited_once_with("eth_sendRawTransaction", ["0xdeadbeef"])


async def test_post_transactions_rlp_body_returns_202(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    tx_hash = "0x" + "cc" * 32
    mock.call.return_value = tx_hash
    client = await _build_client(aiohttp_client, mock)

    raw_bytes = bytes.fromhex("deadbeef")
    resp = await client.post(
        "/transactions",
        data=raw_bytes,
        headers={"Content-Type": "application/vnd.ethereum.rlp"},
    )
    assert resp.status == 202
    body = await resp.json()
    assert body["hash"] == tx_hash
    mock.call.assert_awaited_once_with("eth_sendRawTransaction", ["0xdeadbeef"])


async def test_post_transactions_unsupported_content_type_415(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/transactions",
        data=b"hello",
        headers={"Content-Type": "text/plain"},
    )
    assert resp.status == 415


async def test_post_transactions_malformed_json_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        "/transactions",
        data="not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400


async def test_post_transactions_missing_raw_field_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/transactions", json={})
    assert resp.status == 400


async def test_post_transactions_nonce_too_low_422(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(
        code=-32000, message="nonce too low: have 5 want 8"
    )
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/transactions", json={"raw": "0xdeadbeef"})
    assert resp.status == 422
    body = await resp.json()
    assert body["type"].endswith("/transaction-rejected/nonce-too-low")


async def test_post_transactions_already_known_422(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(code=-32000, message="already known")
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/transactions", json={"raw": "0xdeadbeef"})
    assert resp.status == 422
    body = await resp.json()
    assert body["type"].endswith("/transaction-rejected/already-known")


async def test_post_transactions_empty_hex_raw_400(aiohttp_client):
    """`{"raw": "0x"}` is a zero-byte payload — must be rejected as 400."""
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post("/transactions", json={"raw": "0x"})
    assert resp.status == 400


# ─── GET /transactions/{hash} content negotiation ─────────────────────────


async def test_get_transaction_rlp_accept_returns_bytes(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    raw_hex = "0xf86c"  # short, contrived
    mock.call.return_value = raw_hex
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get(
        f"/transactions/{'0x' + 'aa' * 32}",
        headers={"Accept": "application/vnd.ethereum.rlp"},
    )
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.ethereum.rlp"
    body = await resp.read()
    assert body == bytes.fromhex(raw_hex[2:])
    mock.call.assert_awaited_once_with("debug_getRawTransaction", ["0x" + "aa" * 32])


async def test_get_transaction_unsupported_accept_returns_406(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get(
        f"/transactions/{'0x' + 'aa' * 32}",
        headers={"Accept": "text/html"},
    )
    assert resp.status == 406


async def test_get_transaction_rlp_not_found_404(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get(
        f"/transactions/{'0x' + 'aa' * 32}",
        headers={"Accept": "application/vnd.ethereum.rlp"},
    )
    assert resp.status == 404
    mock.call.assert_awaited_once_with("debug_getRawTransaction", ["0x" + "aa" * 32])


# ─── POST /transactions/{hash}/trace/replay ───────────────────────────────


async def test_post_trace_replay(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"output": "0x", "trace": []}
    client = await _build_client(aiohttp_client, mock)
    h = "0x" + "aa" * 32
    resp = await client.post(
        f"/transactions/{h}/trace/replay",
        json={"tracers": ["trace"]},
    )
    assert resp.status == 200
    mock.call.assert_awaited_once_with("trace_replayTransaction", [h, ["trace"]])


async def test_post_trace_replay_missing_tracers_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.post(
        f"/transactions/{'0x' + 'aa' * 32}/trace/replay", json={}
    )
    assert resp.status == 400


async def test_post_tx_debug_trace(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"gas": "0x5208", "structLogs": []}
    client = await _build_client(aiohttp_client, mock)
    h = "0x" + "aa" * 32
    resp = await client.post(
        f"/transactions/{h}/debug-trace",
        json={"tracer": "callTracer"},
    )
    assert resp.status == 200
    mock.call.assert_awaited_once_with("debug_traceTransaction", [h, {"tracer": "callTracer"}])


async def test_post_tx_debug_trace_empty_body_ok(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {}
    client = await _build_client(aiohttp_client, mock)
    h = "0x" + "aa" * 32
    resp = await client.post(f"/transactions/{h}/debug-trace", json={})
    assert resp.status == 200
    mock.call.assert_awaited_once_with("debug_traceTransaction", [h, {}])


async def test_post_tx_debug_trace_empty_body_400(aiohttp_client):
    """An empty request body fails JSON parsing — should return 400."""
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    h = "0x" + "aa" * 32
    resp = await client.post(f"/transactions/{h}/debug-trace", data=b"")
    assert resp.status == 400
