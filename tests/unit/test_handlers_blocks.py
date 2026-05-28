"""Tests for /blocks/* handlers."""

from typing import Any
from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.blocks import block_header_from_rpc, register_routes
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


def _block_rpc_minimal(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "number": "0x100",
        "hash": "0x" + "ab" * 32,
        "parentHash": "0x" + "cd" * 32,
        "stateRoot": "0x" + "11" * 32,
        "transactionsRoot": "0x" + "22" * 32,
        "receiptsRoot": "0x" + "33" * 32,
        "logsBloom": "0x" + "00" * 256,
        "gasUsed": "0xabcd",
        "gasLimit": "0x1c9c380",
        "timestamp": "0x659a9c8e",
        "miner": "0x" + "44" * 20,
        "difficulty": "0x0",
        "totalDifficulty": "0x1",
        "extraData": "0xdeadbeef",
        "mixHash": "0x" + "55" * 32,
        "nonce": "0x0000000000000000",
        "size": "0x500",
        "transactions": [],
    }
    base.update(overrides)
    return base


# ─── header converter ─────────────────────────────────────────────────────


def test_block_header_from_rpc_minimum_fields():
    rpc = _block_rpc_minimal()
    out = block_header_from_rpc(rpc)
    assert out["number"] == 0x100
    assert out["hash"] == "0x" + "ab" * 32
    assert out["gasUsed"] == 0xABCD
    assert out["timestamp"] == 0x659A9C8E
    assert out["miner"] == "0x" + "44" * 20
    assert out["difficulty"] == "0"
    assert out["totalDifficulty"] == "1"
    # No transactions in header
    assert "transactions" not in out


def test_block_header_from_rpc_post_shanghai_fields():
    rpc = _block_rpc_minimal(
        baseFeePerGas="0x3b9aca00",
        withdrawalsRoot="0x" + "77" * 32,
        blobGasUsed="0x20000",
        excessBlobGas="0x40000",
        parentBeaconBlockRoot="0x" + "88" * 32,
    )
    out = block_header_from_rpc(rpc)
    assert out["baseFeePerGas"] == "1000000000"
    assert out["withdrawalsRoot"] == "0x" + "77" * 32
    assert out["blobGasUsed"] == 0x20000
    assert out["excessBlobGas"] == 0x40000
    assert out["parentBeaconBlockRoot"] == "0x" + "88" * 32


# ─── /blocks/{id} ─────────────────────────────────────────────────────────


async def test_get_block_by_number(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal()
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256")
    assert resp.status == 200
    body = await resp.json()
    assert body["number"] == 256
    assert body["transactions"] == []
    mock.call.assert_awaited_once_with("eth_getBlockByNumber", ["0x100", True])


async def test_get_block_by_tag_latest(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal()
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/latest")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getBlockByNumber", ["latest", True])


async def test_get_block_by_hash(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal()
    client = await _build_client(aiohttp_client, mock)

    h = "0x" + "ab" * 32
    resp = await client.get(f"/blocks/{h}")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getBlockByHash", [h, True])


async def test_get_block_not_found(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/9999999")
    assert resp.status == 404
    body = await resp.json()
    assert body["type"].endswith("/not-found")


async def test_get_block_bad_id_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/NOT_A_BLOCK")
    assert resp.status == 400
    mock.call.assert_not_called()


async def test_get_block_with_full_transactions(aiohttp_client):
    """When a block has full transactions, they are shaped into REST tx objects."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal(
        transactions=[
            {
                "type": "0x2",
                "hash": "0x" + "ee" * 32,
                "blockHash": "0x" + "ab" * 32,
                "blockNumber": "0x100",
                "transactionIndex": "0x0",
                "from": "0x" + "11" * 20,
                "to": "0x" + "22" * 20,
                "value": "0x0",
                "nonce": "0x0",
                "gas": "0x5208",
                "maxFeePerGas": "0x3b9aca00",
                "maxPriorityFeePerGas": "0x1",
                "input": "0x",
                "chainId": "0x1",
                "v": "0x0",
                "r": "0x" + "11" * 32,
                "s": "0x" + "22" * 32,
                "yParity": "0x0",
            }
        ]
    )
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256")
    assert resp.status == 200
    body = await resp.json()
    assert body["transactions"][0]["type"] == "dynamic-fee"
    assert body["transactions"][0]["nonce"] == 0


# ─── /blocks/{id}/header ──────────────────────────────────────────────────


async def test_get_block_header_strips_transactions(aiohttp_client):
    """One RPC, transactions[] removed in the proxy — never two RPCs."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal(
        transactions=[{"hash": "0xshould_not_appear"}]
    )
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/header")
    assert resp.status == 200
    body = await resp.json()
    assert body["number"] == 256
    assert "transactions" not in body
    # CRITICAL: only one RPC, with full=true (proxy strips client-side)
    assert mock.call.call_count == 1
    args, _ = mock.call.call_args
    assert args == ("eth_getBlockByNumber", ["0x100", True])


# ─── /blocks/{id}/transactions ────────────────────────────────────────────


async def test_get_block_transactions(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal(
        transactions=[
            {
                "type": "0x0",
                "hash": "0x" + "ee" * 32,
                "blockHash": "0x" + "ab" * 32,
                "blockNumber": "0x100",
                "transactionIndex": "0x0",
                "from": "0x" + "11" * 20,
                "to": None,
                "value": "0x0",
                "nonce": "0x0",
                "gas": "0x5208",
                "gasPrice": "0x1",
                "input": "0x",
                "v": "0x1c",
                "r": "0x" + "11" * 32,
                "s": "0x" + "22" * 32,
            }
        ]
    )
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/transactions")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    assert body[0]["type"] == "legacy"
    assert body[0]["to"] is None


async def test_get_block_transactions_not_found(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/9999999/transactions")
    assert resp.status == 404


# ─── /blocks/{id}/transactions/{index} ────────────────────────────────────


async def test_get_block_transaction_by_index_number(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "type": "0x0",
        "hash": "0x" + "ee" * 32,
        "blockHash": "0x" + "ab" * 32,
        "blockNumber": "0x100",
        "transactionIndex": "0x3",
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "value": "0x0",
        "nonce": "0x0",
        "gas": "0x5208",
        "gasPrice": "0x1",
        "input": "0x",
        "v": "0x1c",
        "r": "0x" + "11" * 32,
        "s": "0x" + "22" * 32,
    }
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/transactions/3")
    assert resp.status == 200
    body = await resp.json()
    assert body["transactionIndex"] == 3
    mock.call.assert_awaited_once_with(
        "eth_getTransactionByBlockNumberAndIndex", ["0x100", "0x3"]
    )


async def test_get_block_transaction_by_index_hash(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "type": "0x0",
        "hash": "0x" + "ee" * 32,
        "blockHash": "0x" + "ab" * 32,
        "blockNumber": "0x100",
        "transactionIndex": "0x3",
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "value": "0x0",
        "nonce": "0x0",
        "gas": "0x5208",
        "gasPrice": "0x1",
        "input": "0x",
        "v": "0x1c",
        "r": "0x" + "11" * 32,
        "s": "0x" + "22" * 32,
    }
    client = await _build_client(aiohttp_client, mock)

    h = "0x" + "ab" * 32
    resp = await client.get(f"/blocks/{h}/transactions/3")
    assert resp.status == 200
    mock.call.assert_awaited_once_with(
        "eth_getTransactionByBlockHashAndIndex", [h, "0x3"]
    )


async def test_get_block_transaction_by_index_not_found(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/transactions/99")
    assert resp.status == 404


async def test_get_block_transaction_by_index_negative_400(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/transactions/-1")
    assert resp.status == 400


# ─── /blocks/{id}/transaction-count ───────────────────────────────────────


async def test_get_block_transaction_count_by_number(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0xa"
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/transaction-count")
    assert resp.status == 200
    assert await resp.json() == {"count": 10}
    mock.call.assert_awaited_once_with("eth_getBlockTransactionCountByNumber", ["0x100"])


async def test_get_block_transaction_count_by_hash(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0xa"
    client = await _build_client(aiohttp_client, mock)

    h = "0x" + "ab" * 32
    resp = await client.get(f"/blocks/{h}/transaction-count")
    assert resp.status == 200
    mock.call.assert_awaited_once_with("eth_getBlockTransactionCountByHash", [h])


async def test_get_block_transaction_count_null_is_404(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/9999999/transaction-count")
    assert resp.status == 404


# ─── /blocks/{id}/receipts ────────────────────────────────────────────────


def _minimal_receipt() -> dict[str, Any]:
    return {
        "transactionHash": "0x" + "ee" * 32,
        "transactionIndex": "0x0",
        "blockHash": "0x" + "ab" * 32,
        "blockNumber": "0x100",
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "contractAddress": None,
        "logs": [],
        "logsBloom": "0x" + "00" * 256,
        "status": "0x1",
        "type": "0x0",
    }


async def test_get_block_receipts(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [_minimal_receipt()]
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/receipts")
    assert resp.status == 200
    body = await resp.json()
    assert len(body) == 1
    assert body[0]["status"] == "success"
    mock.call.assert_awaited_once_with("eth_getBlockReceipts", ["0x100"])


async def test_get_block_receipts_empty(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = []
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/receipts")
    assert resp.status == 200
    assert await resp.json() == []


async def test_get_block_receipts_null_is_404(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/9999999/receipts")
    assert resp.status == 404


# ─── /blocks/{id}/traces ──────────────────────────────────────────────────


async def test_get_block_traces(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [
        {
            "action": {"from": "0x" + "11" * 20},
            "type": "call",
            "subtraces": 0,
            "traceAddress": [],
            "transactionHash": "0x" + "ee" * 32,
            "blockHash": "0x" + "ab" * 32,
            "blockNumber": "0x100",
        }
    ]
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/traces")
    assert resp.status == 200
    body = await resp.json()
    assert body[0]["blockNumber"] == 256
    mock.call.assert_awaited_once_with("trace_block", ["0x100"])


async def test_get_block_traces_unsupported_upstream_501(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(
        code=-32601, message="the method trace_block does not exist"
    )
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/256/traces")
    assert resp.status == 501


# ─── parallel guarantee ────────────────────────────────────────────────────


async def test_only_one_rpc_for_header_endpoint(aiohttp_client):
    """Header endpoint must NOT issue two RPCs (no separate header fetch)."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = _block_rpc_minimal()
    client = await _build_client(aiohttp_client, mock)

    resp = await client.get("/blocks/latest/header")
    assert resp.status == 200
    assert mock.call.call_count == 1


# ─── RLP content negotiation ──────────────────────────────────────────────


async def test_get_block_rlp_accept(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0xf90100"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get(
        "/blocks/0", headers={"Accept": "application/vnd.ethereum.rlp"}
    )
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.ethereum.rlp"
    assert await resp.read() == bytes.fromhex("f90100")
    mock.call.assert_awaited_once_with("debug_getRawBlock", ["0x0"])


async def test_get_block_header_rlp_accept(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0xc0"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get(
        "/blocks/0/header", headers={"Accept": "application/vnd.ethereum.rlp"}
    )
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.ethereum.rlp"
    mock.call.assert_awaited_once_with("debug_getRawHeader", ["0x0"])


async def test_get_block_receipts_rlp_accept(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0xc1c2"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get(
        "/blocks/0/receipts", headers={"Accept": "application/vnd.ethereum.rlp"}
    )
    assert resp.status == 200
    mock.call.assert_awaited_once_with("debug_getRawReceipts", ["0x0"])


async def test_get_block_receipts_rlp_accept_list_of_hex(aiohttp_client):
    """Geth returns debug_getRawReceipts as an array of hex strings."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = ["0xf6", "0xf7"]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get(
        "/blocks/0/receipts",
        headers={"Accept": "application/vnd.ethereum.rlp"},
    )
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.ethereum.rlp"
    assert await resp.read() == bytes.fromhex("f6f7")
    mock.call.assert_awaited_once_with("debug_getRawReceipts", ["0x0"])


async def test_get_block_406_for_unsupported_accept(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/blocks/0", headers={"Accept": "text/html"})
    assert resp.status == 406
