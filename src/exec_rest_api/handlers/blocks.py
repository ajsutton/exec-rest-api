"""/blocks/* handlers.

Conventions:
- Missing block → 404 `not-found`, never JSON `null` at the resource root.
- `/blocks/{id}/header` does ONE upstream RPC (full=true) then strips
  `transactions[]` in the proxy. Never two RPCs.
"""

from __future__ import annotations

import re
from typing import Any

from aiohttp import web

from exec_rest_api.block_id import BlockId, BlockIdError, parse_block_id
from exec_rest_api.content_neg import (
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_RLP,
    select_representation,
)
from exec_rest_api.encoding import decimal_to_hex, hex_to_int, map_address_lowercase, wei_from_rpc
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.transactions import (
    receipt_from_rpc,
    trace_from_rpc,
    transaction_from_rpc,
)
from exec_rest_api.server import add_get
from exec_rest_api.upstream import UpstreamClient

_NON_NEG_DECIMAL_RE = re.compile(r"^[0-9]+$")


# ─── shape converters ─────────────────────────────────────────────────────


def block_header_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    """Convert a JSON-RPC block (full or summary) to a REST BlockHeader.

    Drops `transactions` and `withdrawals` — those belong on the full Block.
    """
    out: dict[str, Any] = {
        "number": hex_to_int(rpc["number"]),
        "hash": rpc["hash"].lower(),
        "parentHash": rpc["parentHash"].lower(),
        "stateRoot": rpc["stateRoot"].lower(),
        "transactionsRoot": rpc["transactionsRoot"].lower(),
        "receiptsRoot": rpc["receiptsRoot"].lower(),
        "logsBloom": rpc["logsBloom"].lower(),
        "gasUsed": hex_to_int(rpc["gasUsed"]),
        "gasLimit": hex_to_int(rpc["gasLimit"]),
        "timestamp": hex_to_int(rpc["timestamp"]),
        "miner": map_address_lowercase(rpc["miner"]),
        "difficulty": wei_from_rpc(rpc["difficulty"]),
        "totalDifficulty": (
            wei_from_rpc(rpc["totalDifficulty"]) if rpc.get("totalDifficulty") else "0"
        ),
        "extraData": rpc["extraData"].lower(),
        "mixHash": rpc["mixHash"].lower(),
        "nonce": rpc["nonce"].lower(),
        "size": hex_to_int(rpc["size"]),
    }
    if rpc.get("baseFeePerGas") is not None:
        out["baseFeePerGas"] = wei_from_rpc(rpc["baseFeePerGas"])
    if rpc.get("withdrawalsRoot") is not None:
        out["withdrawalsRoot"] = rpc["withdrawalsRoot"].lower()
    if rpc.get("blobGasUsed") is not None:
        out["blobGasUsed"] = hex_to_int(rpc["blobGasUsed"])
    if rpc.get("excessBlobGas") is not None:
        out["excessBlobGas"] = hex_to_int(rpc["excessBlobGas"])
    if rpc.get("parentBeaconBlockRoot") is not None:
        out["parentBeaconBlockRoot"] = rpc["parentBeaconBlockRoot"].lower()
    return out


def block_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    """Convert a full-tx JSON-RPC block to a REST Block (header + transactions + withdrawals)."""
    out = block_header_from_rpc(rpc)
    out["transactions"] = [transaction_from_rpc(tx) for tx in rpc.get("transactions") or []]
    if rpc.get("withdrawals") is not None:
        out["withdrawals"] = [
            {
                "index": hex_to_int(w["index"]),
                "validatorIndex": hex_to_int(w["validatorIndex"]),
                "address": map_address_lowercase(w["address"]),
                "amount": wei_from_rpc(w["amount"]),
            }
            for w in rpc["withdrawals"]
        ]
    return out


# ─── helpers ──────────────────────────────────────────────────────────────


def _parse_id(raw: str, path: str) -> tuple[BlockId | None, web.Response | None]:
    try:
        bid = parse_block_id(raw)
    except BlockIdError as e:
        return None, problem_response(
            Problem(
                status=400,
                type_slug="invalid-request",
                title="Invalid request",
                detail=str(e),
                instance=path,
            )
        )
    return bid, None


def _by_number_method(prefix: str, bid: BlockId) -> str:
    """Build `<prefix>By{Hash,Number}`."""
    return f"{prefix}ByHash" if bid.is_hash() else f"{prefix}ByNumber"


def _not_found(path: str, detail: str) -> web.Response:
    return problem_response(
        Problem(
            status=404,
            type_slug="not-found",
            title="Not found",
            detail=detail,
            instance=path,
        )
    )


def _not_acceptable(path: str, supported: list[str]) -> web.Response:
    return problem_response(
        Problem(
            status=406,
            type_slug="not-acceptable",
            title="Not acceptable",
            detail=f"supported representations: {', '.join(supported)}",
            instance=path,
        )
    )


def _hex_to_bytes(hex_str: str) -> bytes:
    """Decode a hex string with or without `0x` prefix into bytes."""
    return bytes.fromhex(hex_str[2:] if hex_str.startswith("0x") else hex_str)


def _rlp_response(hex_body: str) -> web.Response:
    return web.Response(body=_hex_to_bytes(hex_body), content_type=CONTENT_TYPE_RLP)


# ─── handlers ─────────────────────────────────────────────────────────────


async def _fetch_block(upstream: UpstreamClient, bid: BlockId) -> dict[str, Any] | None:
    method = _by_number_method("eth_getBlock", bid)
    rpc: dict[str, Any] | None = await upstream.call(method, [bid.to_rpc_param(), True])
    return rpc


async def get_block(request: web.Request) -> web.Response:
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    supported = [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]
    chosen = select_representation(request.headers.get("Accept"), supported)
    if chosen is None:
        return _not_acceptable(request.path, supported)
    upstream: UpstreamClient = request.app["upstream"]
    if chosen == CONTENT_TYPE_RLP:
        raw = await upstream.call("debug_getRawBlock", [bid.to_rpc_param()])
        if raw is None or raw == "0x":
            return _not_found(request.path, f"block {request.match_info['id']} not found")
        return _rlp_response(raw)
    rpc = await _fetch_block(upstream, bid)
    if rpc is None:
        return _not_found(request.path, f"block {request.match_info['id']} not found")
    return web.json_response(block_from_rpc(rpc))


async def get_block_header(request: web.Request) -> web.Response:
    """Single full=true RPC; strip transactions[] client-side."""
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    supported = [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]
    chosen = select_representation(request.headers.get("Accept"), supported)
    if chosen is None:
        return _not_acceptable(request.path, supported)
    upstream: UpstreamClient = request.app["upstream"]
    if chosen == CONTENT_TYPE_RLP:
        raw = await upstream.call("debug_getRawHeader", [bid.to_rpc_param()])
        if raw is None or raw == "0x":
            return _not_found(request.path, f"block {request.match_info['id']} not found")
        return _rlp_response(raw)
    rpc = await _fetch_block(upstream, bid)
    if rpc is None:
        return _not_found(request.path, f"block {request.match_info['id']} not found")
    return web.json_response(block_header_from_rpc(rpc))


async def get_block_transactions(request: web.Request) -> web.Response:
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await _fetch_block(upstream, bid)
    if rpc is None:
        return _not_found(request.path, f"block {request.match_info['id']} not found")
    txs = [transaction_from_rpc(tx) for tx in rpc.get("transactions") or []]
    return web.json_response(txs)


async def get_block_transaction_by_index(request: web.Request) -> web.Response:
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    index_raw = request.match_info["index"]
    if not _NON_NEG_DECIMAL_RE.fullmatch(index_raw):
        return problem_response(
            Problem(
                status=400,
                type_slug="invalid-request",
                title="Invalid request",
                detail=f"transaction index must be a non-negative integer, got {index_raw!r}",
                instance=request.path,
            )
        )
    index = int(index_raw)
    method = (
        "eth_getTransactionByBlockHashAndIndex"
        if bid.is_hash()
        else "eth_getTransactionByBlockNumberAndIndex"
    )
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call(method, [bid.to_rpc_param(), decimal_to_hex(index)])
    if rpc is None:
        return _not_found(
            request.path,
            f"transaction at index {index} not found in block {request.match_info['id']}",
        )
    return web.json_response(transaction_from_rpc(rpc))


async def get_block_transaction_count(request: web.Request) -> web.Response:
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    method = _by_number_method("eth_getBlockTransactionCount", bid)
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call(method, [bid.to_rpc_param()])
    if rpc is None:
        return _not_found(request.path, f"block {request.match_info['id']} not found")
    return web.json_response({"count": hex_to_int(rpc)})


async def get_block_receipts(request: web.Request) -> web.Response:
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    supported = [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]
    chosen = select_representation(request.headers.get("Accept"), supported)
    if chosen is None:
        return _not_acceptable(request.path, supported)
    upstream: UpstreamClient = request.app["upstream"]
    if chosen == CONTENT_TYPE_RLP:
        raw = await upstream.call("debug_getRawReceipts", [bid.to_rpc_param()])
        if raw is None:
            return _not_found(request.path, f"block {request.match_info['id']} not found")
        # debug_getRawReceipts returns an array of hex strings; concatenate raw bytes
        if isinstance(raw, list):
            joined = b"".join(_hex_to_bytes(r) for r in raw)
            return web.Response(body=joined, content_type=CONTENT_TYPE_RLP)
        return _rlp_response(raw)
    rpc = await upstream.call("eth_getBlockReceipts", [bid.to_rpc_param()])
    if rpc is None:
        return _not_found(request.path, f"block {request.match_info['id']} not found")
    return web.json_response([receipt_from_rpc(r) for r in rpc])


async def get_block_traces(request: web.Request) -> web.Response:
    bid, err = _parse_id(request.match_info["id"], request.path)
    if err is not None:
        return err
    assert bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("trace_block", [bid.to_rpc_param()])
    if rpc is None:
        return _not_found(request.path, f"block {request.match_info['id']} not found")
    return web.json_response([trace_from_rpc(t) for t in rpc])


def register_routes(app: web.Application) -> None:
    add_get(app, "/blocks/{id}", get_block)
    add_get(app, "/blocks/{id}/header", get_block_header)
    add_get(app, "/blocks/{id}/transactions", get_block_transactions)
    add_get(app, "/blocks/{id}/transactions/{index}", get_block_transaction_by_index)
    add_get(app, "/blocks/{id}/transaction-count", get_block_transaction_count)
    add_get(app, "/blocks/{id}/receipts", get_block_receipts)
    add_get(app, "/blocks/{id}/traces", get_block_traces)
