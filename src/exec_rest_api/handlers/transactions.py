"""/transactions/* handlers and shared JSON-RPC → REST shape converters.

`transaction_from_rpc`, `receipt_from_rpc`, and `log_from_rpc` are also imported
by `handlers/blocks.py` (block + receipts endpoints) and `handlers/logs.py`. The
GET handlers live here because they primarily belong to the transactions resource;
the POST submission handler is added in Plan 3.
"""

from __future__ import annotations

import re
from typing import Any

from aiohttp import web

from exec_rest_api.content_neg import (
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_RLP,
    select_representation,
)
from exec_rest_api.encoding import (
    coerce_rpc_int,
    hex_to_int,
    map_address_lowercase,
    rest_status_from_rpc,
    rest_tx_type_from_rpc,
    wei_from_rpc,
)
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.server import add_get
from exec_rest_api.upstream import UpstreamClient

_TX_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _validate_tx_hash(raw: str) -> str | None:
    """Lowercase and return the hash if valid; None otherwise."""
    if _TX_HASH_RE.fullmatch(raw):
        return raw.lower()
    return None


# ─── shape converters (also used by blocks.py and logs.py) ────────────────


def transaction_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    """Convert a JSON-RPC transaction object to its REST shape."""
    out: dict[str, Any] = {
        "type": rest_tx_type_from_rpc(rpc["type"]),
        "hash": rpc["hash"].lower(),
        "from": map_address_lowercase(rpc["from"]),
        "nonce": hex_to_int(rpc["nonce"]),
        "gas": hex_to_int(rpc["gas"]),
        "input": rpc["input"].lower(),
        "value": wei_from_rpc(rpc["value"]),
        "r": rpc["r"].lower(),
        "s": rpc["s"].lower(),
    }
    # Block-context fields — null for pending txs
    out["blockHash"] = rpc["blockHash"].lower() if rpc.get("blockHash") else None
    out["blockNumber"] = (
        hex_to_int(rpc["blockNumber"]) if rpc.get("blockNumber") is not None else None
    )
    out["transactionIndex"] = (
        hex_to_int(rpc["transactionIndex"])
        if rpc.get("transactionIndex") is not None
        else None
    )
    # `to` is null for contract creation
    to_raw = rpc.get("to")
    out["to"] = map_address_lowercase(to_raw) if to_raw is not None else None
    # Optional fields
    if "chainId" in rpc and rpc["chainId"] is not None:
        out["chainId"] = hex_to_int(rpc["chainId"])
    if "gasPrice" in rpc and rpc["gasPrice"] is not None:
        out["gasPrice"] = wei_from_rpc(rpc["gasPrice"])
    if "maxFeePerGas" in rpc and rpc["maxFeePerGas"] is not None:
        out["maxFeePerGas"] = wei_from_rpc(rpc["maxFeePerGas"])
    if "maxPriorityFeePerGas" in rpc and rpc["maxPriorityFeePerGas"] is not None:
        out["maxPriorityFeePerGas"] = wei_from_rpc(rpc["maxPriorityFeePerGas"])
    if "maxFeePerBlobGas" in rpc and rpc["maxFeePerBlobGas"] is not None:
        out["maxFeePerBlobGas"] = wei_from_rpc(rpc["maxFeePerBlobGas"])
    if "accessList" in rpc and rpc["accessList"] is not None:
        out["accessList"] = [
            {
                "address": map_address_lowercase(entry["address"]),
                "storageKeys": [k.lower() for k in entry.get("storageKeys", [])],
            }
            for entry in rpc["accessList"]
        ]
    if "blobVersionedHashes" in rpc and rpc["blobVersionedHashes"] is not None:
        out["blobVersionedHashes"] = [h.lower() for h in rpc["blobVersionedHashes"]]
    if "v" in rpc and rpc["v"] is not None:
        out["v"] = rpc["v"].lower()
    if "yParity" in rpc and rpc["yParity"] is not None:
        out["yParity"] = hex_to_int(rpc["yParity"])
    return out


def receipt_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    """Convert a JSON-RPC receipt object to its REST shape."""
    out: dict[str, Any] = {
        "transactionHash": rpc["transactionHash"].lower(),
        "transactionIndex": hex_to_int(rpc["transactionIndex"]),
        "blockHash": rpc["blockHash"].lower(),
        "blockNumber": hex_to_int(rpc["blockNumber"]),
        "from": map_address_lowercase(rpc["from"]),
        "cumulativeGasUsed": hex_to_int(rpc["cumulativeGasUsed"]),
        "gasUsed": hex_to_int(rpc["gasUsed"]),
        "logsBloom": rpc["logsBloom"].lower(),
        "logs": [log_from_rpc(log) for log in rpc.get("logs", [])],
        "status": rest_status_from_rpc(rpc["status"]),
        "type": rest_tx_type_from_rpc(rpc["type"]),
    }
    to_raw = rpc.get("to")
    out["to"] = map_address_lowercase(to_raw) if to_raw is not None else None
    ca_raw = rpc.get("contractAddress")
    out["contractAddress"] = map_address_lowercase(ca_raw) if ca_raw is not None else None
    if "effectiveGasPrice" in rpc and rpc["effectiveGasPrice"] is not None:
        out["effectiveGasPrice"] = wei_from_rpc(rpc["effectiveGasPrice"])
    if "blobGasUsed" in rpc and rpc["blobGasUsed"] is not None:
        out["blobGasUsed"] = hex_to_int(rpc["blobGasUsed"])
    if "blobGasPrice" in rpc and rpc["blobGasPrice"] is not None:
        out["blobGasPrice"] = wei_from_rpc(rpc["blobGasPrice"])
    return out


def log_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    """Convert a JSON-RPC log object to its REST shape."""
    return {
        "address": map_address_lowercase(rpc["address"]),
        "topics": [t.lower() for t in rpc["topics"]],
        "data": rpc["data"].lower(),
        "blockHash": rpc["blockHash"].lower(),
        "blockNumber": hex_to_int(rpc["blockNumber"]),
        "transactionHash": rpc["transactionHash"].lower(),
        "transactionIndex": hex_to_int(rpc["transactionIndex"]),
        "logIndex": hex_to_int(rpc["logIndex"]),
        "removed": bool(rpc.get("removed", False)),
    }


def trace_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    """Convert a parity-style trace object to its REST shape.

    `action` and `result` are forwarded as opaque objects per the OpenAPI schema
    (additionalProperties: true) — they contain free-form hex fields that
    different upstreams shape differently.
    """
    out: dict[str, Any] = {
        "action": rpc.get("action", {}),
        "type": rpc["type"],
        "subtraces": int(rpc["subtraces"]),
        "traceAddress": list(rpc.get("traceAddress", [])),
        "transactionHash": rpc["transactionHash"].lower(),
        "blockHash": rpc["blockHash"].lower(),
        # blockNumber may arrive as 0x-hex, decimal string, or bare int across upstreams.
        "blockNumber": coerce_rpc_int(rpc["blockNumber"]),
    }
    if "result" in rpc and rpc["result"] is not None:
        out["result"] = rpc["result"]
    if "error" in rpc and rpc["error"] is not None:
        out["error"] = rpc["error"]
    if "transactionPosition" in rpc and rpc["transactionPosition"] is not None:
        pos = rpc["transactionPosition"]
        out["transactionPosition"] = hex_to_int(pos) if isinstance(pos, str) else int(pos)
    return out


# ─── handlers ─────────────────────────────────────────────────────────────


async def get_transaction(request: web.Request) -> web.Response:
    tx_hash = _validate_tx_hash(request.match_info["hash"])
    if tx_hash is None:
        return _bad_hash(request.path)
    supported = [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]
    chosen = select_representation(request.headers.get("Accept"), supported)
    if chosen is None:
        return _not_acceptable(request.path, supported)
    upstream: UpstreamClient = request.app["upstream"]
    if chosen == CONTENT_TYPE_RLP:
        raw = await upstream.call("debug_getRawTransaction", [tx_hash])
        if raw is None or raw == "0x":
            return _not_found(request.path, f"transaction {tx_hash} not found")
        return _rlp_response(raw)
    rpc = await upstream.call("eth_getTransactionByHash", [tx_hash])
    if rpc is None:
        return _not_found(request.path, f"transaction {tx_hash} not found")
    return web.json_response(transaction_from_rpc(rpc))


async def get_receipt(request: web.Request) -> web.Response:
    tx_hash = _validate_tx_hash(request.match_info["hash"])
    if tx_hash is None:
        return _bad_hash(request.path)
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getTransactionReceipt", [tx_hash])
    if rpc is None:
        return _not_found(request.path, f"receipt for {tx_hash} not found")
    return web.json_response(receipt_from_rpc(rpc))


async def get_trace(request: web.Request) -> web.Response:
    tx_hash = _validate_tx_hash(request.match_info["hash"])
    if tx_hash is None:
        return _bad_hash(request.path)
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("trace_transaction", [tx_hash])
    if rpc is None:
        return _not_found(request.path, f"trace for {tx_hash} not found")
    return web.json_response([trace_from_rpc(t) for t in rpc])


def _bad_hash(path: str) -> web.Response:
    return problem_response(
        Problem(
            status=400,
            type_slug="invalid-request",
            title="Invalid request",
            detail="transaction hash must be 0x-prefixed 32-byte hex",
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


def _rlp_response(hex_body: str) -> web.Response:
    return web.Response(
        body=bytes.fromhex(hex_body[2:] if hex_body.startswith("0x") else hex_body),
        content_type=CONTENT_TYPE_RLP,
    )


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


def _unsupported_media_type(path: str) -> web.Response:
    return problem_response(
        Problem(
            status=415,
            type_slug="unsupported-media-type",
            title="Unsupported media type",
            detail="POST /transactions accepts application/json or application/vnd.ethereum.rlp",
            instance=path,
        )
    )


def _bad_request(path: str, detail: str) -> web.Response:
    return problem_response(
        Problem(
            status=400,
            type_slug="invalid-request",
            title="Invalid request",
            detail=detail,
            instance=path,
        )
    )


_HEX_BYTES_RE = re.compile(r"^0x([0-9a-fA-F]{2})+$")


async def _read_raw_tx(request: web.Request) -> str | web.Response:
    ct = (request.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if ct == CONTENT_TYPE_RLP:
        raw_bytes = await request.read()
        if not raw_bytes:
            return _bad_request(request.path, "RLP body is empty")
        return "0x" + raw_bytes.hex()
    if ct == "application/json" or ct == "":
        try:
            body = await request.json()
        except (ValueError, TypeError):
            return _bad_request(request.path, "request body must be valid JSON")
        if not isinstance(body, dict) or "raw" not in body:
            return _bad_request(request.path, "field `raw` is required")
        raw = body["raw"]
        if not isinstance(raw, str) or not _HEX_BYTES_RE.fullmatch(raw):
            return _bad_request(
                request.path, "field `raw` must be 0x-prefixed hex bytes"
            )
        return raw.lower()
    return _unsupported_media_type(request.path)


async def post_transaction(request: web.Request) -> web.Response:
    raw_or_err = await _read_raw_tx(request)
    if isinstance(raw_or_err, web.Response):
        return raw_or_err
    upstream: UpstreamClient = request.app["upstream"]
    tx_hash = await upstream.call("eth_sendRawTransaction", [raw_or_err])
    if not isinstance(tx_hash, str):
        return problem_response(
            Problem(
                status=502,
                type_slug="upstream-error",
                title="Upstream error",
                detail="eth_sendRawTransaction returned non-string",
                instance=request.path,
            )
        )
    tx_hash_lower = tx_hash.lower()
    return web.json_response(
        {"hash": tx_hash_lower},
        status=202,
        headers={"Location": f"/transactions/{tx_hash_lower}"},
    )


async def post_trace_replay(request: web.Request) -> web.Response:
    tx_hash = _validate_tx_hash(request.match_info["hash"])
    if tx_hash is None:
        return _bad_hash(request.path)
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    tracers = body.get("tracers") if isinstance(body, dict) else None
    if not isinstance(tracers, list) or not tracers:
        return _bad_request(request.path, "field `tracers` (non-empty array) is required")
    allowed = {"trace", "vmTrace", "stateDiff"}
    for t in tracers:
        if t not in allowed:
            return _bad_request(request.path, f"unknown tracer {t!r}")
    upstream: UpstreamClient = request.app["upstream"]
    result = await upstream.call("trace_replayTransaction", [tx_hash, list(tracers)])
    return web.json_response(result)


async def post_debug_trace(request: web.Request) -> web.Response:
    tx_hash = _validate_tx_hash(request.match_info["hash"])
    if tx_hash is None:
        return _bad_hash(request.path)
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request(request.path, "request body must be a JSON object")
    upstream: UpstreamClient = request.app["upstream"]
    result = await upstream.call("debug_traceTransaction", [tx_hash, body])
    return web.json_response(result)


def register_routes(app: web.Application) -> None:
    add_get(app, "/transactions/{hash}", get_transaction)
    add_get(app, "/transactions/{hash}/receipt", get_receipt)
    add_get(app, "/transactions/{hash}/trace", get_trace)
    app.router.add_post("/transactions", post_transaction)
    app.router.add_post("/transactions/", post_transaction)
    app.router.add_post("/transactions/{hash}/trace/replay", post_trace_replay)
    app.router.add_post("/transactions/{hash}/trace/replay/", post_trace_replay)
    app.router.add_post("/transactions/{hash}/debug-trace", post_debug_trace)
    app.router.add_post("/transactions/{hash}/debug-trace/", post_debug_trace)
