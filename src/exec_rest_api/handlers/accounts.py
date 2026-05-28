"""/accounts/* handlers.

Composite endpoints (`/accounts/{addr}` and `/accounts/{addr}/transaction-template`)
fan out their upstream calls with `asyncio.gather`.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from aiohttp import web

from exec_rest_api.block_id import BlockId, BlockIdError, parse_block_id
from exec_rest_api.delegation import detect_delegate
from exec_rest_api.encoding import (
    EncodingError,
    hex_to_int,
    map_address_lowercase,
    wei_from_rpc,
)
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.server import add_get
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError

_HEX_SLOT_RE = re.compile(r"^0x[0-9a-fA-F]{1,64}$")
_DECIMAL_RE = re.compile(r"^[0-9]+$")


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


def _parse_address(raw: str, path: str) -> tuple[str | None, web.Response | None]:
    try:
        return map_address_lowercase(raw), None
    except EncodingError as e:
        return None, _bad_request(path, str(e))


def _parse_at(request: web.Request) -> tuple[BlockId | None, web.Response | None]:
    raw = request.query.get("at", "latest")
    try:
        return parse_block_id(raw), None
    except BlockIdError as e:
        return None, _bad_request(request.path, str(e))


def _parse_slot(raw: str, path: str) -> tuple[str | None, web.Response | None]:
    """Path-level storage slot: 0x-hex (1..64 chars) or decimal int. Returns 0x-hex."""
    if _HEX_SLOT_RE.fullmatch(raw):
        return raw.lower(), None
    if _DECIMAL_RE.fullmatch(raw):
        return f"0x{int(raw):x}", None
    return None, _bad_request(path, f"storage slot must be 0x-hex or decimal, got {raw!r}")


def _pad_slot_to_32_bytes(slot_hex: str) -> str:
    """Zero-pad a 0x-prefixed slot to exactly 32 bytes for eth_getProof.

    eth_getProof's storageKeys are DATA (32 bytes), not QUANTITY, so leading
    zeros matter — `0x5` must be sent as `0x000…05`.
    """
    body = slot_hex[2:]
    if len(body) >= 64:
        return slot_hex
    return "0x" + body.rjust(64, "0")


def _parse_slots_csv(raw: str | None, path: str) -> tuple[list[str] | None, web.Response | None]:
    if raw is None or raw == "":
        return [], None
    out: list[str] = []
    for piece in raw.split(","):
        piece = piece.strip()
        slot, err = _parse_slot(piece, path)
        if err is not None:
            return None, err
        assert slot is not None
        out.append(_pad_slot_to_32_bytes(slot))
    return out, None


# ─── handlers ─────────────────────────────────────────────────────────────


async def account_balance(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    assert addr is not None and bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getBalance", [addr, bid.to_rpc_param()])
    return web.json_response({"wei": wei_from_rpc(rpc)})


async def account_nonce(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    assert addr is not None and bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getTransactionCount", [addr, bid.to_rpc_param()])
    return web.json_response({"nonce": hex_to_int(rpc)})


async def account_code(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    assert addr is not None and bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getCode", [addr, bid.to_rpc_param()])
    return web.json_response({"code": rpc.lower()})


async def account_storage(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    slot, err = _parse_slot(request.match_info["slot"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    assert addr is not None and slot is not None and bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getStorageAt", [addr, slot, bid.to_rpc_param()])
    return web.json_response({"value": rpc.lower()})


def _proof_from_rpc(rpc: dict[str, Any]) -> dict[str, Any]:
    return {
        "address": map_address_lowercase(rpc["address"]),
        "balance": wei_from_rpc(rpc["balance"]),
        "codeHash": rpc["codeHash"].lower(),
        "nonce": hex_to_int(rpc["nonce"]),
        "storageHash": rpc["storageHash"].lower(),
        "accountProof": [p.lower() for p in rpc.get("accountProof", [])],
        "storageProof": [
            {
                "key": entry["key"].lower(),
                "value": entry["value"].lower(),
                "proof": [p.lower() for p in entry.get("proof", [])],
            }
            for entry in rpc.get("storageProof", [])
        ],
    }


async def account_proof(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    slots, err = _parse_slots_csv(request.query.get("slots"), request.path)
    if err is not None:
        return err
    assert addr is not None and bid is not None and slots is not None
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getProof", [addr, slots, bid.to_rpc_param()])
    return web.json_response(_proof_from_rpc(rpc))


async def account_summary(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    assert addr is not None and bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    at = bid.to_rpc_param()
    balance, nonce, code = await asyncio.gather(
        upstream.call("eth_getBalance", [addr, at]),
        upstream.call("eth_getTransactionCount", [addr, at]),
        upstream.call("eth_getCode", [addr, at]),
    )
    code_lower = code.lower()
    has_code = code_lower != "0x"
    delegated_to = detect_delegate(code_lower) if has_code else None
    return web.json_response(
        {
            "address": addr,
            "balance": wei_from_rpc(balance),
            "nonce": hex_to_int(nonce),
            "hasCode": has_code,
            "delegatedTo": delegated_to,
        }
    )


async def post_proof_search(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    assert addr is not None
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    if not isinstance(body, dict) or "slots" not in body:
        return _bad_request(request.path, "field `slots` is required")
    slots_raw = body["slots"]
    if not isinstance(slots_raw, list):
        return _bad_request(request.path, "`slots` must be an array")
    slots: list[str] = []
    for s in slots_raw:
        if not isinstance(s, str):
            return _bad_request(request.path, "slot entries must be strings")
        if _HEX_SLOT_RE.fullmatch(s):
            slots.append(_pad_slot_to_32_bytes(s.lower()))
        else:
            return _bad_request(
                request.path, f"slot must be 0x-hex (1..64 chars), got {s!r}"
            )
    at_raw = body.get("at", "latest")
    if not isinstance(at_raw, str):
        return _bad_request(request.path, "`at` must be a string block identifier")
    try:
        at = parse_block_id(at_raw).to_rpc_param()
    except BlockIdError as e:
        return _bad_request(request.path, str(e))
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_getProof", [addr, slots, at])
    return web.json_response(_proof_from_rpc(rpc))


async def transaction_template(request: web.Request) -> web.Response:
    addr, err = _parse_address(request.match_info["addr"], request.path)
    if err is not None:
        return err
    bid, err = _parse_at(request)
    if err is not None:
        return err
    assert addr is not None and bid is not None
    upstream: UpstreamClient = request.app["upstream"]
    at = bid.to_rpc_param()

    async def _maybe(method: str, params: list[Any] | None = None) -> Any:
        try:
            return await upstream.call(method, params)
        except UpstreamJsonRpcError as e:
            # Method not supported, or upstream can't give a value — degrade gracefully.
            if e.code in (-32601, -32004):
                return None
            raise

    nonce, chain_id, gas_price, priority_fee = await asyncio.gather(
        upstream.call("eth_getTransactionCount", [addr, at]),
        upstream.call("eth_chainId"),
        _maybe("eth_gasPrice"),
        _maybe("eth_maxPriorityFeePerGas"),
    )
    out: dict[str, Any] = {
        "nonce": hex_to_int(nonce),
        "chainId": hex_to_int(chain_id),
    }
    if gas_price is not None:
        out["gasPrice"] = wei_from_rpc(gas_price)
    if priority_fee is not None:
        out["maxPriorityFeePerGas"] = wei_from_rpc(priority_fee)
        # maxFeePerGas suggestion: 2 * baseFee + priorityFee. We don't have baseFee here
        # cheaply; clients usually compute it from `/gas/fee-history`. Skip in v1.
    return web.json_response(out)


def register_routes(app: web.Application) -> None:
    # Note: register specific paths BEFORE the catch-all summary so aiohttp's
    # router matches them first.
    add_get(app, "/accounts/{addr}/balance", account_balance)
    add_get(app, "/accounts/{addr}/nonce", account_nonce)
    add_get(app, "/accounts/{addr}/code", account_code)
    add_get(app, "/accounts/{addr}/storage/{slot}", account_storage)
    add_get(app, "/accounts/{addr}/proof", account_proof)
    app.router.add_post("/accounts/{addr}/proof/search", post_proof_search)
    app.router.add_post("/accounts/{addr}/proof/search/", post_proof_search)
    add_get(app, "/accounts/{addr}/transaction-template", transaction_template)
    add_get(app, "/accounts/{addr}", account_summary)
