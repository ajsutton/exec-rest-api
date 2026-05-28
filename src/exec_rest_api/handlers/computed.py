"""Computed-read POST endpoints (`/call`, `/gas-estimate`, `/access-list`,
`/simulate`, `/debug-traces/call`) and the shared `CallRequest`-to-JSON-RPC
converter used here and by trace_call / trace_callMany / debug_traceCall.

Reverts are 200 responses — we catch `UpstreamJsonRpcError`, check
`is_revert` / `is_out_of_gas`, and emit a `RevertedResult` body. Anything
else re-raises and the server middleware turns it into a Problem.
"""

from __future__ import annotations

import re
from typing import Any

from aiohttp import web

from exec_rest_api.abi_revert import decode_revert_data, is_out_of_gas, is_revert, revert_body
from exec_rest_api.handlers.blocks import block_header_from_rpc
from exec_rest_api.handlers.transactions import log_from_rpc
from exec_rest_api.block_id import parse_block_id
from exec_rest_api.encoding import (
    decimal_to_hex,
    hex_to_int,
    map_address_lowercase,
    parse_input_int,
    parse_input_wei,
)
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError

_HEX_BYTES_RE: re.Pattern[str] = re.compile(r"^0x([0-9a-fA-F]{2})*$")

# Fields on CallRequest that are quantities (decimal-string → 0x-hex).
_INT_FIELDS = ("gas", "nonce", "chainId")
_WEI_FIELDS = ("gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "value")
_ADDRESS_FIELDS = ("from", "to")
_BYTES_FIELDS = ("data",)


def _convert_numeric(out: dict[str, Any], body: dict[str, Any]) -> None:
    for f in _INT_FIELDS:
        if f in body and body[f] is not None:
            out[f] = decimal_to_hex(parse_input_int(body[f]))
    for f in _WEI_FIELDS:
        if f in body and body[f] is not None:
            out[f] = decimal_to_hex(parse_input_wei(body[f]))


def _convert_addresses(out: dict[str, Any], body: dict[str, Any]) -> None:
    for f in _ADDRESS_FIELDS:
        if f in body and body[f] is not None:
            out[f] = map_address_lowercase(body[f])


def _convert_bytes(out: dict[str, Any], body: dict[str, Any]) -> None:
    for f in _BYTES_FIELDS:
        if f in body and body[f] is not None:
            v = body[f]
            if not isinstance(v, str) or not _HEX_BYTES_RE.fullmatch(v):
                raise ValueError(f"field {f!r} must be 0x-prefixed hex bytes (even length)")
            out[f] = v.lower()


def _convert_access_list(out: dict[str, Any], body: dict[str, Any]) -> None:
    al = body.get("accessList")
    if al is None:
        return
    if not isinstance(al, list):
        raise ValueError("accessList must be an array")
    converted = []
    for entry in al:
        if not isinstance(entry, dict):
            raise ValueError("accessList entries must be objects")
        if "address" not in entry:
            raise ValueError("accessList entry missing `address`")
        keys = entry.get("storageKeys", [])
        if not isinstance(keys, list):
            raise ValueError("accessList `storageKeys` must be an array")
        converted_keys: list[str] = []
        for k in keys:
            if not isinstance(k, str):
                raise ValueError("accessList storageKey entries must be strings")
            converted_keys.append(k.lower())
        converted.append(
            {
                "address": map_address_lowercase(entry["address"]),
                "storageKeys": converted_keys,
            }
        )
    out["accessList"] = converted


def _convert_state_overrides(out: dict[str, Any], body: dict[str, Any]) -> None:
    so = body.get("stateOverrides")
    if so is None:
        return
    if not isinstance(so, dict):
        raise ValueError("stateOverrides must be an object")
    converted: dict[str, Any] = {}
    for addr, override in so.items():
        if not isinstance(override, dict):
            raise ValueError(f"stateOverride for {addr} must be an object")
        out_override: dict[str, Any] = {}
        if "balance" in override and override["balance"] is not None:
            out_override["balance"] = decimal_to_hex(parse_input_wei(override["balance"]))
        if "nonce" in override and override["nonce"] is not None:
            out_override["nonce"] = decimal_to_hex(parse_input_int(override["nonce"]))
        if "code" in override and override["code"] is not None:
            out_override["code"] = override["code"].lower()
        if "state" in override and override["state"] is not None:
            out_override["state"] = {
                k.lower(): v.lower() for k, v in override["state"].items()
            }
        if "stateDiff" in override and override["stateDiff"] is not None:
            out_override["stateDiff"] = {
                k.lower(): v.lower() for k, v in override["stateDiff"].items()
            }
        converted[map_address_lowercase(addr)] = out_override
    out["stateOverrides"] = converted


def _convert_block_overrides(out: dict[str, Any], body: dict[str, Any]) -> None:
    bo = body.get("blockOverrides")
    if bo is None:
        return
    if not isinstance(bo, dict):
        raise ValueError("blockOverrides must be an object")
    converted: dict[str, Any] = {}
    if "number" in bo and bo["number"] is not None:
        converted["number"] = decimal_to_hex(parse_input_int(bo["number"]))
    if "timestamp" in bo and bo["timestamp"] is not None:
        converted["timestamp"] = decimal_to_hex(parse_input_int(bo["timestamp"]))
    if "gasLimit" in bo and bo["gasLimit"] is not None:
        converted["gasLimit"] = decimal_to_hex(parse_input_int(bo["gasLimit"]))
    if "baseFeePerGas" in bo and bo["baseFeePerGas"] is not None:
        converted["baseFeePerGas"] = decimal_to_hex(parse_input_wei(bo["baseFeePerGas"]))
    if "difficulty" in bo and bo["difficulty"] is not None:
        converted["difficulty"] = decimal_to_hex(parse_input_wei(bo["difficulty"]))
    if "coinbase" in bo and bo["coinbase"] is not None:
        converted["coinbase"] = map_address_lowercase(bo["coinbase"])
    if "random" in bo and bo["random"] is not None:
        converted["random"] = bo["random"].lower()
    out["blockOverrides"] = converted


def call_request_to_rpc(body: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Convert a REST CallRequest body into the upstream JSON-RPC call object
    plus the resolved `at` block identifier (as the JSON-RPC param string).

    Returns: (rpc_call_object, at_block_rpc).
    Raises: ValueError on any malformed field.
    """
    if not isinstance(body, dict):
        raise ValueError("CallRequest must be an object")
    out: dict[str, Any] = {}
    _convert_addresses(out, body)
    _convert_numeric(out, body)
    _convert_bytes(out, body)
    _convert_access_list(out, body)
    _convert_state_overrides(out, body)
    _convert_block_overrides(out, body)
    at_raw = body.get("at", "latest")
    if not isinstance(at_raw, str):
        raise ValueError("`at` must be a string block identifier")
    at = parse_block_id(at_raw).to_rpc_param()
    return out, at


# ── handlers ──────────────────────────────────────────────────────────────


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


async def _read_call_request(
    request: web.Request,
) -> tuple[dict[str, Any], str] | web.Response:
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    try:
        return call_request_to_rpc(body)
    except (ValueError, KeyError) as e:
        return _bad_request(request.path, str(e))


async def call(request: web.Request) -> web.Response:
    parsed = await _read_call_request(request)
    if isinstance(parsed, web.Response):
        return parsed
    rpc_body, at = parsed
    upstream: UpstreamClient = request.app["upstream"]
    try:
        result = await upstream.call("eth_call", [rpc_body, at])
    except UpstreamJsonRpcError as e:
        if is_revert(e) or is_out_of_gas(e):
            return web.json_response(revert_body(e))
        raise
    if not isinstance(result, str):
        return problem_response(
            Problem(
                status=502,
                type_slug="upstream-error",
                title="Upstream error",
                detail="eth_call returned non-string result",
                instance=request.path,
            )
        )
    return web.json_response({"data": result.lower()})


async def gas_estimate(request: web.Request) -> web.Response:
    parsed = await _read_call_request(request)
    if isinstance(parsed, web.Response):
        return parsed
    rpc_body, at = parsed
    upstream: UpstreamClient = request.app["upstream"]
    try:
        result = await upstream.call("eth_estimateGas", [rpc_body, at])
    except UpstreamJsonRpcError as e:
        if is_revert(e) or is_out_of_gas(e):
            return web.json_response(revert_body(e))
        raise
    if not isinstance(result, str):
        return problem_response(
            Problem(
                status=502,
                type_slug="upstream-error",
                title="Upstream error",
                detail="eth_estimateGas returned non-string result",
                instance=request.path,
            )
        )
    return web.json_response({"gas": hex_to_int(result)})


async def access_list(request: web.Request) -> web.Response:
    parsed = await _read_call_request(request)
    if isinstance(parsed, web.Response):
        return parsed
    rpc_body, at = parsed
    upstream: UpstreamClient = request.app["upstream"]
    try:
        result = await upstream.call("eth_createAccessList", [rpc_body, at])
    except UpstreamJsonRpcError as e:
        if is_revert(e) or is_out_of_gas(e):
            return web.json_response(revert_body(e))
        raise
    out: dict[str, Any] = {
        "accessList": [
            {
                "address": map_address_lowercase(entry["address"]),
                "storageKeys": [k.lower() for k in entry.get("storageKeys", [])],
            }
            for entry in result.get("accessList") or []
        ],
        "gasUsed": hex_to_int(result["gasUsed"]),
    }
    if "error" in result and result["error"] is not None:
        out["error"] = result["error"]
    return web.json_response(out)


def _simulate_call_result(call_rpc: dict[str, Any]) -> dict[str, Any]:
    """Shape one inner call result from eth_simulateV1.

    If `status` indicates failure or `error` is present, emit the revert body.
    Otherwise emit returnData / gasUsed / logs.
    """
    status = call_rpc.get("status")
    if (status is not None and status == "0x0") or call_rpc.get("error"):
        data = call_rpc.get("returnData", "0x")
        if not isinstance(data, str):
            data = "0x"
        reason, panic = decode_revert_data(data)
        return {
            "reverted": True,
            "data": data.lower(),
            "reason": reason,
            "panicCode": panic,
        }
    out: dict[str, Any] = {
        "returnData": call_rpc.get("returnData", "0x").lower(),
        "gasUsed": hex_to_int(call_rpc["gasUsed"]),
    }
    if "logs" in call_rpc and call_rpc["logs"] is not None:
        out["logs"] = [log_from_rpc(log) for log in call_rpc["logs"]]
    return out


async def simulate(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    if not isinstance(body, dict) or "blockStateCalls" not in body:
        return _bad_request(
            request.path, "field `blockStateCalls` is required"
        )
    # Build the RPC payload: convert each call inside each block-state.
    rpc_payload: dict[str, Any] = {}
    try:
        bsc_list: list[dict[str, Any]] = []
        for bsc in body["blockStateCalls"]:
            if not isinstance(bsc, dict):
                raise ValueError("each blockStateCalls entry must be an object")
            rpc_bsc: dict[str, Any] = {}
            if "blockOverrides" in bsc:
                tmp: dict[str, Any] = {}
                _convert_block_overrides(tmp, {"blockOverrides": bsc["blockOverrides"]})
                rpc_bsc["blockOverrides"] = tmp["blockOverrides"]
            if "stateOverrides" in bsc:
                tmp = {}
                _convert_state_overrides(tmp, {"stateOverrides": bsc["stateOverrides"]})
                rpc_bsc["stateOverrides"] = tmp["stateOverrides"]
            calls = bsc.get("calls", [])
            if not isinstance(calls, list):
                raise ValueError("`calls` must be an array")
            rpc_bsc["calls"] = [call_request_to_rpc(c)[0] for c in calls]
            bsc_list.append(rpc_bsc)
        rpc_payload["blockStateCalls"] = bsc_list
        for flag in ("traceTransfers", "validation", "returnFullTransactions"):
            if flag in body:
                rpc_payload[flag] = bool(body[flag])
        at_raw = body.get("at", "latest")
        at = parse_block_id(at_raw).to_rpc_param()
    except (ValueError, KeyError) as e:
        return _bad_request(request.path, str(e))
    upstream: UpstreamClient = request.app["upstream"]
    try:
        result = await upstream.call("eth_simulateV1", [rpc_payload, at])
    except UpstreamJsonRpcError as e:
        if is_revert(e) or is_out_of_gas(e):
            return web.json_response(revert_body(e))
        raise
    if not isinstance(result, list):
        return problem_response(
            Problem(
                status=502,
                type_slug="upstream-error",
                title="Upstream error",
                detail="eth_simulateV1 returned non-list result",
                instance=request.path,
            )
        )
    out_blocks: list[dict[str, Any]] = []
    for block_rpc in result:
        out_blocks.append(
            {
                "block": block_header_from_rpc(block_rpc),
                "calls": [_simulate_call_result(c) for c in block_rpc.get("calls", [])],
            }
        )
    return web.json_response(out_blocks)


async def debug_traces_call(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    if not isinstance(body, dict) or "call" not in body:
        return _bad_request(request.path, "field `call` is required")
    try:
        rpc_call, _at_inside = call_request_to_rpc(body["call"])
        at_raw = body.get("at", "latest")
        at = parse_block_id(at_raw).to_rpc_param()
    except (ValueError, KeyError) as e:
        return _bad_request(request.path, str(e))
    tracer = body.get("tracer") or {}
    if not isinstance(tracer, dict):
        return _bad_request(request.path, "`tracer` must be an object")
    upstream: UpstreamClient = request.app["upstream"]
    try:
        result = await upstream.call("debug_traceCall", [rpc_call, at, tracer])
    except UpstreamJsonRpcError as e:
        if is_revert(e) or is_out_of_gas(e):
            return web.json_response(revert_body(e))
        raise
    return web.json_response(result)


def register_routes(app: web.Application) -> None:
    app.router.add_post("/call", call)
    app.router.add_post("/call/", call)
    app.router.add_post("/gas-estimate", gas_estimate)
    app.router.add_post("/gas-estimate/", gas_estimate)
    app.router.add_post("/access-list", access_list)
    app.router.add_post("/access-list/", access_list)
    app.router.add_post("/simulate", simulate)
    app.router.add_post("/simulate/", simulate)
    app.router.add_post("/debug-traces/call", debug_traces_call)
    app.router.add_post("/debug-traces/call/", debug_traces_call)
