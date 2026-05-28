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

from exec_rest_api.block_id import parse_block_id
from exec_rest_api.encoding import (
    decimal_to_hex,
    map_address_lowercase,
    parse_input_int,
    parse_input_wei,
)

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
