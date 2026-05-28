"""Revert detection + ABI decoding for `Error(string)` and `Panic(uint256)`.

Pure byte manipulation; no crypto, no third-party deps. The two selectors are
fixed by the Solidity ABI:

  Error(string) → keccak256("Error(string)")[:4]   = 0x08c379a0
  Panic(uint256) → keccak256("Panic(uint256)")[:4] = 0x4e487b71

For any other selector (custom errors), we leave `reason` and `panicCode` as
None and pass the raw `data` through so the client can decode against its ABI.
"""

from __future__ import annotations

import re
from typing import Any, Final

from exec_rest_api.upstream import UpstreamJsonRpcError

_ERROR_SELECTOR: Final[str] = "08c379a0"
_PANIC_SELECTOR: Final[str] = "4e487b71"

_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]*$")

# Substring patterns (lowercase) used to detect specific upstream conditions.
_REVERT_MARKERS: Final[tuple[str, ...]] = ("execution reverted",)
_OUT_OF_GAS_MARKERS: Final[tuple[str, ...]] = (
    "out of gas",
    "gas required exceeds allowance",
)

# Sanity ceiling for ABI-decoded string length (bytes). Anything larger means
# the data is malformed; refuse rather than allocate.
_MAX_REASON_BYTES: Final[int] = 1 << 20  # 1 MiB


def is_revert(err: UpstreamJsonRpcError) -> bool:
    """True if `err` is an `eth_call`-family revert (200 in our API, not 4xx/5xx)."""
    if err.code != -32000:
        return False
    msg = err.message.lower()
    return any(m in msg for m in _REVERT_MARKERS)


def is_out_of_gas(err: UpstreamJsonRpcError) -> bool:
    """True if `err` indicates the EVM ran out of gas mid-execution."""
    msg = err.message.lower()
    return any(m in msg for m in _OUT_OF_GAS_MARKERS)


def decode_revert_data(data: str | None) -> tuple[str | None, int | None]:
    """Best-effort decode of revert `data` bytes.

    Returns `(reason, panicCode)` where exactly zero or one is non-None.
    A malformed or unknown-selector blob yields `(None, None)` — the caller
    keeps the raw `data` for the client to decode.
    """
    if data is None or not isinstance(data, str) or not _HEX_RE.fullmatch(data):
        return None, None
    body = data[2:]  # strip "0x"
    if len(body) < 8:
        return None, None
    selector = body[:8].lower()
    tail = body[8:]
    if selector == _ERROR_SELECTOR:
        return _decode_error_string(tail), None
    if selector == _PANIC_SELECTOR:
        return None, _decode_panic_uint(tail)
    return None, None


def _decode_error_string(tail_hex: str) -> str | None:
    """Decode the ABI tail of an `Error(string)` revert.

    Layout: head (32-byte offset to string) + length (32 bytes) + utf-8 bytes
    (zero-padded to 32-byte multiple).
    """
    if len(tail_hex) < 128:  # need at least offset + length
        return None
    try:
        offset = int(tail_hex[:64], 16)
        # The string struct begins at `offset` bytes from the start of tail.
        # In well-formed Solidity output, offset == 0x20 (32). Accept any sane offset.
        struct_start_hex = offset * 2
        if struct_start_hex + 64 > len(tail_hex):
            return None
        length = int(tail_hex[struct_start_hex : struct_start_hex + 64], 16)
        if length > _MAX_REASON_BYTES:
            return None
        data_start_hex = struct_start_hex + 64
        data_end_hex = data_start_hex + length * 2
        if data_end_hex > len(tail_hex):
            return None
        return bytes.fromhex(tail_hex[data_start_hex:data_end_hex]).decode(
            "utf-8", errors="replace"
        )
    except (ValueError, UnicodeDecodeError):
        return None


def _decode_panic_uint(tail_hex: str) -> int | None:
    if len(tail_hex) < 64:
        return None
    try:
        return int(tail_hex[:64], 16)
    except ValueError:
        return None


def revert_body(err: UpstreamJsonRpcError) -> dict[str, Any]:
    """Build the REST revert body for a confirmed-revert upstream error.

    Caller must check `is_revert(err) or is_out_of_gas(err)` first.
    """
    raw_data = err.data if isinstance(err.data, str) else "0x"
    if not _HEX_RE.fullmatch(raw_data):
        raw_data = "0x"
    reason, panic = decode_revert_data(raw_data)
    out: dict[str, Any] = {
        "reverted": True,
        "data": raw_data.lower() if raw_data != "0x" else "0x",
        "reason": reason,
        "panicCode": panic,
    }
    if is_out_of_gas(err):
        out["outOfGas"] = True
    return out
