"""Encoding conversions between JSON-RPC wire format and REST API shapes.

Single source of truth for:
- hex ↔ decimal integer conversion (for things-that-must-fit-in-a-number)
- hex ↔ decimal-string wei conversion (for things-that-may-exceed-2^53)
- status / transaction-type enum mapping
- address case normalization

JSON-RPC encodes all numeric quantities as 0x-hex. The REST API exposes safe
integers as JSON numbers and wei amounts as decimal strings (see API spec §4.1).
"""

from __future__ import annotations

import re
from typing import Final

_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]+$")
_DECIMAL_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]+$")
_ADDRESS_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EncodingError(ValueError):
    """Raised when a value cannot be encoded or decoded in the expected form."""


# ── hex ↔ int ──────────────────────────────────────────────────────────────

def hex_to_int(s: str) -> int:
    """Parse a JSON-RPC 0x-prefixed hex quantity into an int."""
    if not isinstance(s, str) or not _HEX_RE.fullmatch(s):
        raise EncodingError(f"expected 0x-prefixed hex, got {s!r}")
    return int(s, 16)


def decimal_to_hex(n: int) -> str:
    """Render an int as a 0x-prefixed hex quantity (minimal form, no leading zeros)."""
    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise EncodingError(f"expected non-negative int, got {n!r}")
    return f"0x{n:x}"


# ── wei ────────────────────────────────────────────────────────────────────

def wei_from_rpc(rpc: str) -> str:
    """Decode a JSON-RPC hex quantity as a decimal-string wei amount."""
    return str(hex_to_int(rpc))


def wei_to_rpc(value: str | int) -> str:
    """Encode a wei amount (decimal string or int) as a JSON-RPC hex quantity."""
    if isinstance(value, bool):
        raise EncodingError(f"bool is not a wei value: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise EncodingError(f"wei must be non-negative, got {value}")
        return f"0x{value:x}"
    if isinstance(value, str):
        if not _DECIMAL_RE.fullmatch(value):
            raise EncodingError(f"wei string must be decimal digits, got {value!r}")
        return f"0x{int(value):x}"
    raise EncodingError(f"wei must be int or decimal string, got {type(value).__name__}")


# ── lenient input parsing ──────────────────────────────────────────────────

def parse_input_int(value: object) -> int:
    """Accept either a JSON number or a decimal string as a non-negative integer."""
    if isinstance(value, bool):
        raise EncodingError(f"bool is not an int: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise EncodingError(f"expected non-negative int, got {value}")
        return value
    if isinstance(value, str) and _DECIMAL_RE.fullmatch(value):
        return int(value)
    raise EncodingError(f"expected non-negative int or decimal string, got {value!r}")


def parse_input_wei(value: object) -> int:
    """Accept either a JSON number or a decimal string for a wei amount; return int."""
    return parse_input_int(value)


# ── status enum ────────────────────────────────────────────────────────────

_STATUS_RPC_TO_REST: Final[dict[str, str]] = {"0x0": "failed", "0x1": "success"}
_STATUS_REST_TO_RPC: Final[dict[str, str]] = {v: k for k, v in _STATUS_RPC_TO_REST.items()}


def rest_status_from_rpc(rpc: str) -> str:
    if rpc not in _STATUS_RPC_TO_REST:
        raise EncodingError(f"unknown receipt status: {rpc!r}")
    return _STATUS_RPC_TO_REST[rpc]


def rpc_status_from_rest(rest: str) -> str:
    if rest not in _STATUS_REST_TO_RPC:
        raise EncodingError(f"unknown receipt status: {rest!r}")
    return _STATUS_REST_TO_RPC[rest]


# ── transaction type enum ──────────────────────────────────────────────────

_TX_TYPE_RPC_TO_REST: Final[dict[str, str]] = {
    "0x0": "legacy",
    "0x1": "access-list",
    "0x2": "dynamic-fee",
    "0x3": "blob",
}
_TX_TYPE_REST_TO_RPC: Final[dict[str, str]] = {v: k for k, v in _TX_TYPE_RPC_TO_REST.items()}


def rest_tx_type_from_rpc(rpc: str) -> str:
    if rpc not in _TX_TYPE_RPC_TO_REST:
        raise EncodingError(f"unknown transaction type: {rpc!r}")
    return _TX_TYPE_RPC_TO_REST[rpc]


def rpc_tx_type_from_rest(rest: str) -> str:
    if rest not in _TX_TYPE_REST_TO_RPC:
        raise EncodingError(f"unknown transaction type: {rest!r}")
    return _TX_TYPE_REST_TO_RPC[rest]


# ── address case ───────────────────────────────────────────────────────────

def map_address_lowercase(addr: str) -> str:
    """Validate and lowercase an Ethereum address.

    The proxy does no Keccak-256, so EIP-55 checksumming is not applied; we
    simply lowercase. See implementation design §3 for rationale.
    """
    if not isinstance(addr, str) or not _ADDRESS_RE.fullmatch(addr):
        raise EncodingError(f"expected 0x-prefixed 20-byte address, got {addr!r}")
    return addr.lower()
