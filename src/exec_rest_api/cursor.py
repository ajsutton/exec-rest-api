"""Opaque pagination cursors.

Encoded as base64url(JSON). Two flavors:

- `Cursor` for `/logs`: carries the resume block number, last log index already
  emitted, frozen `toBlock`, the boundary block hash (for reorg detection), and
  the original filter.
- `TraceCursor` for `/traces`: carries `after`/`count` state for `trace_filter`,
  the frozen block range, and the original filter.

The encoding is server-internal and may change without notice. Clients treat
cursors as opaque — they receive one in a `Link: rel="next"` header and pass
it back via `?cursor=…`. A tampered-with or malformed cursor maps to a 400.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from typing import Any


class CursorError(ValueError):
    """Raised when a cursor cannot be decoded (malformed, tampered, or missing fields)."""


def _b64url_encode(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(encoded: str) -> dict[str, Any]:
    if not encoded:
        raise CursorError("cursor is empty")
    padding = "=" * (-len(encoded) % 4)
    try:
        raw = base64.urlsafe_b64decode(encoded + padding)
    except (binascii.Error, ValueError) as e:
        raise CursorError(f"cursor is not valid base64url: {e}") from e
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise CursorError(f"cursor payload is not valid JSON: {e}") from e
    if not isinstance(payload, dict):
        raise CursorError(f"cursor payload must be an object, got {type(payload).__name__}")
    return payload


@dataclass(frozen=True)
class Cursor:
    """State carried between pages of a paginated query.

    `last_log_index` is the index of the last log emitted on the *boundary* block.
    A value of -1 means no logs were emitted on the boundary yet (so resume from
    log index 0). On the next page we skip any log on `next_from_block` whose
    `logIndex <= last_log_index`.
    """

    next_from_block: int
    last_log_index: int
    to_block: int
    boundary_block_hash: str
    filter_: dict[str, Any] = field(default_factory=dict)


def encode_cursor(cursor: Cursor) -> str:
    """Encode a logs Cursor as a base64url string (no padding)."""
    return _b64url_encode(
        {
            "kind": "logs",
            "nextFromBlock": cursor.next_from_block,
            "lastLogIndex": cursor.last_log_index,
            "toBlock": cursor.to_block,
            "boundaryBlockHash": cursor.boundary_block_hash,
            "filter": cursor.filter_,
        }
    )


def decode_cursor(encoded: str) -> Cursor:
    """Decode a base64url logs cursor. Raises CursorError on any failure."""
    payload = _b64url_decode(encoded)
    try:
        next_from_block = payload["nextFromBlock"]
        last_log_index = payload["lastLogIndex"]
        to_block = payload["toBlock"]
        boundary_block_hash = payload["boundaryBlockHash"]
        filter_ = payload["filter"]
    except KeyError as e:
        raise CursorError(f"cursor missing field: {e}") from e
    if not isinstance(next_from_block, int) or isinstance(next_from_block, bool):
        raise CursorError("nextFromBlock must be an int")
    if not isinstance(last_log_index, int) or isinstance(last_log_index, bool):
        raise CursorError("lastLogIndex must be an int")
    if not isinstance(to_block, int) or isinstance(to_block, bool):
        raise CursorError("toBlock must be an int")
    if not isinstance(boundary_block_hash, str):
        raise CursorError("boundaryBlockHash must be a string")
    if not isinstance(filter_, dict):
        raise CursorError("filter must be an object")
    return Cursor(
        next_from_block=next_from_block,
        last_log_index=last_log_index,
        to_block=to_block,
        boundary_block_hash=boundary_block_hash,
        filter_=filter_,
    )


@dataclass(frozen=True)
class TraceCursor:
    """Resume state for paginated trace_filter queries."""

    after: int
    from_block: int
    to_block: int
    filter_: dict[str, Any] = field(default_factory=dict)


def encode_trace_cursor(cursor: TraceCursor) -> str:
    return _b64url_encode(
        {
            "kind": "traces",
            "after": cursor.after,
            "fromBlock": cursor.from_block,
            "toBlock": cursor.to_block,
            "filter": cursor.filter_,
        }
    )


def decode_trace_cursor(encoded: str) -> TraceCursor:
    payload = _b64url_decode(encoded)
    try:
        after = payload["after"]
        from_block = payload["fromBlock"]
        to_block = payload["toBlock"]
        filter_ = payload["filter"]
    except KeyError as e:
        raise CursorError(f"cursor missing field: {e}") from e
    if (
        not isinstance(after, int)
        or isinstance(after, bool)
        or not isinstance(from_block, int)
        or isinstance(from_block, bool)
        or not isinstance(to_block, int)
        or isinstance(to_block, bool)
    ):
        raise CursorError("after / fromBlock / toBlock must all be ints")
    if not isinstance(filter_, dict):
        raise CursorError("filter must be an object")
    return TraceCursor(after=after, from_block=from_block, to_block=to_block, filter_=filter_)
