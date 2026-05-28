"""Block identifier parsing.

Accepts the API-level grammar (decimal numbers, 32-byte hashes, named tags) and
converts to the JSON-RPC wire format (hex-encoded numbers, lowercase hashes,
tag strings).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

_BLOCK_TAGS: Final[frozenset[str]] = frozenset(
    {"latest", "safe", "finalized", "pending", "earliest"}
)

_HASH_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]{64}$")
_DECIMAL_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]+$")


class BlockIdError(ValueError):
    """Raised when a string cannot be parsed as a block identifier."""


@dataclass(frozen=True)
class BlockId:
    """A parsed block identifier — exactly one of tag, number, or hash is set."""

    tag: str | None = None
    number: int | None = None
    hash: str | None = None

    def __post_init__(self) -> None:
        set_fields = sum(x is not None for x in (self.tag, self.number, self.hash))
        if set_fields != 1:
            raise ValueError("BlockId must have exactly one of tag, number, hash set")

    def is_tag(self) -> bool:
        return self.tag is not None

    def is_number(self) -> bool:
        return self.number is not None

    def is_hash(self) -> bool:
        return self.hash is not None

    def to_rpc_param(self) -> str:
        """Render as the JSON-RPC `block` parameter (hex for numbers, lowercase for hashes)."""
        if self.tag is not None:
            return self.tag
        if self.number is not None:
            return f"0x{self.number:x}"
        assert self.hash is not None
        return self.hash


def parse_block_id(raw: str) -> BlockId:
    """Parse a user-facing block identifier.

    Accepts: `latest`/`safe`/`finalized`/`pending`/`earliest`, a decimal number,
    or a 0x-prefixed 32-byte hex hash. Hex-encoded block numbers are rejected.
    """
    if not raw:
        raise BlockIdError("block id is empty")
    if raw in _BLOCK_TAGS:
        return BlockId(tag=raw)
    if _DECIMAL_RE.fullmatch(raw):
        return BlockId(number=int(raw))
    if _HASH_RE.fullmatch(raw):
        return BlockId(hash=raw.lower())
    raise BlockIdError(f"unrecognised block identifier: {raw!r}")
