"""Internal block-range chunking for `/logs`.

Some clients (Infura, Alchemy) cap a single `eth_getLogs` call at a fixed number
of blocks or matched results. To keep pagination transparent to callers, this
module fetches in chunks and halves the chunk when the upstream complains.

Pagination state (cursor) is owned by `cursor.py` and the `/logs` handler — this
module is purely about iterating the range and accumulating up to `limit` items.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from exec_rest_api.encoding import decimal_to_hex, hex_to_int
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError

DEFAULT_CHUNK_SIZE: Final[int] = 5000
_MIN_CHUNK_SIZE: Final[int] = 1
_TOO_LARGE_PATTERNS: Final[tuple[str, ...]] = (
    "query returned more than",
    "exceed maximum block range",
)


@dataclass(frozen=True)
class PaginationResult:
    """Result of a paginated logs fetch.

    `next_from_block` is `None` if the requested range was fully scanned.
    Otherwise it carries the block number to resume from, with `last_log_index`
    being the largest log index already emitted on that block.
    """

    items: list[dict[str, Any]]
    next_from_block: int | None
    last_log_index: int


def _is_too_large(err: UpstreamJsonRpcError) -> bool:
    if err.code != -32000:
        return False
    msg = err.message.lower()
    return any(p in msg for p in _TOO_LARGE_PATTERNS)


async def fetch_logs_paginated(
    *,
    upstream: UpstreamClient,
    filter_: dict[str, Any],
    from_block: int,
    to_block: int,
    limit: int,
    skip_until_log_index: int = -1,
) -> PaginationResult:
    """Fetch logs across `[from_block, to_block]` in chunks, up to `limit` items.

    `filter_` is forwarded to `eth_getLogs` with `fromBlock`/`toBlock` overridden
    per chunk. `skip_until_log_index` is used on the very first block to skip
    logs already emitted by the previous page (cursor resume).
    """
    items: list[dict[str, Any]] = []
    cur = from_block
    chunk = DEFAULT_CHUNK_SIZE
    # The skip-window is only active for the very first resume block.
    skip_window_block = from_block if skip_until_log_index >= 0 else -1

    while cur <= to_block and len(items) < limit:
        end = min(cur + chunk - 1, to_block)
        params = {
            **{k: v for k, v in filter_.items() if k not in ("fromBlock", "toBlock")},
            "fromBlock": decimal_to_hex(cur),
            "toBlock": decimal_to_hex(end),
        }
        try:
            page = await upstream.call("eth_getLogs", [params])
        except UpstreamJsonRpcError as e:
            if _is_too_large(e) and chunk > _MIN_CHUNK_SIZE:
                chunk = max(_MIN_CHUNK_SIZE, chunk // 2)
                continue
            raise
        for log in page:
            block_number = hex_to_int(log["blockNumber"])
            log_index = hex_to_int(log["logIndex"])
            if block_number == skip_window_block and log_index <= skip_until_log_index:
                continue
            items.append(log)
            if len(items) >= limit:
                # Stopped mid-block: cursor's `last_log_index` is the index just emitted.
                return PaginationResult(
                    items=items, next_from_block=block_number, last_log_index=log_index
                )
        cur = end + 1
        skip_window_block = -1  # only applies to the very first block
        # Grow back toward the default on success (but never above).
        chunk = min(DEFAULT_CHUNK_SIZE, max(_MIN_CHUNK_SIZE, chunk * 2))
    return PaginationResult(items=items, next_from_block=None, last_log_index=-1)
