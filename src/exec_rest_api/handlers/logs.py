"""/logs handler with cursor pagination and reorg detection.

When the request carries a `cursor`, all other filter params are ignored — the
cursor is the request (per API spec §6.2). The boundary block hash inside the
cursor lets us detect a chain reorg before re-fetching: if the block at the
cursor's `nextFromBlock` no longer has the boundary hash, we return 409
`chain-reorged` and the client must restart with a fresh query.
"""

from __future__ import annotations

import re
from typing import Any

from aiohttp import web

from exec_rest_api.block_id import BlockId, BlockIdError, parse_block_id
from exec_rest_api.cursor import Cursor, CursorError, decode_cursor, encode_cursor
from exec_rest_api.encoding import EncodingError, hex_to_int, map_address_lowercase
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.transactions import log_from_rpc
from exec_rest_api.pagination import fetch_logs_paginated
from exec_rest_api.server import add_get
from exec_rest_api.upstream import UpstreamClient

_TOPIC_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


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


def _chain_reorged(path: str, detail: str) -> web.Response:
    return problem_response(
        Problem(
            status=409,
            type_slug="chain-reorged",
            title="Chain reorganized",
            detail=detail,
            instance=path,
        )
    )


async def _resolve_block_id_to_number(
    upstream: UpstreamClient, bid: BlockId
) -> int | None:
    """Resolve a BlockId to an integer block number. Returns None if not found."""
    if bid.is_number():
        assert bid.number is not None
        return bid.number
    if bid.is_tag():
        if bid.tag == "earliest":
            return 0
        if bid.tag == "latest":
            head_hex = await upstream.call("eth_blockNumber")
            return hex_to_int(head_hex)
        # safe / finalized / pending — fetch the block summary
        rpc = await upstream.call("eth_getBlockByNumber", [bid.tag, False])
        if rpc is None:
            return None
        return hex_to_int(rpc["number"])
    # hash
    assert bid.hash is not None
    rpc = await upstream.call("eth_getBlockByHash", [bid.hash, False])
    if rpc is None:
        return None
    return hex_to_int(rpc["number"])


def _parse_topics(request: web.Request) -> tuple[list[str | None] | None, str | None]:
    """Build the `topics` array from `topic0..topic3` query params. Returns
    (topics, error_message). Trailing nulls are stripped."""
    out: list[str | None] = []
    last_set = -1
    for i in range(4):
        val = request.query.get(f"topic{i}")
        if val is None:
            out.append(None)
        else:
            if not _TOPIC_RE.fullmatch(val):
                return None, f"topic{i} must be 0x-prefixed 32-byte hex, got {val!r}"
            out.append(val.lower())
            last_set = i
    if last_set == -1:
        return [], None
    return out[: last_set + 1], None


def _parse_addresses_csv(raw: str | None) -> tuple[list[str] | None, str | None]:
    if raw is None or raw == "":
        return None, None
    out: list[str] = []
    for piece in raw.split(","):
        piece = piece.strip()
        try:
            out.append(map_address_lowercase(piece))
        except EncodingError as e:
            return None, str(e)
    return out, None


def _build_filter(
    *,
    addresses: list[str] | None,
    topics: list[str | None] | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if addresses is not None:
        out["address"] = addresses
    if topics:
        out["topics"] = topics
    return out


async def _verify_cursor_boundary(
    upstream: UpstreamClient, cursor: Cursor, path: str
) -> web.Response | None:
    """Confirm the cursor's boundary block hash is still canonical. Returns
    a 409 response on reorg, None on success."""
    rpc = await upstream.call(
        "eth_getBlockByNumber", [f"0x{cursor.next_from_block:x}", False]
    )
    if rpc is None:
        return _chain_reorged(
            path, f"block {cursor.next_from_block} no longer exists; restart the query"
        )
    if rpc["hash"].lower() != cursor.boundary_block_hash.lower():
        return _chain_reorged(
            path,
            f"block {cursor.next_from_block} hash changed since cursor was issued; "
            "restart the query",
        )
    return None


async def get_logs(request: web.Request) -> web.Response:
    config = request.app["config"]
    upstream: UpstreamClient = request.app["upstream"]
    limit_raw = request.query.get("limit")
    if limit_raw is not None:
        try:
            requested_limit = int(limit_raw)
            if requested_limit < 1:
                raise ValueError
        except ValueError:
            return _bad_request(
                request.path, f"limit must be a positive integer, got {limit_raw!r}"
            )
    else:
        requested_limit = config.default_page_size
    limit = min(requested_limit, config.max_page_size)

    cursor_raw = request.query.get("cursor")
    if cursor_raw is not None:
        try:
            cursor = decode_cursor(cursor_raw)
        except CursorError as e:
            return _bad_request(request.path, f"invalid cursor: {e}")
        reorg = await _verify_cursor_boundary(upstream, cursor, request.path)
        if reorg is not None:
            return reorg
        filter_ = cursor.filter_
        from_block = cursor.next_from_block
        to_block = cursor.to_block
        skip_until = cursor.last_log_index
    else:
        # Parse range
        try:
            from_bid = parse_block_id(request.query.get("fromBlock", "earliest"))
            to_bid = parse_block_id(request.query.get("toBlock", "latest"))
        except BlockIdError as e:
            return _bad_request(request.path, str(e))
        from_block_resolved = await _resolve_block_id_to_number(upstream, from_bid)
        to_block_resolved = await _resolve_block_id_to_number(upstream, to_bid)
        if from_block_resolved is None or to_block_resolved is None:
            return _bad_request(request.path, "fromBlock/toBlock could not be resolved")
        from_block = from_block_resolved
        to_block = to_block_resolved
        if from_block > to_block:
            return _bad_request(
                request.path, f"fromBlock ({from_block}) must be <= toBlock ({to_block})"
            )
        addresses, err = _parse_addresses_csv(request.query.get("address"))
        if err is not None:
            return _bad_request(request.path, err)
        topics, err = _parse_topics(request)
        if err is not None:
            return _bad_request(request.path, err)
        filter_ = _build_filter(addresses=addresses, topics=topics)
        skip_until = -1

    result = await fetch_logs_paginated(
        upstream=upstream,
        filter_=filter_,
        from_block=from_block,
        to_block=to_block,
        limit=limit,
        skip_until_log_index=skip_until,
    )

    rest_items = [log_from_rpc(log) for log in result.items]
    headers = {"X-Page-Size": str(limit)}
    if result.next_from_block is not None:
        # Need the boundary block's hash for reorg detection on resume
        boundary_rpc = await upstream.call(
            "eth_getBlockByNumber", [f"0x{result.next_from_block:x}", False]
        )
        if boundary_rpc is not None:
            next_cursor = encode_cursor(
                Cursor(
                    next_from_block=result.next_from_block,
                    last_log_index=result.last_log_index,
                    to_block=to_block,
                    boundary_block_hash=boundary_rpc["hash"].lower(),
                    filter_=filter_,
                )
            )
            headers["Link"] = f'</logs?cursor={next_cursor}>; rel="next"'

    return web.json_response(rest_items, headers=headers)


def register_routes(app: web.Application) -> None:
    add_get(app, "/logs", get_logs)
