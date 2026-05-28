"""/traces handlers — paginated `trace_filter` and the `trace_get` lookup.

Trace pagination uses `trace_filter`'s native `after`/`count` parameters, not
block-range chunking — so reorg detection isn't applied here. (If the caller
needs reorg-safe paging they should use `/blocks/{id}/traces` with a stable
block id.)
"""

from __future__ import annotations

import re

from aiohttp import web

from exec_rest_api.block_id import BlockIdError, parse_block_id
from exec_rest_api.cursor import (
    CursorError,
    TraceCursor,
    decode_trace_cursor,
    encode_trace_cursor,
)
from exec_rest_api.encoding import EncodingError, hex_to_int, map_address_lowercase
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.transactions import trace_from_rpc
from exec_rest_api.server import add_get
from exec_rest_api.upstream import UpstreamClient

_TX_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
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


def _parse_addresses_csv(raw: str | None) -> tuple[list[str] | None, str | None]:
    if raw is None or raw == "":
        return None, None
    out: list[str] = []
    for piece in raw.split(","):
        try:
            out.append(map_address_lowercase(piece.strip()))
        except EncodingError as e:
            return None, str(e)
    return out, None


async def _resolve_block_id_to_number(upstream: UpstreamClient, raw: str) -> int:
    bid = parse_block_id(raw)
    if bid.is_number():
        assert bid.number is not None
        return bid.number
    if bid.is_tag():
        if bid.tag == "earliest":
            return 0
        if bid.tag == "latest":
            return hex_to_int(await upstream.call("eth_blockNumber"))
        rpc = await upstream.call("eth_getBlockByNumber", [bid.tag, False])
        if rpc is None:
            raise BlockIdError(f"could not resolve {bid.tag!r}")
        return hex_to_int(rpc["number"])
    assert bid.hash is not None
    rpc = await upstream.call("eth_getBlockByHash", [bid.hash, False])
    if rpc is None:
        raise BlockIdError(f"block hash {bid.hash} not found")
    return hex_to_int(rpc["number"])


# ─── /traces (trace_filter, paginated) ────────────────────────────────────


async def get_traces(request: web.Request) -> web.Response:
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
            cursor = decode_trace_cursor(cursor_raw)
        except CursorError as e:
            return _bad_request(request.path, f"invalid cursor: {e}")
        filter_ = cursor.filter_
        from_block = cursor.from_block
        to_block = cursor.to_block
        after = cursor.after
    else:
        try:
            from_block = await _resolve_block_id_to_number(
                upstream, request.query.get("fromBlock", "earliest")
            )
            to_block = await _resolve_block_id_to_number(
                upstream, request.query.get("toBlock", "latest")
            )
        except BlockIdError as e:
            return _bad_request(request.path, str(e))
        if from_block > to_block:
            return _bad_request(
                request.path, f"fromBlock ({from_block}) must be <= toBlock ({to_block})"
            )
        from_addresses, err = _parse_addresses_csv(request.query.get("fromAddress"))
        if err is not None:
            return _bad_request(request.path, err)
        to_addresses, err = _parse_addresses_csv(request.query.get("toAddress"))
        if err is not None:
            return _bad_request(request.path, err)
        filter_ = {}
        if from_addresses is not None:
            filter_["fromAddress"] = from_addresses
        if to_addresses is not None:
            filter_["toAddress"] = to_addresses
        after = 0

    rpc_filter = {
        **filter_,
        "fromBlock": f"0x{from_block:x}",
        "toBlock": f"0x{to_block:x}",
        "after": after,
        "count": limit,
    }
    rpc = await upstream.call("trace_filter", [rpc_filter])
    items = [trace_from_rpc(t) for t in (rpc or [])]
    headers = {"X-Page-Size": str(limit)}
    if len(items) >= limit:
        next_cursor = encode_trace_cursor(
            TraceCursor(
                after=after + limit,
                from_block=from_block,
                to_block=to_block,
                filter_=filter_,
            )
        )
        headers["Link"] = f'</traces?cursor={next_cursor}>; rel="next"'
    return web.json_response(items, headers=headers)


# ─── /traces/{txHash}/{traceAddress} ──────────────────────────────────────


async def get_trace(request: web.Request) -> web.Response:
    tx = request.match_info["hash"]
    if not _TX_HASH_RE.fullmatch(tx):
        return _bad_request(request.path, "transaction hash must be 0x-prefixed 32-byte hex")
    tx = tx.lower()
    trace_addr_raw = request.match_info.get("trace_address", "")
    try:
        trace_addr: list[int] = []
        if trace_addr_raw not in ("", "/"):
            for piece in trace_addr_raw.split(","):
                segment = piece.strip()
                if not _DECIMAL_RE.fullmatch(segment):
                    raise ValueError(
                        f"trace address segment must be a non-negative integer: {segment!r}"
                    )
                trace_addr.append(int(segment))
    except ValueError as e:
        return _bad_request(request.path, str(e))
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("trace_get", [tx, trace_addr])
    if rpc is None:
        return _not_found(request.path, f"trace not found for {tx} at {trace_addr_raw or 'root'}")
    return web.json_response(trace_from_rpc(rpc))


def register_routes(app: web.Application) -> None:
    add_get(app, "/traces", get_traces)
    # Root trace path: /traces/{hash}/ has empty trace_address. The two-segment
    # form below handles `/traces/{hash}/0,1,2` etc.
    app.router.add_get("/traces/{hash}/", get_trace)
    app.router.add_get("/traces/{hash}/{trace_address}", get_trace)
