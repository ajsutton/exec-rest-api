"""/streams/* SSE handlers.

Shared driver:
  - validate request before opening the SSE response (so pre-stream errors return Problem+JSON)
  - prepare a `text/event-stream` StreamResponse
  - emit `retry: 5000`
  - subscribe via SubscriptionManager
  - stream events with periodic heartbeats
  - on mid-stream error, emit `event: error` and close
  - apply backpressure (drop if transport.get_write_buffer_size() exceeds sse_buffer_bytes)
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import Any, cast

from aiohttp import web

from exec_rest_api.encoding import EncodingError, hex_to_int, map_address_lowercase
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.blocks import block_header_from_rpc
from exec_rest_api.handlers.transactions import log_from_rpc, transaction_from_rpc
from exec_rest_api.server import add_get
from exec_rest_api.sse import format_event, format_retry, stream_with_heartbeat
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable

logger = logging.getLogger("exec_rest_api.handlers.streams")

_TOPIC_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


EventFormatter = Callable[[Any], tuple[str, str | None, Any]]
"""Converts one payload from the SubscriptionManager into (event-name, id, data)."""


def _block_event(payload: Any) -> tuple[str, str | None, Any]:
    header = block_header_from_rpc(payload)
    return "block", str(header["number"]), header


# ── shared driver ─────────────────────────────────────────────────────────


async def _open_sse(request: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    resp.enable_chunked_encoding()
    await resp.prepare(request)
    await resp.write(format_retry(5000))
    return resp


async def _run_stream(
    request: web.Request,
    *,
    kind: str,
    params: Any,
    formatter: EventFormatter,
    gap_event_name: str = "gap",
) -> web.StreamResponse:
    subscriptions = request.app["subscriptions"]
    config = request.app["config"]
    try:
        events: AsyncGenerator[StreamEvent, None] = cast(
            AsyncGenerator[StreamEvent, None],
            await subscriptions.subscribe(kind=kind, params=params),
        )
    except SubscriptionUnavailable as exc:
        return problem_response(
            Problem(
                status=503,
                type_slug="upstream-unavailable",
                title="Upstream unavailable",
                detail=str(exc),
                instance=request.path,
            )
        )

    resp = await _open_sse(request)

    async def to_bytes() -> AsyncIterator[bytes]:
        async for event in events:
            if event is GAP or event.kind == "gap":
                yield format_event(event=gap_event_name, id_=None, data={})
                continue
            try:
                name, ev_id, payload = formatter(event.payload)
            except Exception:
                logger.exception("event formatter raised on %s", kind)
                yield format_event(
                    event="error",
                    id_=None,
                    data={
                        "type": "https://errors.ethereum-rest/internal-error",
                        "title": "Internal error",
                    },
                )
                return
            yield format_event(event=name, id_=ev_id, data=payload)

    try:
        async for chunk in stream_with_heartbeat(
            to_bytes(), interval_seconds=config.sse_heartbeat_seconds
        ):
            if _over_backpressure_threshold(request, config.sse_buffer_bytes):
                logger.info("dropping SSE client over backpressure threshold")
                return resp
            await resp.write(chunk)
    except ConnectionResetError:
        pass
    finally:
        await events.aclose()
    return resp


def _over_backpressure_threshold(request: web.Request, threshold_bytes: int) -> bool:
    transport = request.transport
    if transport is None:
        return False
    try:
        return transport.get_write_buffer_size() > threshold_bytes
    except AttributeError:
        return False


# ── handlers ──────────────────────────────────────────────────────────────


async def get_streams_blocks(request: web.Request) -> web.StreamResponse:
    return await _run_stream(
        request, kind="newHeads", params=None, formatter=_block_event
    )


def _log_event(payload: Any) -> tuple[str, str | None, Any]:
    rest_log = log_from_rpc(payload)
    return "log", f"{rest_log['blockNumber']}-{rest_log['logIndex']}", rest_log


def _parse_log_filter(request: web.Request) -> dict[str, Any] | web.Response:
    """Build an eth_subscribe('logs', filter) params dict from query params, or
    a 400 Problem response if the params are malformed."""
    filter_: dict[str, Any] = {}
    addr_raw = request.query.get("address")
    if addr_raw:
        addrs: list[str] = []
        for piece in addr_raw.split(","):
            try:
                addrs.append(map_address_lowercase(piece.strip()))
            except EncodingError as e:
                return problem_response(
                    Problem(
                        status=400,
                        type_slug="invalid-request",
                        title="Invalid request",
                        detail=str(e),
                        instance=request.path,
                    )
                )
        filter_["address"] = addrs
    topics: list[str | None] = []
    last_set = -1
    for i in range(4):
        val = request.query.get(f"topic{i}")
        if val is None:
            topics.append(None)
        else:
            if not _TOPIC_RE.fullmatch(val):
                return problem_response(
                    Problem(
                        status=400,
                        type_slug="invalid-request",
                        title="Invalid request",
                        detail=f"topic{i} must be 0x-prefixed 32-byte hex, got {val!r}",
                        instance=request.path,
                    )
                )
            topics.append(val.lower())
            last_set = i
    if last_set >= 0:
        filter_["topics"] = topics[: last_set + 1]
    return filter_


async def get_streams_logs(request: web.Request) -> web.StreamResponse:
    filter_or_err = _parse_log_filter(request)
    if isinstance(filter_or_err, web.Response):
        return filter_or_err
    return await _run_stream(
        request, kind="logs", params=filter_or_err, formatter=_log_event
    )


def _pending_event_hash_only(payload: Any) -> tuple[str, str | None, Any]:
    # Upstream sends just the tx hash as a string when subscribed without `true`.
    tx_hash = payload if isinstance(payload, str) else payload.get("hash")
    return "pending-transaction", tx_hash, {"hash": tx_hash}


def _pending_event_full(payload: Any) -> tuple[str, str | None, Any]:
    rest_tx = transaction_from_rpc(payload)
    return "pending-transaction", rest_tx["hash"], rest_tx


async def get_streams_pending(request: web.Request) -> web.StreamResponse:
    full_raw = request.query.get("full")
    full = full_raw is not None and full_raw.lower() == "true"
    formatter = _pending_event_full if full else _pending_event_hash_only
    params = True if full else None
    return await _run_stream(
        request,
        kind="newPendingTransactions",
        params=params,
        formatter=formatter,
        gap_event_name="resumed",
    )


def _sync_status_event(payload: Any) -> tuple[str, str | None, Any]:
    if payload is False:
        return "sync-status", None, {"syncing": False}
    return "sync-status", None, {
        "syncing": True,
        "startingBlock": hex_to_int(payload["startingBlock"]),
        "currentBlock": hex_to_int(payload["currentBlock"]),
        "highestBlock": hex_to_int(payload["highestBlock"]),
    }


async def get_streams_sync_status(request: web.Request) -> web.StreamResponse:
    return await _run_stream(
        request,
        kind="syncing",
        params=None,
        formatter=_sync_status_event,
        gap_event_name="resumed",
    )


def register_routes(app: web.Application) -> None:
    add_get(app, "/streams/blocks", get_streams_blocks)
    add_get(app, "/streams/logs", get_streams_logs)
    add_get(app, "/streams/pending-transactions", get_streams_pending)
    add_get(app, "/streams/sync-status", get_streams_sync_status)
