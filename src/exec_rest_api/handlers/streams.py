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
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import Any, cast

from aiohttp import web

from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.handlers.blocks import block_header_from_rpc
from exec_rest_api.server import add_get
from exec_rest_api.sse import format_event, format_retry, stream_with_heartbeat
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable

logger = logging.getLogger("exec_rest_api.handlers.streams")


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


def register_routes(app: web.Application) -> None:
    add_get(app, "/streams/blocks", get_streams_blocks)
