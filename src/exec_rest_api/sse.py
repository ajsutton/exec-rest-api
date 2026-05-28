"""Server-Sent Event framing helpers.

Outputs always end with the SSE frame terminator (``\\n\\n``). ``data`` payloads
are compacted to a single line so multi-line JSON cannot accidentally split a
frame. Heartbeats are SSE comment lines (`:` prefix) emitted by the stream
helper when the source is quiet — this keeps idle connections from being
closed by intermediaries.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Coroutine
from typing import Any, cast


def format_retry(milliseconds: int) -> bytes:
    return f"retry: {milliseconds}\n\n".encode()


def format_event(
    *,
    event: str,
    id_: str | None,
    data: Any,
) -> bytes:
    """Render one SSE event frame. ``data`` is JSON-encoded as a single line."""
    lines: list[str] = [f"event: {event}"]
    if id_ is not None:
        lines.append(f"id: {id_}")
    payload = json.dumps(data, separators=(",", ":"))
    lines.append(f"data: {payload}")
    return ("\n".join(lines) + "\n\n").encode()


def format_comment(text: str) -> bytes:
    """Render an SSE comment line. Newlines in ``text`` are dropped."""
    safe = text.replace("\n", " ").replace("\r", " ")
    return f": {safe}\n\n".encode()


async def stream_with_heartbeat(
    source: AsyncIterator[bytes],
    *,
    interval_seconds: float,
) -> AsyncIterator[bytes]:
    """Yield from ``source``. If silent for ``interval_seconds``, emit a heartbeat."""
    loop = asyncio.get_running_loop()
    iterator = source.__aiter__()
    next_task: asyncio.Task[bytes] | None = None
    try:
        while True:
            if next_task is None:
                next_task = loop.create_task(
                    cast(Coroutine[Any, Any, bytes], iterator.__anext__())
                )
            try:
                chunk = await asyncio.wait_for(asyncio.shield(next_task), timeout=interval_seconds)
            except asyncio.TimeoutError:
                yield format_comment(f"ping {int(time.time())}")
                continue
            except StopAsyncIteration:
                return
            next_task = None
            yield chunk
    finally:
        if next_task is not None and not next_task.done():
            next_task.cancel()
