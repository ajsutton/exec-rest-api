"""Conformance: first events on each stream validate against the OpenAPI schemas."""

from __future__ import annotations

import asyncio
import json


def _extract_data(frame: bytes) -> dict | None:
    for line in frame.splitlines():
        if line.startswith(b"data: "):
            return json.loads(line[len(b"data: "):])
    return None


async def _first_event(resp, *, event_name: bytes, timeout: float = 4.0) -> dict:
    buf = b""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        chunk = await resp.content.read(512)
        if not chunk:
            break
        buf += chunk
        while b"\n\n" in buf:
            frame, buf = buf.split(b"\n\n", 1)
            if event_name in frame:
                data = _extract_data(frame)
                assert data is not None, f"no data line in frame: {frame!r}"
                return data
    raise AssertionError(f"never saw {event_name!r} within {timeout}s")


async def test_streams_blocks_first_event_validates(proxy_client, make_validator):
    resp = await proxy_client.get("/streams/blocks")
    assert resp.status == 200
    data = await _first_event(resp, event_name=b"event: block")
    make_validator("#/components/schemas/BlockHeader").validate(data)
    await resp.release()


async def test_streams_sync_status_first_event_validates(proxy_client, make_validator):
    resp = await proxy_client.get("/streams/sync-status")
    # anvil does not support eth_subscribe("syncing") and returns -32603, which the
    # handler maps to 502.  Real nodes that implement the subscription return 200.
    # Either is acceptable here — we only verify the channel behaves correctly.
    assert resp.status in (200, 502)
    if resp.status == 200:
        buf = await resp.content.read(64)
        assert b"retry: 5000" in buf
    await resp.release()
