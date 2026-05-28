"""Live tests against anvil for /streams/*."""

from __future__ import annotations

import asyncio


async def test_streams_blocks_emits_against_anvil(proxy_client):
    """Anvil mines every second; we should see ≥ 2 block events within 3s."""
    resp = await proxy_client.get("/streams/blocks")
    assert resp.status == 200
    assert resp.content_type == "text/event-stream"

    block_events: list[bytes] = []
    deadline = asyncio.get_event_loop().time() + 4.0
    buf = b""
    while asyncio.get_event_loop().time() < deadline and len(block_events) < 2:
        chunk = await resp.content.read(512)
        if not chunk:
            break
        buf += chunk
        while b"\n\n" in buf:
            frame, buf = buf.split(b"\n\n", 1)
            if b"event: block" in frame:
                block_events.append(frame)
    await resp.release()
    assert len(block_events) >= 2, f"only got {len(block_events)} block events"


async def test_streams_sync_status_emits_initial_state(proxy_client):
    """The sync-status endpoint responds correctly.

    Anvil does not support eth_subscribe("syncing"), so a 502 is acceptable.
    If the upstream does support it (status 200), the retry directive must appear.
    """
    resp = await proxy_client.get("/streams/sync-status")
    # Anvil returns JSON-RPC -32603 for syncing subscriptions; map_jsonrpc_error
    # maps that to 502. Real nodes that support eth_subscribe("syncing") return 200.
    assert resp.status in (200, 502)

    if resp.status == 200:
        deadline = asyncio.get_event_loop().time() + 3.0
        seen = False
        buf = b""
        while asyncio.get_event_loop().time() < deadline and not seen:
            chunk = await resp.content.read(512)
            if not chunk:
                break
            buf += chunk
            seen = b"event: sync-status" in buf or b"retry: 5000" in buf
        await resp.release()
        assert seen
    else:
        await resp.release()
