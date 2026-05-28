"""End-to-end /traces tests against anvil."""

import pytest


async def test_traces_empty(proxy_client):
    """Anvil supports trace_filter; idle chain has no traces in the range."""
    resp = await proxy_client.get("/traces?fromBlock=0&toBlock=latest")
    if resp.status == 501:
        pytest.skip("anvil build does not support trace_filter")
    assert resp.status == 200
    body = await resp.json()
    assert body == []


async def test_trace_get_not_found(proxy_client):
    h = "0x" + "ff" * 32
    resp = await proxy_client.get(f"/traces/{h}/")
    if resp.status == 501:
        pytest.skip("anvil build does not support trace_get")
    assert resp.status in (404, 200)  # 404 typical; some clients return empty obj
