"""End-to-end /traces tests against anvil."""

import pytest


async def test_traces_empty(proxy_client):
    """Anvil supports trace_filter; response is a well-formed list of traces.

    The anvil instance is shared across the integration suite, so the chain may
    contain traces left by other tests (contract deploys, etc.). We assert the
    endpoint returns a parseable list of trace objects rather than emptiness.
    """
    resp = await proxy_client.get("/traces?fromBlock=0&toBlock=latest")
    if resp.status == 501:
        pytest.skip("anvil build does not support trace_filter")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    for trace in body:
        # Each item must carry the canonical fields produced by trace_from_rpc.
        assert isinstance(trace.get("blockNumber"), int)
        assert trace.get("type")
        assert trace.get("transactionHash", "").startswith("0x")


async def test_trace_get_not_found(proxy_client):
    h = "0x" + "ff" * 32
    resp = await proxy_client.get(f"/traces/{h}/")
    if resp.status == 501:
        pytest.skip("anvil build does not support trace_get")
    assert resp.status in (404, 200)  # 404 typical; some clients return empty obj
