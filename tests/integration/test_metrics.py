"""End-to-end /metrics tests against anvil."""

from __future__ import annotations


async def test_metrics_endpoint_reports_requests_and_upstream(proxy_client):
    await proxy_client.get("/chain")
    await proxy_client.get("/chain/id")
    resp = await proxy_client.get("/metrics")
    assert resp.status == 200
    assert resp.headers["Content-Type"].startswith("text/plain")
    body = await resp.text()
    assert 'path_template="/chain"' in body
    assert 'path_template="/chain/id"' in body
    assert 'method="eth_chainId"' in body


async def test_metrics_request_returns_x_block_height_when_known(proxy_client):
    import asyncio

    for _ in range(50):
        resp = await proxy_client.get("/chain/id")
        if "X-Block-Height" in resp.headers:
            break
        await asyncio.sleep(0.05)
    assert "X-Block-Height" in resp.headers
    assert int(resp.headers["X-Block-Height"]) >= 0


async def test_metrics_request_returns_x_upstream_method(proxy_client):
    resp = await proxy_client.get("/chain/id")
    assert resp.headers["X-Upstream-Method"] == "eth_chainId"


async def test_metrics_chain_head_gauge_eventually_populated(proxy_client):
    import asyncio

    for _ in range(50):
        resp = await proxy_client.get("/metrics")
        body = await resp.text()
        if "exec_rest_api_chain_head_block " in body:
            break
        await asyncio.sleep(0.05)
    assert "exec_rest_api_chain_head_block " in body
