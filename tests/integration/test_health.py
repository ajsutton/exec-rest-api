"""End-to-end tests for /health and /health/ready against anvil."""


async def test_health_liveness(proxy_client):
    resp = await proxy_client.get("/health")
    assert resp.status == 200
    body = await resp.json()
    assert body == {"status": "ok"}


async def test_health_ready_against_anvil(proxy_client):
    resp = await proxy_client.get("/health/ready")
    assert resp.status == 200
    body = await resp.json()
    assert body["ready"] is True
    assert body["upstreamReachable"] is True
    # anvil starts mined; syncing is False
    assert body["syncing"] is False
    assert isinstance(body["blockNumber"], int)
    assert body["blockNumber"] >= 0
