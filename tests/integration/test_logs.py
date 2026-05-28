"""End-to-end /logs tests against anvil."""



async def test_logs_empty(proxy_client):
    """Anvil's idle chain has no logs."""
    resp = await proxy_client.get("/logs?fromBlock=0&toBlock=latest")
    assert resp.status == 200
    body = await resp.json()
    assert body == []
    # X-Page-Size should reflect the default
    assert int(resp.headers["X-Page-Size"]) > 0


async def test_logs_from_gt_to_400(proxy_client):
    resp = await proxy_client.get("/logs?fromBlock=200&toBlock=100")
    assert resp.status == 400


async def test_logs_bad_topic_400(proxy_client):
    resp = await proxy_client.get("/logs?fromBlock=0&toBlock=10&topic0=not-a-topic")
    assert resp.status == 400
