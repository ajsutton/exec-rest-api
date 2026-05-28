"""End-to-end tests for /chain/* against anvil."""


async def test_chain_id(proxy_client):
    resp = await proxy_client.get("/chain/id")
    assert resp.status == 200
    body = await resp.json()
    # anvil defaults to chain id 31337 (foundry's anvil default)
    assert body == {"chainId": 31337}


async def test_chain_client(proxy_client):
    resp = await proxy_client.get("/chain/client")
    body = await resp.json()
    assert "client" in body
    assert isinstance(body["client"], str)
    assert "anvil" in body["client"].lower()


async def test_chain_sync_status(proxy_client):
    resp = await proxy_client.get("/chain/sync-status")
    body = await resp.json()
    # anvil isn't syncing
    assert body == {"syncing": False}


async def test_chain_peers(proxy_client):
    resp = await proxy_client.get("/chain/peers")
    body = await resp.json()
    # anvil does not implement net_peerCount / net_listening; the proxy correctly
    # maps the JSON-RPC -32601 to a 501 Problem response.
    if resp.status == 501:
        assert body["status"] == 501
        return
    # Real execution clients do implement these methods.
    assert resp.status == 200
    assert "peerCount" in body
    assert "listening" in body
    assert isinstance(body["peerCount"], int)
    assert isinstance(body["listening"], bool)


async def test_chain_composite(proxy_client):
    resp = await proxy_client.get("/chain")
    assert resp.status == 200
    body = await resp.json()
    assert body["chainId"] == 31337
    assert "anvil" in body["client"].lower()
    assert body["syncing"] == {"syncing": False}
    assert isinstance(body["blockNumber"], int)


async def test_unknown_path_404(proxy_client):
    """A path with no registered handler returns a Problem with the URL catalogue."""
    resp = await proxy_client.get("/no-such-resource")
    assert resp.status == 404
    assert resp.content_type == "application/problem+json"
    body = await resp.json()
    assert body["type"].endswith("/path-not-supported")
    available = body["data"]["availableUrls"]
    # Spot-check a few well-known routes appear in the catalogue
    assert "/chain" in available
    assert "/chain/id" in available
    assert "/health" in available
    assert "/metrics" in available
    assert "/blocks/{id}" in available


async def test_request_id_round_trip(proxy_client):
    resp = await proxy_client.get("/chain/id", headers={"X-Request-ID": "from-integration-test"})
    assert resp.headers["X-Request-ID"] == "from-integration-test"


async def test_trailing_slash_optional(proxy_client):
    """Both /chain/id and /chain/id/ should reach the same handler."""
    resp1 = await proxy_client.get("/chain/id")
    resp2 = await proxy_client.get("/chain/id/")
    assert resp1.status == 200
    assert resp2.status == 200
    assert await resp1.json() == await resp2.json()
