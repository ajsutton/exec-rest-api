"""End-to-end /transactions/* tests against anvil."""



async def test_get_transaction_not_found(proxy_client):
    h = "0x" + "ff" * 32
    resp = await proxy_client.get(f"/transactions/{h}")
    assert resp.status == 404


async def test_get_receipt_not_found(proxy_client):
    h = "0x" + "ff" * 32
    resp = await proxy_client.get(f"/transactions/{h}/receipt")
    assert resp.status == 404


async def test_get_transaction_bad_hash_400(proxy_client):
    resp = await proxy_client.get("/transactions/0xnope")
    assert resp.status == 400
