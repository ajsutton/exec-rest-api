"""End-to-end /gas/* tests against anvil."""



async def test_gas_price(proxy_client):
    resp = await proxy_client.get("/gas/price")
    assert resp.status == 200
    body = await resp.json()
    assert "wei" in body
    # wei is a decimal-string representation; must parse as a non-negative int
    assert int(body["wei"]) >= 0


async def test_priority_fee(proxy_client):
    resp = await proxy_client.get("/gas/priority-fee")
    assert resp.status == 200
    body = await resp.json()
    assert int(body["wei"]) >= 0


async def test_fee_history(proxy_client):
    resp = await proxy_client.get("/gas/fee-history?blockCount=2&newest=latest")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body["oldestBlock"], int)
    assert isinstance(body["baseFeePerGas"], list)
    assert isinstance(body["gasUsedRatio"], list)


async def test_fee_history_with_percentiles(proxy_client):
    resp = await proxy_client.get(
        "/gas/fee-history?blockCount=2&newest=latest&rewardPercentiles=10,50,90"
    )
    assert resp.status == 200
    body = await resp.json()
    if "reward" in body:
        # Three percentiles requested → each row has 3 entries
        for row in body["reward"]:
            assert len(row) == 3
