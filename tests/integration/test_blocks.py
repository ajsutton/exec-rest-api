"""End-to-end /blocks/* tests against anvil."""

import pytest


async def test_get_block_latest(proxy_client):
    resp = await proxy_client.get("/blocks/latest")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body["number"], int)
    assert body["hash"].startswith("0x")
    assert len(body["hash"]) == 66
    assert "transactions" in body


async def test_get_block_earliest_is_zero(proxy_client):
    resp = await proxy_client.get("/blocks/earliest")
    assert resp.status == 200
    body = await resp.json()
    assert body["number"] == 0


async def test_get_block_by_number_zero(proxy_client):
    resp = await proxy_client.get("/blocks/0")
    assert resp.status == 200
    body = await resp.json()
    assert body["number"] == 0


async def test_get_block_by_hash_round_trip(proxy_client):
    resp = await proxy_client.get("/blocks/0")
    body = await resp.json()
    block_hash = body["hash"]
    resp2 = await proxy_client.get(f"/blocks/{block_hash}")
    assert resp2.status == 200
    body2 = await resp2.json()
    assert body2["number"] == 0


async def test_get_block_not_found(proxy_client):
    resp = await proxy_client.get("/blocks/99999999")
    assert resp.status == 404


async def test_get_block_header(proxy_client):
    resp = await proxy_client.get("/blocks/0/header")
    assert resp.status == 200
    body = await resp.json()
    # Header has no transactions
    assert "transactions" not in body
    assert body["number"] == 0


async def test_get_block_transactions_empty(proxy_client):
    """Genesis block has no transactions."""
    resp = await proxy_client.get("/blocks/0/transactions")
    assert resp.status == 200
    body = await resp.json()
    assert body == []


async def test_get_block_transaction_count_zero(proxy_client):
    resp = await proxy_client.get("/blocks/0/transaction-count")
    assert resp.status == 200
    assert await resp.json() == {"count": 0}


async def test_get_block_receipts_genesis(proxy_client):
    resp = await proxy_client.get("/blocks/0/receipts")
    # anvil returns [] for an empty block's receipts
    assert resp.status == 200
    assert await resp.json() == []


async def test_get_block_bad_id_400(proxy_client):
    resp = await proxy_client.get("/blocks/NOT_A_BLOCK_ID")
    assert resp.status == 400


async def test_get_block_traces_genesis(proxy_client):
    resp = await proxy_client.get("/blocks/0/traces")
    # anvil supports trace_block; genesis has no traces
    if resp.status == 501:
        pytest.skip("anvil build does not support trace_block")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)


async def test_get_block_rlp_accept(proxy_client):
    resp = await proxy_client.get(
        "/blocks/0", headers={"Accept": "application/vnd.ethereum.rlp"}
    )
    if resp.status == 501:
        pytest.skip("anvil build does not support debug_getRawBlock")
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.ethereum.rlp"
    body = await resp.read()
    assert len(body) > 0


async def test_get_block_unsupported_accept_406(proxy_client):
    resp = await proxy_client.get("/blocks/0", headers={"Accept": "text/html"})
    assert resp.status == 406
