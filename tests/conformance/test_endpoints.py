"""Conformance: real responses validate against the OpenAPI 3.1 schema.

For each implemented GET endpoint, we hit the proxy (over anvil) and validate
the response body against the schema declared in the OpenAPI YAML. This catches
drift between spec and implementation.

POST endpoints and SSE streams arrive in later plans.
"""

from __future__ import annotations

PRE_FUNDED = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


async def test_chain_composite(proxy_client, make_validator):
    resp = await proxy_client.get("/chain")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/ChainInfo").validate(body)


async def test_gas_price(proxy_client, make_validator):
    resp = await proxy_client.get("/gas/price")
    assert resp.status == 200
    body = await resp.json()
    # Inline schema: { wei: Wei }
    validator = make_validator("#/components/schemas/Wei")
    assert isinstance(body, dict) and "wei" in body
    validator.validate(body["wei"])


async def test_gas_fee_history(proxy_client, make_validator):
    resp = await proxy_client.get("/gas/fee-history?blockCount=2&newest=latest")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/FeeHistory").validate(body)


async def test_block_full(proxy_client, make_validator):
    resp = await proxy_client.get("/blocks/0")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/Block").validate(body)


async def test_block_header(proxy_client, make_validator):
    resp = await proxy_client.get("/blocks/0/header")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/BlockHeader").validate(body)


async def test_block_receipts(proxy_client, make_validator):
    resp = await proxy_client.get("/blocks/0/receipts")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    receipt_validator = make_validator("#/components/schemas/Receipt")
    for r in body:
        receipt_validator.validate(r)


async def test_account_summary(proxy_client, make_validator):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/AccountSummary").validate(body)


async def test_account_proof(proxy_client, make_validator):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/proof")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/AccountProof").validate(body)


async def test_account_transaction_template(proxy_client, make_validator):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/transaction-template")
    assert resp.status == 200
    body = await resp.json()
    make_validator("#/components/schemas/TransactionTemplate").validate(body)


async def test_logs_empty_array(proxy_client, make_validator):
    resp = await proxy_client.get("/logs?fromBlock=0&toBlock=latest")
    assert resp.status == 200
    body = await resp.json()
    assert isinstance(body, list)
    log_validator = make_validator("#/components/schemas/Log")
    for log in body:
        log_validator.validate(log)


async def test_not_found_is_problem(proxy_client, make_validator):
    resp = await proxy_client.get("/blocks/99999999")
    assert resp.status == 404
    assert resp.content_type == "application/problem+json"
    body = await resp.json()
    make_validator("#/components/schemas/Problem").validate(body)


async def test_invalid_request_is_problem(proxy_client, make_validator):
    resp = await proxy_client.get("/blocks/NOT_A_BLOCK")
    assert resp.status == 400
    assert resp.content_type == "application/problem+json"
    body = await resp.json()
    make_validator("#/components/schemas/Problem").validate(body)


async def test_chain_reorged_is_problem(proxy_client, make_validator):
    """A malformed cursor surfaces as 400; a stale boundary block as 409.

    Without contract activity we can't easily trigger a real reorg, so we
    just verify the 400 path produces a Problem. The 409 path is unit-tested.
    """
    resp = await proxy_client.get("/logs?cursor=tampered!!!")
    assert resp.status == 400
    assert resp.content_type == "application/problem+json"
    body = await resp.json()
    make_validator("#/components/schemas/Problem").validate(body)
