"""End-to-end /accounts/* tests against anvil.

Anvil's default mnemonic gives ten pre-funded accounts each holding 10000 ETH.
The first is `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`.
"""


PRE_FUNDED = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
EMPTY_EOA = "0x000000000000000000000000000000000000dEaD"


async def test_balance_pre_funded(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/balance")
    assert resp.status == 200
    body = await resp.json()
    # 10_000 ETH = 10^22 wei
    assert int(body["wei"]) == 10_000 * 10**18


async def test_balance_empty(proxy_client):
    resp = await proxy_client.get(f"/accounts/{EMPTY_EOA}/balance")
    assert resp.status == 200
    body = await resp.json()
    assert body["wei"] == "0"


async def test_nonce_pre_funded(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/nonce")
    assert resp.status == 200
    body = await resp.json()
    assert body["nonce"] == 0


async def test_code_eoa_is_empty(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/code")
    assert resp.status == 200
    assert await resp.json() == {"code": "0x"}


async def test_storage_slot_zero(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/storage/0")
    assert resp.status == 200
    body = await resp.json()
    # Empty storage slot
    assert body["value"] == "0x" + "00" * 32


async def test_account_summary_eoa(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}")
    assert resp.status == 200
    body = await resp.json()
    assert body["address"] == PRE_FUNDED.lower()
    assert int(body["balance"]) > 0
    assert body["nonce"] == 0
    assert body["hasCode"] is False
    assert body["delegatedTo"] is None


async def test_account_proof(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/proof")
    assert resp.status == 200
    body = await resp.json()
    assert body["address"] == PRE_FUNDED.lower()
    assert int(body["balance"]) > 0
    assert isinstance(body["nonce"], int)
    assert isinstance(body["accountProof"], list)
    assert body["storageProof"] == []


async def test_account_proof_with_slots(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/proof?slots=0x0,0x1")
    assert resp.status == 200
    body = await resp.json()
    assert len(body["storageProof"]) == 2


async def test_transaction_template(proxy_client):
    resp = await proxy_client.get(f"/accounts/{PRE_FUNDED}/transaction-template")
    assert resp.status == 200
    body = await resp.json()
    assert body["nonce"] == 0
    assert body["chainId"] == 31337


async def test_bad_address_400(proxy_client):
    resp = await proxy_client.get("/accounts/0xnope/balance")
    assert resp.status == 400
