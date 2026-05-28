"""Tests for computed-read handlers + shared CallRequest conversion."""

import pytest

from exec_rest_api.handlers.computed import call_request_to_rpc


def test_minimal_call_request():
    body = {"to": "0x" + "ab" * 20}
    rpc, at = call_request_to_rpc(body)
    assert rpc == {"to": "0x" + "ab" * 20}
    assert at == "latest"


def test_at_default_latest_can_be_overridden_with_block_number():
    body = {"to": "0x" + "ab" * 20, "at": "100"}
    rpc, at = call_request_to_rpc(body)
    assert at == "0x64"


def test_at_tag():
    body = {"to": "0x" + "ab" * 20, "at": "safe"}
    _, at = call_request_to_rpc(body)
    assert at == "safe"


def test_at_block_hash():
    h = "0x" + "ab" * 32
    body = {"to": "0x" + "ab" * 20, "at": h}
    _, at = call_request_to_rpc(body)
    assert at == h


def test_numeric_fields_converted_to_hex():
    body = {
        "from": "0x" + "11" * 20,
        "to": "0x" + "22" * 20,
        "gas": 21000,
        "gasPrice": "1000000000",
        "value": "5000000000000000000",
        "nonce": 7,
        "chainId": 1,
        "data": "0xdeadbeef",
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["from"] == "0x" + "11" * 20
    assert rpc["to"] == "0x" + "22" * 20
    assert rpc["gas"] == "0x5208"
    assert rpc["gasPrice"] == "0x3b9aca00"
    assert rpc["value"] == "0x4563918244f40000"
    assert rpc["nonce"] == "0x7"
    assert rpc["chainId"] == "0x1"
    assert rpc["data"] == "0xdeadbeef"


def test_eip1559_fields():
    body = {
        "maxFeePerGas": "2000000000",
        "maxPriorityFeePerGas": "1000000000",
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["maxFeePerGas"] == "0x77359400"
    assert rpc["maxPriorityFeePerGas"] == "0x3b9aca00"


def test_access_list_converted():
    body = {
        "accessList": [
            {
                "address": "0x" + "ab" * 20,
                "storageKeys": ["0x" + "11" * 32, "0x" + "22" * 32],
            }
        ]
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["accessList"] == [
        {
            "address": "0x" + "ab" * 20,
            "storageKeys": ["0x" + "11" * 32, "0x" + "22" * 32],
        }
    ]


def test_state_overrides_passthrough_with_numeric_fields_converted():
    body = {
        "stateOverrides": {
            "0x" + "11" * 20: {
                "balance": "1000000000000000000",
                "nonce": 5,
                "code": "0x60",
            }
        }
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["stateOverrides"]["0x" + "11" * 20] == {
        "balance": "0xde0b6b3a7640000",
        "nonce": "0x5",
        "code": "0x60",
    }


def test_block_overrides_numeric_fields_converted():
    body = {
        "blockOverrides": {
            "number": 18234567,
            "timestamp": 1700000000,
            "baseFeePerGas": "1000000000",
        }
    }
    rpc, _ = call_request_to_rpc(body)
    assert rpc["blockOverrides"] == {
        "number": "0x1163cc7",
        "timestamp": "0x6553f100",
        "baseFeePerGas": "0x3b9aca00",
    }


def test_invalid_address_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc({"to": "not-an-address"})


def test_invalid_numeric_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc({"to": "0x" + "ab" * 20, "gas": "not a number"})


def test_invalid_at_raises():
    with pytest.raises(ValueError):
        call_request_to_rpc({"to": "0x" + "ab" * 20, "at": "garbage"})
