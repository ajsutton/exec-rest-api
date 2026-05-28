"""Tests for ABI revert decoding + revert detection helpers."""


from exec_rest_api.abi_revert import (
    decode_revert_data,
    is_out_of_gas,
    is_revert,
    revert_body,
)
from exec_rest_api.upstream import UpstreamJsonRpcError

# ── decode_revert_data ─────────────────────────────────────────────────────


def test_decode_returns_none_for_short_data():
    assert decode_revert_data("0x") == (None, None)
    assert decode_revert_data("0x00") == (None, None)
    assert decode_revert_data("0xdeadbeef") == (None, None)  # selector only


def test_decode_error_string():
    # Error(string) selector + offset(0x20) + len(0x1c) + "ERC20: transfer amount" padded
    # selector
    sel = "08c379a0"
    # ABI head: offset to string (0x20 = 32)
    offset = "0" * 62 + "20"
    # length: 22 chars = 0x16
    length = "0" * 62 + "16"
    # data: "ERC20: transfer amount" → hex
    text = "ERC20: transfer amount"
    text_hex = text.encode("utf-8").hex()
    # Pad to 32-byte multiple
    pad = "0" * (64 - len(text_hex) % 64) if len(text_hex) % 64 else ""
    data = "0x" + sel + offset + length + text_hex + pad
    reason, panic = decode_revert_data(data)
    assert reason == text
    assert panic is None


def test_decode_panic_uint():
    # Panic(uint256) selector + 32-byte uint
    sel = "4e487b71"
    code = "0" * 62 + "11"  # 0x11 = arithmetic overflow
    data = "0x" + sel + code
    reason, panic = decode_revert_data(data)
    assert reason is None
    assert panic == 0x11


def test_decode_unknown_selector():
    # Custom error: selector + arbitrary tail. Both fields None.
    data = "0xdeadbeef" + "00" * 32
    reason, panic = decode_revert_data(data)
    assert reason is None
    assert panic is None


def test_decode_malformed_error_string_returns_none():
    # Error(string) selector but garbled length
    sel = "08c379a0"
    offset = "0" * 62 + "20"
    # Length claims 1 GB — refuse
    length = "f" * 64
    data = "0x" + sel + offset + length + "00" * 4
    reason, panic = decode_revert_data(data)
    assert reason is None
    assert panic is None


def test_decode_non_hex_data_returns_none():
    assert decode_revert_data("not-hex") == (None, None)
    assert decode_revert_data("0xZZ") == (None, None)


# ── revert detection ───────────────────────────────────────────────────────


def test_is_revert_true_for_execution_reverted_message():
    err = UpstreamJsonRpcError(code=-32000, message="execution reverted")
    assert is_revert(err) is True


def test_is_revert_true_with_reason_in_message():
    err = UpstreamJsonRpcError(
        code=-32000, message="execution reverted: ERC20: insufficient balance"
    )
    assert is_revert(err) is True


def test_is_revert_false_for_other_codes():
    err = UpstreamJsonRpcError(code=-32602, message="execution reverted")
    assert is_revert(err) is False


def test_is_revert_false_for_unrelated_message():
    err = UpstreamJsonRpcError(code=-32000, message="nonce too low")
    assert is_revert(err) is False


def test_is_out_of_gas_patterns():
    for msg in (
        "out of gas",
        "gas required exceeds allowance",
        "intrinsic gas too low",  # NOT out-of-gas — that's tx rejection
    ):
        err = UpstreamJsonRpcError(code=-32000, message=msg)
        if "intrinsic" in msg:
            assert is_out_of_gas(err) is False
        else:
            assert is_out_of_gas(err) is True


# ── revert_body ────────────────────────────────────────────────────────────


def test_revert_body_with_reason():
    sel = "08c379a0"
    offset = "0" * 62 + "20"
    length = "0" * 62 + "05"
    text_hex = b"hello".hex() + "00" * (32 - 5)
    data = "0x" + sel + offset + length + text_hex
    err = UpstreamJsonRpcError(code=-32000, message="execution reverted", data=data)
    body = revert_body(err)
    assert body == {
        "reverted": True,
        "data": data,
        "reason": "hello",
        "panicCode": None,
    }


def test_revert_body_with_panic():
    sel = "4e487b71"
    code_hex = "0" * 62 + "12"  # divide-by-zero
    data = "0x" + sel + code_hex
    err = UpstreamJsonRpcError(code=-32000, message="execution reverted", data=data)
    body = revert_body(err)
    assert body == {
        "reverted": True,
        "data": data,
        "reason": None,
        "panicCode": 0x12,
    }


def test_revert_body_no_data():
    err = UpstreamJsonRpcError(code=-32000, message="execution reverted", data=None)
    body = revert_body(err)
    assert body == {
        "reverted": True,
        "data": "0x",
        "reason": None,
        "panicCode": None,
    }


def test_revert_body_out_of_gas():
    err = UpstreamJsonRpcError(code=-32000, message="out of gas", data=None)
    body = revert_body(err)
    assert body == {
        "reverted": True,
        "data": "0x",
        "reason": None,
        "panicCode": None,
        "outOfGas": True,
    }
