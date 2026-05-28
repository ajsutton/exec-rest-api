"""Tests for encoding conversions between JSON-RPC and REST shapes."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from exec_rest_api.encoding import (
    EncodingError,
    decimal_to_hex,
    hex_to_int,
    map_address_lowercase,
    parse_input_int,
    parse_input_wei,
    rest_status_from_rpc,
    rest_tx_type_from_rpc,
    rpc_status_from_rest,
    rpc_tx_type_from_rest,
    wei_from_rpc,
    wei_to_rpc,
)


# ── hex ↔ int ──────────────────────────────────────────────────────────────

def test_hex_to_int_zero():
    assert hex_to_int("0x0") == 0


def test_hex_to_int_small():
    assert hex_to_int("0xff") == 255


def test_hex_to_int_large():
    # NOTE: 18234567 in hex is 0x1163cc7 (fixed from the plan typo)
    assert hex_to_int("0x1163cc7") == 18234567


def test_hex_to_int_mixed_case():
    assert hex_to_int("0xAbCd") == 0xabcd


def test_hex_to_int_no_prefix_rejected():
    with pytest.raises(EncodingError):
        hex_to_int("ff")


def test_hex_to_int_negative_rejected():
    with pytest.raises(EncodingError):
        hex_to_int("-0x1")


def test_hex_to_int_empty_after_prefix_rejected():
    with pytest.raises(EncodingError):
        hex_to_int("0x")


def test_decimal_to_hex_zero():
    assert decimal_to_hex(0) == "0x0"


def test_decimal_to_hex_round_trip():
    for n in [0, 1, 15, 16, 255, 256, 65535, 18234567, 10**18]:
        assert hex_to_int(decimal_to_hex(n)) == n


# ── wei ────────────────────────────────────────────────────────────────────

def test_wei_from_rpc_zero():
    assert wei_from_rpc("0x0") == "0"


def test_wei_from_rpc_one_ether():
    # 1 ETH = 10^18 wei = 0xde0b6b3a7640000
    assert wei_from_rpc("0xde0b6b3a7640000") == "1000000000000000000"


def test_wei_from_rpc_large():
    # Beyond 2^53 — must be decimal string, never JSON number
    expected = "12345678901234567890"
    rpc = hex(int(expected))
    assert wei_from_rpc(rpc) == expected


def test_wei_to_rpc_zero():
    assert wei_to_rpc("0") == "0x0"


def test_wei_to_rpc_int_accepted():
    assert wei_to_rpc(1000000000000000000) == "0xde0b6b3a7640000"


def test_wei_to_rpc_string_accepted():
    assert wei_to_rpc("1000000000000000000") == "0xde0b6b3a7640000"


def test_wei_to_rpc_negative_rejected():
    with pytest.raises(EncodingError):
        wei_to_rpc("-1")


def test_wei_to_rpc_garbage_rejected():
    with pytest.raises(EncodingError):
        wei_to_rpc("not a number")


# ── input lenience ─────────────────────────────────────────────────────────

def test_parse_input_int_from_int():
    assert parse_input_int(42) == 42


def test_parse_input_int_from_decimal_string():
    assert parse_input_int("42") == 42


def test_parse_input_int_from_hex_string_rejected():
    # Numbers in REST input are decimal-only; hex on input is not lenience we offer.
    with pytest.raises(EncodingError):
        parse_input_int("0x2a")


def test_parse_input_int_from_bool_rejected():
    with pytest.raises(EncodingError):
        parse_input_int(True)


def test_parse_input_wei_from_int():
    assert parse_input_wei(1_000_000_000_000_000_000) == 10**18


def test_parse_input_wei_from_string():
    assert parse_input_wei("1000000000000000000") == 10**18


# ── status enum ────────────────────────────────────────────────────────────

def test_rest_status_from_rpc_success():
    assert rest_status_from_rpc("0x1") == "success"


def test_rest_status_from_rpc_failure():
    assert rest_status_from_rpc("0x0") == "failed"


def test_rest_status_from_rpc_unknown_rejected():
    with pytest.raises(EncodingError):
        rest_status_from_rpc("0x2")


def test_rpc_status_from_rest_success():
    assert rpc_status_from_rest("success") == "0x1"


def test_rpc_status_from_rest_failed():
    assert rpc_status_from_rest("failed") == "0x0"


# ── transaction type enum ──────────────────────────────────────────────────

@pytest.mark.parametrize(
    "rpc, rest",
    [
        ("0x0", "legacy"),
        ("0x1", "access-list"),
        ("0x2", "dynamic-fee"),
        ("0x3", "blob"),
    ],
)
def test_tx_type_round_trip(rpc: str, rest: str):
    assert rest_tx_type_from_rpc(rpc) == rest
    assert rpc_tx_type_from_rest(rest) == rpc


def test_tx_type_unknown_rejected():
    with pytest.raises(EncodingError):
        rest_tx_type_from_rpc("0x9")


# ── address case ───────────────────────────────────────────────────────────

def test_map_address_lowercases():
    mixed = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
    assert map_address_lowercase(mixed) == "0xabcdef0123456789abcdef0123456789abcdef01"


def test_map_address_rejects_wrong_length():
    with pytest.raises(EncodingError):
        map_address_lowercase("0xabcd")


def test_map_address_rejects_no_prefix():
    with pytest.raises(EncodingError):
        map_address_lowercase("abcdef0123456789abcdef0123456789abcdef01")


def test_map_address_rejects_non_hex():
    with pytest.raises(EncodingError):
        map_address_lowercase("0x" + "g" * 40)


# ── hypothesis: hex round-trip on arbitrary ints ───────────────────────────

@given(st.integers(min_value=0, max_value=2**256 - 1))
def test_hex_int_round_trip(n: int):
    assert hex_to_int(decimal_to_hex(n)) == n


@given(st.integers(min_value=0, max_value=2**256 - 1))
def test_wei_round_trip(n: int):
    assert wei_from_rpc(wei_to_rpc(str(n))) == str(n)
