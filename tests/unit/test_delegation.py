"""Tests for EIP-7702 delegation detection."""

import pytest

from exec_rest_api.delegation import DelegationError, detect_delegate


def test_no_code_returns_none():
    assert detect_delegate("0x") is None


def test_ordinary_contract_returns_none():
    # 24-byte run-of-the-mill bytecode (random)
    code = "0x" + "60" * 24
    assert detect_delegate(code) is None


def test_eip7702_delegation_returns_address():
    # 0xef0100 prefix (3 bytes) + 20-byte delegate = 23 bytes total
    delegate = "1234567890abcdef1234567890abcdef12345678"
    code = "0xef0100" + delegate
    assert detect_delegate(code) == "0x" + delegate


def test_eip7702_delegation_mixed_case_lowercased():
    delegate = "ABCDEF0123456789abcdef0123456789ABCDEF01"
    code = "0xEF0100" + delegate
    assert detect_delegate(code) == "0x" + delegate.lower()


def test_22_bytes_with_ef0100_prefix_returns_none():
    # 0xef0100 + 19 bytes — wrong length
    code = "0xef0100" + "11" * 19
    assert detect_delegate(code) is None


def test_24_bytes_with_ef0100_prefix_returns_none():
    # 0xef0100 + 21 bytes — wrong length
    code = "0xef0100" + "11" * 21
    assert detect_delegate(code) is None


def test_23_bytes_without_ef0100_prefix_returns_none():
    code = "0x" + "ab" * 23
    assert detect_delegate(code) is None


def test_23_bytes_with_almost_prefix_returns_none():
    # 0xef0101 — close, but not the magic prefix
    code = "0xef0101" + "11" * 20
    assert detect_delegate(code) is None


def test_empty_string_rejected():
    with pytest.raises(DelegationError):
        detect_delegate("")


def test_missing_0x_rejected():
    with pytest.raises(DelegationError):
        detect_delegate("ef010011" + "11" * 19)


def test_odd_length_hex_rejected():
    with pytest.raises(DelegationError):
        detect_delegate("0xabc")


def test_non_hex_rejected():
    with pytest.raises(DelegationError):
        detect_delegate("0xzz")
