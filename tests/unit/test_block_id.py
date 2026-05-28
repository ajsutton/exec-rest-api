"""Tests for block identifier parsing."""

import pytest

from exec_rest_api.block_id import BlockId, BlockIdError, parse_block_id


def test_parse_tag_latest():
    assert parse_block_id("latest") == BlockId(tag="latest")


def test_parse_tag_safe():
    assert parse_block_id("safe") == BlockId(tag="safe")


def test_parse_tag_finalized():
    assert parse_block_id("finalized") == BlockId(tag="finalized")


def test_parse_tag_pending():
    assert parse_block_id("pending") == BlockId(tag="pending")


def test_parse_tag_earliest():
    assert parse_block_id("earliest") == BlockId(tag="earliest")


def test_parse_block_number_zero():
    assert parse_block_id("0") == BlockId(number=0)


def test_parse_block_number_decimal():
    assert parse_block_id("18234567") == BlockId(number=18234567)


def test_parse_block_hash_lowercase():
    h = "0x" + "ab" * 32
    assert parse_block_id(h) == BlockId(hash=h)


def test_parse_block_hash_mixed_case_lowercased():
    mixed = "0x" + "Ab" * 32
    assert parse_block_id(mixed) == BlockId(hash="0x" + "ab" * 32)


def test_reject_hex_block_number():
    with pytest.raises(BlockIdError):
        parse_block_id("0x4d2")


def test_reject_short_hex():
    with pytest.raises(BlockIdError):
        parse_block_id("0xabcd")


def test_reject_negative_number():
    with pytest.raises(BlockIdError):
        parse_block_id("-1")


def test_reject_empty():
    with pytest.raises(BlockIdError):
        parse_block_id("")


def test_reject_unknown_tag():
    with pytest.raises(BlockIdError):
        parse_block_id("LATEST")  # case-sensitive


def test_reject_garbage():
    with pytest.raises(BlockIdError):
        parse_block_id("not-a-block")


def test_block_id_to_rpc_param_tag():
    assert BlockId(tag="latest").to_rpc_param() == "latest"


def test_block_id_to_rpc_param_number_is_hex():
    # JSON-RPC takes block numbers as 0x-hex
    assert BlockId(number=0).to_rpc_param() == "0x0"
    assert BlockId(number=255).to_rpc_param() == "0xff"
    assert BlockId(number=18234567).to_rpc_param() == "0x1163cc7"


def test_block_id_to_rpc_param_hash():
    h = "0x" + "ab" * 32
    assert BlockId(hash=h).to_rpc_param() == h


def test_block_id_is_hash_or_number():
    h = "0x" + "ab" * 32
    assert BlockId(hash=h).is_hash() is True
    assert BlockId(number=0).is_number() is True
    assert BlockId(tag="latest").is_tag() is True
