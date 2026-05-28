"""Tests for the opaque pagination cursor."""

import pytest

from exec_rest_api.cursor import Cursor, CursorError, decode_cursor, encode_cursor


def _sample() -> Cursor:
    return Cursor(
        next_from_block=18_500_000,
        last_log_index=42,
        to_block=18_999_999,
        boundary_block_hash="0x" + "ab" * 32,
        filter_={"address": "0x" + "cd" * 20, "topics": ["0x" + "ee" * 32]},
    )


def test_round_trip():
    c = _sample()
    encoded = encode_cursor(c)
    assert isinstance(encoded, str)
    assert "=" not in encoded  # base64url, no padding
    decoded = decode_cursor(encoded)
    assert decoded == c


def test_encoding_is_url_safe():
    encoded = encode_cursor(_sample())
    # base64url alphabet: A-Z a-z 0-9 - _
    assert all(c.isalnum() or c in "-_" for c in encoded)


def test_decode_garbage_raises():
    with pytest.raises(CursorError):
        decode_cursor("not-base64url!!!!")


def test_decode_malformed_json_raises():
    import base64

    bad = base64.urlsafe_b64encode(b"not json{{").decode("ascii").rstrip("=")
    with pytest.raises(CursorError):
        decode_cursor(bad)


def test_decode_missing_fields_raises():
    import base64
    import json

    payload = json.dumps({"nextFromBlock": 1}).encode("ascii")
    bad = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    with pytest.raises(CursorError):
        decode_cursor(bad)


def test_decode_wrong_types_raises():
    import base64
    import json

    payload = json.dumps(
        {
            "nextFromBlock": "not an int",
            "lastLogIndex": 0,
            "toBlock": 100,
            "boundaryBlockHash": "0x" + "ab" * 32,
            "filter": {},
        }
    ).encode("ascii")
    bad = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    with pytest.raises(CursorError):
        decode_cursor(bad)


def test_decode_empty_string_raises():
    with pytest.raises(CursorError):
        decode_cursor("")


def test_filter_arbitrary_json_preserved():
    c = Cursor(
        next_from_block=0,
        last_log_index=-1,
        to_block=100,
        boundary_block_hash="0x" + "0" * 64,
        filter_={"any": ["nested", 1, None, True], "more": {"deeper": {}}},
    )
    assert decode_cursor(encode_cursor(c)) == c
