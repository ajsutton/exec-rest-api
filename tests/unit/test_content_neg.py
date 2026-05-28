"""Tests for the Accept-header parser."""

import pytest

from exec_rest_api.content_neg import (
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_RLP,
    select_representation,
)


def test_no_accept_header_defaults_to_first_supported():
    assert select_representation(None, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]) == CONTENT_TYPE_JSON


def test_empty_accept_header_defaults():
    assert select_representation("", [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]) == CONTENT_TYPE_JSON


def test_wildcard_returns_first_supported():
    assert select_representation("*/*", [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]) == CONTENT_TYPE_JSON


def test_exact_json_match():
    assert (
        select_representation(CONTENT_TYPE_JSON, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_JSON
    )


def test_exact_rlp_match():
    assert (
        select_representation(CONTENT_TYPE_RLP, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_RLP
    )


def test_rlp_when_only_json_supported_returns_none():
    assert select_representation(CONTENT_TYPE_RLP, [CONTENT_TYPE_JSON]) is None


def test_q_values_respected():
    header = f"{CONTENT_TYPE_RLP};q=0.5, {CONTENT_TYPE_JSON};q=0.9"
    assert (
        select_representation(header, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_JSON
    )


def test_q_zero_excludes_type():
    header = f"{CONTENT_TYPE_JSON};q=0, {CONTENT_TYPE_RLP}"
    assert (
        select_representation(header, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_RLP
    )


def test_unsupported_type_only_returns_none():
    assert select_representation("text/html", [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP]) is None


def test_partial_wildcard_application_star_matches_first_application_supported():
    # `application/*` should match either supported representation; pick first.
    assert (
        select_representation("application/*", [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_JSON
    )


def test_whitespace_and_parameters_tolerated():
    header = f"  {CONTENT_TYPE_RLP} ;  q = 0.8 ,  {CONTENT_TYPE_JSON} ; q=0.2 "
    assert (
        select_representation(header, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_RLP
    )


def test_default_q_is_1():
    # `application/json` (no q) beats `*/*;q=0.5`
    header = f"{CONTENT_TYPE_JSON}, */*;q=0.5"
    assert (
        select_representation(header, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_JSON
    )


def test_malformed_q_treated_as_one():
    # Garbled q-value: ignore the directive, treat as default q=1
    header = f"{CONTENT_TYPE_RLP};q=zzz"
    assert (
        select_representation(header, [CONTENT_TYPE_JSON, CONTENT_TYPE_RLP])
        == CONTENT_TYPE_RLP
    )
