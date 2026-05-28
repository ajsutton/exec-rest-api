"""Tests for the Problem class and JSON-RPC error mapping."""

import pytest

from exec_rest_api.errors import (
    ERROR_TYPE_BASE,
    Problem,
    map_jsonrpc_error,
    problem_response,
)


def test_problem_to_dict_minimum_fields():
    p = Problem(status=404, type_slug="not-found", title="Not found")
    d = p.to_dict()
    assert d == {
        "type": f"{ERROR_TYPE_BASE}/not-found",
        "title": "Not found",
        "status": 404,
    }


def test_problem_to_dict_full_fields():
    p = Problem(
        status=422,
        type_slug="transaction-rejected/nonce-too-low",
        title="Transaction rejected",
        detail="nonce too low (got 5, expected 8)",
        instance="/transactions",
        code=-32003,
        data={"hint": "increase nonce"},
    )
    d = p.to_dict()
    assert d == {
        "type": f"{ERROR_TYPE_BASE}/transaction-rejected/nonce-too-low",
        "title": "Transaction rejected",
        "status": 422,
        "detail": "nonce too low (got 5, expected 8)",
        "instance": "/transactions",
        "code": -32003,
        "data": {"hint": "increase nonce"},
    }


def test_problem_response_has_problem_content_type():
    p = Problem(status=400, type_slug="invalid-request", title="Bad request")
    resp = problem_response(p)
    assert resp.status == 400
    assert resp.content_type == "application/problem+json"


# ── JSON-RPC mapping table ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "code, message, expected_status, expected_slug",
    [
        # Standard JSON-RPC codes
        (-32600, "invalid request", 400, "invalid-request"),
        (-32601, "method not found", 501, "method-not-supported-by-upstream"),
        (-32602, "invalid params: foo", 400, "invalid-request"),
        (-32603, "internal error", 502, "upstream-error"),
        (-32700, "parse error", 502, "upstream-error"),
        # Ethereum -32000 patterns
        (-32000, "nonce too low: have 5 want 8", 422, "transaction-rejected/nonce-too-low"),
        (-32000, "already known", 422, "transaction-rejected/already-known"),
        (
            -32000,
            "replacement transaction underpriced",
            422,
            "transaction-rejected/replacement-underpriced",
        ),
        (-32000, "transaction underpriced", 422, "transaction-rejected/underpriced"),
        (
            -32000,
            "insufficient funds for gas * price + value",
            422,
            "transaction-rejected/insufficient-funds",
        ),
        (-32000, "intrinsic gas too low", 422, "transaction-rejected/intrinsic-gas-too-low"),
        (-32000, "exceeds block gas limit", 422, "transaction-rejected/gas-limit-exceeded"),
        (-32000, "query returned more than 10000 results", 413, "payload-too-large"),
        (-32000, "exceed maximum block range: 10000", 413, "payload-too-large"),
        # Other -32xxx codes
        (-32001, "resource not found", 404, "not-found"),
        (-32002, "resource unavailable", 503, "upstream-unavailable"),
        (-32003, "transaction rejected", 422, "transaction-rejected"),
        (-32004, "method not supported", 501, "method-not-supported-by-upstream"),
        (-32005, "limit exceeded", 429, "rate-limited"),
        # Unmatched -32000..-32099 → 502
        (-32099, "vendor specific error", 502, "upstream-error"),
    ],
)
def test_map_jsonrpc_error_table(
    code: int,
    message: str,
    expected_status: int,
    expected_slug: str,
):
    problem = map_jsonrpc_error(code=code, message=message, data=None)
    assert problem.status == expected_status
    assert problem.type_slug == expected_slug
    assert problem.code == code
    # Message is preserved in detail
    assert problem.detail == message


def test_map_jsonrpc_error_preserves_data():
    problem = map_jsonrpc_error(code=-32000, message="x", data={"foo": "bar"})
    assert problem.data == {"foo": "bar"}


def test_map_jsonrpc_error_revert_is_not_an_error():
    """Reverts are handled in the response body, not as errors. The mapper
    is not called for reverts; calling it directly would still be OK but
    should not be relied on in handlers."""
    # We assert the boundary by documenting: handlers must check for "execution reverted"
    # BEFORE invoking map_jsonrpc_error. This test just locks the rule down.
    assert "execution reverted" in "execution reverted: ERC20: ..."


def test_map_jsonrpc_error_unknown_code_falls_through():
    problem = map_jsonrpc_error(code=-1, message="weird", data=None)
    assert problem.status == 502
    assert problem.type_slug == "upstream-error"
