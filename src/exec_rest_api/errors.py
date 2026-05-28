"""RFC 9457 Problem Details bodies + JSON-RPC error mapping.

`Problem` is the canonical error shape; every error response (4xx and 5xx) the
API emits is built from one of these. `map_jsonrpc_error` translates an upstream
JSON-RPC error object into a `Problem` per the table in implementation design §10.

Reverts are explicitly NOT handled here — they are successful responses with a
revert body, not errors. Handlers must check for "execution reverted" before
delegating to this mapper.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Final

from aiohttp import web

ERROR_TYPE_BASE: Final[str] = "https://errors.ethereum-rest"


@dataclass(frozen=True)
class Problem:
    """RFC 9457 Problem Details with Ethereum-specific extensions."""

    status: int
    type_slug: str  # appended to ERROR_TYPE_BASE
    title: str
    detail: str | None = None
    instance: str | None = None
    code: int | None = None
    data: Any = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "type": f"{ERROR_TYPE_BASE}/{self.type_slug}",
            "title": self.title,
            "status": self.status,
        }
        if self.detail is not None:
            out["detail"] = self.detail
        if self.instance is not None:
            out["instance"] = self.instance
        if self.code is not None:
            out["code"] = self.code
        if self.data is not None:
            out["data"] = self.data
        return out


def problem_response(problem: Problem) -> web.Response:
    """Construct an aiohttp Response carrying a Problem body."""
    body = json.dumps(problem.to_dict()).encode("utf-8")
    return web.Response(
        status=problem.status,
        body=body,
        content_type="application/problem+json",
    )


# ── JSON-RPC → Problem mapping ────────────────────────────────────────────

# Standard JSON-RPC 2.0 codes
_STANDARD_CODES: Final[dict[int, tuple[int, str, str]]] = {
    -32600: (400, "invalid-request", "Invalid request"),
    -32601: (501, "method-not-supported-by-upstream", "Method not supported by upstream"),
    -32602: (400, "invalid-request", "Invalid request"),
    -32603: (502, "upstream-error", "Upstream error"),
    -32700: (502, "upstream-error", "Upstream error"),
    -32001: (404, "not-found", "Not found"),
    -32002: (503, "upstream-unavailable", "Upstream unavailable"),
    -32003: (422, "transaction-rejected", "Transaction rejected"),
    -32004: (501, "method-not-supported-by-upstream", "Method not supported by upstream"),
    -32005: (429, "rate-limited", "Rate limited"),
}

# Message-pattern → (status, type_slug, title) for the -32000 family
_M32000_PATTERNS: Final[list[tuple[str, int, str, str]]] = [
    ("nonce too low", 422, "transaction-rejected/nonce-too-low", "Transaction rejected"),
    ("already known", 422, "transaction-rejected/already-known", "Transaction rejected"),
    (
        "replacement transaction underpriced",
        422,
        "transaction-rejected/replacement-underpriced",
        "Transaction rejected",
    ),
    ("transaction underpriced", 422, "transaction-rejected/underpriced", "Transaction rejected"),
    (
        "insufficient funds",
        422,
        "transaction-rejected/insufficient-funds",
        "Transaction rejected",
    ),
    (
        "intrinsic gas too low",
        422,
        "transaction-rejected/intrinsic-gas-too-low",
        "Transaction rejected",
    ),
    (
        "exceeds block gas limit",
        422,
        "transaction-rejected/gas-limit-exceeded",
        "Transaction rejected",
    ),
    ("query returned more than", 413, "payload-too-large", "Payload too large"),
    ("exceed maximum block range", 413, "payload-too-large", "Payload too large"),
]


def map_jsonrpc_error(*, code: int, message: str, data: Any) -> Problem:
    """Translate a JSON-RPC error into a Problem.

    Caller MUST NOT pass reverts here (-32000 with message containing
    "execution reverted"). Those are handled in the response body per API spec §5.3.
    """
    if code in _STANDARD_CODES:
        status, slug, title = _STANDARD_CODES[code]
        return Problem(
            status=status,
            type_slug=slug,
            title=title,
            detail=message,
            code=code,
            data=data,
        )
    if code == -32000:
        msg_lower = message.lower()
        for pattern, status, slug, title in _M32000_PATTERNS:
            if pattern in msg_lower:
                return Problem(
                    status=status,
                    type_slug=slug,
                    title=title,
                    detail=message,
                    code=code,
                    data=data,
                )
    # Default: -32000..-32099 vendor errors and anything unmatched
    return Problem(
        status=502,
        type_slug="upstream-error",
        title="Upstream error",
        detail=message,
        code=code,
        data=data,
    )
