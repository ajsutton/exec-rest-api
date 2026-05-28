"""HTTP Accept-header parsing for content negotiation.

Two representations are supported in v1:
- `application/json` (default)
- `application/vnd.ethereum.rlp` (raw RLP bytes on selected GETs)

`select_representation` returns the chosen media type, or `None` if none of the
caller-supported types matches. Caller emits a 406 Problem on `None`.
"""

from __future__ import annotations

from typing import Final

CONTENT_TYPE_JSON: Final[str] = "application/json"
CONTENT_TYPE_RLP: Final[str] = "application/vnd.ethereum.rlp"


def select_representation(accept_header: str | None, supported: list[str]) -> str | None:
    """Pick the best supported representation for the client's Accept header.

    `supported` is in server preference order — used as a tiebreaker for `*/*`
    and equal q-values.
    """
    if not accept_header or not accept_header.strip():
        return supported[0]
    candidates = _parse_accept(accept_header)
    if not candidates:
        return supported[0]
    # Sort: highest q first, then preserve client's relative order.
    candidates.sort(key=lambda c: -c[1])
    # For each candidate (already in q-desc order), find first server type that matches.
    for media_type, q in candidates:
        if q <= 0.0:
            continue
        match = _match(media_type, supported)
        if match is not None:
            return match
    return None


def _parse_accept(header: str) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for raw in header.split(","):
        piece = raw.strip()
        if not piece:
            continue
        parts = [p.strip() for p in piece.split(";")]
        media_type = parts[0].lower()
        q = 1.0
        for param in parts[1:]:
            if "=" not in param:
                continue
            key, _, val = param.partition("=")
            if key.strip().lower() == "q":
                try:
                    q = float(val.strip())
                except ValueError:
                    q = 1.0
        out.append((media_type, q))
    return out


def _match(client_type: str, supported: list[str]) -> str | None:
    if client_type == "*/*":
        return supported[0]
    if client_type.endswith("/*"):
        prefix = client_type[:-2] + "/"
        for s in supported:
            if s.startswith(prefix):
                return s
        return None
    for s in supported:
        if s == client_type:
            return s
    return None
