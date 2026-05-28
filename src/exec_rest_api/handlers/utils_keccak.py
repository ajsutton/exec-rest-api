"""POST /utils/keccak256 — forwards to upstream `web3_sha3`.

The proxy does no Keccak-256 itself. This endpoint exists so that clients which
don't want to bundle their own implementation can still hash bytes.
"""

from __future__ import annotations

import re

from aiohttp import web

from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.upstream import UpstreamClient

_HEX_BYTES_RE = re.compile(r"^0x([0-9a-fA-F]{2})*$")


def _bad_request(path: str, detail: str) -> web.Response:
    return problem_response(
        Problem(
            status=400,
            type_slug="invalid-request",
            title="Invalid request",
            detail=detail,
            instance=path,
        )
    )


async def keccak256(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return _bad_request(request.path, "request body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request(request.path, "request body must be a JSON object")
    data = body.get("data")
    if not isinstance(data, str) or not _HEX_BYTES_RE.fullmatch(data):
        return _bad_request(
            request.path, "field `data` must be a 0x-prefixed hex byte string"
        )
    upstream: UpstreamClient = request.app["upstream"]
    digest = await upstream.call("web3_sha3", [data])
    if not isinstance(digest, str):
        return problem_response(
            Problem(
                status=502,
                type_slug="upstream-error",
                title="Upstream error",
                detail="web3_sha3 returned non-string result",
                instance=request.path,
            )
        )
    return web.json_response({"hash": digest.lower()})


def register_routes(app: web.Application) -> None:
    app.router.add_post("/utils/keccak256", keccak256)
    app.router.add_post("/utils/keccak256/", keccak256)
