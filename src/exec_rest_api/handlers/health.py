"""/health (liveness) and /health/ready (readiness with upstream check)."""

from __future__ import annotations

from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.encoding import hex_to_int
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.upstream import UpstreamClient, UpstreamError


async def health(request: web.Request) -> web.Response:
    """Liveness: server process is up. No upstream calls."""
    return web.json_response({"status": "ok"})


async def ready(request: web.Request) -> web.Response:
    """Readiness: upstream reachable AND sync lag within configured threshold."""
    upstream: UpstreamClient = request.app["upstream"]
    config: Config = request.app["config"]
    try:
        sync = await upstream.call("eth_syncing")
        block_hex = await upstream.call("eth_blockNumber")
    except UpstreamError as e:
        return problem_response(
            Problem(
                status=503,
                type_slug="upstream-unavailable",
                title="Upstream unavailable",
                detail=str(e),
                instance=request.path,
            )
        )
    block_number = hex_to_int(block_hex)
    if sync is False:
        return web.json_response(
            {
                "ready": True,
                "upstreamReachable": True,
                "syncing": False,
                "blockNumber": block_number,
            }
        )
    # sync is a dict
    highest = hex_to_int(sync["highestBlock"])
    current = hex_to_int(sync["currentBlock"])
    lag = highest - current
    if lag <= config.ready_sync_lag:
        return web.json_response(
            {
                "ready": True,
                "upstreamReachable": True,
                "syncing": True,
                "blockNumber": current,
            }
        )
    return problem_response(
        Problem(
            status=503,
            type_slug="upstream-unavailable",
            title="Upstream still syncing",
            detail=f"sync lag {lag} blocks exceeds threshold {config.ready_sync_lag}",
            instance=request.path,
        )
    )


def register_routes(app: web.Application) -> None:
    app.router.add_get("/health", health)
    app.router.add_get("/health/ready", ready)
