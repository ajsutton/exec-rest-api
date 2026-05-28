"""GET /metrics — Prometheus text format exporter.

Only registered when `config.metrics_enabled` is true. The handler itself does
nothing beyond reading the registry's current state.
"""

from __future__ import annotations

from aiohttp import web

from exec_rest_api.metrics import PROMETHEUS_CONTENT_TYPE, Metrics
from exec_rest_api.server import add_get


async def metrics(request: web.Request) -> web.Response:
    registry: Metrics = request.app["metrics"]
    return web.Response(
        body=registry.render().encode("utf-8"),
        headers={"Content-Type": PROMETHEUS_CONTENT_TYPE},
    )


def register_routes(app: web.Application) -> None:
    config = app["config"]
    if not getattr(config, "metrics_enabled", True):
        return
    add_get(app, "/metrics", metrics)
