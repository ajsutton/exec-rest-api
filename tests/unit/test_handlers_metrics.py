"""Tests for the GET /metrics handler."""

from __future__ import annotations

from unittest.mock import AsyncMock

from exec_rest_api.config import Config
from exec_rest_api.handlers.metrics import register_routes
from exec_rest_api.metrics import PROMETHEUS_CONTENT_TYPE, Metrics
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


def _config(metrics_enabled: bool = True) -> Config:
    return Config(
        upstream_http="http://localhost:8545",
        upstream_ws="ws://localhost:8545",
        listen="127.0.0.1:8080",
        upstream_timeout_seconds=30.0,
        default_page_size=1000,
        max_page_size=10000,
        sse_buffer_bytes=65536,
        sse_replay_window=1024,
        sse_heartbeat_seconds=30,
        ready_sync_lag=10,
        log_level="info",
        log_format=None,
        metrics_enabled=metrics_enabled,
    )


async def _client(aiohttp_client, *, metrics: Metrics, enabled: bool = True):
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_config(metrics_enabled=enabled), upstream=mock)
    app["metrics"] = metrics
    register_routes(app)
    return await aiohttp_client(app)


async def test_metrics_endpoint_returns_prometheus_text(aiohttp_client):
    metrics = Metrics()
    metrics.inc_request(method="GET", path_template="/chain", status=200)
    client = await _client(aiohttp_client, metrics=metrics)
    resp = await client.get("/metrics")
    assert resp.status == 200
    assert resp.headers["Content-Type"].startswith("text/plain")
    body = await resp.text()
    assert "exec_rest_api_requests_total" in body


async def test_metrics_endpoint_content_type_header(aiohttp_client):
    metrics = Metrics()
    metrics.inc_request(method="GET", path_template="/x", status=200)
    client = await _client(aiohttp_client, metrics=metrics)
    resp = await client.get("/metrics")
    assert resp.headers["Content-Type"] == PROMETHEUS_CONTENT_TYPE


async def test_metrics_endpoint_empty_when_no_observations(aiohttp_client):
    """An empty registry must still return 200 — just no series."""
    metrics = Metrics()
    client = await _client(aiohttp_client, metrics=metrics)
    resp = await client.get("/metrics")
    assert resp.status == 200
    body = await resp.text()
    assert "exec_rest_api_" not in body


async def test_metrics_endpoint_not_registered_when_disabled(aiohttp_client):
    """When metrics_enabled is False, /metrics returns 404."""
    metrics = Metrics()
    client = await _client(aiohttp_client, metrics=metrics, enabled=False)
    resp = await client.get("/metrics")
    assert resp.status == 404
