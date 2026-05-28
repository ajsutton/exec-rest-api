"""aiohttp Application factory + middleware chain.

Three middlewares execute in order:
  1. request_id_middleware — generates or honors X-Request-ID; stores on request.
  2. access_log_middleware — logs one structured line per response.
  3. error_mapping_middleware — converts UpstreamError / UpstreamJsonRpcError /
     unhandled exceptions into Problem responses.

Handlers receive `request.app["upstream"]` for upstream calls, and
`request.app["config"]` for runtime parameters.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from aiohttp import web
from aiohttp.typedefs import Handler

from exec_rest_api.config import Config
from exec_rest_api.errors import Problem, map_jsonrpc_error, problem_response
from exec_rest_api.metrics import Metrics, current_request_upstream_methods
from exec_rest_api.upstream import UpstreamClient, UpstreamError, UpstreamJsonRpcError

logger = logging.getLogger("exec_rest_api")


@web.middleware
async def request_id_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request["request_id"] = rid
    response = await handler(request)
    response.headers["X-Request-ID"] = rid
    return response


@web.middleware
async def access_log_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    start = time.monotonic()
    status = 500
    try:
        response = await handler(request)
        status = response.status
        return response
    except web.HTTPException as e:
        status = e.status
        raise
    finally:
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        logger.info(
            "request",
            extra={
                "request_id": request.get("request_id"),
                "method": request.method,
                "path": request.path,
                "status": status,
                "latency_ms": elapsed_ms,
            },
        )


def _path_template(request: web.Request) -> str:
    """The matched route template (e.g. ``/blocks/{id}``) or ``__not_found__``."""
    match_info = request.match_info
    route = match_info.route if match_info is not None else None
    resource = route.resource if route is not None else None
    if resource is None:
        return "__not_found__"
    return resource.canonical


@web.middleware
async def metrics_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    metrics: Metrics | None = request.app.get("metrics")
    chain_head: Any = request.app.get("chain_head")
    token = current_request_upstream_methods.set([])
    start = time.monotonic()
    status = 500
    response: web.StreamResponse | None = None
    try:
        response = await handler(request)
        status = response.status
        return response
    except web.HTTPException as e:
        status = e.status
        raise
    finally:
        duration = time.monotonic() - start
        if metrics is not None:
            metrics.inc_request(
                method=request.method,
                path_template=_path_template(request),
                status=status,
            )
            metrics.observe_request_duration(duration)
        if response is not None:
            methods = current_request_upstream_methods.get()
            if methods:
                response.headers["X-Upstream-Method"] = ",".join(methods)
            if chain_head is not None:
                value = getattr(chain_head, "current", None)
                if value is not None:
                    response.headers["X-Block-Height"] = str(value)
        current_request_upstream_methods.reset(token)


@web.middleware
async def error_mapping_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    try:
        return await handler(request)
    except UpstreamJsonRpcError as e:
        problem = map_jsonrpc_error(code=e.code, message=e.message, data=e.data)
        return problem_response(_with_instance(problem, request.path))
    except UpstreamError as e:
        problem = Problem(
            status=502,
            type_slug="upstream-error",
            title="Upstream error",
            detail=str(e),
            instance=request.path,
        )
        return problem_response(problem)
    except web.HTTPException:
        # aiohttp's own HTTP exceptions (e.g. 404 from router) pass through.
        raise
    except Exception:
        logger.exception(
            "unhandled exception",
            extra={"request_id": request.get("request_id"), "path": request.path},
        )
        problem = Problem(
            status=500,
            type_slug="internal-error",
            title="Internal error",
            instance=request.path,
        )
        return problem_response(problem)


def _with_instance(problem: Problem, instance: str) -> Problem:
    """Return a copy of `problem` with `instance` set."""
    return Problem(
        status=problem.status,
        type_slug=problem.type_slug,
        title=problem.title,
        detail=problem.detail,
        instance=instance,
        code=problem.code,
        data=problem.data,
    )


def create_app(*, config: Config, upstream: UpstreamClient) -> web.Application:
    """Build the aiohttp Application with middleware and shared state."""
    app = web.Application(
        middlewares=[
            request_id_middleware,
            access_log_middleware,
            metrics_middleware,
            error_mapping_middleware,
        ],
    )
    app["config"] = config
    app["upstream"] = upstream
    return app


def add_get(app: web.Application, path: str, handler: Handler) -> None:
    """Register a GET handler that matches both with and without a trailing slash."""
    app.router.add_get(path, handler)
    if not path.endswith("/"):
        app.router.add_get(path + "/", handler)
