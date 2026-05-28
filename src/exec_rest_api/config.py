"""CLI flag + environment variable configuration.

Every CLI flag has an env-var equivalent: ``--upstream-http`` is also
``EXEC_REST_API_UPSTREAM_HTTP``. Flags override env vars; env vars override
defaults. No configuration file in v1.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Final, TypeVar

ENV_PREFIX: Final[str] = "EXEC_REST_API_"
_LOG_LEVELS: Final[frozenset[str]] = frozenset({"debug", "info", "warn", "error"})

_T = TypeVar("_T")


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    upstream_http: str
    upstream_ws: str
    listen: str
    upstream_timeout_seconds: float
    default_page_size: int
    max_page_size: int
    sse_buffer_bytes: int
    sse_replay_window: int
    sse_heartbeat_seconds: int
    ready_sync_lag: int
    log_level: str
    log_format: str | None  # None means auto (TTY → human, otherwise JSON)
    metrics_enabled: bool


def _derive_ws_from_http(http_url: str) -> str:
    if http_url.startswith("https://"):
        return "wss://" + http_url[len("https://"):]
    if http_url.startswith("http://"):
        return "ws://" + http_url[len("http://"):]
    raise ConfigError(f"upstream-http URL must be http(s)://, got {http_url!r}")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="exec-rest-api",
        description="REST proxy in front of an Ethereum execution client.",
    )
    p.add_argument("--upstream-http", help="JSON-RPC HTTP endpoint URL")
    p.add_argument("--upstream-ws", help="JSON-RPC WS endpoint (default: derived from http URL)")
    p.add_argument("--listen", help="Listen address (default 127.0.0.1:8080)")
    p.add_argument("--upstream-timeout", type=float, help="Per-request timeout (s)")
    p.add_argument(
        "--default-page-size", type=int, help="Default items per page for /logs, /traces"
    )
    p.add_argument("--max-page-size", type=int, help="Max items per page for /logs, /traces")
    p.add_argument("--sse-buffer-bytes", type=int, help="SSE backpressure threshold (bytes)")
    p.add_argument(
        "--sse-replay-window", type=int, help="Max blocks replayable on SSE reconnect"
    )
    p.add_argument("--sse-heartbeat-seconds", type=int, help="SSE heartbeat interval (s)")
    p.add_argument("--ready-sync-lag", type=int, help="Max blocks behind to report ready")
    p.add_argument("--log-format", choices=["human", "json"], help="Log format")
    p.add_argument("--log-level", choices=sorted(_LOG_LEVELS), help="Log level")
    p.add_argument("--metrics", choices=["on", "off"], help="Enable /metrics endpoint")
    p.add_argument("--version", action="store_true", help="Print version and exit")
    return p


def _env_get(env: dict[str, str], env_name: str) -> str | None:
    """Return the env-var value for *env_name* (without prefix), or None."""
    return env.get(ENV_PREFIX + env_name)


def _str_field(
    flag: str | None,
    env: dict[str, str],
    env_name: str,
    default: str,
) -> str:
    if flag is not None:
        return flag
    raw = _env_get(env, env_name)
    if raw is not None:
        return raw
    return default


def _opt_str_field(
    flag: str | None,
    env: dict[str, str],
    env_name: str,
    default: str | None,
) -> str | None:
    if flag is not None:
        return flag
    raw = _env_get(env, env_name)
    if raw is not None:
        return raw
    return default


def _int_field(
    flag: int | None,
    env: dict[str, str],
    env_name: str,
    default: int,
) -> int:
    if flag is not None:
        return flag
    raw = _env_get(env, env_name)
    if raw is not None:
        return int(raw)
    return default


def _float_field(
    flag: float | None,
    env: dict[str, str],
    env_name: str,
    default: float,
) -> float:
    if flag is not None:
        return flag
    raw = _env_get(env, env_name)
    if raw is not None:
        return float(raw)
    return default


def parse_config(*, argv: list[str], env: dict[str, str]) -> Config:
    parser = _build_parser()
    try:
        ns = parser.parse_args(argv)
    except SystemExit as exc:
        raise ConfigError(f"Invalid arguments (exit code {exc.code})") from exc

    upstream_http: str | None = _opt_str_field(
        ns.upstream_http, env, "UPSTREAM_HTTP", None
    )
    if not upstream_http:
        raise ConfigError("--upstream-http (or EXEC_REST_API_UPSTREAM_HTTP) is required")

    upstream_ws: str | None = _opt_str_field(ns.upstream_ws, env, "UPSTREAM_WS", None)
    if upstream_ws is None:
        upstream_ws = _derive_ws_from_http(upstream_http)

    log_level = _str_field(ns.log_level, env, "LOG_LEVEL", "info")
    if log_level not in _LOG_LEVELS:
        raise ConfigError(f"--log-level must be one of {sorted(_LOG_LEVELS)}, got {log_level!r}")

    metrics_raw = _str_field(ns.metrics, env, "METRICS", "on")
    metrics_enabled = metrics_raw == "on"

    return Config(
        upstream_http=upstream_http,
        upstream_ws=upstream_ws,
        listen=_str_field(ns.listen, env, "LISTEN", "127.0.0.1:8080"),
        upstream_timeout_seconds=_float_field(
            ns.upstream_timeout, env, "UPSTREAM_TIMEOUT", 30.0
        ),
        default_page_size=_int_field(ns.default_page_size, env, "DEFAULT_PAGE_SIZE", 1000),
        max_page_size=_int_field(ns.max_page_size, env, "MAX_PAGE_SIZE", 10000),
        sse_buffer_bytes=_int_field(ns.sse_buffer_bytes, env, "SSE_BUFFER_BYTES", 65536),
        sse_replay_window=_int_field(ns.sse_replay_window, env, "SSE_REPLAY_WINDOW", 1024),
        sse_heartbeat_seconds=_int_field(
            ns.sse_heartbeat_seconds, env, "SSE_HEARTBEAT_SECONDS", 30
        ),
        ready_sync_lag=_int_field(ns.ready_sync_lag, env, "READY_SYNC_LAG", 10),
        log_level=log_level,
        log_format=_opt_str_field(ns.log_format, env, "LOG_FORMAT", None),
        metrics_enabled=metrics_enabled,
    )
