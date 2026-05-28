"""Entrypoint for `python -m exec_rest_api` and the `exec-rest-api` console script."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys

import aiohttp
from aiohttp import web

from exec_rest_api import __version__
from exec_rest_api.config import Config, ConfigError, parse_config
from exec_rest_api.handlers import (
    accounts,
    blocks,
    chain,
    gas,
    health,
    logs,
    traces,
    transactions,
    utils_keccak,
)
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


def _setup_logging(level: str, format_: str | None) -> None:
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "error": logging.ERROR,
    }
    handler = logging.StreamHandler(sys.stderr)
    use_json = format_ == "json" or (format_ is None and not sys.stderr.isatty())
    if use_json:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level_map[level])


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "request_id", "method", "path", "status", "latency_ms",
            "upstream_method", "upstream_latency_ms", "listen", "upstream_http",
        ):
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _split_listen(listen: str) -> tuple[str, int]:
    host, _, port = listen.rpartition(":")
    if not host or not port:
        raise ConfigError(f"--listen must be host:port, got {listen!r}")
    try:
        port_int = int(port)
    except ValueError as e:
        raise ConfigError(f"--listen port must be numeric, got {port!r}") from e
    return host, port_int


async def _run(config: Config) -> None:
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=config.upstream_timeout_seconds)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        upstream = UpstreamClient(
            session=session,
            http_url=config.upstream_http,
            default_timeout_seconds=config.upstream_timeout_seconds,
        )
        app = create_app(config=config, upstream=upstream)
        health.register_routes(app)
        chain.register_routes(app)
        gas.register_routes(app)
        transactions.register_routes(app)
        blocks.register_routes(app)
        accounts.register_routes(app)
        logs.register_routes(app)
        traces.register_routes(app)
        utils_keccak.register_routes(app)

        host, port = _split_listen(config.listen)
        runner = web.AppRunner(app, access_log=None)  # we have our own access-log middleware
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()

        logging.getLogger("exec_rest_api").info(
            "listening on http://%s (upstream %s)",
            config.listen,
            config.upstream_http,
            extra={"listen": config.listen, "upstream_http": config.upstream_http},
        )

        # Run until SIGINT / SIGTERM
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):  # Windows
                loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
        await runner.cleanup()


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if "--version" in argv:
        print(f"exec-rest-api {__version__}")
        return 0
    try:
        config = parse_config(argv=argv, env=dict(os.environ))
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    _setup_logging(level=config.log_level, format_=config.log_format)
    try:
        asyncio.run(_run(config))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
