"""Tests for CLI/env configuration resolution."""

import pytest

from exec_rest_api.config import ConfigError, parse_config


def test_minimum_args():
    cfg = parse_config(argv=["--upstream-http", "http://localhost:8545"], env={})
    assert cfg.upstream_http == "http://localhost:8545"
    assert cfg.upstream_ws == "ws://localhost:8545"  # derived
    assert cfg.listen == "127.0.0.1:8080"
    assert cfg.upstream_timeout_seconds == 30.0
    assert cfg.default_page_size == 1000
    assert cfg.max_page_size == 10000
    assert cfg.sse_buffer_bytes == 65536
    assert cfg.sse_replay_window == 1024
    assert cfg.sse_heartbeat_seconds == 30
    assert cfg.ready_sync_lag == 10
    assert cfg.log_level == "info"
    assert cfg.metrics_enabled is True


def test_upstream_http_required():
    with pytest.raises(ConfigError):
        parse_config(argv=[], env={})


def test_upstream_https_derives_wss():
    cfg = parse_config(argv=["--upstream-http", "https://node.example.com:8545"], env={})
    assert cfg.upstream_ws == "wss://node.example.com:8545"


def test_env_var_falls_through_when_flag_missing():
    cfg = parse_config(
        argv=[],
        env={"EXEC_REST_API_UPSTREAM_HTTP": "http://node:8545"},
    )
    assert cfg.upstream_http == "http://node:8545"


def test_flag_overrides_env_var():
    cfg = parse_config(
        argv=["--upstream-http", "http://flag:8545"],
        env={"EXEC_REST_API_UPSTREAM_HTTP": "http://env:8545"},
    )
    assert cfg.upstream_http == "http://flag:8545"


def test_listen_override():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--listen", "0.0.0.0:9000"],
        env={},
    )
    assert cfg.listen == "0.0.0.0:9000"


def test_metrics_off():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--metrics", "off"],
        env={},
    )
    assert cfg.metrics_enabled is False


def test_metrics_on():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--metrics", "on"],
        env={},
    )
    assert cfg.metrics_enabled is True


def test_max_page_size_override():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--max-page-size", "5000"],
        env={},
    )
    assert cfg.max_page_size == 5000


def test_log_level_invalid_rejected():
    with pytest.raises(ConfigError):
        parse_config(
            argv=["--upstream-http", "http://x:8545", "--log-level", "spam"],
            env={},
        )
