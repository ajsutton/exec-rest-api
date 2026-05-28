"""Tests for the in-memory metrics registry + Prometheus text exporter."""

from __future__ import annotations

from exec_rest_api.metrics import Metrics


def test_counter_inc_zero_value_default():
    m = Metrics()
    # A counter that has never been incremented does not appear in the output
    # (no labels seen yet).
    out = m.render()
    assert "exec_rest_api_requests_total" not in out


def test_counter_inc_with_labels():
    m = Metrics()
    m.inc_request(method="GET", path_template="/chain", status=200)
    out = m.render()
    assert (
        'exec_rest_api_requests_total{method="GET",path_template="/chain",status="200"} 1'
        in out
    )


def test_counter_inc_increments():
    m = Metrics()
    for _ in range(3):
        m.inc_request(method="GET", path_template="/chain", status=200)
    out = m.render()
    assert (
        'exec_rest_api_requests_total{method="GET",path_template="/chain",status="200"} 3'
        in out
    )


def test_counter_separate_label_sets():
    m = Metrics()
    m.inc_request(method="GET", path_template="/chain", status=200)
    m.inc_request(method="GET", path_template="/chain", status=500)
    m.inc_request(method="POST", path_template="/call", status=200)
    out = m.render()
    assert (
        'exec_rest_api_requests_total{method="GET",path_template="/chain",status="200"} 1'
        in out
    )
    assert (
        'exec_rest_api_requests_total{method="GET",path_template="/chain",status="500"} 1'
        in out
    )
    assert (
        'exec_rest_api_requests_total{method="POST",path_template="/call",status="200"} 1'
        in out
    )


def test_label_value_escaping():
    """Backslash, newline, and double-quote must be escaped per Prometheus 0.0.4."""
    m = Metrics()
    m.inc_request(method="GET", path_template='a"b\\c\nd', status=200)
    out = m.render()
    assert 'path_template="a\\"b\\\\c\\nd"' in out


def test_histogram_observe_buckets():
    m = Metrics()
    # Observations: 1ms, 50ms, 300ms, 5s, 20s
    for v in (0.001, 0.050, 0.300, 5.0, 20.0):
        m.observe_request_duration(v)
    out = m.render()
    # Cumulative buckets per Prometheus convention
    assert 'exec_rest_api_request_duration_seconds_bucket{le="0.005"} 1' in out
    assert 'exec_rest_api_request_duration_seconds_bucket{le="0.05"} 2' in out
    assert 'exec_rest_api_request_duration_seconds_bucket{le="0.5"} 3' in out
    assert 'exec_rest_api_request_duration_seconds_bucket{le="10"} 4' in out
    assert 'exec_rest_api_request_duration_seconds_bucket{le="+Inf"} 5' in out
    assert "exec_rest_api_request_duration_seconds_count 5" in out
    # 0.001 + 0.05 + 0.3 + 5 + 20 = 25.351
    assert "exec_rest_api_request_duration_seconds_sum 25.351" in out


def test_upstream_histogram_separate_from_request_histogram():
    m = Metrics()
    m.observe_request_duration(0.5)
    m.observe_upstream_duration(method="eth_chainId", duration_seconds=0.1)
    out = m.render()
    assert "exec_rest_api_request_duration_seconds_count 1" in out
    assert (
        'exec_rest_api_upstream_duration_seconds_count{method="eth_chainId"} 1' in out
    )


def test_upstream_request_counter():
    m = Metrics()
    m.inc_upstream(method="eth_chainId", status="ok")
    m.inc_upstream(method="eth_chainId", status="ok")
    m.inc_upstream(method="eth_call", status="error")
    out = m.render()
    assert (
        'exec_rest_api_upstream_requests_total{method="eth_chainId",status="ok"} 2'
        in out
    )
    assert (
        'exec_rest_api_upstream_requests_total{method="eth_call",status="error"} 1'
        in out
    )


def test_gauge_set():
    m = Metrics()
    m.set_chain_head_block(18234567)
    out = m.render()
    assert "exec_rest_api_chain_head_block 18234567" in out


def test_gauge_overwrites():
    m = Metrics()
    m.set_chain_head_block(1)
    m.set_chain_head_block(2)
    out = m.render()
    # Only the latest value appears, exactly once
    assert out.count("exec_rest_api_chain_head_block ") == 1
    assert "exec_rest_api_chain_head_block 2" in out


def test_labelled_gauge_set():
    m = Metrics()
    m.set_sse_connections(stream="blocks", value=3)
    m.set_sse_connections(stream="logs", value=1)
    out = m.render()
    assert 'exec_rest_api_sse_connections{stream="blocks"} 3' in out
    assert 'exec_rest_api_sse_connections{stream="logs"} 1' in out


def test_labelled_gauge_overwrites_per_labelset():
    m = Metrics()
    m.set_sse_connections(stream="blocks", value=3)
    m.set_sse_connections(stream="blocks", value=2)
    m.set_sse_connections(stream="logs", value=1)
    out = m.render()
    assert 'exec_rest_api_sse_connections{stream="blocks"} 2' in out
    assert 'exec_rest_api_sse_connections{stream="logs"} 1' in out


def test_upstream_subscriptions_gauge():
    m = Metrics()
    m.set_upstream_subscriptions(stream="newHeads", value=1)
    m.set_upstream_subscriptions(stream="logs", value=4)
    out = m.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 1' in out
    assert 'exec_rest_api_upstream_subscriptions{stream="logs"} 4' in out


def test_render_includes_help_and_type_lines():
    """Every emitted series must be preceded by a # HELP and # TYPE line."""
    m = Metrics()
    m.inc_request(method="GET", path_template="/chain", status=200)
    out = m.render()
    assert "# HELP exec_rest_api_requests_total" in out
    assert "# TYPE exec_rest_api_requests_total counter" in out


def test_render_omits_empty_series():
    """A series that has zero observations is not emitted at all."""
    m = Metrics()
    out = m.render()
    # No counters, no histograms, no gauges → no exec_rest_api lines
    assert "exec_rest_api" not in out


def test_render_is_idempotent():
    m = Metrics()
    m.inc_request(method="GET", path_template="/chain", status=200)
    a = m.render()
    b = m.render()
    assert a == b


def test_content_type_header_value():
    """The exporter advertises the standard Prometheus text format content-type."""
    from exec_rest_api.metrics import PROMETHEUS_CONTENT_TYPE
    assert PROMETHEUS_CONTENT_TYPE == "text/plain; version=0.0.4; charset=utf-8"
