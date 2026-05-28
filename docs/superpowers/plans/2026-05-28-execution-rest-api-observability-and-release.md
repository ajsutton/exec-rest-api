# Execution REST API — Observability + release pipeline (Plan 5 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the operational-polish layer (Prometheus metrics, `X-Upstream-Method` / `X-Block-Height` response headers, chain-head tracker) and the publish pipeline (CI on PRs, signed PyPI / `.pyz` / OCI image on `v*` tags) so the proxy is production-deployable end-to-end.

**Architecture:** A new `metrics.py` module holds an in-memory registry of counters/gauges/histograms with a hand-written Prometheus text exporter (no client library). `UpstreamClient` gains a single `on_call` observer hook fired on every JSON-RPC call — the bootstrap installs one that simultaneously updates upstream metrics and appends the method name to a contextvar that `server.py` consults to set `X-Upstream-Method`. A new `chain_head.py` owns a single background task that consumes the upstream `newHeads` subscription (via `SubscriptionManager`) and falls back to periodic `eth_blockNumber` polling when the WS isn't available; its latest value backs both `X-Block-Height` and the `chain_head_block` gauge. SSE handlers update an `sse_connections{stream}` gauge on connect/disconnect. The release side is two GitHub Actions workflows (`ci.yml` for PR validation, `release.yml` triggered on `v*` tags) plus a multi-stage `Dockerfile`; signing is done via cosign keyless (GitHub OIDC) and PyPI uses Trusted Publishing — no long-lived tokens anywhere.

**Tech Stack:** Same Python core as Plans 1–4 (aiohttp, asyncio, pytest). New CI/release tools (used only inside GitHub Actions, not as runtime deps): `shiv` (single-file `.pyz`), `docker buildx` (multi-arch images), `cosign` (sigstore signatures), `cyclonedx-py` (SBOM).

---

## Companion documents

These contain the authoritative requirements that this plan implements. Keep them open while implementing.

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — §5.5 (cross-cutting headers — `X-Upstream-Method`, `X-Block-Height`, `Retry-After`).
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — §5 (`--metrics` flag), §6 (Observability — logging + metrics + request IDs), §11 (request lifecycle includes a "metrics middleware" slot).
- `docs/superpowers/plans/roadmap.md` Plan 5 — full list of metric series and release artefacts.

---

## File structure (created or modified by this plan)

```
src/exec_rest_api/
├── metrics.py                       (NEW) Prometheus registry + text exporter
├── chain_head.py                    (NEW) ChainHeadTracker (subscribe-with-poll-fallback)
├── upstream.py                      (MODIFIED) on_call observer hook
├── server.py                        (MODIFIED) metrics middleware, X-Upstream-Method header, X-Block-Height header
├── subscriptions.py                 (MODIFIED) metrics integration for upstream_subscriptions gauge
├── handlers/
│   ├── metrics.py                   (NEW) GET /metrics handler
│   └── streams.py                   (MODIFIED) update sse_connections gauge on connect/disconnect
├── __main__.py                      (MODIFIED) construct Metrics + ChainHeadTracker, wire observer hook, register /metrics
.github/workflows/
├── ci.yml                           (NEW) lint + typecheck + unit/integration/conformance on push/PR
├── release.yml                      (NEW) tag-triggered: sdist/wheel/pyz/OCI, cosign, SBOM
Dockerfile                           (NEW) multi-stage; distroless base where possible
docs/
└── operations.md                    (NEW) systemd + container deployment guide
tests/
├── unit/
│   ├── test_metrics.py              (NEW) counter/gauge/histogram + Prometheus text format
│   ├── test_chain_head.py           (NEW) subscription path + poll fallback + stop()
│   ├── test_handlers_metrics.py     (NEW) GET /metrics behaviour
│   ├── test_upstream.py             (MODIFIED) on_call observer is invoked
│   ├── test_server.py               (MODIFIED) metrics middleware, X-Upstream-Method, X-Block-Height
│   ├── test_subscriptions.py        (MODIFIED) gauge callback fires on subscribe/unsubscribe
│   └── test_handlers_streams.py     (MODIFIED) sse_connections gauge gates on connect/disconnect
├── integration/
│   └── test_metrics.py              (NEW) /metrics returns parseable text against anvil
└── conftest.py                      (MODIFIED) construct Metrics + ChainHeadTracker in proxy_client
pyproject.toml                       (MODIFIED) bump version to 0.5.0
README.md                            (MODIFIED) install methods + cosign verification + /metrics + operations link
docs/superpowers/plans/roadmap.md    (MODIFIED) mark Plan 5 DONE (final task)
```

Files NOT created in this plan: no new handler families. No prometheus_client dependency. No new runtime libraries.

---

## Task 1: `metrics.py` — counters, gauges, histograms + Prometheus text exporter

The whole observability layer hangs off this module. Hand-written, ~250 LOC, zero runtime dependencies. The exporter writes the Prometheus 0.0.4 text format (`text/plain; version=0.0.4; charset=utf-8`).

Buckets for the two histograms are the Prometheus defaults: `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, +Inf` (seconds). One bucket set covers both `exec_rest_api_request_duration_seconds` and `exec_rest_api_upstream_duration_seconds`.

Label-value escaping per Prometheus: `\` → `\\`, `\n` → `\n`, `"` → `\"`.

**Files:**
- Create: `src/exec_rest_api/metrics.py`
- Create: `tests/unit/test_metrics.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_metrics.py`:

```python
"""Tests for the in-memory metrics registry + Prometheus text exporter."""

from __future__ import annotations

import pytest

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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_metrics.py -v
```

Expected: every test errors with `ImportError: cannot import name 'Metrics' from 'exec_rest_api.metrics'` (module doesn't exist).

- [ ] **Step 3: Implement `metrics.py`**

Create `src/exec_rest_api/metrics.py`:

```python
"""In-memory Prometheus metrics registry + text exporter.

No client library — Prometheus text format 0.0.4 is trivial to emit, and avoiding
the dependency keeps the runtime tree to `aiohttp` only. All metric values live in
a single `Metrics` instance owned by the application bootstrap and accessible via
``app["metrics"]``.

The exporter is content-type `text/plain; version=0.0.4; charset=utf-8`. Series
with zero observations are omitted from output.
"""

from __future__ import annotations

from typing import Final

PROMETHEUS_CONTENT_TYPE: Final[str] = "text/plain; version=0.0.4; charset=utf-8"

# Prometheus default histogram bucket boundaries (seconds).
_DEFAULT_BUCKETS: Final[tuple[float, ...]] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


def _format_number(v: float) -> str:
    """Render a float without trailing zeros or scientific notation for typical values."""
    if v == int(v):
        return str(int(v))
    # Avoid floating-point cruft in test assertions by rounding to 3 decimals
    # (millisecond precision is plenty for both bucket boundaries and sums).
    return f"{round(v, 3)}"


def _escape_label_value(v: str) -> str:
    """Escape per Prometheus exposition format: backslash, newline, double-quote."""
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    parts = ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in labels)
    return "{" + parts + "}"


class _Counter:
    """Counter: monotonically increasing, labelled."""

    def __init__(self, name: str, help_text: str) -> None:
        self.name = name
        self.help_text = help_text
        self._values: dict[tuple[tuple[str, str], ...], int] = {}

    def inc(self, labels: tuple[tuple[str, str], ...], amount: int = 1) -> None:
        self._values[labels] = self._values.get(labels, 0) + amount

    def render(self) -> list[str]:
        if not self._values:
            return []
        out = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} counter",
        ]
        for labels in sorted(self._values, key=lambda lbls: lbls):
            out.append(f"{self.name}{_format_labels(labels)} {self._values[labels]}")
        return out


class _Gauge:
    """Gauge: settable, labelled."""

    def __init__(self, name: str, help_text: str) -> None:
        self.name = name
        self.help_text = help_text
        self._values: dict[tuple[tuple[str, str], ...], float] = {}

    def set(self, labels: tuple[tuple[str, str], ...], value: float) -> None:
        self._values[labels] = value

    def render(self) -> list[str]:
        if not self._values:
            return []
        out = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} gauge",
        ]
        for labels in sorted(self._values, key=lambda lbls: lbls):
            out.append(
                f"{self.name}{_format_labels(labels)} {_format_number(self._values[labels])}"
            )
        return out


class _Histogram:
    """Cumulative histogram with optional labels.

    Per Prometheus convention: each bucket's count is cumulative (le="0.01"
    includes everything in le="0.005"). The implicit `+Inf` bucket equals the
    total observation count.
    """

    def __init__(self, name: str, help_text: str, label_keys: tuple[str, ...] = ()) -> None:
        self.name = name
        self.help_text = help_text
        self.label_keys = label_keys
        self._buckets: dict[tuple[tuple[str, str], ...], list[int]] = {}
        self._sums: dict[tuple[tuple[str, str], ...], float] = {}
        self._counts: dict[tuple[tuple[str, str], ...], int] = {}

    def observe(self, labels: tuple[tuple[str, str], ...], value: float) -> None:
        if labels not in self._buckets:
            self._buckets[labels] = [0] * len(_DEFAULT_BUCKETS)
            self._sums[labels] = 0.0
            self._counts[labels] = 0
        for i, b in enumerate(_DEFAULT_BUCKETS):
            if value <= b:
                self._buckets[labels][i] += 1
        self._sums[labels] += value
        self._counts[labels] += 1

    def render(self) -> list[str]:
        if not self._counts:
            return []
        out = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} histogram",
        ]
        for labels in sorted(self._counts, key=lambda lbls: lbls):
            label_str = _format_labels(labels)
            for i, b in enumerate(_DEFAULT_BUCKETS):
                bucket_labels = labels + (("le", _format_number(b)),)
                out.append(
                    f"{self.name}_bucket{_format_labels(bucket_labels)} {self._buckets[labels][i]}"
                )
            inf_labels = labels + (("le", "+Inf"),)
            out.append(
                f"{self.name}_bucket{_format_labels(inf_labels)} {self._counts[labels]}"
            )
            out.append(f"{self.name}_sum{label_str} {_format_number(self._sums[labels])}")
            out.append(f"{self.name}_count{label_str} {self._counts[labels]}")
        return out


class Metrics:
    """The single registry owned by the application.

    Exposes typed methods (`inc_request`, `observe_request_duration`, …) rather
    than a generic API; the metric set is fixed by the spec, and typed methods
    keep callers honest about labels and units.
    """

    def __init__(self) -> None:
        self._requests = _Counter(
            "exec_rest_api_requests_total",
            "Total HTTP requests handled by the proxy",
        )
        self._request_duration = _Histogram(
            "exec_rest_api_request_duration_seconds",
            "HTTP request duration in seconds",
        )
        self._upstream_requests = _Counter(
            "exec_rest_api_upstream_requests_total",
            "Total JSON-RPC calls made to the upstream",
        )
        self._upstream_duration = _Histogram(
            "exec_rest_api_upstream_duration_seconds",
            "Upstream JSON-RPC call duration in seconds",
            label_keys=("method",),
        )
        self._sse_connections = _Gauge(
            "exec_rest_api_sse_connections",
            "Live SSE client connections per stream",
        )
        self._upstream_subscriptions = _Gauge(
            "exec_rest_api_upstream_subscriptions",
            "Active upstream eth_subscribe subscriptions per stream kind",
        )
        self._chain_head_block = _Gauge(
            "exec_rest_api_chain_head_block",
            "Latest known chain head block number",
        )

    # ── Counters / histograms ────────────────────────────────────────────

    def inc_request(self, *, method: str, path_template: str, status: int) -> None:
        self._requests.inc(
            (("method", method), ("path_template", path_template), ("status", str(status))),
        )

    def observe_request_duration(self, duration_seconds: float) -> None:
        self._request_duration.observe((), duration_seconds)

    def inc_upstream(self, *, method: str, status: str) -> None:
        self._upstream_requests.inc(
            (("method", method), ("status", status)),
        )

    def observe_upstream_duration(self, *, method: str, duration_seconds: float) -> None:
        self._upstream_duration.observe((("method", method),), duration_seconds)

    # ── Gauges ──────────────────────────────────────────────────────────

    def set_sse_connections(self, *, stream: str, value: int) -> None:
        self._sse_connections.set((("stream", stream),), value)

    def set_upstream_subscriptions(self, *, stream: str, value: int) -> None:
        self._upstream_subscriptions.set((("stream", stream),), value)

    def set_chain_head_block(self, value: int) -> None:
        self._chain_head_block.set((), value)

    # ── Render ──────────────────────────────────────────────────────────

    def render(self) -> str:
        sections: list[list[str]] = [
            self._requests.render(),
            self._request_duration.render(),
            self._upstream_requests.render(),
            self._upstream_duration.render(),
            self._sse_connections.render(),
            self._upstream_subscriptions.render(),
            self._chain_head_block.render(),
        ]
        lines: list[str] = []
        for section in sections:
            if section:
                lines.extend(section)
        if not lines:
            return ""
        # Prometheus text format requires a trailing newline.
        return "\n".join(lines) + "\n"
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_metrics.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Type-check + lint**

```bash
mypy src/exec_rest_api/metrics.py
ruff check src/exec_rest_api/metrics.py tests/unit/test_metrics.py
```

Expected: both succeed.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/metrics.py tests/unit/test_metrics.py
git commit -m "Add in-memory metrics registry with Prometheus text exporter"
```

---

## Task 2: Instrument `UpstreamClient` — `on_call` observer hook

`UpstreamClient.call` gains an optional observer callback. The callback receives `(method, status, duration_seconds)` after every call, regardless of whether it succeeded, returned a JSON-RPC error, or raised an `UpstreamError`. The bootstrap registers a single composite observer that (a) increments the upstream counter + histogram, and (b) appends the method name to a contextvar list that `server.py` consults to set the `X-Upstream-Method` header.

`status` strings:
- `"ok"` → result returned (no error).
- `"error"` → upstream JSON-RPC error (`UpstreamJsonRpcError`).
- `"transport-error"` → HTTP / connection failure (`UpstreamError`).

The contextvar lives in `metrics.py` so the wiring stays in one module.

**Files:**
- Modify: `src/exec_rest_api/metrics.py` — add `current_request_upstream_methods` contextvar + helpers
- Modify: `src/exec_rest_api/upstream.py` — accept `on_call` observer
- Modify: `tests/unit/test_upstream.py` — add observer-fired tests

- [ ] **Step 1: Add the contextvar to `metrics.py` (no test for the contextvar itself — covered indirectly by Task 4)**

Edit `src/exec_rest_api/metrics.py`. Add at module top, just below `_DEFAULT_BUCKETS`:

```python
import contextvars

# Per-request accumulator of upstream JSON-RPC method names. Set by the metrics
# middleware at request start; appended to by UpstreamClient's on_call observer;
# read by server.py to populate the X-Upstream-Method response header.
current_request_upstream_methods: contextvars.ContextVar[list[str] | None] = (
    contextvars.ContextVar("current_request_upstream_methods", default=None)
)
```

Place `import contextvars` with the other standard-library imports near the top of the file (just below the `from __future__ import annotations` line).

- [ ] **Step 2: Write the new failing tests for `UpstreamClient`**

Add to `tests/unit/test_upstream.py` (append at end of file):

```python
async def test_on_call_observer_fired_on_success(stub_upstream):
    server, _ = stub_upstream
    observed: list[tuple[str, str, float]] = []

    def observer(method: str, status: str, duration_seconds: float) -> None:
        observed.append((method, status, duration_seconds))

    async with ClientSession() as session:
        client = UpstreamClient(
            session=session,
            http_url=str(server.make_url("/")),
            on_call=observer,
        )
        await client.call("rpc_ok", [])
    assert len(observed) == 1
    method, status, duration = observed[0]
    assert method == "rpc_ok"
    assert status == "ok"
    assert duration >= 0.0


async def test_on_call_observer_fired_on_jsonrpc_error(stub_upstream):
    server, _ = stub_upstream
    observed: list[tuple[str, str, float]] = []

    def observer(method: str, status: str, duration_seconds: float) -> None:
        observed.append((method, status, duration_seconds))

    async with ClientSession() as session:
        client = UpstreamClient(
            session=session,
            http_url=str(server.make_url("/")),
            on_call=observer,
        )
        with pytest.raises(UpstreamJsonRpcError):
            await client.call("rpc_error", [])
    assert len(observed) == 1
    assert observed[0][0] == "rpc_error"
    assert observed[0][1] == "error"


async def test_on_call_observer_fired_on_transport_error(stub_upstream):
    server, _ = stub_upstream
    observed: list[tuple[str, str, float]] = []

    def observer(method: str, status: str, duration_seconds: float) -> None:
        observed.append((method, status, duration_seconds))

    async with ClientSession() as session:
        client = UpstreamClient(
            session=session,
            http_url=str(server.make_url("/")),
            on_call=observer,
        )
        with pytest.raises(UpstreamError):
            await client.call("rpc_http_500", [])
    assert len(observed) == 1
    assert observed[0][0] == "rpc_http_500"
    assert observed[0][1] == "transport-error"


async def test_on_call_observer_exception_does_not_break_the_call(stub_upstream):
    """A misbehaving observer must never break the upstream call."""
    server, _ = stub_upstream

    def bad_observer(method: str, status: str, duration_seconds: float) -> None:
        raise RuntimeError("observer is broken")

    async with ClientSession() as session:
        client = UpstreamClient(
            session=session,
            http_url=str(server.make_url("/")),
            on_call=bad_observer,
        )
        result = await client.call("rpc_ok", [])
    assert result == "hello"
```

- [ ] **Step 3: Run the new tests to verify they fail**

```bash
pytest tests/unit/test_upstream.py::test_on_call_observer_fired_on_success -v
```

Expected: `TypeError: UpstreamClient.__init__() got an unexpected keyword argument 'on_call'`.

- [ ] **Step 4: Update `UpstreamClient` to accept and invoke the observer**

Edit `src/exec_rest_api/upstream.py`. Replace the existing class definition with:

```python
"""JSON-RPC HTTP client.

One `UpstreamClient` per process. Owns no session — the caller passes in an
`aiohttp.ClientSession` so connection pool configuration lives in the server
bootstrap. No retries: JSON-RPC isn't universally idempotent, and the proxy
prefers to surface failure to the caller rather than risk double-submits.

The optional `on_call` observer fires once per call with
``(method, status, duration_seconds)`` where status is ``"ok"``,
``"error"`` (JSON-RPC error returned), or ``"transport-error"``. Observer
exceptions are swallowed.
"""

from __future__ import annotations

import itertools
import logging
import time
from collections.abc import Callable
from typing import Any

import aiohttp
from aiohttp import ClientSession

logger = logging.getLogger(__name__)


class UpstreamError(Exception):
    """Transport-level failure talking to the upstream (HTTP status, garbled body, timeout)."""


class UpstreamJsonRpcError(Exception):
    """JSON-RPC error object returned by the upstream.

    Carries the raw `code`, `message`, and `data` so the error mapper can
    translate it into a Problem.
    """

    def __init__(self, *, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"jsonrpc error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


OnCall = Callable[[str, str, float], None]


class UpstreamClient:
    """Async JSON-RPC client over HTTP."""

    def __init__(
        self,
        *,
        session: ClientSession,
        http_url: str,
        default_timeout_seconds: float = 30.0,
        on_call: OnCall | None = None,
    ) -> None:
        self._session = session
        self._url = http_url
        self._timeout = aiohttp.ClientTimeout(total=default_timeout_seconds)
        self._id_counter = itertools.count(1)
        self._on_call = on_call

    def _notify(self, method: str, status: str, duration_seconds: float) -> None:
        if self._on_call is None:
            return
        try:
            self._on_call(method, status, duration_seconds)
        except Exception:
            logger.exception("UpstreamClient on_call observer raised; ignoring")

    async def call(
        self,
        method: str,
        params: list[Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Issue one JSON-RPC request. Returns the `result` field on success.

        Raises:
            UpstreamError: transport failure (timeout, HTTP non-2xx, malformed response).
            UpstreamJsonRpcError: upstream returned a JSON-RPC `error` object.
        """
        body = {
            "jsonrpc": "2.0",
            "id": next(self._id_counter),
            "method": method,
            "params": params or [],
        }
        timeout = (
            aiohttp.ClientTimeout(total=timeout_seconds)
            if timeout_seconds is not None
            else self._timeout
        )
        start = time.monotonic()
        status = "ok"
        try:
            try:
                async with self._session.post(self._url, json=body, timeout=timeout) as resp:
                    if resp.status != 200:
                        raise UpstreamError(f"upstream HTTP {resp.status}")
                    try:
                        payload = await resp.json(content_type=None)
                    except (aiohttp.ContentTypeError, ValueError) as e:
                        raise UpstreamError(f"upstream returned non-JSON body: {e}") from e
            except aiohttp.ClientError as e:
                raise UpstreamError(f"upstream transport error: {e}") from e
            if not isinstance(payload, dict):
                raise UpstreamError(f"upstream returned non-object: {payload!r}")
            if "error" in payload:
                err = payload["error"]
                if not isinstance(err, dict):
                    raise UpstreamError(f"upstream error object malformed: {err!r}")
                status = "error"
                raise UpstreamJsonRpcError(
                    code=int(err.get("code", -32603)),
                    message=str(err.get("message", "")),
                    data=err.get("data"),
                )
            if "result" not in payload:
                raise UpstreamError(f"upstream response has neither result nor error: {payload!r}")
            return payload["result"]
        except UpstreamJsonRpcError:
            raise
        except UpstreamError:
            status = "transport-error"
            raise
        finally:
            self._notify(method, status, time.monotonic() - start)
```

- [ ] **Step 5: Run the full upstream test module**

```bash
pytest tests/unit/test_upstream.py -v
```

Expected: all tests pass — both the originals and the four new observer tests.

- [ ] **Step 6: Type-check + lint**

```bash
mypy src/exec_rest_api/upstream.py src/exec_rest_api/metrics.py
ruff check src/exec_rest_api/upstream.py src/exec_rest_api/metrics.py tests/unit/test_upstream.py
```

Expected: both clean.

- [ ] **Step 7: Commit**

```bash
git add src/exec_rest_api/upstream.py src/exec_rest_api/metrics.py tests/unit/test_upstream.py
git commit -m "Add UpstreamClient.on_call observer hook and request-scoped method contextvar"
```

---

## Task 3: `chain_head.py` — chain head tracker (subscribe with poll fallback)

A single long-running task that maintains the current chain head and pushes it to the metrics gauge. Tries to subscribe to `newHeads` via `SubscriptionManager`; if the WS isn't connected, falls back to polling `eth_blockNumber` on a fixed interval. The latest known value backs both the `X-Block-Height` response header and `exec_rest_api_chain_head_block` gauge.

`current` returns `int | None` — `None` until the first successful update. The middleware skips the header when current is None.

**Files:**
- Create: `src/exec_rest_api/chain_head.py`
- Create: `tests/unit/test_chain_head.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_chain_head.py`:

```python
"""Tests for the chain-head tracker (subscribe-with-poll-fallback)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from exec_rest_api.chain_head import ChainHeadTracker
from exec_rest_api.metrics import Metrics
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable


class _FakeStream:
    """Minimal async iterator matching SubscriptionManager.subscribe()'s return type."""

    def __init__(self, events: list[StreamEvent]) -> None:
        self._queue: asyncio.Queue[StreamEvent] = asyncio.Queue()
        for e in events:
            self._queue.put_nowait(e)
        self._closed = False

    def __aiter__(self) -> "_FakeStream":
        return self

    async def __anext__(self) -> StreamEvent:
        if self._closed:
            raise StopAsyncIteration
        return await self._queue.get()

    async def aclose(self) -> None:
        self._closed = True

    def push(self, event: StreamEvent) -> None:
        self._queue.put_nowait(event)


async def test_starts_with_no_current_value():
    """Before start(), and before any event arrives, current is None."""
    metrics = Metrics()
    upstream = AsyncMock()
    subs = AsyncMock()
    tracker = ChainHeadTracker(upstream=upstream, subscriptions=subs, metrics=metrics)
    assert tracker.current is None


async def test_subscription_path_updates_current_and_gauge():
    """When subscribe() succeeds, newHeads events update the current value."""
    metrics = Metrics()
    upstream = AsyncMock()
    subs = AsyncMock()
    stream = _FakeStream(
        [
            StreamEvent(kind="event", payload={"number": "0x10", "hash": "0xabc"}),
        ]
    )
    subs.subscribe.return_value = stream

    tracker = ChainHeadTracker(upstream=upstream, subscriptions=subs, metrics=metrics)
    await tracker.start()
    # Allow the consumer task to drain at least one event
    for _ in range(50):
        if tracker.current is not None:
            break
        await asyncio.sleep(0.01)
    assert tracker.current == 16
    assert "exec_rest_api_chain_head_block 16" in metrics.render()
    await tracker.stop()


async def test_subscription_path_ignores_gap_events():
    """A gap event must not crash the consumer; current keeps its prior value."""
    metrics = Metrics()
    upstream = AsyncMock()
    subs = AsyncMock()
    stream = _FakeStream(
        [
            StreamEvent(kind="event", payload={"number": "0x1"}),
            GAP,
            StreamEvent(kind="event", payload={"number": "0x2"}),
        ]
    )
    subs.subscribe.return_value = stream

    tracker = ChainHeadTracker(upstream=upstream, subscriptions=subs, metrics=metrics)
    await tracker.start()
    for _ in range(50):
        if tracker.current == 2:
            break
        await asyncio.sleep(0.01)
    assert tracker.current == 2
    await tracker.stop()


async def test_polling_path_when_ws_unavailable():
    """If subscribe raises SubscriptionUnavailable, falls back to polling."""
    metrics = Metrics()
    upstream = AsyncMock()
    upstream.call.side_effect = ["0x5", "0x6", "0x7"]
    subs = AsyncMock()
    subs.subscribe.side_effect = SubscriptionUnavailable("ws not connected")

    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=subs,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    for _ in range(100):
        if tracker.current == 5:
            break
        await asyncio.sleep(0.01)
    assert tracker.current == 5
    upstream.call.assert_any_call("eth_blockNumber")
    await tracker.stop()


async def test_polling_continues_through_transient_errors():
    """A transient upstream error during polling must not kill the tracker."""
    from exec_rest_api.upstream import UpstreamError

    metrics = Metrics()
    upstream = AsyncMock()
    upstream.call.side_effect = [UpstreamError("down"), "0x9"]
    subs = AsyncMock()
    subs.subscribe.side_effect = SubscriptionUnavailable("ws not connected")

    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=subs,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    for _ in range(100):
        if tracker.current == 9:
            break
        await asyncio.sleep(0.02)
    assert tracker.current == 9
    await tracker.stop()


async def test_stop_idempotent():
    metrics = Metrics()
    subs = AsyncMock()
    subs.subscribe.side_effect = SubscriptionUnavailable("ws not connected")
    upstream = AsyncMock()
    upstream.call.return_value = "0x1"
    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=subs,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    await tracker.stop()
    await tracker.stop()  # second call must not raise


async def test_works_without_subscription_manager():
    """If subscriptions is None, tracker polls."""
    metrics = Metrics()
    upstream = AsyncMock()
    upstream.call.return_value = "0x4"
    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=None,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    for _ in range(100):
        if tracker.current == 4:
            break
        await asyncio.sleep(0.02)
    assert tracker.current == 4
    await tracker.stop()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_chain_head.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `chain_head.py`**

Create `src/exec_rest_api/chain_head.py`:

```python
"""Chain-head tracker.

Owns a single background task that keeps `current` populated with the latest
block number known to the upstream. Source order:

1. Subscribe to ``newHeads`` via the SubscriptionManager. The manager handles
   WS reconnect transparently, so we stay subscribed for the process lifetime.
2. If subscribing fails (no WS at all, or it dropped before subscribe time),
   fall back to polling ``eth_blockNumber`` at a fixed interval.

On every update, push the value to the ``chain_head_block`` Prometheus gauge.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from exec_rest_api.encoding import EncodingError, hex_to_int
from exec_rest_api.metrics import Metrics
from exec_rest_api.subscriptions import SubscriptionManager, SubscriptionUnavailable
from exec_rest_api.upstream import UpstreamClient, UpstreamError, UpstreamJsonRpcError

logger = logging.getLogger("exec_rest_api.chain_head")


class ChainHeadTracker:
    """Maintains the latest known chain head."""

    def __init__(
        self,
        *,
        upstream: UpstreamClient,
        subscriptions: SubscriptionManager | None,
        metrics: Metrics,
        poll_interval_seconds: float = 12.0,
    ) -> None:
        self._upstream = upstream
        self._subscriptions = subscriptions
        self._metrics = metrics
        self._poll_interval = poll_interval_seconds
        self._current: int | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    @property
    def current(self) -> int | None:
        return self._current

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="chain-head-tracker")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None

    # ── internals ────────────────────────────────────────────────────────

    def _update(self, block_number: int) -> None:
        self._current = block_number
        self._metrics.set_chain_head_block(block_number)

    async def _run(self) -> None:
        # Try the subscription path first; on failure, fall back to polling.
        stream = None
        if self._subscriptions is not None:
            try:
                stream = await self._subscriptions.subscribe(kind="newHeads", params=None)
            except SubscriptionUnavailable as exc:
                logger.info("newHeads subscribe unavailable (%s); polling instead", exc)
            except Exception as exc:
                logger.warning("newHeads subscribe failed (%r); polling instead", exc)

        if stream is not None:
            try:
                await self._consume_subscription(stream)
            finally:
                await stream.aclose()
        else:
            await self._poll_loop()

    async def _consume_subscription(self, stream: Any) -> None:
        async for event in stream:
            if self._stop_event.is_set():
                return
            if event.kind != "event":
                # GAP — SubscriptionManager has re-subscribed; events resume shortly.
                continue
            payload = event.payload or {}
            number_hex = payload.get("number") if isinstance(payload, dict) else None
            if not isinstance(number_hex, str):
                continue
            try:
                self._update(hex_to_int(number_hex))
            except EncodingError:
                logger.debug("ignoring malformed newHeads number: %r", number_hex)

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                hex_value = await self._upstream.call("eth_blockNumber")
                self._update(hex_to_int(hex_value))
            except (UpstreamError, UpstreamJsonRpcError, EncodingError) as exc:
                logger.debug("chain head poll failed: %r", exc)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                continue
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_chain_head.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Type-check + lint**

```bash
mypy src/exec_rest_api/chain_head.py
ruff check src/exec_rest_api/chain_head.py tests/unit/test_chain_head.py
```

Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/chain_head.py tests/unit/test_chain_head.py
git commit -m "Add ChainHeadTracker (subscribe with poll fallback)"
```

---

## Task 4: Wire metrics middleware + X-Upstream-Method + X-Block-Height in `server.py`

The metrics middleware sits between request-id and access-log, before error-mapping. It:

1. Initialises `current_request_upstream_methods.set([])` so the contextvar is populated for the request.
2. Times the request.
3. After the inner handler returns (or raises), updates `inc_request(...)` + `observe_request_duration(...)`.
4. Sets `X-Upstream-Method` header from the contextvar if non-empty.
5. Sets `X-Block-Height` header from the ChainHeadTracker if available.

Path-template extraction: `request.match_info.route.resource.canonical` — when the route is unmatched, `resource` is `None`; substitute `"__not_found__"`. The middleware reads `request.app["metrics"]` and (optionally) `request.app["chain_head"]`. If either is absent (back-compat for tests that build apps directly), the middleware is a no-op for that part.

The `X-Upstream-Method` value is a comma-separated list (preserves call order, no duplicates collapsing — useful for fan-out debugging).

**Files:**
- Modify: `src/exec_rest_api/server.py`
- Modify: `tests/unit/test_server.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_server.py`:

```python
# ── Plan 5: metrics middleware, X-Upstream-Method, X-Block-Height ──────────


class _StubChainHead:
    def __init__(self, value: int | None) -> None:
        self.current = value


async def test_metrics_middleware_counts_and_times(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/ok", ok)
    client = await aiohttp_client(app)
    await client.get("/_test/ok")
    out = metrics.render()
    assert (
        'exec_rest_api_requests_total{method="GET",path_template="/_test/ok",status="200"} 1'
        in out
    )
    assert "exec_rest_api_request_duration_seconds_count 1" in out


async def test_metrics_middleware_path_template_for_dynamic_routes(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/{id}", ok)
    client = await aiohttp_client(app)
    await client.get("/_test/123")
    out = metrics.render()
    assert 'path_template="/_test/{id}"' in out


async def test_metrics_middleware_path_template_for_unmatched(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics
    client = await aiohttp_client(app)
    await client.get("/this-does-not-exist")
    out = metrics.render()
    assert 'path_template="__not_found__"' in out
    assert 'status="404"' in out


async def test_x_upstream_method_header_from_contextvar(aiohttp_client):
    from exec_rest_api.metrics import (
        Metrics,
        current_request_upstream_methods,
    )
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def calls_upstream(request: web.Request) -> web.Response:
        methods = current_request_upstream_methods.get()
        # Simulate UpstreamClient appending two methods
        if methods is not None:
            methods.append("eth_chainId")
            methods.append("net_version")
        return web.Response(text="ok")

    app.router.add_get("/_test/multi", calls_upstream)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/multi")
    assert resp.headers["X-Upstream-Method"] == "eth_chainId,net_version"


async def test_x_upstream_method_header_absent_when_no_upstream_calls(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def no_upstream(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/no-upstream", no_upstream)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/no-upstream")
    assert "X-Upstream-Method" not in resp.headers


async def test_x_block_height_header_from_tracker(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics
    app["chain_head"] = _StubChainHead(value=18234567)

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/height", ok)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/height")
    assert resp.headers["X-Block-Height"] == "18234567"


async def test_x_block_height_header_absent_when_unknown(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics
    app["chain_head"] = _StubChainHead(value=None)

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/no-height", ok)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/no-height")
    assert "X-Block-Height" not in resp.headers


async def test_metrics_middleware_records_500_status_on_exception(aiohttp_client, app_with_test_route):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = await app_with_test_route(mock)
    app["metrics"] = metrics
    client = await aiohttp_client(app)
    await client.get("/_test/unexpected")
    out = metrics.render()
    assert 'status="500"' in out
```

If `from typing import Any` isn't already present at the top of the file, add it with the other imports — the new tests reference the `Any` type.

- [ ] **Step 2: Run the new tests to verify they fail**

```bash
pytest tests/unit/test_server.py -v
```

Expected: the new tests error with `KeyError: 'metrics'` or the headers are not set.

- [ ] **Step 3: Add the middleware to `server.py`**

Edit `src/exec_rest_api/server.py`. Add imports at the top of the file (with the others):

```python
import time
from typing import Any

from exec_rest_api.metrics import Metrics, current_request_upstream_methods
```

(`time` is already imported; the others are new.)

Add the new middleware between `access_log_middleware` and `error_mapping_middleware`:

```python
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
    chain_head: Any = request.app.get("chain_head")  # may be None or a ChainHeadTracker-like
    # Establish the contextvar so UpstreamClient.on_call can append into it.
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
        # Headers — only set on a normal response object (StreamResponse exposes .headers).
        if response is not None:
            methods = current_request_upstream_methods.get()
            if methods:
                response.headers["X-Upstream-Method"] = ",".join(methods)
            if chain_head is not None:
                value = getattr(chain_head, "current", None)
                if value is not None:
                    response.headers["X-Block-Height"] = str(value)
        current_request_upstream_methods.reset(token)
```

Update `create_app` to add the new middleware in the chain (after `access_log_middleware`, before `error_mapping_middleware`):

```python
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
```

- [ ] **Step 4: Run the server tests to verify they pass**

```bash
pytest tests/unit/test_server.py -v
```

Expected: all tests pass (originals + new Plan 5 tests).

- [ ] **Step 5: Type-check + lint**

```bash
mypy src/exec_rest_api/server.py
ruff check src/exec_rest_api/server.py tests/unit/test_server.py
```

Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/server.py tests/unit/test_server.py
git commit -m "Add metrics middleware, X-Upstream-Method and X-Block-Height response headers"
```

---

## Task 5: SubscriptionManager + streams handler — gauge integration

`SubscriptionManager` updates `upstream_subscriptions{stream}` whenever a slot is created or torn down. The streams handler updates `sse_connections{stream}` on connect and disconnect. Both are no-ops when no `metrics` is set on the app, preserving back-compat for tests that don't build the full bootstrap.

**Files:**
- Modify: `src/exec_rest_api/subscriptions.py`
- Modify: `src/exec_rest_api/handlers/streams.py`
- Modify: `tests/unit/test_subscriptions.py`
- Modify: `tests/unit/test_handlers_streams.py`

- [ ] **Step 1: Write the failing test for SubscriptionManager**

Append to `tests/unit/test_subscriptions.py`:

```python
# ── Plan 5: metrics integration ────────────────────────────────────────────


async def test_subscriptions_gauge_updated_on_subscribe_and_unsubscribe():
    """The upstream_subscriptions gauge tracks live slot count per kind."""
    from exec_rest_api.metrics import Metrics

    metrics = Metrics()
    ws = AsyncMock()
    ws.connected = True
    ws.request = AsyncMock(side_effect=["sub-1", "sub-2", None, None])
    mgr = SubscriptionManager(ws=ws, metrics=metrics)

    stream_a = await mgr.subscribe(kind="newHeads", params=None)
    out = metrics.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 1' in out

    stream_b = await mgr.subscribe(kind="logs", params={"address": []})
    out = metrics.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 1' in out
    assert 'exec_rest_api_upstream_subscriptions{stream="logs"} 1' in out

    await stream_a.aclose()
    out = metrics.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 0' in out
    assert 'exec_rest_api_upstream_subscriptions{stream="logs"} 1' in out

    await stream_b.aclose()


async def test_subscriptions_gauge_dedupes_per_unique_filter():
    """Multiple consumers sharing one slot still count as one upstream subscription."""
    from exec_rest_api.metrics import Metrics

    metrics = Metrics()
    ws = AsyncMock()
    ws.connected = True
    ws.request = AsyncMock(side_effect=["sub-1", None])
    mgr = SubscriptionManager(ws=ws, metrics=metrics)

    a = await mgr.subscribe(kind="newHeads", params=None)
    b = await mgr.subscribe(kind="newHeads", params=None)
    out = metrics.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 1' in out
    await a.aclose()
    out = metrics.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 1' in out
    await b.aclose()
    out = metrics.render()
    assert 'exec_rest_api_upstream_subscriptions{stream="newHeads"} 0' in out
```

- [ ] **Step 2: Update `SubscriptionManager` to accept and update metrics**

Edit `src/exec_rest_api/subscriptions.py`. Add import:

```python
from exec_rest_api.metrics import Metrics
```

Update `__init__`:

```python
def __init__(self, *, ws: _WebSocketLike, metrics: Metrics | None = None) -> None:
    self._ws = ws
    self._metrics = metrics
    self._slots: dict[tuple[StreamKind, str], _Slot] = {}
    self._slot_by_subscription_id: dict[str, _Slot] = {}
    self._lock = asyncio.Lock()
```

Add a helper method:

```python
def _publish_gauge(self, kind: StreamKind) -> None:
    if self._metrics is None:
        return
    count = sum(1 for slot in self._slots.values() if slot.kind == kind)
    self._metrics.set_upstream_subscriptions(stream=kind, value=count)
```

Call `self._publish_gauge(kind)` at three points:

1. End of the `if slot is None:` branch in `subscribe`, after the slot is added to `self._slots`.
2. End of the `else:` branch in `subscribe` — actually skip; the count hasn't changed.
3. In `_remove_consumer`, after the slot has been popped from `self._slots` (whether the slot is still live or empty). Easiest: call right before the function returns.

The final `subscribe` looks like:

```python
async def subscribe(self, *, kind: StreamKind, params: Any) -> AsyncIterator[StreamEvent]:
    if not self._ws.connected:
        raise SubscriptionUnavailable("upstream WS not connected")

    key = (kind, _canonicalize(params))
    queue: asyncio.Queue[StreamEvent] = asyncio.Queue(maxsize=1024)

    async with self._lock:
        slot = self._slots.get(key)
        if slot is None:
            params_list = _params_to_subscribe_args(kind, params)
            try:
                sub_id = await self._ws.request("eth_subscribe", params_list)
            except UpstreamWsClosed as exc:
                raise SubscriptionUnavailable(str(exc)) from exc
            slot = _Slot(
                kind=kind, params=params, subscription_id=sub_id, consumers=[queue]
            )
            self._slots[key] = slot
            self._slot_by_subscription_id[sub_id] = slot
            self._publish_gauge(kind)
        else:
            slot.consumers.append(queue)

    return _ConsumerStream(
        queue=queue,
        on_close=lambda: self._remove_consumer(key, slot, queue),
    )
```

And `_remove_consumer`:

```python
async def _remove_consumer(
    self,
    key: tuple[StreamKind, str],
    slot: _Slot,
    queue: asyncio.Queue[StreamEvent],
) -> None:
    async with self._lock:
        try:
            slot.consumers.remove(queue)
        except ValueError:
            return
        if slot.consumers:
            return
        sub_id = slot.subscription_id
        self._slots.pop(key, None)
        if sub_id is not None:
            self._slot_by_subscription_id.pop(sub_id, None)
        self._publish_gauge(slot.kind)
    if sub_id is not None and self._ws.connected:
        try:
            await self._ws.request("eth_unsubscribe", [sub_id])
        except Exception as exc:
            logger.debug("eth_unsubscribe failed (cleaning up anyway): %r", exc)
```

- [ ] **Step 3: Run the subscriptions tests to verify they pass**

```bash
pytest tests/unit/test_subscriptions.py -v
```

Expected: all tests pass (originals + the two new gauge tests).

- [ ] **Step 4: Write the failing test for SSE gauge in streams handler**

Append a test to `tests/unit/test_handlers_streams.py`. Reuse the existing imports at the top of the file (`AsyncMock`, `web`, `Any`, `pytest`, `create_app`, `register_routes`, `_config`, `UpstreamClient`). The test asserts `app["metrics"]` reports `stream="blocks"` value 1 while a client is connected, and value 0 after disconnect.

Append:

```python
# ── Plan 5: sse_connections gauge ──────────────────────────────────────────


async def test_sse_connections_gauge_increments_and_decrements(aiohttp_client):
    """Connecting to /streams/blocks bumps the gauge; disconnect returns it to 0."""
    import asyncio

    from exec_rest_api.metrics import Metrics
    from exec_rest_api.subscriptions import StreamEvent

    metrics = Metrics()

    class _Stream:
        def __init__(self) -> None:
            self._q: asyncio.Queue[StreamEvent] = asyncio.Queue()

        def __aiter__(self) -> "_Stream":
            return self

        async def __anext__(self) -> StreamEvent:
            return await self._q.get()

        async def aclose(self) -> None:
            return

    class _FakeManager:
        def __init__(self) -> None:
            self.stream = _Stream()

        async def subscribe(self, *, kind: str, params: Any) -> Any:
            return self.stream

    mock_upstream = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_config(), upstream=mock_upstream)
    app["metrics"] = metrics
    app["subscriptions"] = _FakeManager()
    register_routes(app)
    client = await aiohttp_client(app)

    async with client.get("/streams/blocks") as resp:
        assert resp.status == 200
        # Give the handler a moment to update the gauge after the response starts
        await asyncio.sleep(0.05)
        assert 'exec_rest_api_sse_connections{stream="blocks"} 1' in metrics.render()

    # After exiting the `async with` the connection closes; allow the handler
    # to run its cleanup.
    for _ in range(50):
        if 'exec_rest_api_sse_connections{stream="blocks"} 0' in metrics.render():
            break
        await asyncio.sleep(0.02)
    assert 'exec_rest_api_sse_connections{stream="blocks"} 0' in metrics.render()
```

(Adapt the existing imports/fixtures in this file: `register_routes`, `_config`, `create_app`, `AsyncMock`, `UpstreamClient`, `Any`, `web` — most should already be imported at the top of `tests/unit/test_handlers_streams.py`.)

- [ ] **Step 5: Update `handlers/streams.py` to publish the gauge**

All four stream handlers funnel through `_run_stream(...)`, so the gauge logic only needs to live in one place. Edit `src/exec_rest_api/handlers/streams.py`.

Add at module top with the existing imports:

```python
from exec_rest_api.metrics import Metrics
```

Add a helper near the top of the module (just below the existing `_block_event` function):

```python
def _publish_sse_gauge(app: web.Application, stream: str, delta: int) -> None:
    metrics: Metrics | None = app.get("metrics")
    if metrics is None:
        return
    counts: dict[str, int] = app.setdefault("__sse_counts__", {})
    counts[stream] = counts.get(stream, 0) + delta
    metrics.set_sse_connections(stream=stream, value=counts[stream])
```

Change the signature of `_run_stream` to accept a `stream_label: str` keyword arg, then bracket the function body with the gauge update:

```python
async def _run_stream(
    request: web.Request,
    *,
    kind: str,
    params: Any,
    formatter: EventFormatter,
    stream_label: str,
    gap_event_name: str = "gap",
    replay: ReplayFn | None = None,
) -> web.StreamResponse:
    _publish_sse_gauge(request.app, stream=stream_label, delta=1)
    try:
        # ...all existing function body unchanged...
    finally:
        _publish_sse_gauge(request.app, stream=stream_label, delta=-1)
```

Then update each of the four call sites:

| Handler | New keyword |
|---|---|
| `get_streams_blocks` (`kind="newHeads"`) | `stream_label="blocks"` |
| `get_streams_logs` (`kind="logs"`) | `stream_label="logs"` |
| `get_streams_pending` (`kind="newPendingTransactions"`) | `stream_label="pending-transactions"` |
| `get_streams_sync_status` (`kind="syncing"`) | `stream_label="sync-status"` |

The labels match the URL path basenames (`/streams/blocks`, `/streams/pending-transactions`, …), so the gauge values align with what an operator sees in the API surface.

- [ ] **Step 6: Run the streams handler tests to verify they pass**

```bash
pytest tests/unit/test_handlers_streams.py -v
```

Expected: all tests pass.

- [ ] **Step 7: Type-check + lint**

```bash
mypy src/exec_rest_api/subscriptions.py src/exec_rest_api/handlers/streams.py
ruff check src/exec_rest_api/subscriptions.py src/exec_rest_api/handlers/streams.py \
    tests/unit/test_subscriptions.py tests/unit/test_handlers_streams.py
```

Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add src/exec_rest_api/subscriptions.py src/exec_rest_api/handlers/streams.py \
    tests/unit/test_subscriptions.py tests/unit/test_handlers_streams.py
git commit -m "Track upstream_subscriptions and sse_connections gauges"
```

---

## Task 6: `GET /metrics` handler

Returns the Prometheus text format from `app["metrics"].render()`. Only registered when `config.metrics_enabled` is true. Always 200 OK with the right `Content-Type`; empty-but-still-valid output when no metrics have been recorded yet.

**Files:**
- Create: `src/exec_rest_api/handlers/metrics.py`
- Create: `tests/unit/test_handlers_metrics.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_handlers_metrics.py`:

```python
"""Tests for the GET /metrics handler."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from aiohttp import web

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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_handlers_metrics.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `handlers/metrics.py`**

Create `src/exec_rest_api/handlers/metrics.py`:

```python
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
        text=registry.render(),
        content_type="text/plain",
        charset="utf-8",
        headers={"Content-Type": PROMETHEUS_CONTENT_TYPE},
    )


def register_routes(app: web.Application) -> None:
    config = app["config"]
    if not getattr(config, "metrics_enabled", True):
        return
    add_get(app, "/metrics", metrics)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_handlers_metrics.py -v
```

Expected: all four tests pass.

- [ ] **Step 5: Type-check + lint**

```bash
mypy src/exec_rest_api/handlers/metrics.py
ruff check src/exec_rest_api/handlers/metrics.py tests/unit/test_handlers_metrics.py
```

Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/handlers/metrics.py tests/unit/test_handlers_metrics.py
git commit -m "Add GET /metrics handler"
```

---

## Task 7: Bootstrap wiring — `__main__.py` + `tests/conftest.py`

Construct the `Metrics` registry, install the composite observer on `UpstreamClient`, start the `ChainHeadTracker`, and register `/metrics`. The same wiring is added (in slimmer form) to `tests/conftest.py` so integration and conformance suites exercise the full pipeline.

The composite observer:

```python
def observer(method: str, status: str, duration_seconds: float) -> None:
    metrics.inc_upstream(method=method, status=status)
    metrics.observe_upstream_duration(method=method, duration_seconds=duration_seconds)
    methods = current_request_upstream_methods.get()
    if methods is not None:
        methods.append(method)
```

**Files:**
- Modify: `src/exec_rest_api/__main__.py`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Modify `__main__.py`**

Edit `src/exec_rest_api/__main__.py`. Add imports near the existing ones:

```python
from exec_rest_api.chain_head import ChainHeadTracker
from exec_rest_api.handlers import metrics as metrics_handler
from exec_rest_api.metrics import Metrics, current_request_upstream_methods
```

In `_run`, replace the block from `connector = aiohttp.TCPConnector(limit=100)` through the end of the function with this version (annotations marked **NEW**):

```python
async def _run(config: Config) -> None:
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=config.upstream_timeout_seconds)
    metrics = Metrics()  # NEW

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        def observe_upstream(method: str, status: str, duration_seconds: float) -> None:  # NEW
            metrics.inc_upstream(method=method, status=status)
            metrics.observe_upstream_duration(method=method, duration_seconds=duration_seconds)
            current = current_request_upstream_methods.get()
            if current is not None:
                current.append(method)

        upstream = UpstreamClient(
            session=session,
            http_url=config.upstream_http,
            default_timeout_seconds=config.upstream_timeout_seconds,
            on_call=observe_upstream,  # NEW
        )

        ws_client = UpstreamWebSocket(
            session=session,
            url=config.upstream_ws,
            on_notification=lambda _: None,
        )
        subscriptions = SubscriptionManager(ws=ws_client, metrics=metrics)  # NEW: metrics
        ws_client.on_notification = subscriptions.on_notification
        ws_client.on_reconnect = subscriptions.on_reconnect

        try:
            await asyncio.wait_for(ws_client.start(), timeout=5.0)
        except (asyncio.TimeoutError, Exception) as exc:
            logging.getLogger("exec_rest_api").warning(
                "upstream WS unreachable at startup (%r); /streams/* will 503 until it recovers",
                exc,
            )

        chain_head = ChainHeadTracker(  # NEW
            upstream=upstream,
            subscriptions=subscriptions,
            metrics=metrics,
        )
        await chain_head.start()

        app = create_app(config=config, upstream=upstream)
        app["subscriptions"] = subscriptions
        app["metrics"] = metrics  # NEW
        app["chain_head"] = chain_head  # NEW
        health.register_routes(app)
        chain.register_routes(app)
        gas.register_routes(app)
        transactions.register_routes(app)
        blocks.register_routes(app)
        accounts.register_routes(app)
        logs.register_routes(app)
        traces.register_routes(app)
        computed.register_routes(app)
        utils_keccak.register_routes(app)
        streams_handler.register_routes(app)
        metrics_handler.register_routes(app)  # NEW

        host, port = _split_listen(config.listen)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()

        logging.getLogger("exec_rest_api").info(
            "listening on http://%s (upstream %s)",
            config.listen,
            config.upstream_http,
            extra={"listen": config.listen, "upstream_http": config.upstream_http},
        )

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
        await runner.cleanup()
        await chain_head.stop()  # NEW
        await ws_client.stop()
```

- [ ] **Step 2: Modify `tests/conftest.py`**

Edit `tests/conftest.py`. Inside `proxy_client`, add the metrics + chain_head wiring. Replace the function body with:

```python
@pytest_asyncio.fixture
async def proxy_client(anvil_url, aiohttp_client):
    """Build the proxy app talking to anvil and return an aiohttp test client."""
    from exec_rest_api.chain_head import ChainHeadTracker
    from exec_rest_api.handlers import metrics as metrics_handler
    from exec_rest_api.handlers import streams as streams_handler
    from exec_rest_api.metrics import Metrics, current_request_upstream_methods
    from exec_rest_api.subscriptions import SubscriptionManager
    from exec_rest_api.upstream_ws import UpstreamWebSocket

    ws_url = anvil_url.replace("http://", "ws://")
    metrics = Metrics()
    async with aiohttp.ClientSession() as session:
        def observe_upstream(method: str, status: str, duration_seconds: float) -> None:
            metrics.inc_upstream(method=method, status=status)
            metrics.observe_upstream_duration(method=method, duration_seconds=duration_seconds)
            current = current_request_upstream_methods.get()
            if current is not None:
                current.append(method)

        upstream = UpstreamClient(
            session=session,
            http_url=anvil_url,
            on_call=observe_upstream,
        )
        ws_client = UpstreamWebSocket(
            session=session,
            url=ws_url,
            on_notification=lambda _: None,
            backoff_schedule=(0.1,),
        )
        manager = SubscriptionManager(ws=ws_client, metrics=metrics)
        ws_client.on_notification = manager.on_notification
        ws_client.on_reconnect = manager.on_reconnect
        with contextlib.suppress(asyncio.TimeoutError, Exception):
            await asyncio.wait_for(ws_client.start(), timeout=5.0)

        chain_head = ChainHeadTracker(
            upstream=upstream,
            subscriptions=manager,
            metrics=metrics,
            poll_interval_seconds=1.0,
        )
        await chain_head.start()

        app = create_app(config=_build_config(anvil_url), upstream=upstream)
        app["subscriptions"] = manager
        app["metrics"] = metrics
        app["chain_head"] = chain_head
        health.register_routes(app)
        chain.register_routes(app)
        gas.register_routes(app)
        transactions.register_routes(app)
        blocks.register_routes(app)
        accounts.register_routes(app)
        logs.register_routes(app)
        traces.register_routes(app)
        computed.register_routes(app)
        utils_keccak.register_routes(app)
        streams_handler.register_routes(app)
        metrics_handler.register_routes(app)
        try:
            client = await aiohttp_client(app)
            yield client
        finally:
            await chain_head.stop()
            await ws_client.stop()
```

- [ ] **Step 3: Run the full unit suite**

```bash
pytest tests/unit -v
```

Expected: every test passes.

- [ ] **Step 4: Run the full integration + conformance suites**

```bash
pytest tests/integration tests/conformance -v
```

Expected: all green if anvil is on PATH; otherwise skipped (with the install hint already in `tests/conftest.py`).

- [ ] **Step 5: Sanity-check the live process**

```bash
scripts/run.sh --upstream-http http://localhost:8545 &
APP_PID=$!
sleep 1
curl -s -i http://127.0.0.1:8080/chain | head -20
curl -s http://127.0.0.1:8080/metrics | head -40
kill $APP_PID
```

Expected:
- The `/chain` response has `X-Request-ID: …`, `X-Upstream-Method: eth_chainId,net_version,web3_clientVersion,eth_syncing,eth_blockNumber`, and `X-Block-Height: <some-number>`.
- The `/metrics` response is Prometheus text with `exec_rest_api_requests_total`, `exec_rest_api_upstream_requests_total`, and `exec_rest_api_chain_head_block` populated.

(Skip if no node is reachable on 8545; integration tests already cover this.)

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/__main__.py tests/conftest.py
git commit -m "Wire Metrics, ChainHeadTracker, and /metrics into bootstrap and test fixture"
```

---

## Task 8: Integration test for `/metrics` against anvil

End-to-end check: hit a couple of endpoints, then assert `/metrics` reports them.

**Files:**
- Create: `tests/integration/test_metrics.py`

- [ ] **Step 1: Write the integration test**

Create `tests/integration/test_metrics.py`:

```python
"""End-to-end /metrics tests against anvil."""

from __future__ import annotations


async def test_metrics_endpoint_reports_requests_and_upstream(proxy_client):
    # Drive a couple of requests through
    await proxy_client.get("/chain")
    await proxy_client.get("/chain/id")
    resp = await proxy_client.get("/metrics")
    assert resp.status == 200
    assert resp.headers["Content-Type"].startswith("text/plain")
    body = await resp.text()
    # Request counters: at minimum, we should see /chain and /chain/id
    assert 'path_template="/chain"' in body
    assert 'path_template="/chain/id"' in body
    # Upstream counters: at minimum, eth_chainId
    assert 'method="eth_chainId"' in body


async def test_metrics_request_returns_x_block_height_when_known(proxy_client):
    import asyncio

    # Wait briefly for the chain-head tracker to populate
    for _ in range(50):
        resp = await proxy_client.get("/chain/id")
        if "X-Block-Height" in resp.headers:
            break
        await asyncio.sleep(0.05)
    assert "X-Block-Height" in resp.headers
    assert int(resp.headers["X-Block-Height"]) >= 0


async def test_metrics_request_returns_x_upstream_method(proxy_client):
    resp = await proxy_client.get("/chain/id")
    assert resp.headers["X-Upstream-Method"] == "eth_chainId"


async def test_metrics_chain_head_gauge_eventually_populated(proxy_client):
    import asyncio

    for _ in range(50):
        resp = await proxy_client.get("/metrics")
        body = await resp.text()
        if "exec_rest_api_chain_head_block " in body:
            break
        await asyncio.sleep(0.05)
    assert "exec_rest_api_chain_head_block " in body
```

- [ ] **Step 2: Run the integration tests**

```bash
pytest tests/integration/test_metrics.py -v
```

Expected: all four pass when anvil is available; skipped otherwise.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_metrics.py
git commit -m "Add integration test for /metrics, X-Block-Height, X-Upstream-Method"
```

---

## Task 9: CI workflow — `.github/workflows/ci.yml`

Triggered on push and on PRs targeting `main`. Matrix: Python 3.10/3.11/3.12 × ubuntu-latest/macos-latest/windows-latest.

Jobs:
1. `lint` — `ruff check`.
2. `typecheck` — `mypy`.
3. `tests` — unit tests on every cell; integration + conformance only where `anvil` can be installed (Linux / macOS via the foundry installer). On Windows the foundry install step is skipped — the existing anvil-not-found check in `tests/conftest.py` causes integration and conformance suites to skip cleanly, leaving unit tests to validate Python compatibility.

The anvil binary is cached via `actions/cache` keyed on a pinned foundry version so we don't re-download on every run.

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

permissions:
  contents: read

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - name: Install dev deps
        run: pip install -e ".[dev]"
      - name: ruff
        run: ruff check src tests

  typecheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - name: Install dev deps
        run: pip install -e ".[dev]"
      - name: mypy
        run: mypy src

  tests:
    name: tests (py${{ matrix.python }} on ${{ matrix.os }})
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python: ["3.10", "3.11", "3.12"]
    env:
      FOUNDRY_VERSION: nightly-2025-04-15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python }}
          cache: pip
      - name: Install dev deps
        run: pip install -e ".[dev]"
      - name: Cache foundry
        if: runner.os != 'Windows'
        id: foundry-cache
        uses: actions/cache@v4
        with:
          path: ~/.foundry/bin
          key: foundry-${{ runner.os }}-${{ env.FOUNDRY_VERSION }}
      - name: Install foundry (if cache miss)
        if: runner.os != 'Windows' && steps.foundry-cache.outputs.cache-hit != 'true'
        run: |
          curl -L https://foundry.paradigm.xyz | bash
          ~/.foundry/bin/foundryup -i ${FOUNDRY_VERSION}
      - name: Add foundry to PATH
        if: runner.os != 'Windows'
        run: echo "$HOME/.foundry/bin" >> $GITHUB_PATH
      - name: Verify anvil
        if: runner.os != 'Windows'
        run: anvil --version
      - name: pytest (with integration on Linux/macOS)
        if: runner.os != 'Windows'
        run: pytest -v --tb=short
      - name: pytest (unit only on Windows)
        if: runner.os == 'Windows'
        run: pytest tests/unit -v --tb=short
```

- [ ] **Step 2: Validate the workflow file syntax locally**

```bash
python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
```

Expected: no output (parses cleanly).

If `act` is installed locally, also try:

```bash
act push --dry-run
```

Expected: prints the parsed workflow with no errors. Skip this if `act` isn't installed.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "Add CI workflow: lint + typecheck + tests on push and PR"
```

---

## Task 10: `Dockerfile` — multi-stage minimal image

Multi-stage build:
1. Builder: `python:3.12-slim` — install build deps, build the wheel.
2. Runtime: `python:3.12-slim` — copy in the wheel + dependencies, run as non-root, expose 8080.

Distroless was tempting but `aiohttp` brings native extensions (yarl, multidict, frozenlist) that bind to glibc; `python:3.12-slim` is the simplest reliable runtime.

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Create `Dockerfile`**

Create `Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1.6

FROM python:3.12-slim AS builder
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip build \
    && python -m build --wheel --outdir /dist

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1
COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl \
    && rm -f /tmp/*.whl
# Non-root user (nobody = 65534) — see operations.md for hardening notes.
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["exec-rest-api"]
CMD ["--listen", "0.0.0.0:8080"]
```

- [ ] **Step 2: Create `.dockerignore`**

Create `.dockerignore`:

```dockerignore
.git
.github
.venv
__pycache__
*.pyc
*.pyo
*.egg-info
.mypy_cache
.ruff_cache
.pytest_cache
.coverage
htmlcov
docs
tests
scripts
.anvil-cache
*.pyz
build
dist
```

- [ ] **Step 3: Build the image locally and smoke-test**

```bash
docker build -t exec-rest-api:dev .
docker run --rm exec-rest-api:dev --version
```

Expected: prints `exec-rest-api 0.5.0` (after the version bump in Task 13).

(If you don't have Docker locally, skip the smoke-test step — the release workflow builds + pushes the same Dockerfile.)

- [ ] **Step 4: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "Add multi-stage Dockerfile (python:3.12-slim, non-root)"
```

---

## Task 11: Release workflow — `.github/workflows/release.yml`

Triggered on `v*` tags. Five concurrent jobs, all using GitHub Actions OIDC (no API tokens):

1. `pypi` — Build sdist + wheel, publish to PyPI via the official Trusted Publishing action.
2. `pyz` — Build a single-file `.pyz` via `shiv`, sign with cosign keyless, attach to the GitHub release.
3. `oci` — Build multi-arch image (`linux/amd64`, `linux/arm64`) via `docker buildx`, push to `ghcr.io/<owner>/exec-rest-api:<tag>`, sign with cosign keyless.
4. `sbom` — Generate a CycloneDX SBOM with `cyclonedx-py`, attach to the GitHub release.
5. `release-notes` — Create the GitHub release as draft with auto-generated notes (GitHub's built-in `generate_release_notes` populates the body from PR titles since the previous tag); other jobs attach artefacts.

For PyPI Trusted Publishing the project must first be registered at https://pypi.org/manage/account/publishing/ (one-time manual setup; documented in `docs/operations.md`).

GHCR namespace uses the repository owner (`github.repository_owner`) for portability.

**Files:**
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: Create the workflow**

Create `.github/workflows/release.yml`:

```yaml
name: Release

on:
  push:
    tags: ["v*"]

permissions:
  contents: write     # create GitHub release
  id-token: write     # OIDC for cosign + PyPI trusted publishing
  packages: write     # push to ghcr.io

concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false

jobs:
  release-notes:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: Draft release with auto-generated notes (from PR titles)
        uses: softprops/action-gh-release@v2
        with:
          draft: true
          name: ${{ github.ref_name }}
          tag_name: ${{ github.ref_name }}
          generate_release_notes: true

  pypi:
    runs-on: ubuntu-latest
    needs: release-notes
    environment:
      name: pypi
      url: https://pypi.org/project/exec-rest-api/
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Build sdist + wheel
        run: |
          pip install --upgrade pip build
          python -m build
      - name: Upload artefacts to release
        uses: softprops/action-gh-release@v2
        with:
          files: dist/*
          tag_name: ${{ github.ref_name }}
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          attestations: true

  pyz:
    runs-on: ubuntu-latest
    needs: release-notes
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install shiv
        run: pip install --upgrade pip shiv
      - name: Build .pyz
        run: |
          shiv -c exec-rest-api -p '/usr/bin/env python3' \
               --reproducible \
               -o exec-rest-api.pyz .
      - name: Generate SHA256SUMS
        run: |
          sha256sum exec-rest-api.pyz > SHA256SUMS
      - uses: sigstore/cosign-installer@v3
      - name: Sign .pyz (cosign keyless)
        env:
          COSIGN_EXPERIMENTAL: "1"
        run: |
          cosign sign-blob --yes \
            --output-signature exec-rest-api.pyz.sig \
            --output-certificate exec-rest-api.pyz.crt \
            exec-rest-api.pyz
      - name: Sign SHA256SUMS (cosign keyless)
        env:
          COSIGN_EXPERIMENTAL: "1"
        run: |
          cosign sign-blob --yes \
            --output-signature SHA256SUMS.sig \
            --output-certificate SHA256SUMS.crt \
            SHA256SUMS
      - name: Attach to release
        uses: softprops/action-gh-release@v2
        with:
          files: |
            exec-rest-api.pyz
            exec-rest-api.pyz.sig
            exec-rest-api.pyz.crt
            SHA256SUMS
            SHA256SUMS.sig
            SHA256SUMS.crt
          tag_name: ${{ github.ref_name }}

  oci:
    runs-on: ubuntu-latest
    needs: release-notes
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - name: Login to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.repository_owner }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - name: Compute image tag (no leading "v" for OCI compatibility)
        id: tag
        run: echo "tag=${GITHUB_REF_NAME#v}" >> "$GITHUB_OUTPUT"
      - name: Build + push (multi-arch)
        id: build
        uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          provenance: true
          sbom: true
          tags: |
            ghcr.io/${{ github.repository_owner }}/exec-rest-api:${{ steps.tag.outputs.tag }}
            ghcr.io/${{ github.repository_owner }}/exec-rest-api:latest
      - uses: sigstore/cosign-installer@v3
      - name: Sign image (cosign keyless)
        env:
          COSIGN_EXPERIMENTAL: "1"
        run: |
          cosign sign --yes \
            ghcr.io/${{ github.repository_owner }}/exec-rest-api@${{ steps.build.outputs.digest }}

  sbom:
    runs-on: ubuntu-latest
    needs: release-notes
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install cyclonedx-py
        run: pip install --upgrade pip cyclonedx-bom
      - name: Generate SBOM
        run: |
          pip install -e .
          cyclonedx-py environment \
            --output-format json \
            --output-file exec-rest-api-sbom.cdx.json
      - name: Attach SBOM to release
        uses: softprops/action-gh-release@v2
        with:
          files: exec-rest-api-sbom.cdx.json
          tag_name: ${{ github.ref_name }}

  publish-release:
    runs-on: ubuntu-latest
    needs: [pypi, pyz, oci, sbom]
    steps:
      - name: Promote draft release to published
        uses: softprops/action-gh-release@v2
        with:
          draft: false
          tag_name: ${{ github.ref_name }}
```

- [ ] **Step 2: Validate the workflow file syntax locally**

```bash
python -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"
```

Expected: no output (parses cleanly).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "Add release workflow: PyPI + .pyz + multi-arch OCI image + SBOM, signed via cosign"
```

---

## Task 12: Operations docs — `docs/operations.md`

Concrete deployment guidance: systemd unit, hardened container invocation, signature verification, PyPI Trusted Publishing setup.

**Files:**
- Create: `docs/operations.md`

- [ ] **Step 1: Write `docs/operations.md`**

Create `docs/operations.md`:

````markdown
# Operations

Deployment and verification guidance for `exec-rest-api`.

## Install options

### PyPI (recommended)

```sh
pipx install exec-rest-api
```

`pipx` keeps the binary isolated in its own virtualenv. Plain `pip install` inside a virtualenv works too.

### Single-file `.pyz`

Download `exec-rest-api.pyz` from the [latest GitHub release](https://github.com/ajsutton/exec-rest-api/releases/latest) and run it directly:

```sh
chmod +x exec-rest-api.pyz
./exec-rest-api.pyz --upstream-http http://localhost:8545
```

Requires Python 3.10+ on `PATH`. The shebang resolves to `/usr/bin/env python3`.

### OCI container

```sh
docker run --rm -p 8080:8080 \
  ghcr.io/<owner>/exec-rest-api:<tag> \
  --upstream-http http://host.docker.internal:8545
```

Recommended hardening:

```sh
docker run --rm \
  --read-only \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --user 65534:65534 \
  -p 8080:8080 \
  ghcr.io/<owner>/exec-rest-api:<tag> \
  --upstream-http http://your-node:8545 \
  --listen 0.0.0.0:8080
```

## Verification

All release artefacts are signed with [cosign](https://docs.sigstore.dev/cosign/) keyless signatures using GitHub Actions OIDC.

### Verify the `.pyz`

Two equivalent options — either verify the `.pyz` directly, or verify `SHA256SUMS` and then check the hash:

```sh
# Option A: verify the .pyz signature directly
cosign verify-blob \
  --certificate exec-rest-api.pyz.crt \
  --signature exec-rest-api.pyz.sig \
  --certificate-identity-regexp '^https://github.com/ajsutton/exec-rest-api/' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  exec-rest-api.pyz

# Option B: verify SHA256SUMS, then check the .pyz against it
cosign verify-blob \
  --certificate SHA256SUMS.crt \
  --signature SHA256SUMS.sig \
  --certificate-identity-regexp '^https://github.com/ajsutton/exec-rest-api/' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  SHA256SUMS
sha256sum -c SHA256SUMS
```

### Verify the OCI image

```sh
cosign verify \
  --certificate-identity-regexp '^https://github.com/ajsutton/exec-rest-api/' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  ghcr.io/<owner>/exec-rest-api:<tag>
```

## systemd unit

`/etc/systemd/system/exec-rest-api.service`:

```ini
[Unit]
Description=Ethereum execution REST API proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
ExecStart=/usr/local/bin/exec-rest-api \
  --upstream-http http://127.0.0.1:8545 \
  --listen 127.0.0.1:8080
Restart=on-failure
RestartSec=2s

# Hardening
DynamicUser=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
NoNewPrivileges=yes
RestrictAddressFamilies=AF_INET AF_INET6
LockPersonality=yes
MemoryDenyWriteExecute=yes
RestrictRealtime=yes
SystemCallArchitectures=native
CapabilityBoundingSet=
AmbientCapabilities=

# Limits
MemoryMax=512M
CPUQuota=200%
TasksMax=128

[Install]
WantedBy=multi-user.target
```

`systemctl daemon-reload && systemctl enable --now exec-rest-api`.

## Metrics

Prometheus scrape target:

```yaml
scrape_configs:
  - job_name: exec-rest-api
    static_configs:
      - targets: ["127.0.0.1:8080"]
    metrics_path: /metrics
```

Disable with `--metrics off` (e.g. for the smallest possible footprint in ad-hoc use).

## Release process (maintainer)

One-time setup:

1. Add the project to PyPI Trusted Publishing: https://pypi.org/manage/account/publishing/
   - Owner: `ajsutton`
   - Repository: `exec-rest-api`
   - Workflow: `release.yml`
   - Environment: `pypi`

To cut a release:

```sh
git tag v0.5.0
git push origin v0.5.0
```

The release workflow draft-creates a GitHub release, publishes to PyPI, builds and signs the `.pyz`, builds and signs the multi-arch OCI image, attaches the SBOM, then promotes the draft to published.
````

- [ ] **Step 2: Commit**

```bash
git add docs/operations.md
git commit -m "Add operations.md: install methods, signature verification, systemd unit"
```

---

## Task 13: README update + version bump + roadmap update

Final task: surface the four install methods + signature verification in the README, bump the package version to `0.5.0`, and mark Plan 5 done in the roadmap.

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/exec_rest_api/__init__.py`
- Modify: `README.md`
- Modify: `docs/superpowers/plans/roadmap.md`

- [ ] **Step 1: Bump version**

Edit `pyproject.toml`. Change:

```toml
version = "0.1.0"
```

to:

```toml
version = "0.5.0"
```

Edit `src/exec_rest_api/__init__.py`. Change:

```python
__version__ = "0.1.0"
```

to:

```python
__version__ = "0.5.0"
```

- [ ] **Step 2: Update README**

Replace `README.md` with:

````markdown
# exec-rest-api

REST + SSE proxy in front of any Ethereum execution client. Talks JSON-RPC
to your upstream node and serves a developer-friendly REST API (RFC 9457
problem details, RFC 8288 cursor pagination, SSE streams, content
negotiation for raw RLP, no hex quantities).

## Status

`v0.5` — feature-complete. Endpoints: `/chain/*`, `/blocks/*`, `/accounts/*`,
`/transactions/*`, `/logs`, `/traces/*`, `/gas/*`, `/utils/keccak256`,
`/health/*`, `/streams/{blocks,logs,pending-transactions,sync-status}`,
`/metrics`.

## Install

Four supported install methods. Pick the one that fits your environment:

### `pipx` (recommended)

```sh
pipx install exec-rest-api
```

### `pip`

```sh
pip install exec-rest-api
```

### Single-file `.pyz`

Download from the [latest release](https://github.com/ajsutton/exec-rest-api/releases/latest):

```sh
curl -LO https://github.com/ajsutton/exec-rest-api/releases/latest/download/exec-rest-api.pyz
chmod +x exec-rest-api.pyz
./exec-rest-api.pyz --upstream-http http://localhost:8545
```

### OCI container

```sh
docker run --rm -p 8080:8080 \
  ghcr.io/ajsutton/exec-rest-api:latest \
  --upstream-http http://host.docker.internal:8545
```

All release artefacts (wheel attestations, `.pyz`, OCI image) are signed via cosign keyless using GitHub Actions OIDC. Verification commands and a hardened systemd unit are in [`docs/operations.md`](docs/operations.md).

## Run

```sh
exec-rest-api --upstream-http http://localhost:8545
```

Or from a source checkout, without any setup steps:

```sh
scripts/run.sh --upstream-http http://localhost:8545
```

(`scripts/run.sh` creates `.venv/` and installs dependencies on first run.)

Then:

```sh
curl http://127.0.0.1:8080/chain
# → { "chainId": 1, "networkId": "1", "client": "Geth/v1.13.5...", "blockNumber": 18234567, "syncing": {"syncing": false} }

curl http://127.0.0.1:8080/health/ready
# → { "ready": true, "upstreamReachable": true, "syncing": false, "blockNumber": 18234567 }

curl http://127.0.0.1:8080/metrics
# → exec_rest_api_requests_total{method="GET",path_template="/chain",status="200"} 1
# → ...
```

Each response carries `X-Request-ID`, `X-Upstream-Method` (the JSON-RPC method(s) invoked), and `X-Block-Height` (current chain head when known) — useful for ad-hoc debugging without touching `/metrics`.

## Configuration

Every CLI flag has an env-var equivalent: `--upstream-http` is also
`EXEC_REST_API_UPSTREAM_HTTP`. Flags override env vars.

| Flag | Env var | Default |
|---|---|---|
| `--upstream-http URL` | `EXEC_REST_API_UPSTREAM_HTTP` | required |
| `--upstream-ws URL` | `EXEC_REST_API_UPSTREAM_WS` | derived from http URL |
| `--listen HOST:PORT` | `EXEC_REST_API_LISTEN` | `127.0.0.1:8080` |
| `--upstream-timeout SECONDS` | `EXEC_REST_API_UPSTREAM_TIMEOUT` | `30` |
| `--log-level LEVEL` | `EXEC_REST_API_LOG_LEVEL` | `info` |
| `--log-format FMT` | `EXEC_REST_API_LOG_FORMAT` | auto (human on TTY, JSON otherwise) |
| `--metrics on\|off` | `EXEC_REST_API_METRICS` | `on` |

Full list: `exec-rest-api --help`.

## Development

```sh
git clone https://github.com/ajsutton/exec-rest-api
cd exec-rest-api
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Integration tests require `anvil` (from
[Foundry](https://book.getfoundry.sh/getting-started/installation)) on PATH;
they are skipped otherwise.

## Operations

See [`docs/operations.md`](docs/operations.md) for systemd, container hardening, and signature verification.

## Design docs

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — API contract.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-openapi.yaml` — OpenAPI 3.1.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — implementation strategy.

## License

Apache 2.0.
````

- [ ] **Step 3: Mark Plan 5 done in the roadmap**

Edit `docs/superpowers/plans/roadmap.md`. Change:

```markdown
## Plan 5 — Observability + release pipeline
```

to:

```markdown
## Plan 5 — Observability + release pipeline `[DONE]`
```

- [ ] **Step 4: Final full-suite check**

```bash
ruff check src tests
mypy src
pytest -v
```

Expected: all green.

- [ ] **Step 5: Sanity-check `--version` reports the new version**

```bash
python -m exec_rest_api --version
```

Expected: `exec-rest-api 0.5.0`.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/exec_rest_api/__init__.py README.md \
    docs/superpowers/plans/roadmap.md
git commit -m "Bump to 0.5.0; update README with install methods and verification; mark Plan 5 done"
```

- [ ] **Step 7: Tag (optional — only when actually ready to release)**

When the maintainer is ready to ship:

```bash
git tag v0.5.0
git push origin v0.5.0
```

This triggers `release.yml` and publishes the full set of signed artefacts.

---

## Plan 5 complete

End state:

- `GET /metrics` returns parseable Prometheus text with all seven series populated (counters for requests/upstream, two histograms, three gauges).
- Every response carries `X-Request-ID`; non-streaming responses additionally carry `X-Upstream-Method` (when the handler called the upstream) and `X-Block-Height` (when the chain head is known).
- A pushed `v*` tag produces: a PyPI release (Trusted Publishing, with attestations), a signed `.pyz` attached to the GitHub release, a signed multi-arch OCI image at `ghcr.io/<owner>/exec-rest-api`, and a CycloneDX SBOM.
- `cosign verify-blob` validates the `.pyz` signature, and `cosign verify` validates the OCI image signature, both using the GitHub Actions OIDC issuer.
- README documents all four install methods; `docs/operations.md` documents systemd, container hardening, and signature verification.
- Full unit + integration + conformance coverage; mypy strict + ruff clean.

The proxy is now production-deployable end-to-end.
