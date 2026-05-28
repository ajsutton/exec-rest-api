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
        out: list[str] = []
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
