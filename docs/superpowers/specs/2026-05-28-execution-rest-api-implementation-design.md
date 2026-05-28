# Execution REST API — Implementation Design

**Date:** 2026-05-28
**Status:** Approved design, ready for writing-plans.
**Companion docs:** `2026-05-28-execution-rest-api-design.md` (API contract), `2026-05-28-execution-rest-api-openapi.yaml` (OpenAPI 3.1).

## 1. Goals & constraints

The proxy from the companion API design must be:

- **Trivially installable on any host.** No "find the right binary for your arch" step. No compile toolchain on the target machine. Drop-in for ad-hoc use ("I want a better API to query this node, spin one up now") and equally fine as a long-running service.
- **Easy to maintain.** Small, boring code. One language, minimal moving parts.
- **Highly secure.** Tiny attack surface. Minimal supply-chain exposure. No key custody, no signing, no curve math.
- **Performant enough.** Proxy is I/O-bound; thousands of req/s and thousands of concurrent SSE streams on a single core is plenty.

## 2. Language and runtime

**Python 3.10+.**

Selected over Go / Rust because:

- Python ships with virtually every Linux distribution and macOS install. No "download the right binary for your arch / OS / libc" step.
- No compile toolchain required on the target host.
- asyncio's I/O performance is more than sufficient for a proxy workload that's dominated by upstream latency.
- The implementation has no CPU-bound hot path. We do no crypto; we do no parsing more demanding than JSON.
- 3.10 is the floor because it's available on every currently-supported LTS distro (Ubuntu 22.04, RHEL 9, Debian 12) and gives us `match`-statement pattern matching, which is materially nicer for JSON-RPC method dispatch.

## 3. Dependencies

### Runtime

| Dep | Purpose |
|---|---|
| `aiohttp` | HTTP server + HTTP client + WebSocket client (single library covering all three). |

That is the entire runtime dependency tree. Everything else is hand-written and lives in this repo.

### Development-only

`pytest`, `pytest-asyncio`, `hypothesis`, `ruff`, `mypy`, `jsonschema`, `pip-audit`, `cyclonedx-py`, `shiv`. None of these ship with the binary or wheel.

### Zero crypto code

The proxy carries no Keccak-256 implementation, and therefore no other crypto dependency:

- The API surfaces addresses as **lowercase** (EIP-55 checksumming dropped from the API design as a consequence).
- The `/utils/keccak256` endpoint is implemented by forwarding the input to the upstream's `web3_sha3` JSON-RPC method, which is part of every execution client's standard surface. The proxy never computes a hash.
- EIP-7702 delegation detection is pure byte comparison: check the first 3 bytes of `eth_getCode` output for `0xef0100`, then slice the next 20 bytes. No hashing involved.

This removes the single largest supply-chain risk category (cryptographic libraries are disproportionately targeted), and makes the dep audit boundary literally one library wide.

## 4. Project layout

```
exec-rest-api/
├── pyproject.toml              # PEP 621 metadata, pinned runtime deps
├── requirements.lock           # pip-compile --generate-hashes output (runtime)
├── requirements-dev.lock       # dev tools, separate
├── src/
│   └── exec_rest_api/
│       ├── __init__.py
│       ├── __main__.py         # `python -m exec_rest_api` entrypoint
│       ├── config.py           # CLI parsing + env-var resolution
│       ├── server.py           # aiohttp Application factory + middleware chain
│       ├── upstream.py         # JSON-RPC HTTP client + WS subscription manager
│       ├── handlers/
│       │   ├── chain.py
│       │   ├── gas.py
│       │   ├── blocks.py
│       │   ├── accounts.py
│       │   ├── transactions.py
│       │   ├── logs.py
│       │   ├── computed.py     # /call, /gas-estimate, /access-list, /simulate
│       │   ├── traces.py
│       │   ├── streams.py
│       │   ├── utils_keccak.py # forwards to web3_sha3
│       │   └── health.py
│       ├── encoding.py         # JSON-RPC ↔ REST type conversion
│       ├── block_id.py         # block identifier parser (tag / number / hash)
│       ├── content_neg.py      # Accept-header negotiation (JSON vs RLP)
│       ├── rlp.py              # hand-rolled RLP encode/decode (~200 LOC)
│       ├── abi_revert.py       # Error(string) / Panic(uint256) decoder (~50 LOC)
│       ├── delegation.py       # EIP-7702 0xef0100-prefix detection
│       ├── cursor.py           # base64url pagination cursor
│       ├── errors.py           # RFC 9457 problem details + upstream error mapping
│       ├── pagination.py       # /logs and /traces internal block-range chunking
│       └── sse.py              # SSE framing, heartbeat, backpressure, Last-Event-ID
└── tests/
    ├── unit/                   # one test module per source module
    ├── integration/            # against anvil
    └── conformance/            # validates every endpoint against the OpenAPI spec
```

### Module boundaries

- `handlers/*` contain request handlers only — parse params, call upstream, shape response. No business logic.
- `upstream.py` owns JSON-RPC translation: takes a method name + Python args, returns Python values. Handlers never touch hex encoding directly.
- `encoding.py` is the single place that knows about hex ↔ decimal conversion, status enum mapping, and address case normalization. Both handlers and `upstream.py` use it.
- `streams.py` does not subscribe directly — it asks `upstream.SubscriptionManager` for a filtered event stream, which handles WS lifecycle, multiplexing, and replay.

## 5. Configuration

CLI flags with environment-variable equivalents (each `--upstream-http` is also `EXEC_REST_API_UPSTREAM_HTTP`). No configuration file in v1 — the surface is small enough that flags + env vars cover it.

```
Usage: exec-rest-api [OPTIONS]

Required:
  --upstream-http URL          JSON-RPC HTTP endpoint (e.g. http://localhost:8545)

Optional:
  --upstream-ws URL            JSON-RPC WS endpoint (default: derived from http URL).
                               If unreachable, /streams/* return 501.
  --listen ADDR                Listen address (default: 127.0.0.1:8080)
  --upstream-timeout SECONDS   Per-request timeout (default: 30)
  --max-page-size N            /logs and /traces max items per page (default: 10000)
  --default-page-size N        /logs and /traces default items per page (default: 1000)
  --sse-buffer-bytes N         SSE backpressure threshold (default: 65536)
  --sse-replay-window N        Max blocks replayable on SSE reconnect (default: 1024)
  --sse-heartbeat-seconds N    SSE heartbeat interval (default: 30)
  --ready-sync-lag N           Max blocks behind to be "ready" (default: 10)
  --log-format FMT             human|json (default: human if TTY, json otherwise)
  --log-level LEVEL            debug|info|warn|error (default: info)
  --metrics                    Enable /metrics endpoint (default: enabled; pass `off` to disable)
  --version
```

Defaults are tuned for ad-hoc use (localhost listen, modest limits). Operators tune for production.

## 6. Observability

### Logging

stdlib `logging` with a JSON formatter for non-TTY output, human-readable for TTY. One log line per HTTP request with: `method`, `path`, `status`, `latency_ms`, `upstream_method`, `upstream_latency_ms`, `request_id`. SSE streams log one connect line and one disconnect line — never per-event (too noisy).

### Metrics

`GET /metrics` exposes Prometheus text format, hand-written from an in-memory counter map (no client library — text format is trivial to emit):

- `exec_rest_api_requests_total{method,path_template,status}` — counter
- `exec_rest_api_request_duration_seconds` — histogram
- `exec_rest_api_upstream_requests_total{method,status}` — counter
- `exec_rest_api_upstream_duration_seconds` — histogram
- `exec_rest_api_sse_connections{stream}` — gauge
- `exec_rest_api_upstream_subscriptions{stream}` — gauge
- `exec_rest_api_chain_head_block` — gauge

Disabled with `--metrics=off` for minimal ad-hoc footprint.

### Request IDs

`X-Request-ID` is honored if the client sends it; otherwise generated (UUID4). Propagated to upstream as `X-Request-ID`. Logged on every line related to the request.

## 7. Upstream connection management

### HTTP client

Single `aiohttp.ClientSession` per process:

- Connection pool to the upstream HTTP endpoint (default limit: 100 concurrent connections).
- HTTP/1.1 keep-alive.
- Default per-request timeout from `--upstream-timeout` (30s). Trace/debug methods get longer overrides.
- No retries by default — JSON-RPC isn't universally idempotent. The sole exception is one retry on transient connection errors (TCP RST, EOF mid-response) for clearly read-only methods. Real errors (HTTP 4xx/5xx, JSON-RPC error responses) are never retried.

### Per-request JSON-RPC, not batching

Each upstream call is a single JSON-RPC 2.0 request. We don't use JSON-RPC batching because:

- The REST API doesn't expose batching.
- A single REST request maps to 1 (occasionally 2) upstream calls.
- Batching complicates error handling per sub-response.

REST endpoints that fan out to multiple upstream calls (e.g. `GET /chain`, `GET /accounts/{addr}`) do so via `asyncio.gather`, not batching.

### WS subscription manager

`upstream.SubscriptionManager` owns the WebSocket lifecycle:

1. Maintains a single persistent WS connection to the upstream. On disconnect, reconnects with exponential backoff (1s → 2s → 5s → 30s cap).
2. Translates "client wants newHeads" / "client wants logs(filter)" into upstream `eth_subscribe` calls. Holds **one upstream subscription per unique (kind, filter) tuple**, reusing it across all client SSE streams with identical filters.
3. Fans out incoming subscription messages to all registered local consumers.
4. On upstream reconnect, re-issues all active `eth_subscribe` calls (subscription IDs do not survive WS reconnects). Emits a synthetic `event: gap` to each affected client SSE stream so they know there was a discontinuity.

Interface:

```python
class SubscriptionManager:
    async def subscribe(
        self,
        kind: Literal["newHeads", "logs", "newPendingTransactions", "syncing"],
        params: dict | None,
    ) -> AsyncIterator[dict]:
        """Yields events for this (kind, params). Multiplexed across consumers."""
```

## 8. Concurrency model

- **Single process, single event loop** by default. Plenty for the targeted workload.
- **Multi-process scaling** for production: deploy N copies behind a load balancer. Each process holds its own upstream connection pool and WS connection. We deliberately do not share WS subscriptions across processes — the complexity isn't worth the upstream-load savings.
- **No threading inside the process.** Pure asyncio (aiohttp may use a small thread pool internally for DNS; invisible to us).

## 9. Backpressure

- **HTTP responses:** aiohttp handles flow control via coroutine suspension. A slow client only stalls its own handler.
- **SSE streams:** the proxy monitors the per-connection send-buffer high-water mark. When it exceeds `--sse-buffer-bytes`, the connection is dropped. The client's automatic SSE reconnect resumes via `Last-Event-ID` (with replay for blocks/logs streams). This bounds proxy memory under pathological consumers.
- **Upstream WS:** if a client SSE consumer can't keep up, dropping it is correct. The upstream WS is shared, so a slow client must not stall delivery to other clients.

## 10. Error mapping (JSON-RPC → RFC 9457)

A single `errors.py` mapper translates upstream JSON-RPC errors into Problem+JSON responses per §5 of the API design.

| JSON-RPC `error.code` | Pattern in `error.message` | HTTP | `type` |
|---|---|---|---|
| -32600 | (invalid request) | 400 | `invalid-request` |
| -32601 | "method not found" | 501 | `method-not-supported-by-upstream` |
| -32602 | (invalid params) | 400 | `invalid-request` |
| -32603 | (internal error) | 502 | `upstream-error` |
| -32700 | (parse error) | 502 | `upstream-error` |
| -32000 | "nonce too low" | 422 | `transaction-rejected/nonce-too-low` |
| -32000 | "already known" | 422 | `transaction-rejected/already-known` |
| -32000 | "replacement transaction underpriced" | 422 | `transaction-rejected/replacement-underpriced` |
| -32000 | "transaction underpriced" | 422 | `transaction-rejected/underpriced` |
| -32000 | "insufficient funds" | 422 | `transaction-rejected/insufficient-funds` |
| -32000 | "intrinsic gas too low" | 422 | `transaction-rejected/intrinsic-gas-too-low` |
| -32000 | "exceeds block gas limit" | 422 | `transaction-rejected/gas-limit-exceeded` |
| -32000 | "execution reverted" | 200 (handled in body — see API design §5.3) | — |
| -32000 | "query returned more than … results" | 413 | `payload-too-large` |
| -32000 | "exceed maximum block range" | 413 | `payload-too-large` |
| -32001 | (resource not found) | 404 | `not-found` |
| -32002 | (resource unavailable) | 503 | `upstream-unavailable` |
| -32003 | (transaction rejected) | 422 | `transaction-rejected` |
| -32004 | "method not supported" | 501 | `method-not-supported-by-upstream` |
| -32005 | "limit exceeded" | 429 | `rate-limited` |
| any (-32000..-32099) unmatched | — | 502 | `upstream-error` |

Pattern matches are lowercase substring checks. Vendor quirks (Erigon vs Geth phrasing) get test cases. Unmatched falls through to generic 502 with upstream message in `detail` and JSON-RPC code preserved in `code`.

### Revert decoding

When an `eth_call` / `eth_estimateGas` / `eth_createAccessList` / etc. returns `-32000` with "execution reverted" and `data` is present:

- If `data` starts with `0x08c379a0` (`Error(string)` selector) → decode the ABI string from offset 4 into `reason`.
- If `data` starts with `0x4e487b71` (`Panic(uint256)` selector) → decode the uint256 from offset 4 into `panicCode`.
- Otherwise → both `null`, raw `data` passed through.

Pure byte manipulation; no crypto, no dependencies.

## 11. Request lifecycle

```
client request → aiohttp app
              → request-id middleware (generates or honors X-Request-ID)
              → access-log middleware
              → metrics middleware (timing, counters)
              → content-negotiation middleware (selects representation)
              → handler
                → upstream.call("eth_xxx", [...])     # encoding.py converts args
                ← response
              ← shaped response                        # encoding.py converts back
              → error-mapping middleware (catches & maps)
              → response sent
```

## 12. Testing strategy

### Unit tests (`tests/unit/`)

One test module per source module. Heavy use of fixtures that return canned JSON-RPC responses so handlers can be exercised in isolation without an upstream. Targets: 95%+ coverage of `encoding.py`, `rlp.py`, `abi_revert.py`, `block_id.py`, `cursor.py`, `delegation.py`, `errors.py` — the pure-function code where correctness is critical.

### Integration tests (`tests/integration/`)

Spin up `anvil` (Foundry's local execution client) on a free port, point the proxy at it, hit every endpoint. Covers:

- End-to-end request flow through the real aiohttp stack.
- Real WebSocket subscriptions for SSE streams.
- Real transaction submission and mining.
- Real revert behavior (deploy a small `revert(...)` contract).

Anvil is a single static binary; CI downloads and caches it. Tests pin a known anvil version.

### Conformance tests (`tests/conformance/`)

Parse the OpenAPI YAML and verify, for every operation:

- The path is reachable.
- The response `Content-Type` matches the spec.
- The response body validates against the response schema (via `jsonschema`).
- Error responses validate against `Problem`.

Catches drift between spec and implementation. Spec is the source of truth.

### Property tests (sparing)

For encoders/decoders with crisp invariants — `decode(encode(x)) == x` for RLP, base64url cursor, block-id parser; "any decimal string we accept is the same number we emit on read". Hypothesis is the harness.

### Static checks

- `ruff` — lint + format.
- `mypy` — strict mode.
- `pip-audit` — CVE scan on the lockfile each CI run.

### CI matrix

GitHub Actions on push and PR:

- Python: 3.10, 3.11, 3.12 (latest patch of each).
- OS: ubuntu-latest, macos-latest, windows-latest.
- Stages: lint → typecheck → unit → integration → conformance.

## 13. Release pipeline

Triggered by pushing a `v*` tag. One workflow builds and publishes:

1. **PyPI package** — via PyPI Trusted Publishing (OIDC from GitHub Actions; no API tokens stored).
2. **`.pyz` zipapp** — built with `shiv`, named `exec-rest-api-<version>.pyz`. One-file, runs as `python3 exec-rest-api-<version>.pyz …`.
3. **OCI image** — multi-arch (linux/amd64, linux/arm64) via `docker buildx`. Built from a minimal base (`python:3.12-slim` or `gcr.io/distroless/python3`). Published to `ghcr.io/<org>/exec-rest-api`.
4. **GitHub release** — assets: `.pyz`, source tarball, `SHA256SUMS`, `SHA256SUMS.sig`. Release notes from merged PR titles since the prior tag.
5. **SBOM** — CycloneDX JSON, generated by `cyclonedx-py` from the locked dep tree, attached to the release and to the OCI image.

### Signing

- **PyPI:** Trusted Publishing with sigstore-backed provenance attestations.
- **OCI image:** signed with `cosign` using GitHub Actions' OIDC identity (keyless).
- **GitHub release assets:** `SHA256SUMS` signed with `cosign sign-blob`; `.pyz` independently signed too so a single-file download is independently verifiable.

### Reproducibility (best-effort)

- Locked deps with hashes (`requirements.lock`).
- Pinned Python version in CI.
- `SOURCE_DATE_EPOCH` set during builds.
- `.pyz` produced reproducibly by `shiv`.
- OCI image: cosign attestation links to the exact commit + GHA run so verifiers can rebuild and compare.

### Supply-chain hygiene

- **Lockfile required** for every install path.
- **Renovate** for dep updates, auto-merge disabled. Every dep PR is human-reviewed.
- **One runtime dep** (`aiohttp`) bounds the review burden. New `aiohttp` versions are read before being accepted.
- **No post-install scripts**, no source builds during install, no dynamic dep resolution.
- **`pip install --require-hashes`** in container builds.

## 14. Deployment recommendations (documented, not enforced)

Install docs include:

- **systemd unit:** with `DynamicUser=yes`, `ProtectSystem=strict`, `NoNewPrivileges=yes`, `PrivateTmp=yes`, `RestrictAddressFamilies=AF_INET AF_INET6`, `MemoryMax`, `CPUQuota`. The proxy needs no filesystem access at runtime.
- **Container:** run with `--read-only`, `--cap-drop=ALL`, `--security-opt=no-new-privileges`, non-root user (`USER 65534:65534` in the image), no volumes.
- **Ad-hoc binary:** documented as "trusted local use only" — the threat model assumes you're on the same machine or LAN as the upstream node.

## 15. Out of scope for v1

- **Authentication.** Operators front the proxy with their existing API gateway / TLS terminator.
- **Configuration file.** CLI flags + env vars cover the surface.
- **Multi-chain routing.** One proxy = one upstream = one chain.
- **OpenTelemetry trace propagation.** Add later if needed (the request-ID flow gives us a single-hop equivalent for now).
- **Persistent state.** The proxy is stateless. SSE subscription state is per-connection only.

## 16. Decision summary

- **Language:** Python 3.10+, asyncio.
- **Framework:** aiohttp (sole runtime dep).
- **Crypto code in the proxy:** none.
- **API artifacts:** PyPI package, `.pyz`, OCI image, signed GitHub release.
- **Signing:** sigstore/cosign keyless via GitHub Actions OIDC.
- **Testing:** unit + integration (against anvil) + conformance (against the OpenAPI doc).
- **Observability:** structured JSON logs, hand-written Prometheus metrics, `/health` + `/health/ready`.
