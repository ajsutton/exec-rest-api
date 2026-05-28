# Roadmap

The execution REST API ships in five plans. Each plan is independently testable and produces working software. All plans build on the foundation (Plan 1) and follow its conventions: aiohttp + asyncio, TDD per module, full mypy strict, ruff clean, RFC 9457 Problem Details for all errors, and the `add_get` helper for trailing-slash-tolerant route registration.

**Authoritative specs** — consult for any detail not below:

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — API contract (endpoint shapes, encoding, errors, pagination, streams).
- `docs/superpowers/specs/2026-05-28-execution-rest-api-openapi.yaml` — OpenAPI 3.1 (machine-readable, ground truth for schemas).
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — implementation strategy (upstream client, error mapping table, concurrency).

**How to use this doc:** Tell a fresh agent "pick up Plan N from `docs/superpowers/plans/roadmap.md`". The agent reads that section, reads the foundation plan for conventions (`2026-05-28-execution-rest-api-foundation.md`), reads the relevant spec sections, then implements task by task with TDD. A detailed plan can also be written first (using the `writing-plans` skill) if the agent prefers.

---

## Plan 1 — Foundation `[DONE]`

See `2026-05-28-execution-rest-api-foundation.md`.

Delivers: project skeleton, config, upstream JSON-RPC HTTP client, encoding helpers, errors module with JSON-RPC mapping, block-id parser, server scaffolding (3 middlewares + `add_get`), `/chain/*` and `/health/*` handlers, anvil integration fixture, runnable binary, `scripts/run.sh`.

---

## Plan 2 — Read endpoints

GET endpoints for blocks, accounts, transactions, logs (paginated), traces, and gas/fees. Mostly mechanical handler work on top of the foundation; introduces cursor pagination, block-id path parameters, and EIP-7702 delegation detection.

### Endpoints (API spec §3.2–§3.8)

| Path | JSON-RPC |
|---|---|
| `GET /blocks/{id}` | `eth_getBlockBy{Number,Hash}` (full=true) |
| `GET /blocks/{id}/header` | same call, strip transactions[] |
| `GET /blocks/{id}/transactions` | derived from full-tx block |
| `GET /blocks/{id}/transactions/{index}` | `eth_getTransactionByBlock{Number,Hash}AndIndex` |
| `GET /blocks/{id}/transaction-count` | `eth_getBlockTransactionCountBy*` |
| `GET /blocks/{id}/receipts` | `eth_getBlockReceipts` |
| `GET /blocks/{id}/traces` | `trace_block` |
| `GET /accounts/{addr}` | composite: balance + nonce + code + 7702 delegation |
| `GET /accounts/{addr}/balance` | `eth_getBalance` |
| `GET /accounts/{addr}/nonce` | `eth_getTransactionCount` |
| `GET /accounts/{addr}/code` | `eth_getCode` |
| `GET /accounts/{addr}/storage/{slot}` | `eth_getStorageAt` |
| `GET /accounts/{addr}/proof` | `eth_getProof` (slots via `?slots=…`) |
| `GET /accounts/{addr}/transaction-template` | composite: nonce + chainId + fee suggestions |
| `GET /transactions/{hash}` | `eth_getTransactionByHash` |
| `GET /transactions/{hash}/receipt` | `eth_getTransactionReceipt` |
| `GET /transactions/{hash}/trace` | `trace_transaction` |
| `GET /logs` | `eth_getLogs` (paginated) |
| `GET /traces` | `trace_filter` (paginated) |
| `GET /traces/{txHash}/{traceAddress}` | `trace_get` |
| `GET /gas/price` | `eth_gasPrice` |
| `GET /gas/priority-fee` | `eth_maxPriorityFeePerGas` |
| `GET /gas/blob-base-fee` | `eth_blobBaseFee` |
| `GET /gas/fee-history` | `eth_feeHistory` |

### New files

- `src/exec_rest_api/handlers/{blocks,accounts,transactions,logs,traces,gas}.py`
- `src/exec_rest_api/delegation.py` — detect EIP-7702 `0xef0100`-prefixed code; return delegate address or `None`.
- `src/exec_rest_api/cursor.py` — opaque base64url-encoded JSON cursor (encode/decode + tamper-tolerant: invalid → 400).
- `src/exec_rest_api/pagination.py` — internal block-range chunking for `/logs` and `/traces` when upstream caps are exceeded; emits `Link: …; rel="next"` per RFC 8288.
- Unit tests per module; integration tests against anvil per endpoint family.
- Wire each new module's `register_routes` in `src/exec_rest_api/__main__.py` `_run()`.

### Implementation notes (not in spec)

- Block-id path segments use the existing `block_id.parse_block_id`.
- Missing block/tx/account ⇒ 404 Problem (`not-found`), never JSON `null` at the resource root.
- `/accounts/{addr}` composite uses `asyncio.gather` for the three RPC calls. Code is parsed for the 7702 indicator (`code` starts with `0xef0100` and is 23 bytes ⇒ `delegatedTo` = next 20 bytes, else `null`).
- `/blocks/{id}/header` does **one** RPC (full=true) then drops `transactions[]` in the proxy — never two RPCs.
- Pagination cursor contains: `nextFromBlock`, `lastLogIndex`, frozen `toBlock`, boundary block hash (for reorg detection), and the original filter. Treat as opaque to clients.
- `/logs` and `/traces` enforce a `limit` query param (default 1000, max 10000); cursors override all other filter params on the request.
- Reorg detected on follow-up cursor (boundary block hash no longer canonical) ⇒ 409 `chain-reorged`.

### Test approach

Unit tests mock `UpstreamClient` and verify (a) the JSON-RPC method + params sent, (b) the response shape, (c) error mapping. Integration tests use the anvil fixture: deploy an ERC-20-like contract via raw tx submission (anvil pre-funded account), then exercise logs filtering, get-tx-by-hash, balance-of, getProof. Pagination tests run a wide block range with a small `limit` and verify cursor-follow yields contiguous, non-overlapping results.

### Done when

- All endpoints respond per OpenAPI schema (validated by a new `tests/conformance/` runner that loads the OpenAPI YAML and validates real responses against it).
- Logs pagination demonstrably handles upstream cap errors via internal chunking.
- Mypy strict, ruff clean, all tests pass.

---

## Plan 3 — Computed reads + tx submission + RLP content negotiation

POST endpoints that take a body (request bodies can exceed URL limits), transaction submission, and the `Accept: application/vnd.ethereum.rlp` representation on the four GET endpoints that support raw bytes.

### Endpoints (API spec §3.5, §3.7)

| Path | JSON-RPC |
|---|---|
| `POST /transactions` | `eth_sendRawTransaction` (body JSON or RLP) |
| `POST /call` | `eth_call` |
| `POST /gas-estimate` | `eth_estimateGas` |
| `POST /access-list` | `eth_createAccessList` |
| `POST /simulate` | `eth_simulateV1` |
| `POST /traces/call` | `trace_call` |
| `POST /traces/call-many` | `trace_callMany` |
| `POST /traces/raw-transaction` | `trace_rawTransaction` |
| `POST /traces/replay-transaction/{hash}` | `trace_replayTransaction` |
| `POST /blocks/{id}/traces/replay` | `trace_replayBlockTransactions` |
| `POST /debug-traces/call` | `debug_traceCall` |
| `POST /transactions/{hash}/debug-trace` | `debug_traceTransaction` |
| `POST /blocks/{id}/debug-traces` | `debug_traceBlock{ByNumber,ByHash}` |
| `POST /utils/keccak256` | `web3_sha3` (forward verbatim) |
| `POST /logs/search` | `eth_getLogs` with body filter (paginated, shares helpers with `GET /logs`) |
| `POST /accounts/{addr}/proof/search` | `eth_getProof` with body slot list |
| `POST /traces/search` | `trace_filter` with body filter (paginated) |

### Accept-header alternates (API spec §4.7)

On these endpoints, when `Accept: application/vnd.ethereum.rlp` is requested, return raw RLP binary body:

- `GET /blocks/{id}` → `debug_getRawBlock`
- `GET /blocks/{id}/header` → `debug_getRawHeader`
- `GET /blocks/{id}/receipts` → `debug_getRawReceipts`
- `GET /transactions/{hash}` → `debug_getRawTransaction`

Unsupported representation on any GET ⇒ 406 `not-acceptable` Problem.

`POST /transactions` symmetrically accepts `Content-Type: application/vnd.ethereum.rlp` (body is raw RLP bytes) in addition to the JSON form.

### New files

- `src/exec_rest_api/handlers/computed.py` — `/call`, `/gas-estimate`, `/access-list`, `/simulate`, debug-traces/call.
- `src/exec_rest_api/handlers/utils_keccak.py` — single endpoint, forwards to upstream `web3_sha3`.
- `src/exec_rest_api/abi_revert.py` — decode `Error(string)` (selector `0x08c379a0`) and `Panic(uint256)` (selector `0x4e487b71`). Pure byte manipulation, no crypto.
- `src/exec_rest_api/rlp.py` — minimal RLP encode/decode in ~200 LOC (only what's needed for outbound binary; decode not strictly required for v1).
- `src/exec_rest_api/content_neg.py` — `Accept` header parser + selection helper.
- Extend `handlers/transactions.py` (from Plan 2) with `POST /transactions`.
- Unit tests per module; integration tests that deploy + call a reverting contract on anvil and assert the `reverted: true` body shape.

### Implementation notes (not in spec)

- **Reverts are not errors.** When upstream returns `-32000 "execution reverted"` with `data`, return **200** with `{ "reverted": true, "data": "0x…", "reason": …, "panicCode": … }`. Apply to `/call`, `/gas-estimate`, `/access-list`, `/simulate`, `/traces/call`, `/debug-traces/call`. Detect by code AND message substring `"execution reverted"`; never call `map_jsonrpc_error` for reverts (error mapping table excludes them by design).
- Out-of-gas appears as a revert with `outOfGas: true` (detected by upstream message; spec §5.3 lists the patterns).
- `POST /transactions` on success ⇒ **202 Accepted** + `Location: /transactions/{hash}` + body `{ "hash": "0x…" }`. Mempool rejection ⇒ 422 with a `transaction-rejected/*` sub-type per spec §5.4.
- Accept-header negotiation: when `Accept: application/vnd.ethereum.rlp`, the proxy calls `debug_getRaw*` and returns the bytes verbatim with `Content-Type: application/vnd.ethereum.rlp`. JSON-RPC returns hex; the proxy decodes hex → bytes before returning.
- The OpenAPI YAML is the schema source-of-truth for every request/response body; do not invent extra fields.

### Test approach

Unit tests mock upstream; integration tests submit and trace a real transaction on anvil. Specifically: deploy a contract whose function reverts with a string reason; call it via `POST /call` and assert 200 + body has `reverted: true` and the decoded `reason`. For RLP: `GET /blocks/0` with `Accept: application/vnd.ethereum.rlp` returns binary bytes that `debug_getRawBlock` would return verbatim.

### Done when

- Every revert path returns 200 with the discriminated body shape; non-revert errors map cleanly via the existing table.
- Raw-RLP endpoints serve binary bytes matching upstream `debug_getRaw*` output, byte-for-byte.
- Tx submission round-trip works on anvil end-to-end (sign offline → POST raw → GET receipt).

---

## Plan 4 — SSE streams + WS subscription manager

Server-Sent Event streams for `newHeads`, `logs`, `newPendingTransactions`, `syncing`. Introduces the WebSocket connection to the upstream and the subscription multiplexer.

### Endpoints (API spec §3.9, §7)

| Path | Upstream subscription |
|---|---|
| `GET /streams/blocks` | `eth_subscribe("newHeads")` |
| `GET /streams/logs` | `eth_subscribe("logs", filter)` |
| `GET /streams/pending-transactions` | `eth_subscribe("newPendingTransactions")` |
| `GET /streams/sync-status` | `eth_subscribe("syncing")` |

All endpoints emit `text/event-stream`.

### New files

- `src/exec_rest_api/upstream_ws.py` — single persistent WS connection to upstream, exponential backoff reconnect (1 s → 2 s → 5 s → 30 s cap), JSON-RPC framing.
- `src/exec_rest_api/subscriptions.py` — `SubscriptionManager` with multiplexing: one upstream `eth_subscribe` per unique `(kind, params)`, fan-out to N client streams. On WS reconnect, re-issues all active subscriptions and emits `event: gap` to each client.
- `src/exec_rest_api/sse.py` — SSE framing (`event:`, `id:`, `data:`, blank-line terminator), heartbeat (`: ping <ts>` every `--sse-heartbeat-seconds`), Last-Event-ID replay (block/logs replay via `eth_getBlockByNumber`/`eth_getLogs` for missed range, bounded by `--sse-replay-window`), backpressure (drop connection if kernel send buffer exceeds `--sse-buffer-bytes`).
- `src/exec_rest_api/handlers/streams.py` — the four route handlers.
- Tests: unit tests for SSE framing, subscription multiplexing (with mocked WS), replay-on-reconnect, gap emission. Integration tests: subscribe to `newHeads`, watch anvil mine 3 blocks, assert 3 `event: block` lines.

### Implementation notes (not in spec)

- Initialize `SubscriptionManager` in `__main__.py` `_run()` alongside `UpstreamClient`; pass it to `app["subscriptions"]`. Subscription manager owns its own asyncio task that drives the WS read loop.
- Pre-stream errors (bad filter, unsupported upstream method) return normal Problem+JSON before any SSE body is sent.
- Mid-stream errors emit `event: error\ndata: <problem JSON>\n\n` then close the connection; client auto-reconnects via `Last-Event-ID`.
- Event IDs are derived from chain data: blocks ⇒ `<blockNumber>`, logs ⇒ `<blockNumber>-<logIndex>`, pending ⇒ tx hash, sync ⇒ monotonic counter.
- `pending-transactions` and `sync-status` have no replay; on reconnect emit one `event: resumed` and continue live.
- `?full=true` on `/streams/pending-transactions` passes the optional second arg to `eth_subscribe("newPendingTransactions", true)` (Geth/Erigon); fall back to hashes if upstream rejects.
- Multiplexing: a second client requesting the same `(kind, params)` shares the upstream subscription. Reference-counted; teardown happens when the last consumer disconnects.

### Test approach

Stand up anvil with `--block-time 1`; subscribe via SSE to `/streams/blocks`, collect events for 3 seconds, assert ≥ 2 `event: block` frames with increasing IDs. For replay: subscribe, disconnect, reconnect with `Last-Event-ID` pointing 2 blocks back, assert the missed blocks come through as `event: block` before the live stream resumes. For multiplexing: open two SSE clients to the same filter, assert that upstream `eth_subscribe` is called once (mock the WS layer to count).

### Done when

- All four streams emit correct framing against anvil and survive a WS reconnect with `event: gap` notification.
- `Last-Event-ID` replay works within the configured window for blocks and logs.
- A reorg on the logs stream re-emits affected logs with `removed: true`.

---

## Plan 5 — Observability + release pipeline

Operational polish: Prometheus metrics, structured-log refinement, OpenTelemetry-style request-ID propagation already in place, plus the full publish pipeline (PyPI / `.pyz` / OCI image, all signed via GitHub Actions OIDC).

### Observability

- `src/exec_rest_api/metrics.py` — in-memory counters/gauges/histograms (no client library; hand-format Prometheus text). Exports:
  - `exec_rest_api_requests_total{method,path_template,status}` counter
  - `exec_rest_api_request_duration_seconds` histogram
  - `exec_rest_api_upstream_requests_total{method,status}` counter
  - `exec_rest_api_upstream_duration_seconds` histogram
  - `exec_rest_api_sse_connections{stream}` gauge
  - `exec_rest_api_upstream_subscriptions{stream}` gauge
  - `exec_rest_api_chain_head_block` gauge
- `GET /metrics` handler returning `text/plain; version=0.0.4` (Prometheus format). Toggle with `config.metrics_enabled`.
- Middleware (extend `server.py`): time every request, count by method/path-template/status. Extend upstream client to record per-method timing.
- Set `X-Upstream-Method` response header on every response (diagnostic; per spec §5.5).
- Set `X-Block-Height` response header where the proxy knows the chain head (cache the latest `eth_blockNumber` in `SubscriptionManager.newHeads` consumer; if no WS, periodic poll).

### Release pipeline

- `.github/workflows/ci.yml` — lint + typecheck + unit + integration + conformance on push/PR. Matrix: Python 3.10/3.11/3.12 × ubuntu/macos/windows. Cache anvil binary across runs.
- `.github/workflows/release.yml` — triggered on `v*` tag:
  - Build sdist + wheel; publish to PyPI via Trusted Publishing (OIDC, no API tokens).
  - Build single-file `.pyz` via `shiv`; attach to release.
  - Build multi-arch OCI image (`linux/amd64`, `linux/arm64`) via `docker buildx`; push to `ghcr.io/<org>/exec-rest-api`; sign with `cosign` (keyless via GitHub OIDC).
  - Generate CycloneDX SBOM via `cyclonedx-py`; attach to release and image.
  - Generate `SHA256SUMS`; sign with `cosign sign-blob`; attach.
  - Auto-draft release notes from merged PR titles since previous tag.
- `Dockerfile` — multi-stage from `python:3.12-slim` (or `gcr.io/distroless/python3` if dep tree allows); `USER 65534:65534`, `EXPOSE 8080`, `ENTRYPOINT ["exec-rest-api"]`.
- Operational docs in `docs/operations.md`: systemd unit example with `DynamicUser=yes`, `ProtectSystem=strict`, `NoNewPrivileges=yes`, `RestrictAddressFamilies=AF_INET AF_INET6`, `MemoryMax`, `CPUQuota`. Container deployment notes (`--read-only`, `--cap-drop=ALL`, `--security-opt=no-new-privileges`).

### Test approach

Unit tests for metrics (counter increments, histogram observations, text format output). CI workflow tested via `act` locally before pushing. Release workflow tested on a throwaway tag (`v0.0.0-test`) targeting a test PyPI repo / ghcr.io tag prefix.

### Done when

- `GET /metrics` returns parseable Prometheus text with all listed series populated.
- A pushed `v*` tag results in a PyPI release, a `.pyz` attached to the GitHub release, and a signed OCI image at `ghcr.io/...`.
- `cosign verify` validates the OCI image signature and the `.pyz` blob signature using the GitHub Actions OIDC issuer.
- README updated with all four install methods and verification examples.
