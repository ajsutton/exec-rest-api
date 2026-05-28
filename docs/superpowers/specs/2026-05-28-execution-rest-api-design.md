# Ethereum Execution REST API — Design

**Date:** 2026-05-28
**Status:** Approved design, ready for implementation planning.

## 1. Goals & non-goals

### Goals

- Provide a developer-friendly REST + SSE API that maps onto the standard Ethereum execution client JSON-RPC surface (`eth_*`, `net_*`, `web3_*`, `debug_*`, `trace_*`).
- Implementable as a **stateless reverse proxy** in front of any unmodified execution client. No client modifications required.
- Eliminate hex-encoded quantities from the developer experience.
- First-class HTTP resource modeling (blocks, accounts, transactions, logs, traces).
- Use HTTP idioms: status codes, content negotiation, RFC 9457 problem details, RFC 8288 Link pagination, WHATWG Server-Sent Events.

### Non-goals

- **Signing or key custody.** The proxy never holds private keys. `eth_sign`, `eth_signTransaction`, `eth_sendTransaction`, `eth_accounts`, `eth_coinbase` are not exposed.
- **JSON-RPC escape hatch.** There is no `/rpc` pass-through endpoint. Every supported method must be modeled.
- **Authentication.** Operators front the proxy with their existing API gateway / TLS terminator for auth, rate limiting, and tenancy.
- **Multi-chain routing.** One proxy instance serves one upstream serving one chain. Multi-chain deployments compose multiple proxy instances at a higher layer.
- **Filter-poll API.** `eth_newFilter` and friends are superseded by SSE streams plus paginated `/logs`.
- **PoW relics.** `eth_mining`, `eth_hashrate`, `eth_protocolVersion`, all `eth_getUncle*` methods are not exposed.
- **Compiler methods.** `eth_compileSolidity` etc. are not exposed.

## 2. Architecture

```
┌──────────┐ HTTP/JSON      ┌──────────────┐ JSON-RPC over HTTP  ┌────────────────┐
│  client  │ ─────────────▶ │              │ ───────────────────▶│                │
│          │ ◀───────────── │   REST/SSE   │ ◀───────────────────│ execution      │
│          │                │   proxy      │                     │ client         │
│          │ SSE            │              │ JSON-RPC over WS    │ (geth/reth/    │
│          │ ─────────────▶ │              │ ───────────────────▶│  besu/erigon/  │
│          │ ◀───────────── │              │ ◀───────────────────│  nethermind)   │
└──────────┘                └──────────────┘                     └────────────────┘
```

- **Stateless.** No database. No per-user state. SSE streams hold transient subscription state per connection only.
- **One upstream per proxy.** The chain identity is implied by the upstream and surfaced via `GET /chain/id`.
- **Upstream feature negotiation.** If the upstream lacks a method (e.g. `trace_*` on Geth without `--gcmode archive`), the corresponding REST endpoint returns `501 Not Implemented` with a problem document.
- **No URL versioning.** Future versioning, if needed, uses media-type content negotiation (`Accept: application/vnd.ethereum-exec+json; version=N`).

## 3. Resource map

Path conventions:

- `{id}` — block identifier (see §4.3 for grammar).
- `{addr}` — `0x`-prefixed Ethereum address.
- `{hash}` — 32-byte hash (block, transaction, etc.).
- `?at={id}` — block-context query for state-reading endpoints; defaults to `latest`.

### 3.1 Chain & node

| Path | Method | JSON-RPC mapping |
|---|---|---|
| `/chain` | GET | Composite: `eth_chainId` + `net_version` + `web3_clientVersion` + `eth_syncing` + `eth_blockNumber` |
| `/chain/id` | GET | `eth_chainId` |
| `/chain/sync-status` | GET | `eth_syncing` |
| `/chain/client` | GET | `web3_clientVersion` |
| `/chain/peers` | GET | `net_peerCount` + `net_listening` |

### 3.2 Gas & fees

| Path | Method | JSON-RPC mapping |
|---|---|---|
| `/gas/price` | GET | `eth_gasPrice` |
| `/gas/priority-fee` | GET | `eth_maxPriorityFeePerGas` |
| `/gas/blob-base-fee` | GET | `eth_blobBaseFee` |
| `/gas/fee-history` | GET | `eth_feeHistory` (query: `blockCount`, `newest`, `rewardPercentiles`) |

### 3.3 Blocks

| Path | Method | Default body | Alternate via `Accept` |
|---|---|---|---|
| `/blocks/{id}` | GET | JSON block with full transactions (`eth_getBlockBy{Number,Hash}` full=true) | `application/vnd.ethereum.rlp` → `debug_getRawBlock` |
| `/blocks/{id}/header` | GET | JSON header fields only (block fetched, transactions stripped) | `application/vnd.ethereum.rlp` → `debug_getRawHeader` |
| `/blocks/{id}/transactions` | GET | JSON array of full tx objects (derived from same fetch) | — |
| `/blocks/{id}/transactions/{index}` | GET | JSON tx (`eth_getTransactionByBlock{Number,Hash}AndIndex`) | — |
| `/blocks/{id}/transaction-count` | GET | `{ "count": N }` (`eth_getBlockTransactionCountBy*`) | — |
| `/blocks/{id}/receipts` | GET | JSON array of receipts (`eth_getBlockReceipts`) | `application/vnd.ethereum.rlp` → `debug_getRawReceipts` |
| `/blocks/{id}/traces` | GET | JSON traces (`trace_block`) | — |
| `/blocks/{id}/traces/replay` | POST | `trace_replayBlockTransactions` | — |
| `/blocks/{id}/debug-traces` | POST | `debug_traceBlock{ByNumber,ByHash}` | — |

### 3.4 Accounts

| Path | Method | JSON-RPC mapping |
|---|---|---|
| `/accounts/{addr}` | GET | Composite: balance + nonce + has-code + EIP-7702 delegation (`?at={id}`) |
| `/accounts/{addr}/balance` | GET | `eth_getBalance` (`?at={id}`) |
| `/accounts/{addr}/nonce` | GET | `eth_getTransactionCount` (`?at={id}`) |
| `/accounts/{addr}/code` | GET | `eth_getCode` (`?at={id}`) |
| `/accounts/{addr}/storage/{slot}` | GET | `eth_getStorageAt` (`?at={id}`) |
| `/accounts/{addr}/proof` | GET | `eth_getProof` (`?at={id}&slots=…`) |
| `/accounts/{addr}/proof/search` | POST | `eth_getProof` with body filter (for long slot lists) |
| `/accounts/{addr}/transaction-template` | GET | Composite "prepare" helper: nonce + chainId + fee suggestions |

### 3.5 Transactions

| Path | Method | Default body | Alternate via `Accept` |
|---|---|---|---|
| `/transactions` | POST | `eth_sendRawTransaction` — body `{ "raw": "0x…" }` or RLP via `Content-Type: application/vnd.ethereum.rlp` | — |
| `/transactions/{hash}` | GET | JSON tx (`eth_getTransactionByHash`) | `application/vnd.ethereum.rlp` → `debug_getRawTransaction` |
| `/transactions/{hash}/receipt` | GET | JSON receipt (`eth_getTransactionReceipt`) | — |
| `/transactions/{hash}/trace` | GET | JSON trace (`trace_transaction`) | — |
| `/transactions/{hash}/trace/replay` | POST | `trace_replayTransaction` | — |
| `/transactions/{hash}/debug-trace` | POST | `debug_traceTransaction` | — |

### 3.6 Logs

| Path | Method | JSON-RPC mapping |
|---|---|---|
| `/logs` | GET | `eth_getLogs` — query params: `fromBlock`, `toBlock`, `address`, `topic0`..`topic3`, `limit`, `cursor` |
| `/logs/search` | POST | `eth_getLogs` with full filter body (large address lists, topic OR-arrays) |

### 3.7 Computed reads

POST bodies, never URL parameters — avoids URL length limits and clearly signals "this is a computation, not a CRUD read".

| Path | JSON-RPC mapping |
|---|---|
| `POST /call` | `eth_call` |
| `POST /gas-estimate` | `eth_estimateGas` |
| `POST /access-list` | `eth_createAccessList` |
| `POST /simulate` | `eth_simulateV1` |
| `POST /traces/call` | `trace_call` |
| `POST /traces/call-many` | `trace_callMany` |
| `POST /traces/raw-transaction` | `trace_rawTransaction` |
| `POST /debug-traces/call` | `debug_traceCall` |

### 3.8 Trace queries

| Path | Method | JSON-RPC mapping |
|---|---|---|
| `/traces` | GET | `trace_filter` (query: `fromBlock`, `toBlock`, `fromAddress`, `toAddress`, `limit`, `cursor`) |
| `/traces/search` | POST | `trace_filter` with large filter body |
| `/traces/{txHash}/{traceAddress}` | GET | `trace_get` |

### 3.9 Streams (Server-Sent Events)

All streams set `Content-Type: text/event-stream`.

| Path | JSON-RPC mapping |
|---|---|
| `/streams/blocks` | `eth_subscribe("newHeads")` |
| `/streams/logs` | `eth_subscribe("logs", filter)` (query-param filter) |
| `/streams/pending-transactions` | `eth_subscribe("newPendingTransactions")` (`?full=true` if upstream supports) |
| `/streams/sync-status` | `eth_subscribe("syncing")` |

### 3.10 Utilities & operational

| Path | Method | Meaning |
|---|---|---|
| `/utils/keccak256` | POST | `web3_sha3` — body `{ "data": "0x…" }` |
| `/health` | GET | Liveness (no upstream calls) |
| `/health/ready` | GET | Readiness — upstream reachable, sync within threshold |

### 3.11 Explicitly excluded

- **Signing / unlocked accounts:** `eth_sign`, `eth_signTransaction`, `eth_sendTransaction`, `eth_accounts`, `eth_coinbase`. The proxy is signer-free by design.
- **PoW relics (post-merge):** `eth_mining`, `eth_hashrate`, `eth_protocolVersion`.
- **Uncles (post-merge):** `eth_getUncle*`, `eth_getUncleCount*`.
- **Filter-poll API:** `eth_newFilter`, `eth_newBlockFilter`, `eth_newPendingTransactionFilter`, `eth_uninstallFilter`, `eth_getFilterChanges`, `eth_getFilterLogs`. Use SSE streams or paginated `/logs`.
- **Compiler methods:** `eth_compileSolidity`, `eth_compileLLL`, `eth_compileSerpent`, `eth_getCompilers`.

## 4. Encoding

### 4.1 Numeric quantities

| Kind | JSON encoding | Examples |
|---|---|---|
| Always-safe integers — block number, tx index, log index, gas units, chainId, timestamp (unix seconds), peer count, nonce, percentile, confirmations | **JSON number** | `"blockNumber": 18234567` |
| Potentially-large integers — wei-denominated amounts (balance, value, gasPrice, baseFeePerGas, maxFeePerGas, maxPriorityFeePerGas, blobGasPrice, reward, burnt fees) | **JSON string of decimal digits** | `"value": "1500000000000000000"` |
| Booleans | **JSON boolean** | `"removed": false` |

Hex quantities (`"0x4d2"`) are never used for numeric values, in any direction.

**Input lenience:** request bodies and query parameters accept both JSON numbers and decimal strings for any numeric field. Output is always the canonical form above.

### 4.2 Hex (identifiers, not quantities)

| Kind | Format | Notes |
|---|---|---|
| Block hash, tx hash, state/receipts/storage root, code hash | `0x` + 64 lowercase hex chars | |
| Address | `0x` + 40 chars, **EIP-55 checksummed on output**, any case accepted on input | |
| Topic | `0x` + 64 hex chars | |
| Calldata, bytecode, signature, raw tx body, blob data | `0x` + even number of lowercase hex chars | |
| Storage slot (path param) | `0x` + up to 64 hex chars, **or** decimal integer (proxy converts) | |

### 4.3 Block identifier grammar

The `{id}` path segment (and the `at` query parameter) accepts:

| Shape | Interpreted as |
|---|---|
| `latest`, `safe`, `finalized`, `pending`, `earliest` (exact, lowercase) | Tag |
| All decimal digits | Block number |
| `0x` + exactly 64 hex chars | Block hash |
| Anything else | `400 Bad Request` |

Hex-encoded block numbers are not accepted — decimal only. Disambiguation is purely structural (no flag needed).

### 4.4 Enums replacing hex sentinels

| JSON-RPC field | Native value | REST field | REST value |
|---|---|---|---|
| Receipt `status` | `"0x1"` / `"0x0"` | `status` | `"success"` / `"failed"` |
| Transaction `type` | `"0x0"`..`"0x3"` | `type` | `"legacy"` / `"access-list"` / `"dynamic-fee"` / `"blob"` |

### 4.5 Field naming

camelCase throughout. Existing JSON-RPC field names (`blockNumber`, `transactionHash`, `gasUsed`, etc.) are preserved verbatim. New fields follow the same convention.

### 4.6 Null & missing

- Missing resource → `404 Not Found`, not `200` with `null` body.
- Semantically nullable fields (contract-creation tx `to`, pending tx `blockHash`) → JSON `null`.
- Empty collections → `[]`, never `null`.

### 4.7 RLP representation

Selected via `Accept: application/vnd.ethereum.rlp`. Body is raw RLP bytes (binary, not hex-encoded). Available on:

- `GET /blocks/{id}` → `debug_getRawBlock`
- `GET /blocks/{id}/header` → `debug_getRawHeader`
- `GET /blocks/{id}/receipts` → `debug_getRawReceipts`
- `GET /transactions/{hash}` → `debug_getRawTransaction`

Symmetrically, `POST /transactions` accepts `Content-Type: application/vnd.ethereum.rlp` with the raw RLP as the entire body.

Where unsupported, the proxy returns `406 Not Acceptable` with the set of supported types listed in the problem body.

### 4.8 Wei vs ETH

The API returns wei. No automatic conversion to ETH / gwei. Lossless integer arithmetic outweighs convenience, and conversion is one line of client code.

## 5. Error model

### 5.1 Body shape — RFC 9457 Problem Details

All errors (4xx and 5xx) return `Content-Type: application/problem+json`:

```json
{
  "type": "https://errors.ethereum-rest/transaction-rejected",
  "title": "Transaction rejected",
  "status": 422,
  "detail": "nonce too low (got 5, expected 8)",
  "instance": "/transactions",
  "code": -32003,
  "data": null
}
```

- `type` — stable URI identifying the error class.
- `title` — short human summary, stable for a given `type`.
- `status` — duplicates the HTTP status (per RFC 9457).
- `detail` — specifics of this occurrence.
- `instance` — request path that produced the error.
- `code` — upstream JSON-RPC error code when the error originated upstream; `null` otherwise.
- `data` — upstream JSON-RPC error data when present.

### 5.2 HTTP status mapping

| Condition | HTTP | `type` slug |
|---|---|---|
| Malformed input (bad block id, bad address, missing field) | 400 | `invalid-request` |
| Auth (when gateway-provided) | 401 / 403 | `unauthorized` / `forbidden` |
| Unknown block / tx / account | 404 | `not-found` |
| Wrong HTTP verb for path | 405 | `method-not-allowed` |
| `Accept` requests an unsupported representation | 406 | `not-acceptable` |
| Request body `Content-Type` not supported | 415 | `unsupported-media-type` |
| Result set too large (range exceeds caps) | 413 | `payload-too-large` |
| Tx rejected by mempool | 422 | `transaction-rejected` (with sub-types) |
| Rate limit (with `Retry-After`) | 429 | `rate-limited` |
| Proxy internal failure | 500 | `internal-error` |
| Upstream client returned unclassifiable error | 502 | `upstream-error` |
| Method not supported by upstream | 501 | `method-not-supported-by-upstream` |
| Upstream timeout or unreachable | 504 | `upstream-unavailable` |
| Pagination cursor's chain context invalidated (reorg) | 409 | `chain-reorged` |

### 5.3 Reverts are not errors

A call that the EVM executes and reverts is a successful API call with a revert result. Reverts are response bodies, not HTTP errors:

```json
// POST /call — execution succeeded
{ "data": "0x000000000000000000000000000000000000000000000000000000000000002a" }

// POST /call — call reverted
{
  "reverted": true,
  "data": "0x08c379a000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000002645524332…",
  "reason": "ERC20: transfer amount exceeds balance",
  "panicCode": null
}
```

- The two shapes form a discriminated union on `reverted` (absent = false).
- `reason` is populated when the revert was a standard `Error(string)` selector (`0x08c379a0`) — proxy ABI-decodes.
- `panicCode` is populated when the revert was `Panic(uint256)` (`0x4e487b71`) — proxy decodes the uint.
- Out-of-gas: `{ "reverted": true, "outOfGas": true, "data": "0x", "reason": null, "panicCode": null }`.
- For custom errors, `reason` and `panicCode` are `null` and the client decodes `data` against its ABI.

Same convention applies to `POST /gas-estimate`, `POST /access-list`, `POST /simulate`, `POST /traces/call`, `POST /debug-traces/call`.

### 5.4 Transaction submission

`POST /transactions`:

- Malformed RLP body → `400 invalid-request`.
- Valid RLP, mempool rejection → `422 transaction-rejected` with a sub-type:
  - `…/transaction-rejected/nonce-too-low`
  - `…/transaction-rejected/already-known`
  - `…/transaction-rejected/underpriced`
  - `…/transaction-rejected/replacement-underpriced`
  - `…/transaction-rejected/insufficient-funds`
  - `…/transaction-rejected/intrinsic-gas-too-low`
  - `…/transaction-rejected/gas-limit-exceeded`
  - Anything else → bare `…/transaction-rejected` with `detail` carrying the upstream message verbatim.
- Successful submission → `202 Accepted`, `Location: /transactions/{hash}`, body `{ "hash": "0x…" }`.

### 5.5 Cross-cutting headers

| Header | Used on | Meaning |
|---|---|---|
| `Retry-After` | 429, 503 | Seconds to wait |
| `Location` | 202 (tx submission) | URI of the submitted resource |
| `X-Upstream-Method` | any response | The JSON-RPC method(s) the proxy invoked — diagnostic |
| `X-Block-Height` | responses where relevant | Chain head height at request time |

## 6. Pagination

Applies only to `/logs` (+ `/logs/search`) and `/traces` (+ `/traces/search`).

### 6.1 Strategy

Cursor-based, with pagination state carried in RFC 8288 `Link` headers. Response body is a plain JSON array of items — no envelope.

```
HTTP/1.1 200 OK
Content-Type: application/json
Link: </logs?cursor=eyJuZXh0RnJvbUJsb2NrIjoxODAwMTUwMH0>; rel="next"
X-Page-Size: 1000
X-Block-Height: 18234567

[ {…log…}, {…log…}, … ]
```

The absence of a `rel="next"` Link signals end-of-results.

### 6.2 Cursor

- Opaque base64url-encoded blob. Format is server-internal and may change without notice.
- Contains the original filter, the next block to scan from, the last log index emitted on the boundary block, the frozen `toBlock`, and the boundary block's hash (for reorg detection).
- Stateless: no per-cursor server state.
- When a cursor is present on a request, all other filter parameters are ignored — the cursor is the request.

### 6.3 Limit

- `limit` query param: caller's preferred max items per page.
- Default: 1000. Maximum: 10000. Values above max are clamped silently.

### 6.4 Internal chunking

When the requested range would blow an upstream cap, the proxy:

1. Issues `eth_getLogs` for a sub-range it estimates fits.
2. If upstream still errors with "query too large", halves the range and retries (bounded).
3. Collects until `limit` reached or `toBlock` reached.
4. Sets `next` cursor if scanning didn't complete.

Transparent to the client.

### 6.5 Edge cases

- **`toBlock=latest` with chain advancing:** the cursor freezes `toBlock` to head height observed on the first page. Subsequent pages don't include later blocks. To get newer events, restart with a fresh request or subscribe via SSE.
- **Reorg during pagination:** cursor contains boundary block hash. On resume, if that hash is no longer canonical, the proxy returns `409 Conflict` with `type: chain-reorged`. Client must restart with a fresh query.
- **`fromBlock > toBlock`:** `400 invalid-request`.
- **No matches:** `200 OK`, empty array, no `next` Link.

## 7. Server-Sent Events (streams)

### 7.1 Event framing

Standard SSE. Each event:

```
event: <type>
id: <stable-event-id>
data: <single-line JSON>

```

The proxy emits `retry: 5000` early in each stream to set the reconnect hint to 5 seconds.

### 7.2 Per-stream contracts

#### `/streams/blocks` — `eth_subscribe("newHeads")`

```
event: block
id: 18234568
data: {"number":18234568,"hash":"0x…","parentHash":"0x…","timestamp":1700000000,"gasUsed":12345678,"gasLimit":30000000,"baseFeePerGas":"5000000000",…}
```

`id` is the block number.

#### `/streams/logs` — `eth_subscribe("logs", filter)`

Filter via query params: `address`, `topic0`..`topic3`, optional `fromBlock` for catch-up.

```
event: log
id: 18234568-7
data: {"blockNumber":18234568,"logIndex":7,"address":"0xa0b86…","topics":["0xddf2…",…],"data":"0x…","removed":false,"transactionHash":"0x…"}
```

`id` is `<blockNumber>-<logIndex>`. Reorg-removed logs are re-emitted with `"removed": true`.

#### `/streams/pending-transactions` — `eth_subscribe("newPendingTransactions")`

Default emits `event: pending-transaction` with `{ "hash": "0x…" }`. With `?full=true` and a supporting upstream (Geth, Erigon), emits the full tx object. `id` is the tx hash.

#### `/streams/sync-status` — `eth_subscribe("syncing")`

```
event: sync-status
data: {"syncing":true,"currentBlock":17234567,"highestBlock":18234567,"startingBlock":16000000}
```

When syncing finishes: `{"syncing":false}`.

### 7.3 Reconnect & resume

`Last-Event-ID` is honored where possible:

- **blocks:** proxy backfills missed blocks via `eth_getBlockByNumber` between last id and current head, then resumes the live subscription. Bounded by a max-replay window (default 1024 blocks). Beyond the window, an `event: gap` carrying `{ "from": …, "to": … }` is emitted and the client decides whether to backfill via `GET /blocks/...` itself.
- **logs:** proxy replays via `eth_getLogs` for the missed range using the same filter (taken from the URL it's still serving), then resumes live. Same gap behavior beyond the window.
- **pending-transactions, sync-status:** no replay — ephemeral state. A one-time `event: resumed` is emitted and the stream continues live.

### 7.4 Heartbeats

Every 30 seconds the proxy emits a comment line:

```
: ping 1700000030

```

These keep intermediaries from closing idle connections. Comments are ignored by SSE clients.

### 7.5 Backpressure

If the kernel TCP send buffer to a client backs up past a configured threshold (default 64 KB), the proxy drops the connection. The client's automatic reconnect resumes via `Last-Event-ID` (with replay where supported). This bounds proxy memory in pathological cases.

### 7.6 Errors inside a stream

Once the stream is open and the first byte sent, errors cannot use HTTP status codes. Instead:

```
event: error
data: {"type":"https://errors.ethereum-rest/upstream-unavailable","title":"Upstream lost","detail":"WS reconnect failed after 3 attempts"}

```

The proxy then closes the connection. The client's automatic SSE reconnect kicks in.

Pre-stream errors (bad filter, unsupported upstream method) use normal Problem+JSON responses before any SSE body is sent.

### 7.7 Upstream subscription lifecycle

Implementation note: the proxy MAY share a single upstream WS subscription across concurrent client streams with identical filters. Clients must not depend on per-stream isolation of upstream sequence numbers — `id` values are always derived from chain data (block number, log index, tx hash), never from upstream subscription frame counters.

## 8. Worked examples

### 8.1 Read the latest block

```http
GET /blocks/latest HTTP/1.1
Accept: application/json
```

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-Upstream-Method: eth_getBlockByNumber
X-Block-Height: 18234567

{
  "number": 18234567,
  "hash": "0xabc…",
  "parentHash": "0xdef…",
  "timestamp": 1700000000,
  "gasUsed": 12345678,
  "gasLimit": 30000000,
  "baseFeePerGas": "5000000000",
  "miner": "0x1234567890AbcdEF1234567890aBcdef12345678",
  "transactions": [ {…} ]
}
```

### 8.2 Prepare, simulate, and submit a transaction

```http
GET /accounts/0xAlice…/transaction-template?at=latest
```
```json
{ "nonce": 42, "chainId": 1, "maxFeePerGas": "20000000000", "maxPriorityFeePerGas": "1500000000" }
```

```http
POST /call
Content-Type: application/json

{ "from": "0xAlice…", "to": "0xUSDC…", "data": "0xa9059cbb000…", "value": "0" }
```
```json
{ "data": "0x000…01" }
```

Client signs locally, then:

```http
POST /transactions
Content-Type: application/vnd.ethereum.rlp

<binary RLP of signed tx>
```
```http
HTTP/1.1 202 Accepted
Location: /transactions/0xfeed…beef

{ "hash": "0xfeed…beef" }
```

### 8.3 Subscribe to ERC-20 Transfer events

```http
GET /streams/logs?address=0xUSDC…&topic0=0xddf252ad… HTTP/1.1
Accept: text/event-stream
```

```
HTTP/1.1 200 OK
Content-Type: text/event-stream

retry: 5000

event: log
id: 18234568-7
data: {"blockNumber":18234568,"logIndex":7,"address":"0xa0b86…","topics":["0xddf2…","0x000…alice","0x000…bob"],"data":"0x000…","removed":false,"transactionHash":"0xabc…"}

: ping 1700000030

event: log
id: 18234569-2
data: {…}
```

### 8.4 Paginate logs across a large range

```http
GET /logs?fromBlock=18000000&toBlock=18100000&address=0xUSDC…&limit=1000
```
```http
HTTP/1.1 200 OK
Link: </logs?cursor=eyJuZXh0RnJvbUJsb2NrIjoxODAwMTUwMH0>; rel="next"

[ …1000 logs… ]
```

Follow `next`:

```http
GET /logs?cursor=eyJuZXh0RnJvbUJsb2NrIjoxODAwMTUwMH0
```
```http
HTTP/1.1 200 OK
Link: </logs?cursor=eyJuZXh0RnJvbUJsb2NrIjoxODAwMzAwMH0>; rel="next"

[ …1000 more logs… ]
```

### 8.5 Revert in a call

```http
POST /call
Content-Type: application/json

{ "to": "0xUSDC…", "data": "0xa9059cbb…huge_transfer…" }
```
```json
{
  "reverted": true,
  "data": "0x08c379a000…",
  "reason": "ERC20: transfer amount exceeds balance",
  "panicCode": null
}
```

### 8.6 Fetch raw RLP block

```http
GET /blocks/18234567 HTTP/1.1
Accept: application/vnd.ethereum.rlp
```
```http
HTTP/1.1 200 OK
Content-Type: application/vnd.ethereum.rlp
Content-Length: 12345

<binary RLP bytes>
```

## 9. Open questions / future work

- **Batch endpoint.** JSON-RPC supports batch requests; REST has no native batching idiom. Potential `POST /batch` taking an array of sub-requests, returning an array of sub-responses. Deferred — clients can fan out HTTP/2 requests cheaply.
- ~~**EIP-7702 account-abstraction flows.**~~ Folded into the design: `/accounts/{addr}` includes `delegatedTo`, derived by the proxy from `eth_getCode`. When the returned code is exactly 23 bytes and begins with the `0xef0100` magic prefix, the remaining 20 bytes are surfaced as the delegate address; otherwise `delegatedTo` is `null`. Future EIP-7702-shaped extensions (e.g. delegation history) can be added without protocol churn.
- **Blob sidecars.** EIP-4844 blobs are not currently exposed beyond what appears on tx receipts (`blobGasUsed`, `blobGasPrice`). A future `/transactions/{hash}/blobs` endpoint can expose blob data when needed.
- **Authentication conventions.** Out of scope here, but operators commonly want a documented header convention for forwarded auth identity. Standardize once a pattern emerges.
- **OpenTelemetry trace propagation.** The proxy SHOULD honor `traceparent` headers and propagate context to upstream. Spec exists; not part of this design's contract.
