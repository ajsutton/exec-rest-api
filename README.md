# exec-rest-api

REST + SSE proxy in front of any Ethereum execution client. Talks JSON-RPC
to your upstream node and serves a developer-friendly REST API (RFC 9457
problem details, RFC 8288 cursor pagination, SSE streams, content
negotiation for raw RLP, no hex quantities).

## Status

`v0.4` — streams added. Endpoints: `/chain/*`, `/blocks/*`, `/accounts/*`,
`/transactions/*`, `/logs`, `/traces/*`, `/gas/*`, `/utils/keccak256`,
`/health/*`, `/streams/{blocks,logs,pending-transactions,sync-status}`.

## Install

```sh
pipx install exec-rest-api
```

(or `pip install exec-rest-api` inside a virtualenv.)

## Run

```sh
exec-rest-api --upstream-http http://localhost:8545
```

Or from a source checkout, without any setup steps:

```sh
scripts/run.sh --upstream-http http://localhost:8545
```

(`scripts/run.sh` creates `.venv/` and installs dependencies on first run, then re-uses them on subsequent invocations.)

Then:

```sh
curl http://127.0.0.1:8080/chain
# → { "chainId": 1, "networkId": "1", "client": "Geth/v1.13.5...", "blockNumber": 18234567, "syncing": {"syncing": false} }

curl http://127.0.0.1:8080/health/ready
# → { "ready": true, "upstreamReachable": true, "syncing": false, "blockNumber": 18234567 }
```

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

## Design docs

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — API contract.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-openapi.yaml` — OpenAPI 3.1.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — implementation strategy.

## License

Apache 2.0.
