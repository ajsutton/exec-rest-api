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
