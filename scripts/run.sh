#!/usr/bin/env bash
# Run exec-rest-api from the source tree.
# Creates the venv and installs dependencies on first run, refreshes them
# if pyproject.toml has changed since the last install.
#
# Usage:
#   scripts/run.sh --upstream-http http://localhost:8545
#   scripts/run.sh --help
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

VENV=.venv
MARKER="$VENV/.installed"

if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 not found on PATH" >&2
    exit 1
fi

if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
    echo "error: Python 3.10+ required; found $(python3 --version 2>&1)" >&2
    exit 1
fi

if [ ! -d "$VENV" ]; then
    echo "Creating virtualenv in $VENV..." >&2
    python3 -m venv "$VENV"
fi

if [ ! -f "$MARKER" ] || [ pyproject.toml -nt "$MARKER" ]; then
    echo "Installing dependencies..." >&2
    "$VENV/bin/pip" install --quiet --upgrade pip
    "$VENV/bin/pip" install --quiet -e .
    touch "$MARKER"
fi

exec "$VENV/bin/exec-rest-api" "$@"
