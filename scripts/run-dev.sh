#!/usr/bin/env bash
# Levanta CaimanDB en modo desarrollo con datos en ./data-dev
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export CAIMANDB_DATA="${CAIMANDB_DATA:-./data-dev}"
export CAIMANDB_LOG_LEVEL="${CAIMANDB_LOG_LEVEL:-debug}"
export CAIMANDB_FAST_STARTUP="${CAIMANDB_FAST_STARTUP:-true}"

go run ./cmd/caimandb
