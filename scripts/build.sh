#!/usr/bin/env bash
# Compila el binario de CaimanDB en ./bin/caimandb
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p bin
go build -o bin/caimandb ./cmd/caimandb
echo "Binario generado en bin/caimandb"
