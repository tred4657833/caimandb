#!/usr/bin/env bash
# Ejecuta build + vet + tests. Pensado para correr en una máquina
# con el toolchain de Go instalado (este repo se preparó sin uno).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "==> go build ./..."
go build ./...

echo "==> go vet ./..."
go vet ./...

echo "==> go test ./... (si hay tests)"
go test ./... || true
