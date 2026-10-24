#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

echo "======================================"
echo "        Building CaimanDB"
echo "======================================"
echo

echo "[1/5] Resolving newer engine dependencies (Ristretto, Roaring, msgpack, etc.)..."
# These are intentionally not pinned in go.mod (see the note there) --
# this fetches real, resolvable versions from the module proxy instead of
# a guessed pin, and inserts them into go.mod/go.sum for the tidy step
# below.
go get github.com/dgraph-io/ristretto/v2@latest
go get github.com/RoaringBitmap/roaring@latest
go get golang.org/x/sync@latest
go get github.com/vmihailenco/msgpack/v5@latest
go get github.com/google/uuid@latest
go get github.com/natefinch/atomic@latest
go get github.com/smallnest/ringbuffer@latest

echo
echo "[2/5] Running go mod tidy..."
go mod tidy

echo
echo "[3/5] Downloading dependencies..."
go mod download

echo
echo "[4/5] Building cli..."
go build -o cli ./cli/main.go

echo
echo "[5/5] Building CaimanDB..."
go build ./cmd/caimandb

echo
echo "======================================"
echo "      Build completed successfully"
echo "======================================"