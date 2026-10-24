# api/

This directory is reserved for CaimanDB's API surface: OpenAPI/Swagger
specs, protobuf/gRPC definitions, or generated client stubs, as those
get added.

The actual HTTP handlers currently live in
`internal/caimandb/http_query.go` and `internal/caimandb/http_admin.go`.
They were **not** moved into a separate `api` package during this
restructuring: both construct their responses directly from `*Engine`
and `*Config` (unexported fields and methods), and the composition
root (`internal/caimandb/app.go`'s `Run`) constructs them inline. Pulling
them out cleanly would mean inverting that wiring (main.go building the
`Engine` and handing it to `api.NewQueryServer(engine)`, rather than
`Engine.Run()` constructing the servers itself) -- a real, behavior-
sensitive change to the startup path, not a mechanical move, and this
pass didn't have a Go compiler available to verify it safely. See
`MIGRATION_NOTES.md` at the repository root.

For now, the existing HTTP API is documented in
[`docs/api/http-api.md`](../docs/api/http-api.md).
