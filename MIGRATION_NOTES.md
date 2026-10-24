# Migration notes: novodb → caimandb + package restructuring

This document explains what changed, what didn't, and why -- so the
next person (or Claude session) working on this repo understands the
shape of the codebase without having to rediscover it.

## 1. Rename

Every occurrence of `novodb` / `NovoDB` / `NOVODB` was renamed to
`caimandb` / `CaimanDB` / `CAIMANDB`: the Go module path, the root and
`cmd` package names, directory names, file names, and references in
docs/configs/Docker/Makefile. Nothing else changed as part of this
step.

## 2. Why the restructuring is partial

The original `internal/novodb` was a single flat Go package: 78 files,
one dominant `Engine` type (a "god object") whose methods are spread
across most of those files, plus a handful of query/domain types
(`Query`, `Filter`, `SortField`, `QueryResult`, `Document`) referenced
from 4 to 70+ files each.

In Go, **a directory is a package**. Splitting files into
`storage/`, `raft/`, `wal/`, etc. as *real, separately-compiled
packages* (not just folders) means:

- Anything a moved file needs from the root package must either travel
  with it, or be re-imported -- and the root package almost always
  needs the moved type back (to hold it in `Engine`, call its
  constructor, etc.). That's an import cycle unless the dependency
  only goes one way.
- Every method the root package calls on a moved type must be
  *exported* (Go enforces this across package boundaries) -- so
  moving a type isn't just relocating text, it's also an audit of
  every call site.

This sandbox has **no Go compiler**, so every one of those steps was
verified by hand (grep + reading call sites), not by `go build`. Given
that, each candidate move was only taken if it was possible to
*prove*, by inspection, that it wouldn't create a cycle -- rather than
guessing and leaving something broken with no way to catch it.

## 3. What actually moved (and is now a real, independent package)

| Package | Contents | Why it was safe |
|---|---|---|
| `internal/caimandb/storage` | Badger handle pool (`DBPool`), directory layout (`DirectoryManager`), buffer pooling, external/large-doc blob storage, compression, `BlockMeta` | None of these hold an `*Engine`, `*Config`, or query-domain type. `BlockMeta` moved with `DirectoryManager` since its Save/LoadBlockMeta methods return it. |
| `internal/caimandb/raft` | `BadgerLogStore` (a `raft.LogStore` implementation) | Fully self-contained; only used via its exported constructor from `cluster.go`. |
| `internal/caimandb/wal` | `WAL`, segment/rotation/fsync logic | The one method that needed `*Engine` (`RecoverApply`) was extracted out -- see below. |
| `internal/caimandb/cache` | `L1Cache`, `L2IndexCache` | Self-contained; several unexported methods (`get`/`set`/`del`/`stats`/`cleanupExpired`) had to be exported since `Engine` calls them. |
| `internal/caimandb/cluster` | `ChangeBus` (real-time change feed), `WorkerPool` | **Not** `ClusterManager` (Raft cluster coordination) -- that holds a direct `*Engine` and stays in the root package. This package only has the two genuinely standalone pieces. |
| `internal/caimandb/metrics` | All Prometheus metric definitions + `InitMetrics` | Vars were unexported (`metricOpsTotal` etc.); exported and ~36 call sites across 14 files updated to `metrics.MetricOpsTotal` etc. |
| `internal/caimandb/query` | SELECT-list parsing (`SelectItem`, `ParseSelectItem`, arithmetic/COUNT evaluation) | Self-contained; does **not** include `Query`/`Filter`/`QueryResult` themselves (see below). |
| `internal/caimandb/logging` | A tiny shared `zap.Logger` accessor | Exists so the packages above can log without importing the root package (which would cycle). The root package calls `logging.SetLogger(...)` once at startup so everything shares one configured sink -- see `logging.go`. |

`wal.WAL.RecoverApply` became a free function `RecoverWAL(w *wal.WAL,
engine *Engine)` in the root package's new `wal_recovery.go`, because
applying recovered entries needs `Engine`'s unexported local
insert/update/delete paths. `wal` itself only exposes the generic
primitives (`ReadAll`, `IsClean`, `MarkRecovered`,
`PruneToLastSegment`) that `RecoverWAL` composes.

## 4. What deliberately stayed in the root `caimandb` package

- **`Engine` and everything hung off it**: `app.go`, `engine_main.go`,
  `engine_core.go`, all `cmd_*.go` (command dispatch), all `ops_*.go`
  (the actual find/insert/update/delete/aggregate logic), `keys.go`,
  `index_secondary.go`, `flexcolumn.go`, `maintenance.go`,
  `block_repair.go`, `shard_manager.go`, `dist_query.go`,
  `relations*.go`, `group_by.go`, `dsl_parser.go`,
  `raft_fsm.go`, `cluster.go`, `transaction.go`, `users_auth.go`,
  `auth_jwt.go`, `audit.go`, `risk_engine.go`, `ratelimit.go`,
  `http_admin.go`, `http_query.go`.
- **`Query`, `Filter`, `SortField`, `QueryResult`** (`ops_find.go`) and
  **`Document`** (`document.go`): these are threaded through 20-70+
  files each. Moving them into `query`/a domain package is
  *architecturally* the right call, but doing it by hand -- capitalizing
  a type's use as a bare word everywhere it's a type, while leaving
  alone every place the same word is a struct *field* name (e.g. a
  `Query` field on an unrelated struct, or `cmd_view.go`'s field
  literally named `Query`) -- is exactly the kind of change a compiler
  should be checking, not a person with grep. It was left alone rather
  than risk a silent, undetectable mistake.

## 5. `pkg/` and `api/`

Both exist per the requested layout but are intentionally close to
empty right now (see the `doc.go` / `README.md` in each). The natural
contents -- HTTP request/response types, a public client, `Version` --
are all still entangled with the root package for the same reasons as
above. Moving them is a reasonable next step, ideally with a compiler
available to verify it.

## 6. Confidence level

This was done through careful static analysis (no `go build` in this
environment), not compilation. High confidence in the mechanical parts
(the rename; moves with zero call-site ambiguity, like `raft` and
`storage`). Lower confidence anywhere a rename was scoped by pattern
matching across many files (the `metrics` qualification, `cache`
method exports) -- these were spot-checked but not exhaustively
verified. **Recommended next step: run `go build ./...` (and `go
vet`) in an environment with the Go toolchain and fix whatever
surfaces** -- if you paste the errors back, they're straightforward to
resolve given the map above.
