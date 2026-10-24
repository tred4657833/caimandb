# CaimanDB Architecture

CaimanDB is a document-oriented data engine with sharding,
Raft-based clustering, a Write-Ahead Log (WAL), ACID
transactions, and an SQL-like command language (NQL).

## Binary Design

```
cmd/caimandb/main.go   → entry point (func main), delegates everything to internal/caimandb.Run()
internal/caimandb/      → all engine, server and CLI logic (a single Go package)
```

`internal/caimandb` is kept as **a single package** by design.
The central `Engine` type (defined in `engine_main.go`) is referenced
directly — including unexported fields like `pool`, `wal`,
`l1Cache`, `shardMgr`, `lockManager`, etc. — from over 40 files
(`ops_*.go`, `cmd_*.go`, `raft_fsm.go`, `transaction.go`...). Splitting
that into multiple Go packages would require exporting a good portion
of the engine's internal state, a mechanical but high-risk change that
in an environment without a compiler (like the one used to prepare
this repo) cannot be safely verified. See [`known-limitations.md`](./known-limitations.md).

Instead, the code inside `internal/caimandb` is already grouped by
file prefix, which is the usual convention in large single-package
Go projects:

| Prefix / file | Responsibility |
|---|---|
| `engine_core.go`, `engine_main.go`, `app.go` | Engine core, lifecycle, startup (`Run()`) |
| `ops_*.go` | Low-level operations on the `Engine` (CRUD, backup, compact, search, aggregation...) |
| `cmd_*.go` | NQL command handlers (INSERT, FIND, CREATE, VIEW, DROP, RENAME, TX...) |
| `dsl_parser.go`, `tokenizer.go`, `query_filter.go`, `query_optimizer.go` | NQL parsing and query optimization |
| `badger_pool.go`, `wal.go`, `buffer_pool.go`, `compression.go`, `storage_external.go`, `directory.go`, `keys.go` | Persistent storage (BadgerDB, WAL, buffers, compression) |
| `cache.go` | L1 (documents) and L2 (indexes) cache |
| `cluster.go`, `shard_manager.go`, `raft_fsm.go`, `raft_logstore.go`, `dist_query.go` | Raft clustering, sharding and distributed queries |
| `transaction.go`, `locks.go` | ACID transactions and locking |
| `flexcolumn.go`, `nested_fields.go`, `index_secondary.go` | Columnar engine (FLEX-COLUMN) and secondary indexes |
| `http_admin.go`, `http_query.go` | HTTP servers (admin and query APIs) |
| `auth_jwt.go`, `users_auth.go`, `ratelimit.go`, `risk_engine.go`, `audit.go` | Authentication, rate limiting, risk engine, auditing |
| `metrics.go`, `logging.go` | Observability (native Prometheus metrics, no external dependencies) |
| `config.go`, `defaults.go`, `constants.go` | Configuration |
| `document.go`, `doc.go` | Document model |
| `block_repair.go`, `worker_pool.go`, `misc_utils.go`, `sort_utils.go`, `help.go` | Various utilities |

## Startup Flow

`cmd/caimandb/main.go` → `caimandb.Run()` (`app.go`):

1. Loads `configs/caimandb.conf` if it exists (or creates one there,
   creating the folder if necessary, with default values — see
   `configs/caimandb.conf.example`).
2. Initializes the `Engine`: BadgerDB pool, WAL, L1/L2 caches,
   buffer pool, directory manager, rate limiter, auditing.
3. If `auto_cluster` is enabled, starts Raft and node discovery
   (`cluster.go`).
4. Starts the query and admin HTTP servers
   (`http_query.go`, `http_admin.go`) on the configured ports.
5. Enters the interactive REPL for NQL commands.

## Disk Storage

```
./data/
├── <db_name>/                 <- Database directory
│   ├── __meta/                <- Metadata (db.json)
│   ├── <block_name>/          <- Block directory (equivalent to "collection")
│   │   ├── __data/            <- BadgerDB data (documents)
│   │   ├── __index/           <- Secondary indexes
│   │   └── __meta/            <- Block metadata
│   └── ...
├── __users/                    <- Authentication (Argon2id)
├── __raft/                     <- Raft cluster state
├── __shards/                   <- Shard management data
├── __cluster/                  <- Cluster node information
├── __system/wal/               <- Write-Ahead Log
└── __external/                 <- Large documents (>5MB) stored outside BadgerDB
```

## Key Subsystems

- **ACID Transactions**: `BEGIN` / `COMMIT` / `ROLLBACK` with
  isolation levels `read_committed`, `repeatable_read` (default) and
  `serializable` (`transaction.go`).
- **Sharding and Clustering**: consistent hashing, auto-splitting/
  merging, predictive auto-scaling and distributed query execution
  with shard pruning (`shard_manager.go`, `dist_query.go`).
- **FLEX-COLUMN**: optional columnar engine with automatic detection
  of "hot" fields and materialized views (`flexcolumn.go`).
- **Security**: Argon2id hashing, JWT, rate limiting, account
  locking, JSON auditing and a risk engine that can block suspicious
  operations (`risk_engine.go`).

For the complete NQL command details see
[`docs/nql-reference.md`](./nql-reference.md); for HTTP endpoints
see [`docs/api/http-api.md`](./api/http-api.md).
