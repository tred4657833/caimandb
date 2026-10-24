# Configuration

CaimanDB is configured through two methods, which are combined in this order:

1. **`configs/caimandb.conf` file** (JSON), relative to the working
   directory. If it doesn't exist, it's automatically created on
   startup with default values, also creating the `configs/` folder
   if necessary (see `internal/caimandb/app.go` and
   `internal/caimandb/defaults.go`). A commented template is in
   [`configs/caimandb.conf.example`](../configs/caimandb.conf.example)
   — copy it as `configs/caimandb.conf` to customize it.
2. **Environment variables**, which override whatever is in the
   file (`internal/caimandb/defaults.go`).

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `CAIMANDB_DATA` | Data root directory | `./data` |
| `CAIMANDB_BACKUP_ROOT` | Backups directory (EXPORT/BACKUP files and the encrypted-JSONL automatic backup store's `arw/` subdirectory) — always a single directory, deliberately a sibling of the data root, not nested inside it | sibling of `CAIMANDB_DATA` (e.g. `./data` → `./backups`) |
| `CAIMANDB_CACHE` | L1 cache (MB) | `2048` |
| `CAIMANDB_L2_CACHE` | L2 cache index entries | `4096` |
| `CAIMANDB_WORKERS` | Worker goroutines | auto |
| `CAIMANDB_LOG_LEVEL` | `debug`\|`info`\|`warn`\|`error` | `info` |
| `CAIMANDB_METRICS` | Metrics port | `9090` |
| `CAIMANDB_NODE_ID` | Node identifier | auto |
| `CAIMANDB_RAFT_PORT` | Raft port | `2335` |
| `CAIMANDB_RAFT_BIND` | Raft bind address | `0.0.0.0` |
| `CAIMANDB_QUERY_PORT` | Query server port | `1555` |
| `CAIMANDB_ADMIN_PORT` | Admin API port | `1556` |
| `CAIMANDB_SHARD_COUNT` | Number of shards | `16` |
| `CAIMANDB_REPLICA_COUNT` | Replication factor | `3` |
| `CAIMANDB_QUERY_USER` / `CAIMANDB_QUERY_PASSWORD` | Credentials | — |
| `CAIMANDB_FAST_STARTUP` | Fast startup (no WAL replay) | `true` |
| `CAIMANDB_COMPRESS_LARGE` | Compress large documents | `true` |
| `CAIMANDB_MAX_DOC_SIZE` | Max document size (bytes) | `10485760` |
| `CAIMANDB_JWT_SECRET` | JWT secret | auto-generated |
| `CAIMANDB_TLS_ENABLED` | Enable TLS | `false` |
| `CAIMANDB_TLS_CERT_FILE` | TLS certificate | — |
| `CAIMANDB_STORAGE_AI_ENABLED` | Enable per-block adaptive Badger sizing (Storage AI) | `true` |
| `CAIMANDB_STORAGE_AI_RAM_FRACTION` | Fraction (0,1] of detected RAM that Storage AI can reserve collectively | `0.5` |
| `CAIMANDB_STORAGE_AI_MAX_BUDGET_MB` | Absolute cap in MB (overrides fraction; useful in containers) | `0` (no explicit cap) |

## Main Configuration Blocks

- **Core**: `data_root`, `backup_root`, `cache_mb`, `l2_cache_mb`, `workers`.
- **Network**: `raft_port`, `raft_bind`, `query_port`, `admin_port`.
- **Sharding**: `shard_count`, `replica_count`, `max_shards_per_node`,
  `auto_merge_enabled`, `auto_split_enabled`, `predictive_scaling`.
- **Security**: `jwt_secret`, `token_expiry`, `max_login_attempts`,
  `lockout_duration`, `tls_enabled`.
- **FLEX-COLUMN** (columnar engine, `flex_column` block): columnar
  cache, hot field detection, materialized views, compression
  (`zstd` by default).

See the full structure in
`internal/caimandb/config.go` (struct `Config`) and
`internal/caimandb/defaults.go` (default values + environment overrides).
