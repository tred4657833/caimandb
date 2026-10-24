// Package main implements CaimanDB, the "DataEngine" enterprise big data
// engine.
//
// CaimanDB V1.0 "DataEngine" - Enterprise Big Data Engine
// Optimized for large documents - production ready - complete
// With FLEX-COLUMN hybrid storage
// All rights reserved
//
// This file (doc.go) exists purely to hold the package-level documentation
// banner. The engine's implementation is split across the other files in
// this package, grouped by subsystem for maintainability:
//
//	constants.go          global constants and sentinel errors
//	config.go              Config struct and FLEX-COLUMN config
//	defaults.go             DefaultConfig()
//	flexcolumn.go            FLEX-COLUMN hybrid row/columnar engine
//	logging.go               zap logger setup
//	metrics.go                Prometheus metrics
//	compression.go            zstd/gzip compression helpers
//	cache.go                  L1/L2 cache system
//	worker_pool.go            background worker pool
//	locks.go                  sharded lock manager
//	ratelimit.go               per-user rate limiter
//	auth_jwt.go                JWT token manager
//	audit.go                   audit logger
//	storage_external.go        large-document external storage
//	buffer_pool.go             byte buffer pooling
//	wal.go                     write-ahead log
//	transaction.go             ACID transactions + tx helper
//	shard_manager.go           shard manager
//	raft_fsm.go                Raft FSM
//	cluster.go                 cluster manager
//	raft_logstore.go           Badger-backed Raft log store
//	directory.go                directory / DB layout manager
//	badger_pool.go              BadgerDB connection pool
//	document.go                  document model
//	engine_core.go               intelligent engine core
//	query_optimizer.go           cost based query optimizer
//	risk_engine.go                risk / safety engine
//	keys.go                       storage key generation
//	index_secondary.go            secondary index management
//	ops_local.go                  local (single node) operations
//	ops_database.go                database lifecycle operations
//	ops_block.go                   storage block operations
//	ops_insert.go                  insert operations
//	ops_find.go                    find / query operations
//	ops_search.go                  full text search
//	ops_update.go                  update operations
//	ops_delete.go                  delete + count operations
//	ops_aggregate.go               aggregation pipeline
//	ops_export.go                  export / import
//	ops_backup.go                  backup / restore
//	ops_compact.go                 compaction + block repair
//	users_auth.go                  user management (Argon2id)
//	query_filter.go                query filter matching
//	dist_query.go                  distributed query executor
//	engine_main.go                 top level Engine (FLEX-COLUMN aware)
//	block_repair.go                RebuildBlock/CheckBlock/RepairBlock
//	sort_utils.go                  sort helpers
//	nested_fields.go               nested field helpers
//	tokenizer.go                   text tokenizer
//	misc_utils.go                  misc utilities
//	http_admin.go                  HTTP admin server
//	http_query.go                  HTTP query server
//	dsl_parser.go                  NQL DSL parser + executor
//	cmd_create_show.go             CREATE / SHOW commands
//	cmd_insert.go                  INSERT command
//	cmd_find.go                    FIND / GET / SEARCH commands
//	cmd_update.go                  UPDATE command
//	cmd_delete.go                  DELETE / CLEAR / COUNT commands
//	cmd_aggregate.go               AGGREGATE / GROUP / JOIN commands
//	cmd_view.go                    VIEW command
//	cmd_admin.go                   ANALYZE/EXPORT/BACKUP/SHARD/CLUSTER commands
//	cmd_filters_util.go            parseFilters + isInteractive helpers
//	help.go                        interactive HELP text
//	main.go                        CLI entrypoint (func main)
package caimandb
