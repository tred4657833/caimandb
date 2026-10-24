package caimandb

import (
	"time"
)

type Config struct {
	DataRoot string `json:"data_root"`
	// BackupRoot is where the single, shared backups directory lives
	// (both EXPORT/legacy-BACKUP files and the encrypted-JSONL
	// automatic backup store's "arw" subdirectory) -- see
	// storage.DirectoryManager.BackupRoot(). Empty (the default)
	// means "a sibling of DataRoot" (e.g. DataRoot "./data" gets
	// BackupRoot "./backups"), deliberately NOT nested inside
	// DataRoot, and there's exactly one such directory, not the two
	// ("<DataRoot>/backups" and "<DataRoot>/backup/arw") earlier
	// versions of this project created.
	BackupRoot    string `json:"backup_root"`
	CacheMB       int    `json:"cache_mb"`
	L2CacheMB     int    `json:"l2_cache_mb"`
	Workers       int    `json:"workers"`
	LogLevel      string `json:"log_level"`
	MetricsPort   int    `json:"metrics_port"`
	RaftPort      int    `json:"raft_port"`
	RaftBind      string `json:"raft_bind"`
	QueryPort     int    `json:"query_port"`
	AdminPort     int    `json:"admin_port"`
	QueryUser     string `json:"query_user"`
	QueryPassword string `json:"query_password"`
	// UIDir is the filesystem path to the pure HTML/CSS/JS admin console
	// (the ui/ folder). The query server serves it directly at "/" on
	// QueryPort, alongside /query, /status, /health and /watch, so the
	// whole thing works same-origin with no separate static server and
	// no file:// CORS restrictions. Empty/missing directory just means
	// "/" 404s; the JSON API endpoints are unaffected.
	UIDir        string `json:"ui_dir"`
	ShardCount   int    `json:"shard_count"`
	ReplicaCount int    `json:"replica_count"`
	WALSegments  int    `json:"wal_segments"`
	AutoCluster  bool   `json:"auto_cluster"`

	// WALSyncPolicy controls fsync/group-commit behavior: "always" (fsync
	// every batch, safest/slowest), "interval" (group-commit: fsync on the
	// periodic ticker only — default, best throughput/durability balance
	// for sustained real-time write load), or "off" (never explicit fsync).
	WALSyncPolicy string `json:"wal_sync_policy"`

	FastStartup       bool `json:"fast_startup"`
	LazyIndexLoading  bool `json:"lazy_index_loading"`
	PreloadCache      bool `json:"preload_cache"`
	CompressLargeDocs bool `json:"compress_large_docs"`
	SkipLargeIndexes  bool `json:"skip_large_indexes"`
	UseStreamingWAL   bool `json:"use_streaming_wal"`

	MaxShardsPerNode  int   `json:"max_shards_per_node"`
	MaxClusterNodes   int   `json:"max_cluster_nodes"`
	AutoScaleInterval int   `json:"auto_scale_interval"`
	PredictionWindow  int   `json:"prediction_window"`
	MinShardDocs      int64 `json:"min_shard_docs"`
	MaxShardDocs      int64 `json:"max_shard_docs"`
	ScaleUpCooldown   int   `json:"scale_up_cooldown"`
	ScaleDownCooldown int   `json:"scale_down_cooldown"`
	AutoMergeEnabled  bool  `json:"auto_merge_enabled"`
	AutoSplitEnabled  bool  `json:"auto_split_enabled"`
	PredictiveScaling bool  `json:"predictive_scaling"`
	AggressiveScaling bool  `json:"aggressive_scaling"`
	MaxReplicas       int   `json:"max_replicas"`
	MinReplicas       int   `json:"min_replicas"`

	MaxDocumentSize    int64 `json:"max_document_size"`
	CompressThreshold  int64 `json:"compress_threshold"`
	IndexSkipThreshold int64 `json:"index_skip_threshold"`
	LargeDocThreshold  int64 `json:"large_doc_threshold"`

	MaxConcurrentQueries int           `json:"max_concurrent_queries"`
	QueryTimeout         time.Duration `json:"query_timeout"`
	TransactionTimeout   time.Duration `json:"transaction_timeout"`
	MaxQueryDepth        int           `json:"max_query_depth"`
	MaxBatchSize         int           `json:"max_batch_size"`
	RateLimit            int           `json:"rate_limit"`

	JWTSecret        string        `json:"jwt_secret"`
	TokenExpiry      time.Duration `json:"token_expiry"`
	MaxLoginAttempts int           `json:"max_login_attempts"`
	LockoutDuration  time.Duration `json:"lockout_duration"`
	TLSEnabled       bool          `json:"tls_enabled"`
	TLSCertFile      string        `json:"tls_cert_file"`
	TLSKeyFile       string        `json:"tls_key_file"`
	// TLSClientCAFile, when set alongside TLSEnabled, turns on mutual
	// TLS: the server presents its own cert (TLSCertFile/TLSKeyFile) as
	// before, but now also requires and verifies a client certificate
	// signed by a CA in this file (tls.RequireAndVerifyClientCert).
	// Leaving it empty preserves the previous server-only-TLS behavior
	// exactly -- this is additive, not a breaking change to existing
	// TLSEnabled deployments. See tls_mtls.go.
	TLSClientCAFile string `json:"tls_client_ca_file"`
	// TLSCertReloadInterval controls how often the server re-checks
	// TLSCertFile/TLSKeyFile's mtime on disk and, if changed, swaps in
	// the new certificate for all *new* TLS handshakes without a
	// restart (existing connections keep whatever cert they already
	// negotiated). 0 disables reload checking (cert is loaded once at
	// startup, same as before this field existed).
	TLSCertReloadInterval time.Duration `json:"tls_cert_reload_interval"`

	// RaftTLS secures inter-node Raft traffic (the raft.StreamLayer
	// initRaftWithCustomLogStore builds in cluster.go), independent of
	// TLSEnabled above which only covers the admin/query HTTP APIs.
	// Every node in a cluster needs a certificate signed by the same
	// RaftTLSCAFile for nodes to accept each other's connections --
	// see tls_raft.go for how the CA pool doubles as both ClientCAs
	// (verifying an inbound peer's cert) and RootCAs (verifying an
	// outbound peer's cert when dialing, since Raft addresses are
	// IP:port and don't match a cert's DNS SAN, so hostname
	// verification is skipped in favor of still-enforced chain
	// verification against this CA).
	RaftTLSEnabled  bool   `json:"raft_tls_enabled"`
	RaftTLSCertFile string `json:"raft_tls_cert_file"`
	RaftTLSKeyFile  string `json:"raft_tls_key_file"`
	RaftTLSCAFile   string `json:"raft_tls_ca_file"`

	// FLEX-COLUMN Configuration
	FlexColumn *FlexColumnConfig `json:"flex_column"`

	// Autonomous maintenance: a low-priority background sweep that (a)
	// verifies block integrity and auto-repairs corruption it finds, and
	// (b) compacts blocks that haven't been written to in a while, to
	// keep disk usage down without an operator having to run `COMPACT`
	// or `REPAIR` by hand. Deliberately throttled (BlockPause between
	// each block) so it never competes meaningfully with live traffic
	// for CPU/IO — this is "administración casi autónoma", not a
	// background batch job that gets to use whatever resources it wants.
	MaintenanceEnabled     bool          `json:"maintenance_enabled"`
	MaintenanceInterval    time.Duration `json:"maintenance_interval"`
	MaintenanceBlockPause  time.Duration `json:"maintenance_block_pause"`
	MaintenanceAutoRepair  bool          `json:"maintenance_auto_repair"`
	MaintenanceAutoCompact bool          `json:"maintenance_auto_compact"`
	MaintenanceColdAfter   time.Duration `json:"maintenance_cold_after"`

	// Backup: automatic encrypted/compressed JSONL backups (see
	// ops_backup_jsonl.go, internal/caimandb/backup). BackupInterval
	// controls how often the scheduler takes an incremental (or first-
	// ever full) backup of every database; BackupFullEvery controls how
	// many of those scheduled runs are forced to a full snapshot instead
	// of an incremental, so incrementals don't chain indefinitely without
	// ever being consolidated. Manual BACKUP/RESTORE/VERIFY BACKUP
	// commands (admin-only, cmd_backup_jsonl.go) work regardless of
	// whether the scheduler is enabled.
	BackupAutoEnabled bool          `json:"backup_auto_enabled"`
	BackupInterval    time.Duration `json:"backup_interval"`
	BackupFullEvery   int           `json:"backup_full_every"`

	// Checkpoints: periódicamente (o vía el comando CHECKPOINT) se
	// sincronizan a disco todas las bases Badger abiertas y se sincroniza
	// y rota el WAL a un segmento nuevo (ver checkpoint.go). Esto acota
	// cuánto WAL hay que reproducir en un REDO tras un fallo -- el
	// checkpoint es el punto a partir del cual empieza esa recuperación,
	// en vez de tener que reproducir todo el historial desde el arranque.
	CheckpointEnabled  bool          `json:"checkpoint_enabled"`
	CheckpointInterval time.Duration `json:"checkpoint_interval"`

	// Auto-relations: when a user reads the same document repeatedly
	// (AutoRelationThreshold times within AutoRelationWindow), CaimanDB
	// automatically records a self-relation between that user and the
	// document, carrying a relevance score derived from access
	// frequency and recency plus a small metadata sample of the doc.
	// These relations are strictly temporal: every further access
	// slides their expiry (AutoRelationTTL) forward, and once the
	// pair stops being accessed it expires and is swept away by the
	// background cleanup loop. See relations_auto.go and the
	// SHOW AUTORELATIONS <block> command.
	AutoRelationsEnabled  bool          `json:"auto_relations_enabled"`
	AutoRelationThreshold int           `json:"auto_relation_threshold"`
	AutoRelationWindow    time.Duration `json:"auto_relation_window"`
	AutoRelationTTL       time.Duration `json:"auto_relation_ttl"`

	// Storage AI: adaptive per-block BadgerDB sizing (see
	// internal/caimandb/storage/adaptive.go). Fixes new/small blocks all
	// paying the same fixed on-disk footprint (tens of MB) regardless of
	// document count, by picking a tuning tier per block from what's
	// already on disk, capped by a shared RAM budget across every
	// currently-open block. Disabling it reproduces this project's
	// original fixed-size Badger behavior exactly.
	StorageAIEnabled     bool    `json:"storage_ai_enabled"`
	StorageAIRAMFraction float64 `json:"storage_ai_ram_fraction"`
	StorageAIMaxBudgetMB int     `json:"storage_ai_max_budget_mb"`

	// Turbo engine: adaptive worker pool + priority scheduler + an
	// auto-batching ("group commit") front door in front of standalone
	// INSERTs. Concurrent single-document INSERTs landing on the same
	// block within TurboBatchMaxWait (or until TurboBatchMaxSize
	// accumulate) are coalesced into one lock acquisition / WriteBatch
	// flush instead of one each. Disabling it reproduces the original
	// one-lock-per-INSERT behavior exactly. See internal/caimandb/turbo
	// and ops_turbo.go.
	TurboEnabled          bool          `json:"turbo_enabled"`
	TurboMinWorkers       int           `json:"turbo_min_workers"`
	TurboMaxWorkers       int           `json:"turbo_max_workers"`
	TurboBatchMaxSize     int           `json:"turbo_batch_max_size"`
	TurboBatchMaxWait     time.Duration `json:"turbo_batch_max_wait"`
	TurboBulkBatchMaxSize int           `json:"turbo_bulk_batch_max_size"`
	TurboBulkBatchMaxWait time.Duration `json:"turbo_bulk_batch_max_wait"`

	// GenerateBatchFlushSize is how many prepared documents accumulate in
	// a single Badger WriteBatch (inside handleInsertGenerate's commit
	// loop, ops_insert.go) before it's flushed and a new one is opened.
	// 0 resolves to a sensible default (see defaultGenerateBatchFlushSize
	// in ops_insert.go). Only bounds memory for a single WriteBatch, not
	// WAL fsync grouping (that's governed by WALSyncPolicy separately).
	GenerateBatchFlushSize int `json:"generate_batch_flush_size"`

	// LineDat: the high-speed temporal ingestion buffer that sits in
	// front of CaimanDB's permanent storage (see internal/caimandb/
	// linedat.go). Accepted documents are durably staged in LineDat's
	// own on-disk journal and acknowledged immediately, then
	// transferred progressively to CaimanDB's permanent Badger/WAL
	// storage by a background goroutine -- letting a huge load (e.g.
	// 1,000,000 documents) return control to the caller almost
	// instantly while the actual commit continues in the background.
	// FindByID/Find consult LineDat for anything not yet transferred,
	// so reads stay consistent with what was accepted. Only applies to
	// the local/standalone write paths (InsertFast, GENERATE, IMPORT);
	// Raft-clustered writes are unaffected, same restriction InsertFast
	// already has. Disabling it reproduces the previous fully-
	// synchronous behavior for those paths exactly.
	LineDatEnabled       bool          `json:"linedat_enabled"`
	LineDatBatchSize     int           `json:"linedat_batch_size"`
	LineDatFlushInterval time.Duration `json:"linedat_flush_interval"`
	// LineDatDir overrides where LineDat's durability journal is
	// stored. Empty resolves to <DataRoot>/__system/linedat.
	LineDatDir string `json:"linedat_dir"`

	// Resource governance for LineDat -- all 0 -> sane built-in defaults
	// (see defaultLineDat* constants in linedat.go). These exist so a
	// sustained massive load (e.g. a very large GENERATE/IMPORT, or many
	// concurrent high-rate clients) is absorbed by slowing the effective
	// accept rate down to whatever CaimanDB can actually commit, instead
	// of growing LineDat's in-memory queue without bound.
	//
	// LineDatMaxPending / LineDatMaxPendingBytes cap how many documents
	// (and how many estimated bytes of document data) may sit in the
	// temporary blocks at once, across every block combined.
	// LineDatMaxBatchBytes additionally caps each single commit to
	// CaimanDB by size, on top of LineDatBatchSize's document-count cap.
	// LineDatBackpressureTimeout is the longest an INSERT/GENERATE/IMPORT
	// caller is slowed down waiting for room before being accepted
	// anyway. LineDatDrainWorkers is the fixed-size worker pool that
	// transfers pending documents to CaimanDB -- bounded so a burst of
	// many concurrently written blocks can't spawn unbounded goroutines.
	LineDatMaxPending          int           `json:"linedat_max_pending"`
	LineDatMaxPendingBytes     int64         `json:"linedat_max_pending_bytes"`
	LineDatMaxBatchBytes       int64         `json:"linedat_max_batch_bytes"`
	LineDatBackpressureTimeout time.Duration `json:"linedat_backpressure_timeout"`
	LineDatDrainWorkers        int           `json:"linedat_drain_workers"`
	// LineDatShards is the number of independent lanes LineDat is split
	// into (own lock, own per-block queues, own on-disk journal file). A
	// given block always hashes to the same shard, so this is what turns
	// "many blocks/clients writing concurrently" into real parallelism
	// instead of funneling through one lock and one journal file. 0 ->
	// defaultLineDatShards (16). Raising it costs a bit of idle memory
	// per shard, not CPU, so it's safe to size generously.
	LineDatShards int `json:"linedat_shards"`

	// GenerateAutoScaleMaxMultiplier caps how far runRateWatchdog
	// (cmd_insert.go) is allowed to grow the GENERATE worker pool
	// automatically, as a multiple of GOMAXPROCS -- e.g. 4 means the
	// watchdog won't push automatic scaling past GOMAXPROCS*4 workers,
	// even though maxGenerateWorkers (64) is the hard ceiling for an
	// operator who sets WORKERS explicitly. 0 resolves to a default of 4.
	// logic ("rate dropped -> add workers") could itself be the cause of
	// the rate drop once workers exceed available cores (lock contention
	// on the block being written, extra scheduling overhead) -- without a
	// ceiling tied to actual core count, that becomes a runaway loop that
	// only ever adds workers and never backs off. See runRateWatchdog's
	// doc comment in cmd_insert.go for the full explanation.
	GenerateAutoScaleMaxMultiplier int `json:"generate_autoscale_max_multiplier"`
}

type FlexColumnConfig struct {
	Enabled              bool          `json:"enabled"`
	DocumentCacheMB      int           `json:"document_cache_mb"`
	ColumnarIndexEnabled bool          `json:"columnar_index_enabled"`
	AutoDetectHotFields  bool          `json:"auto_detect_hot_fields"`
	MaterializedViews    bool          `json:"materialized_views"`
	ColumnarCompression  string        `json:"columnar_compression"`
	MaxColumnarFields    int           `json:"max_columnar_fields"`
	HotFieldThreshold    float64       `json:"hot_field_threshold"`
	WorkerCount          int           `json:"worker_count"`
	UpdateQueueSize      int           `json:"update_queue_size"`
	ViewRefreshInterval  time.Duration `json:"view_refresh_interval"`
}

func DefaultFlexColumnConfig() *FlexColumnConfig {
	return &FlexColumnConfig{
		Enabled:              true,
		DocumentCacheMB:      defaultColumnarCacheMB,
		ColumnarIndexEnabled: true,
		AutoDetectHotFields:  true,
		MaterializedViews:    true,
		ColumnarCompression:  "zstd",
		MaxColumnarFields:    maxColumnarFields,
		HotFieldThreshold:    defaultHotFieldThreshold,
		WorkerCount:          4,
		UpdateQueueSize:      10000,
		ViewRefreshInterval:  5 * time.Minute,
	}
}
