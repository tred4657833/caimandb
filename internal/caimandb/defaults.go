package caimandb

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func DefaultConfig() *Config {
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		log().Fatal("Failed to generate secure JWT secret (crypto/rand failed)",
			zap.Error(err))
	}
	jwtSecret := base64.URLEncoding.EncodeToString(secretBytes)

	return &Config{
		DataRoot:              defaultDataRoot,
		CacheMB:               defaultCacheMB,
		L2CacheMB:             defaultL2CacheMB,
		Workers:               defaultWorkers,
		LogLevel:              defaultLogLevel,
		MetricsPort:           defaultMetricsPort,
		RaftPort:              defaultRaftPort,
		RaftBind:              defaultRaftBind,
		QueryPort:             defaultQueryPort,
		AdminPort:             defaultAdminPort,
		QueryUser:             "admin",
		QueryPassword:         generateRandomPassword(),
		UIDir:                 defaultUIDir,
		ShardCount:            defaultShardCount,
		ReplicaCount:          defaultReplicaCount,
		WALSegments:           defaultWALSegments,
		WALSyncPolicy:         "interval",
		AutoCluster:           true,
		FastStartup:           false,
		LazyIndexLoading:      true,
		PreloadCache:          false,
		CompressLargeDocs:     true,
		SkipLargeIndexes:      true,
		UseStreamingWAL:       true,
		MaxShardsPerNode:      1000000,
		MaxClusterNodes:       10000000,
		AutoScaleInterval:     10,
		PredictionWindow:      48,
		MinShardDocs:          10000,
		MaxShardDocs:          1000000,
		ScaleUpCooldown:       60,
		ScaleDownCooldown:     300,
		AutoMergeEnabled:      true,
		AutoSplitEnabled:      true,
		PredictiveScaling:     true,
		AggressiveScaling:     true,
		MaxReplicas:           10,
		MinReplicas:           1,
		MaxDocumentSize:       10 << 20,
		CompressThreshold:     100 << 10,
		IndexSkipThreshold:    1 << 20,
		LargeDocThreshold:     5 << 20,
		MaxConcurrentQueries:  100000,
		QueryTimeout:          60 * time.Second,
		TransactionTimeout:    30 * time.Second,
		MaxQueryDepth:         10,
		MaxBatchSize:          10000,
		RateLimit:             10000,
		JWTSecret:             jwtSecret,
		TokenExpiry:           defaultTokenExpiry,
		MaxLoginAttempts:      maxLoginAttempts,
		LockoutDuration:       lockoutDuration,
		TLSEnabled:            false,
		TLSCertReloadInterval: 10 * time.Minute,
		FlexColumn:            DefaultFlexColumnConfig(),

		MaintenanceEnabled:     true,
		MaintenanceInterval:    6 * time.Hour,
		MaintenanceBlockPause:  200 * time.Millisecond,
		MaintenanceAutoRepair:  true,
		MaintenanceAutoCompact: true,
		MaintenanceColdAfter:   24 * time.Hour,

		// Encrypted/compressed JSONL backups, automatic by default so a
		// fresh install is protected without any operator setup beyond
		// (optionally) setting CAIMANDB_BACKUP_KEY for a real passphrase
		// -- see internal/caimandb/backup.LoadOrCreateKey. Every 6th
		// scheduled run (once a day, at the default 4-hour interval) is
		// forced to a full snapshot so incrementals get consolidated
		// periodically instead of chaining forever.
		BackupAutoEnabled: true,
		BackupInterval:    4 * time.Hour,
		BackupFullEvery:   6,

		CheckpointEnabled:  true,
		CheckpointInterval: 5 * time.Minute,

		AutoRelationsEnabled:  true,
		AutoRelationThreshold: 5,
		AutoRelationWindow:    10 * time.Minute,
		AutoRelationTTL:       24 * time.Hour,

		StorageAIEnabled:     true,
		StorageAIRAMFraction: 0.5,
		StorageAIMaxBudgetMB: 0,

		TurboEnabled: true,
		// 0 lets the turbo worker pool derive min/max from GOMAXPROCS
		// (see turbo.NewAdaptiveWorkerPool).
		TurboMinWorkers:       0,
		TurboMaxWorkers:       0,
		TurboBatchMaxSize:     200,
		TurboBatchMaxWait:     3 * time.Millisecond,
		TurboBulkBatchMaxSize: 2000,
		TurboBulkBatchMaxWait: 20 * time.Millisecond,

		// 0 -> defaultGenerateBatchFlushSize (2000) / GOMAXPROCS-based
		// autoscale ceiling (x4). See the doc comments on these fields in
		// config.go for why they exist.
		GenerateBatchFlushSize:         0,
		GenerateAutoScaleMaxMultiplier: 0,

		LineDatEnabled: true,
		// LineDatDir empty -> resolves to <DataRoot>/__system/linedat.
		LineDatDir:           "",
		LineDatBatchSize:     2000,
		LineDatFlushInterval: 25 * time.Millisecond,
		// 0 -> built-in defaults (250k docs / ~256MB pending, ~8MB per
		// commit, 3s backpressure timeout, up to 8 drain workers,
		// capped by GOMAXPROCS). See the doc comment on these fields
		// in config.go.
		LineDatMaxPending:          0,
		LineDatMaxPendingBytes:     0,
		LineDatMaxBatchBytes:       0,
		LineDatBackpressureTimeout: 0,
		LineDatDrainWorkers:        0,
		// 0 -> defaultLineDatShards (16 independent lanes).
		LineDatShards: 0,
	}
}

func (c *Config) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, c)
}

func (c *Config) SaveToFile(path string) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	cfgCopy := *c
	cfgCopy.QueryPassword = "REDACTED"
	data, err := json.MarshalIndent(cfgCopy, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func (c *Config) MergeEnv() {
	if v := os.Getenv("CAIMANDB_DATA"); v != "" {
		c.DataRoot = v
	}
	if v := os.Getenv("CAIMANDB_BACKUP_ROOT"); v != "" {
		c.BackupRoot = v
	}
	if v := os.Getenv("CAIMANDB_CACHE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.CacheMB = n
		}
	}
	if v := os.Getenv("CAIMANDB_L2_CACHE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.L2CacheMB = n
		}
	}
	if v := os.Getenv("CAIMANDB_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Workers = n
		}
	}
	if v := os.Getenv("CAIMANDB_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
	if v := os.Getenv("CAIMANDB_METRICS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.MetricsPort = n
		}
	}
	if v := os.Getenv("CAIMANDB_RAFT_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.RaftPort = n
		}
	}
	if v := os.Getenv("CAIMANDB_RAFT_BIND"); v != "" {
		c.RaftBind = v
	}
	if v := os.Getenv("CAIMANDB_SHARD_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.ShardCount = n
		}
	}
	if v := os.Getenv("CAIMANDB_REPLICA_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.ReplicaCount = n
		}
	}
	if v := os.Getenv("CAIMANDB_QUERY_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.QueryPort = n
		}
	}
	if v := os.Getenv("CAIMANDB_QUERY_USER"); v != "" {
		c.QueryUser = v
	}
	if v := os.Getenv("CAIMANDB_QUERY_PASSWORD"); v != "" {
		c.QueryPassword = v
	}
	if v := os.Getenv("CAIMANDB_UI_DIR"); v != "" {
		c.UIDir = v
	}
	if v := os.Getenv("CAIMANDB_FAST_STARTUP"); v != "" {
		c.FastStartup = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv("CAIMANDB_COMPRESS_LARGE"); v != "" {
		c.CompressLargeDocs = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv("CAIMANDB_MAX_DOC_SIZE"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.MaxDocumentSize = n
		}
	}
	if v := os.Getenv("CAIMANDB_JWT_SECRET"); v != "" {
		c.JWTSecret = v
	}
	if v := os.Getenv("CAIMANDB_TLS_ENABLED"); v != "" {
		c.TLSEnabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv("CAIMANDB_TLS_CERT_FILE"); v != "" {
		c.TLSCertFile = v
	}
	if v := os.Getenv("CAIMANDB_TLS_KEY_FILE"); v != "" {
		c.TLSKeyFile = v
	}
	// CAIMANDB_TLS_CLIENT_CA_FILE is the mTLS switch: setting it (with
	// TLSEnabled/cert/key also set) makes the admin and query HTTP
	// servers require and verify a client certificate signed by this
	// CA on every connection, on top of the server-side TLS that
	// CAIMANDB_TLS_ENABLED alone already provided. See tls_mtls.go.
	if v := os.Getenv("CAIMANDB_TLS_CLIENT_CA_FILE"); v != "" {
		c.TLSClientCAFile = v
	}
	if v := os.Getenv("CAIMANDB_TLS_CERT_RELOAD_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.TLSCertReloadInterval = d
		}
	}
	if v := os.Getenv("CAIMANDB_RAFT_TLS_ENABLED"); v != "" {
		c.RaftTLSEnabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv("CAIMANDB_RAFT_TLS_CERT_FILE"); v != "" {
		c.RaftTLSCertFile = v
	}
	if v := os.Getenv("CAIMANDB_RAFT_TLS_KEY_FILE"); v != "" {
		c.RaftTLSKeyFile = v
	}
	if v := os.Getenv("CAIMANDB_RAFT_TLS_CA_FILE"); v != "" {
		c.RaftTLSCAFile = v
	}
	if v := os.Getenv("CAIMANDB_STORAGE_AI_ENABLED"); v != "" {
		c.StorageAIEnabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv("CAIMANDB_STORAGE_AI_RAM_FRACTION"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.StorageAIRAMFraction = f
		}
	}
	if v := os.Getenv("CAIMANDB_STORAGE_AI_MAX_BUDGET_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.StorageAIMaxBudgetMB = n
		}
	}
}

func (c *Config) Validate() error {
	if c.DataRoot == "" {
		return fmt.Errorf("data_root cannot be empty")
	}
	if c.CacheMB < 64 {
		return fmt.Errorf("cache_mb must be at least 64")
	}
	if c.L2CacheMB < 64 {
		return fmt.Errorf("l2_cache_mb must be at least 64")
	}
	if c.ShardCount < 1 {
		return fmt.Errorf("shard_count must be at least 1")
	}
	if c.ReplicaCount < 1 {
		return fmt.Errorf("replica_count must be at least 1")
	}
	if c.MaxShardsPerNode < 1 {
		return fmt.Errorf("max_shards_per_node must be at least 1")
	}
	if c.MaxClusterNodes < 1 {
		return fmt.Errorf("max_cluster_nodes must be at least 1")
	}
	if c.QueryTimeout <= 0 {
		return fmt.Errorf("query_timeout must be positive")
	}
	if c.TransactionTimeout <= 0 {
		return fmt.Errorf("transaction_timeout must be positive")
	}
	if c.MaxDocumentSize < 1024 {
		return fmt.Errorf("max_document_size must be at least 1KB")
	}
	if c.MaxBatchSize < 1 {
		return fmt.Errorf("max_batch_size must be at least 1")
	}
	if c.MaxQueryDepth < 1 {
		return fmt.Errorf("max_query_depth must be at least 1")
	}
	if c.RateLimit < 1 {
		return fmt.Errorf("rate_limit must be at least 1")
	}
	if c.JWTSecret == "" {
		return fmt.Errorf("jwt_secret cannot be empty")
	}
	return nil
}
