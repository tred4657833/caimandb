package caimandb

import (
	"caimandb/internal/caimandb/cache"
	"caimandb/internal/caimandb/cluster"
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/storage"
	"caimandb/internal/caimandb/turbo"
	"caimandb/internal/caimandb/wal"
	"context"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Engine struct {
	dataRoot      string
	pool          *storage.DBPool
	dirMgr        *storage.DirectoryManager
	l1Cache       *cache.L1Cache
	l2Cache       *cache.L2IndexCache
	wp            *cluster.WorkerPool
	lockManager   *shardedLockManager
	opCount       atomic.Int64
	metrics       *MetricsCollector
	queryOpt      *QueryOptimizer
	cluster       *ClusterManager
	shardMgr      *ShardManager
	distQuery     *DistributedQueryExecutor
	intelEngine   *IntelligentEngine
	flexEngine    *FlexColumnEngine
	nodeID        string
	wal           *wal.WAL
	config        *Config
	startupTime   time.Time
	auditLogger   *AuditLogger
	tokenManager  *TokenManager
	rateLimiter   *RateLimiter
	externalStore storage.ExternalStorage
	riskEngine    *RiskEngine
	stopChan      chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	bufferPool    *storage.BufferPool
	txManager     *TxManager
	relMgr        *RelationManager
	autoRelMgr    *AutoRelationManager
	changeBus     *cluster.ChangeBus

	// Turbo engine: adaptive worker pool + priority scheduler + auto-batching
	// front doors that coalesce concurrent single-document requests into
	// fewer calls to the existing batch write paths. See ops_turbo.go.
	turbo                 *turbo.Coordinator
	turboInserts          *turbo.BatcherGroup[map[string]any]
	bulkModePrevWALPolicy wal.WALSyncPolicy

	// LineDat: temporal high-speed ingestion buffer, one more step of
	// this same insertion pipeline (not a separate process) that sits in
	// front of permanent storage for the local/standalone write paths
	// (InsertFast, GENERATE, IMPORT). See linedat.go.
	lineDat *LineDat

	renamingMu sync.RWMutex
	renaming   map[string]struct{}

	// serverMgr, cuando no es nil, permite comandos multi-servidor
	// (CREATE SERVER) y el enrutado de autenticación entre servidores
	// que comparten el mismo puerto. Lo asigna app.go tras crear el
	// Engine raíz; los Engine de cada servidor adicional (creados por
	// ServerManager) lo dejan en nil. Ver server_manager.go.
	serverMgr *ServerManager
}

type MetricsCollector struct {
	mu            sync.RWMutex
	queryCount    atomic.Int64
	queryDuration atomic.Int64
	slowQuery     atomic.Int64
	errors        atomic.Int64
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

func (m *MetricsCollector) RecordQuery(duration time.Duration) {
	m.queryCount.Add(1)
	m.queryDuration.Add(int64(duration.Microseconds()))
	if duration > time.Second {
		m.slowQuery.Add(1)
	}
}

func (m *MetricsCollector) Stats() map[string]any {
	count := m.queryCount.Load()
	dur := m.queryDuration.Load()
	avg := float64(0)
	if count > 0 {
		avg = float64(dur) / float64(count) / 1000
	}
	return map[string]any{
		"queries":      count,
		"avg_ms":       avg,
		"slow_queries": m.slowQuery.Load(),
		"errors":       m.errors.Load(),
	}
}

func NewEngine(config *Config) *Engine {
	startTime := time.Now()

	ctx, cancel := context.WithCancel(context.Background())

	nodeID := os.Getenv("CAIMANDB_NODE_ID")
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%10000)
	}

	dirMgr := storage.NewDirectoryManagerWithBackupRoot(config.DataRoot, config.BackupRoot)
	if err := dirMgr.Init(); err != nil {
		log().Fatal("Failed to initialize directory structure", zap.Error(err))
	}

	l1 := cache.NewL1Cache(int64(config.CacheMB), true, 1*time.Hour)
	l2 := cache.NewL2IndexCache(config.L2CacheMB*1024, config.LazyIndexLoading, 24*time.Hour)

	intelEngine := NewIntelligentEngine(config)

	engine := &Engine{
		dataRoot: config.DataRoot,
		pool: storage.NewDBPoolWithAdaptive(l1, l2, dirMgr, storage.AdaptiveConfig{
			Enabled:           config.StorageAIEnabled,
			RAMBudgetFraction: config.StorageAIRAMFraction,
			MaxBudgetBytes:    int64(config.StorageAIMaxBudgetMB) << 20,
		}),
		dirMgr:      dirMgr,
		l1Cache:     l1,
		l2Cache:     l2,
		wp:          cluster.NewWorkerPool(config.Workers),
		lockManager: newShardedLockManager(100000),
		metrics:     NewMetricsCollector(),
		queryOpt:    NewQueryOptimizer(),
		nodeID:      nodeID,
		intelEngine: intelEngine,
		config:      config,
		changeBus:   cluster.NewChangeBus(),
		startupTime: startTime,
		stopChan:    make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		bufferPool:  storage.NewBufferPool(128 << 20),
	}

	engine.tokenManager = NewTokenManager(config.JWTSecret, config.TokenExpiry)
	engine.rateLimiter = NewRateLimiter(config.RateLimit, time.Minute)
	engine.riskEngine = NewRiskEngine()

	if config.LargeDocThreshold > 0 {
		externalPath := filepath.Join(config.DataRoot, "__external")
		if extStore, err := storage.NewFileSystemStorage(externalPath); err == nil {
			engine.externalStore = extStore
			log().Info("External storage initialized", zap.String("path", externalPath))
		}
	}

	engine.shardMgr = NewShardManager(nodeID, config, intelEngine)
	engine.relMgr = NewRelationManager(config.DataRoot)
	engine.autoRelMgr = NewAutoRelationManager(config.DataRoot, config)

	// Initialize FLEX-COLUMN
	flexConfig := config.FlexColumn
	if flexConfig == nil {
		flexConfig = DefaultFlexColumnConfig()
	}
	engine.flexEngine = NewFlexColumnEngine(engine, flexConfig)

	walDir := filepath.Join(config.DataRoot, systemDB, "wal")
	if w, err := wal.NewWAL(walDir, config.WALSegments, config.UseStreamingWAL); err == nil {
		engine.wal = w
		switch config.WALSyncPolicy {
		case "always":
			w.SetSyncPolicy(wal.WALSyncAlways)
		case "off":
			w.SetSyncPolicy(wal.WALSyncOff)
		default:
			w.SetSyncPolicy(wal.WALSyncInterval)
		}
		log().Info("WAL initialized",
			zap.String("dir", walDir),
			zap.Bool("streaming", config.UseStreamingWAL),
			zap.String("sync_policy", config.WALSyncPolicy))
	} else {
		log().Warn("Failed to initialize WAL", zap.Error(err))
	}

	if engine.wal != nil {
		engine.txManager = NewTxManager(engine, engine.wal, config.TransactionTimeout)
		log().Info("ACID Transaction Manager initialized",
			zap.String("isolation", engine.txManager.isolation),
			zap.Duration("timeout", config.TransactionTimeout))
	} else {
		log().Warn("ACID Transaction Manager initialized without WAL (durability not guaranteed)")
		engine.txManager = NewTxManager(engine, nil, config.TransactionTimeout)
	}

	auditPath := filepath.Join(config.DataRoot, "__audit.log")
	if auditLogger, err := NewAuditLogger(auditPath); err == nil {
		engine.auditLogger = auditLogger
		log().Info("Audit logger initialized", zap.String("path", auditPath))
	} else {
		log().Warn("Failed to initialize audit logger", zap.Error(err))
	}

	if engine.wal != nil && !config.FastStartup {
		if applied, err := RecoverWAL(engine.wal, engine); err == nil {
			log().Info("WAL recovery completed", zap.Int64("entries", applied))
		} else {
			log().Warn("WAL recovery failed", zap.Error(err))
		}
	} else if engine.wal != nil && config.FastStartup {
		log().Warn("FAST STARTUP: WAL recovery skipped - data loss possible on crash")
		if err := engine.wal.Truncate(); err != nil {
			log().Warn("Failed to truncate WAL", zap.Error(err))
		}
	}

	safeGoLoop("engine_background_cleanup", engine.backgroundCleanup)
	safeGoLoop("engine_maintenance_loop", engine.maintenanceLoop)
	safeGoLoop("engine_checkpoint_loop", engine.checkpointLoop)
	safeGoLoop("engine_auto_relation_cleanup_loop", engine.autoRelationCleanupLoop)
	safeGoLoop("engine_backup_scheduler_loop", engine.backupSchedulerLoop)
	engine.startBlobGC()

	if config.LineDatEnabled {
		if ld, err := newLineDat(engine); err == nil {
			engine.lineDat = ld
			ld.Start()
			log().Info("LineDat initialized",
				zap.Int64("recovered_pending", ld.pendingCount.Load()),
				zap.Int("batch_size", ld.batchSize),
				zap.Duration("flush_interval", ld.flushEvery),
				zap.Int("drain_workers", ld.drainWorkers),
				zap.Int("shards", ld.numShards),
				zap.Int64("max_pending_docs", ld.maxPending),
				zap.Int64("max_pending_bytes", ld.maxPendingBytes),
				zap.String("journal_dir", ld.journalDir))
		} else {
			log().Warn("Failed to initialize LineDat; local inserts will use the direct synchronous path", zap.Error(err))
		}
	}

	newTurboCoordinator(engine)
	log().Info("Turbo engine initialized",
		zap.Bool("enabled", config.TurboEnabled),
		zap.Int("min_workers", config.TurboMinWorkers),
		zap.Int("max_workers", config.TurboMaxWorkers),
		zap.Int("batch_max_size", config.TurboBatchMaxSize),
		zap.Duration("batch_max_wait", config.TurboBatchMaxWait))

	log().Info("Engine initialized successfully",
		zap.String("node_id", nodeID),
		zap.Duration("startup_time", time.Since(startTime)),
		zap.Bool("acid_enabled", engine.txManager != nil),
		zap.String("isolation_level", engine.txManager.isolation),
		zap.Int("shards", config.ShardCount),
		zap.Int("replicas", config.ReplicaCount),
		zap.Int("max_nodes", config.MaxClusterNodes),
		zap.Int("max_shards_per_node", config.MaxShardsPerNode),
		zap.Int64("max_document_size", config.MaxDocumentSize),
		zap.Int64("compress_threshold", config.CompressThreshold),
		zap.Int64("large_doc_threshold", config.LargeDocThreshold),
		zap.Bool("fast_startup", config.FastStartup),
		zap.Bool("compress_large_docs", config.CompressLargeDocs),
		zap.Bool("lazy_index_loading", config.LazyIndexLoading),
		zap.Bool("predictive_scaling", config.PredictiveScaling),
		zap.Bool("auto_cluster", config.AutoCluster),
		zap.String("wal_status", map[bool]string{true: "enabled", false: "disabled"}[engine.wal != nil]),
		zap.Bool("flex_column_enabled", flexConfig.Enabled),
		zap.Bool("flex_columnar_index", flexConfig.ColumnarIndexEnabled),
		zap.Bool("flex_materialized_views", flexConfig.MaterializedViews),
	)

	return engine
}

func (e *Engine) backgroundCleanup() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.cleanupExpiredData()
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) cleanupExpiredData() {
	e.l1Cache.CleanupExpired()
	if e.tokenManager != nil {
		e.tokenManager.Cleanup()
	}
	log().Debug("Background cleanup completed")
}

// autoRelationCleanupLoop periodically sweeps expired temporal
// auto-relations (see relations_auto.go). It runs far more often than
// backgroundCleanup's hourly tick because auto-relation TTLs are
// typically much shorter (minutes to a day) than cache/token expiry,
// so a coarser sweep would let visibly-stale relations linger in
// SHOW AUTORELATIONS output.
func (e *Engine) autoRelationCleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if removed := e.autoRelMgr.CleanupExpired(); removed > 0 {
				log().Debug("Auto-relation cleanup completed", zap.Int("removed", removed))
			}
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) InitCluster() error {
	if !e.config.AutoCluster {
		log().Info("Auto-cluster disabled, running in standalone mode")
		return nil
	}

	cluster, err := NewClusterManager(e, e.config)
	if err != nil {
		return err
	}
	e.cluster = cluster
	e.distQuery = NewDistributedQueryExecutor(e, cluster, e.shardMgr)
	cluster.Start()

	if e.wal != nil && !e.config.FastStartup {
		if applied, err := RecoverWAL(e.wal, e); err == nil {
			log().Info("WAL recovery applied", zap.Int64("entries", applied))
		} else {
			log().Warn("WAL recovery failed", zap.Error(err))
		}
	} else if e.wal != nil && e.config.FastStartup {
		if err := e.wal.Truncate(); err != nil {
			log().Warn("Failed to truncate WAL", zap.Error(err))
		}
	}

	startupDuration := time.Since(e.startupTime)
	metrics.MetricStartupTime.Set(startupDuration.Seconds())
	log().Info("Engine startup completed",
		zap.Duration("duration", startupDuration),
		zap.Bool("fast_startup", e.config.FastStartup),
		zap.Bool("compress_large", e.config.CompressLargeDocs),
		zap.Int64("max_doc_size", e.config.MaxDocumentSize))

	log().Info("Auto-cluster initialized",
		zap.String("node_id", e.nodeID),
		zap.Int("shards", e.shardMgr.shardCount),
		zap.Int("replicas", e.shardMgr.replicaCount),
		zap.Int("max_nodes", e.config.MaxClusterNodes))

	return nil
}
