package caimandb

import (
	"errors"
	"time"
)

const (
	Version = "1.0.0"
	Build   = "production"

	prefixDoc       = "d/"
	prefixMeta      = "m/"
	prefixSecondary = "s/"
	prefixWAL       = "wal/"
	prefixSnapshot  = "snap/"

	usersDBName = "__users"
	raftDBName  = "__raft"
	shardDBName = "__shards"
	clusterDB   = "__cluster"
	systemDB    = "__system"

	defaultCacheMB            = 2048
	defaultL2CacheMB          = 4096
	defaultShardCount         = 16
	defaultReplicaCount       = 3
	defaultWorkers            = 0
	defaultDataRoot           = "./data"
	defaultLogLevel           = "info"
	defaultMetricsPort        = 9090
	defaultRaftPort           = 2335
	defaultRaftBind           = "0.0.0.0"
	defaultQueryPort          = 1555
	defaultAdminPort          = 1556
	defaultUIDir              = "ui"
	defaultWALSegments        = 64
	defaultConfigFile         = "configs/caimandb.conf"
	defaultQueryTimeout       = 60
	defaultTransactionTimeout = 30

	badgerValueLogSize       = 64 << 20
	badgerMemTableSize       = 128 << 20
	badgerNumMemTables       = 4
	badgerNumLevelZeroTables = 8
	badgerBlockCacheSize     = 512 << 20
	badgerMaxBatchSize       = 10000
	badgerValueThreshold     = 1024

	shardMaxDocs         = 1000000
	shardMinDocs         = 10000
	shardMaxSizeBytes    = 50 << 30
	clusterNodeThreshold = 1000000
	nodeJoinInterval     = 30 * time.Second
	scaleCheckInterval   = 10 * time.Second
	// scaleGracePeriod holds off any auto-scale decision for this long
	// after the shard manager starts. checkAndScale judges shard load
	// purely from totalDocs/shardCount, and right after startup (or at
	// the very beginning of a large IMPORT/bulk load) totalDocs is still
	// near zero simply because writes haven't caught up yet -- not
	// because the dataset is actually small. Without this grace period
	// that reads as "way under MinShardDocs" and triggers an immediate,
	// spurious scaleDown a few seconds in (observed: shardCount 16->8
	// ~9s after a fresh IMPORT started), which is pure overhead for a
	// decision that had no real signal behind it yet.
	scaleGracePeriod = 60 * time.Second
	walSegmentSize       = 128 << 20
	maxWALSegments       = 32

	maxShardsPerNode = 1000000
	maxClusterNodes  = 10000000

	maxDocumentSize    = 10 << 20
	compressThreshold  = 100 << 10
	indexSkipThreshold = 1 << 20
	largeDocThreshold  = 5 << 20

	defaultTokenExpiry = 24 * time.Hour
	maxLoginAttempts   = 5
	lockoutDuration    = 15 * time.Minute

	// Argon2id parameters
	argonTime    = 2
	argonMemory  = 64 * 1024
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16
)

const (
	raftSnapshotRetain = 10
	raftMaxPool        = 20
	raftTimeout        = 30 * time.Second
	heartbeatInterval  = 500 * time.Millisecond
	electionTimeout    = 2 * time.Second
	leaderLeaseTimeout = 500 * time.Millisecond
)

const (
	colPrefix      = "col/"
	colMetaPrefix  = "colmeta/"
	viewPrefix     = "view/"
	viewDataPrefix = "viewdata/"
	hotFieldPrefix = "hot/"
	colUpdateQueue = "colqueue/"

	defaultColumnarCacheMB    = 512
	defaultHotFieldThreshold  = 0.1
	maxColumnarFields         = 20
	minQueriesForHotDetection = 100
	columnarBatchSize         = 1000
	columnarUpdateInterval    = 30 * time.Second
	maxColumnarValueSize      = 64 << 20
)

var (
	ErrDBNotFound       = errors.New("database not found")
	ErrBlockNotFound    = errors.New("block not found")
	ErrDocNotFound      = errors.New("document not found")
	ErrDocTooLarge      = errors.New("document too large")
	ErrUnauthorized     = errors.New("unauthorized")
	ErrForbidden        = errors.New("forbidden")
	ErrInvalidQuery     = errors.New("invalid query")
	ErrTimeout          = errors.New("operation timed out")
	ErrShardUnavailable = errors.New("shard unavailable")
	ErrNodeUnavailable  = errors.New("node unavailable")
	ErrDuplicateKey     = errors.New("duplicate key")
	ErrTransactionAbort = errors.New("transaction aborted")
	ErrRateLimited      = errors.New("rate limited")
	ErrRiskBlocked      = errors.New("operation blocked by risk engine")
)
