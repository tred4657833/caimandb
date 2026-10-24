// Package metrics defines and registers CaimanDB's Prometheus metrics.
// Every other package (including the root caimandb package) increments/
// observes these directly; this package owns only their definition and
// one-time registration (InitMetrics).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

var (
	MetricOpsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "caimandb_operations_total",
			Help: "Total number of operations",
		},
		[]string{"type", "db", "block", "shard", "node", "status"},
	)

	MetricOpsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "caimandb_operation_duration_seconds",
			Help:    "Duration of operations",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20),
		},
		[]string{"type", "db", "block", "shard", "node"},
	)

	MetricCacheHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_cache_hits_total",
			Help: "Total cache hits",
		},
	)

	MetricCacheMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_cache_misses_total",
			Help: "Total cache misses",
		},
	)

	MetricActiveQueries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "caimandb_active_queries",
			Help: "Number of active queries",
		},
	)

	MetricIndexSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_index_size_bytes",
			Help: "Size of secondary indexes",
		},
		[]string{"db", "block", "field", "shard", "node"},
	)

	MetricDocumentsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_documents_total",
			Help: "Total number of documents",
		},
		[]string{"db", "block", "shard", "node"},
	)

	MetricShardDistribution = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_shard_distribution",
			Help: "Distribution of documents across shards",
		},
		[]string{"db", "block", "shard", "node"},
	)

	MetricClusterStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_cluster_status",
			Help: "Cluster node status",
		},
		[]string{"node", "role"},
	)

	MetricRaftCommits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_raft_commits_total",
			Help: "Total Raft commits",
		},
	)

	MetricAutoScaleEvents = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_autoscale_events_total",
			Help: "Total auto-scale events",
		},
	)

	MetricWALWrites = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_wal_writes_total",
			Help: "Total WAL writes",
		},
	)

	MetricCompactions = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_compactions_total",
			Help: "Total compactions performed",
		},
	)

	MetricTransactionCommits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_transaction_commits_total",
			Help: "Total transaction commits",
		},
	)

	MetricTransactionRollbacks = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_transaction_rollbacks_total",
			Help: "Total transaction rollbacks",
		},
	)

	MetricStartupTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "caimandb_startup_duration_seconds",
			Help: "Time taken to start up",
		},
	)

	MetricDocSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "caimandb_document_size_bytes",
			Help:    "Size of documents inserted",
			Buckets: []float64{1024, 10240, 102400, 1048576, 5242880, 10485760, 52428800},
		},
	)

	MetricQueryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "caimandb_query_latency_seconds",
			Help:    "Query latency",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"db", "block", "type"},
	)

	MetricMemoryUsage = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "caimandb_memory_usage_bytes",
			Help: "Memory usage in bytes",
		},
	)

	MetricDiskUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_disk_usage_bytes",
			Help: "Disk usage in bytes",
		},
		[]string{"db", "type"},
	)

	MetricAuthFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_auth_failures_total",
			Help: "Total authentication failures",
		},
	)

	MetricWALSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "caimandb_wal_size_bytes",
			Help: "WAL size in bytes",
		},
	)

	MetricRiskBlocks = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_risk_blocks_total",
			Help: "Total operations blocked by risk engine",
		},
	)

	MetricChangeEventsDropped = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "caimandb_change_events_dropped_total",
			Help: "Total real-time change-stream events dropped due to a slow/full subscriber",
		},
	)

	// MetricRaftReplicationLag is, per node, the number of seconds
	// since that node's Raft instance last had successful contact with
	// its leader (hashicorp/raft's own (*raft.Raft).LastContact(),
	// which every node already tracks internally for election-timeout
	// purposes -- this just exposes it). 0 on the leader itself (it
	// doesn't have a leader to be behind) and on a freshly-started
	// single-node cluster; a healthy follower stays near 0 (leader
	// heartbeats every HeartbeatTimeout); a growing value on a
	// follower means it isn't hearing from the leader -- a real,
	// wall-clock replication-health signal, not an approximation.
	// Set by (*ClusterManager).reportRaftLagMetric in cluster.go,
	// polled periodically the same way MetricClusterStatus already is
	// -- see that function's doc comment for exactly what "lag" means
	// here and its known imprecision (it's index distance, not wall
	// time, since hashicorp/raft doesn't expose a per-peer replication
	// timestamp).
	MetricRaftReplicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_raft_replication_lag",
			Help: "Seconds since this node's Raft instance last had successful contact with its leader (0 on the leader itself)",
		},
		[]string{"node"},
	)

	// MetricShardHealth is a 0/1 gauge per shard: 1 means the shard's
	// primary node considers it reachable/serving (the same check
	// ShardManager already performs for routing decisions), 0 means it
	// doesn't. This complements MetricShardDistribution (document
	// counts) with an explicit up/down signal that doesn't require
	// inferring health from a count changing or staying at zero. Set
	// by (*Engine).reportShardHealthMetrics, polled the same way
	// MetricDocumentsTotal already is.
	MetricShardHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "caimandb_shard_health",
			Help: "Shard health: 1 if the shard's assigned node is currently considered reachable/serving, 0 otherwise",
		},
		[]string{"shard", "node"},
	)

	metricsOnce sync.Once
)

func InitMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(
			MetricOpsTotal,
			MetricOpsDuration,
			MetricCacheHits,
			MetricCacheMisses,
			MetricActiveQueries,
			MetricIndexSize,
			MetricDocumentsTotal,
			MetricShardDistribution,
			MetricClusterStatus,
			MetricRaftCommits,
			MetricAutoScaleEvents,
			MetricWALWrites,
			MetricCompactions,
			MetricTransactionCommits,
			MetricTransactionRollbacks,
			MetricStartupTime,
			MetricDocSize,
			MetricQueryLatency,
			MetricMemoryUsage,
			MetricDiskUsage,
			MetricAuthFailures,
			MetricWALSize,
			MetricRiskBlocks,
			MetricChangeEventsDropped,
			MetricRaftReplicationLag,
			MetricShardHealth,
		)
	})
}
