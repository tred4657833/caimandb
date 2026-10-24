package caimandb

// observability_metrics_test.go covers reportRaftLagMetric
// (cluster.go) and reportShardHealthMetrics (shard_manager.go) against
// real Prometheus gauge values (via testutil.ToFloat64), not just
// "did it panic" -- confirming both the leader/follower and
// healthy/degraded branches actually set the number they claim to.
import (
	"testing"
	"time"

	"caimandb/internal/caimandb/metrics"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gaugeValue reads back a *prometheus.GaugeVec's current value for the
// given label values, without pulling in the testutil package (which
// isn't yet a direct go.mod dependency here -- this avoids a
// go.mod/go.sum change just for tests).
func gaugeValue(t *testing.T, g *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.WithLabelValues(labels...).Write(&m); err != nil {
		t.Fatalf("reading gauge value: %v", err)
	}
	return m.GetGauge().GetValue()
}

// TestReportRaftLagMetric_Leader confirms a real single-node Raft
// cluster (which is always its own leader) reports 0 lag for itself.
func TestReportRaftLagMetric_Leader(t *testing.T) {
	metrics.InitMetrics()
	e := newConcurrencyTestEngine(t)
	cfg := DefaultConfig()
	cfg.DataRoot = e.dirMgr.DataRoot()
	cfg.AutoCluster = true
	cfg.RaftBind = "127.0.0.1"
	cfg.RaftPort = freeTCPPort(t)
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	e.config = cfg
	if err := e.InitCluster(); err != nil {
		t.Fatalf("InitCluster: %v", err)
	}

	if err := e.cluster.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("never became leader of its own single-node cluster: %v", err)
	}
	if e.cluster.raft.State() != raft.Leader {
		t.Fatalf("expected State()==Leader for a single-node cluster, got %v", e.cluster.raft.State())
	}

	e.cluster.reportRaftLagMetric()

	got := gaugeValue(t, metrics.MetricRaftReplicationLag, e.cluster.nodeID)
	if got != 0 {
		t.Fatalf("expected 0 lag for the leader, got %v", got)
	}
}

// TestReportRaftLagMetric_NoCluster confirms calling this on a
// standalone (non-cluster) engine is a safe no-op rather than a panic
// -- both nil-cluster and nil-raft-instance cases matter since
// (*Engine).cluster is a *ClusterManager that can itself be non-nil
// with cluster.raft still nil before InitCluster fully completes.
func TestReportRaftLagMetric_NoCluster(t *testing.T) {
	var cm *ClusterManager
	cm.reportRaftLagMetric() // must not panic on a nil receiver

	cm2 := &ClusterManager{}
	cm2.reportRaftLagMetric() // must not panic with raft==nil
}

// TestReportShardHealthMetrics_HealthyAndDegraded confirms
// reportShardHealthMetrics sets 1 for a healthy shard and 0 for one
// HandleNodeFailure has already marked "degraded" (the same real
// code path a lost primary with no available replica goes through in
// production), against a real ShardManager, not a hand-built one.
func TestReportShardHealthMetrics_HealthyAndDegraded(t *testing.T) {
	metrics.InitMetrics()
	e := newConcurrencyTestEngine(t)
	cfg := DefaultConfig()
	cfg.ShardCount = 4
	cfg.ReplicaCount = 1

	sm := NewShardManager("node-a", cfg, e.intelEngine)
	sm.InitializeShards()

	// Give shard_0 a real primary that reports "active", and simulate
	// that same node failing with no eligible replica to take over --
	// exactly what HandleNodeFailure does to mark a shard "degraded".
	sm.mu.Lock()
	sm.clusterNodes["node-a"] = &NodeInfo{ID: "node-a", Status: "active"}
	sm.shards["shard_0"].Primary = "node-a"
	sm.shards["shard_0"].Nodes = []string{"node-a"}
	sm.mu.Unlock()

	sm.reportShardHealthMetrics()
	if got := gaugeValue(t, metrics.MetricShardHealth, "shard_0", "node-a"); got != 1 {
		t.Fatalf("expected shard_0 to report healthy (1) before failure, got %v", got)
	}

	sm.HandleNodeFailure("node-a")
	sm.reportShardHealthMetrics()
	if got := gaugeValue(t, metrics.MetricShardHealth, "shard_0", "node-a"); got != 0 {
		t.Fatalf("expected shard_0 to report unhealthy (0) after HandleNodeFailure with no replica, got %v", got)
	}
}

// TestReportShardHealthMetrics_NilManager confirms this is a safe
// no-op on a nil *ShardManager (e.g. a standalone engine with
// clustering/sharding never initialized).
func TestReportShardHealthMetrics_NilManager(t *testing.T) {
	var sm *ShardManager
	sm.reportShardHealthMetrics() // must not panic
}

// TestReportShardHealthMetrics_SetsDistribution confirms
// MetricShardDistribution's DocCount actually gets Set() now --
// previously defined and registered but never once written to
// anywhere in the codebase (see the CHANGELOG entry that first
// flagged this).
func TestReportShardHealthMetrics_SetsDistribution(t *testing.T) {
	metrics.InitMetrics()
	cfg := DefaultConfig()
	cfg.ShardCount = 2
	cfg.ReplicaCount = 1
	sm := NewShardManager("node-x", cfg, nil)
	sm.InitializeShards()

	sm.mu.Lock()
	sm.clusterNodes["node-x"] = &NodeInfo{ID: "node-x", Status: "active"}
	for _, shard := range sm.shards {
		shard.Primary = "node-x"
		shard.DocCount = 42
	}
	sm.mu.Unlock()

	sm.reportShardHealthMetrics()

	for shardID := range sm.shards {
		got := gaugeValue(t, metrics.MetricShardDistribution, "", "", shardID, "node-x")
		if got != 42 {
			t.Errorf("shard %s: expected MetricShardDistribution=42, got %v", shardID, got)
		}
	}
}

// TestReportClusterStatusMetric confirms MetricClusterStatus -- like
// MetricShardDistribution above, defined and registered but never
// Set() anywhere until now -- correctly reports exactly one role as
// active (1) for a real single-node Raft cluster (its own leader) and
// the other three roles as 0.
func TestReportClusterStatusMetric(t *testing.T) {
	metrics.InitMetrics()
	e := newConcurrencyTestEngine(t)
	cfg := DefaultConfig()
	cfg.DataRoot = e.dirMgr.DataRoot()
	cfg.AutoCluster = true
	cfg.RaftBind = "127.0.0.1"
	cfg.RaftPort = freeTCPPort(t)
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	e.config = cfg
	if err := e.InitCluster(); err != nil {
		t.Fatalf("InitCluster: %v", err)
	}
	if err := e.cluster.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("never became leader of its own single-node cluster: %v", err)
	}

	e.cluster.reportRaftLagMetric() // also calls reportClusterStatusMetric

	wantActive := map[string]float64{"leader": 1, "follower": 0, "candidate": 0, "shutdown": 0}
	for role, want := range wantActive {
		got := gaugeValue(t, metrics.MetricClusterStatus, e.cluster.nodeID, role)
		if got != want {
			t.Errorf("role %s: expected %v, got %v", role, want, got)
		}
	}
}
