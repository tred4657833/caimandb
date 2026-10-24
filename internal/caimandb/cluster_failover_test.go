package caimandb

// TestCluster_LeaderFailover_DataSurvives covers the last row of the
// original reliability matrix: nodo Raft caído. It builds a real
// 3-node cluster -- real hashicorp/raft instances, real TCP transports
// bound to 127.0.0.1, real consensus -- then takes the current leader
// down and checks the other two elect a new leader and still have every
// document that was committed before the leader died.
//
// whole session (RaftFSM.Restore() decoding into unexported struct
// fields and silently restoring nothing) on the line: if that fix were
// wrong, or if there's a second problem in the snapshot/replication path
// other test in this package works against a single Engine and never
// exercises Restore() or real multi-node replication at all.
//
// Two deliberate simplifications from the crash-recovery harness in
// over:
//
//     auto-discovery (NodeDiscoverer/autoDiscoveryLoop) entirely,
//     joining nodes with direct raft.AddVoter calls instead. Broadcast
//     discovery depends on OS/network broadcast behavior on whatever
//     machine or CI runner this executes on -- firewalls, VPNs, and
//     Windows network-profile settings can all silently break it -- and
//     a test that hangs or flakes because of that would be
//     indistinguishable from a real bug without a lot of unrelated
//     debugging. AddVoter is the same primitive the auto-join path
//     itself calls internally (see handleAutoJoin in cluster.go); this
//     test just calls it directly instead of waiting for discovery to
//     trigger it, which makes cluster formation deterministic without
//     testing a different code path for the replication/failover
//     behavior this test is actually about.
//  2. "Node caído" here means calling the leader's raft.Shutdown()
//     directly -- a real, exported hashicorp/raft primitive that stops
//     that node's participation in the cluster (no more heartbeats, no
//     more responding to RPCs) -- rather than killing a whole separate
//     OS process the way crash_recovery_test.go does. That's the
//     standard, well-supported way multi-node Raft tests simulate a
//     node becoming unreachable without needing separate OS processes
//     per node, and it's sufficient for what this test checks: does the
//     rest of the cluster detect the loss and elect a new leader, and
//     does replicated data survive. It does NOT exercise what happens
//     to the dead node's own on-disk state (that's what
//     crash_recovery_test.go and wal/wal_test.go are for) or a true
//     process-level crash of a cluster node -- combining the two would
//     be a reasonable further test, not attempted here.
//
// initRaftWithCustomLogStore bootstraps *every* fresh node as its own
// single-server cluster before any joining happens (see its comment in
// cluster.go), so node2 and node3 below each briefly exist as their own
// independent one-node "clusters" before being folded into node1's via
// AddVoter. Raft's protocol is supposed to handle a node with stale/
// tiny log history being told to follow a real leader with a higher
// merge pattern isn't something this package's other tests exercise
// smoothly in this codebase rather than just in theory.
import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free TCP port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newClusterNode(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataRoot = t.TempDir()
	cfg.AutoCluster = true
	cfg.RaftBind = "127.0.0.1"
	cfg.RaftPort = freeTCPPort(t)
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.WALSyncPolicy = "off"
	cfg.LargeDocThreshold = 0
	// See the matching comment in newConcurrencyTestEngine
	// (ops_insert_concurrency_test.go): DefaultConfig's 2GB cache default
	// is sized for production, not for a test process that spins up
	// several of these (three per cluster here) in the same run.
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64

	e := NewEngine(cfg)
	if err := e.InitCluster(); err != nil {
		t.Fatalf("InitCluster failed (raft port %d): %v", cfg.RaftPort, err)
	}
	return e
}

// waitForConfigSize polls a node's own view of the Raft configuration
// until it has exactly `want` servers or times out. Needed after
// AddVoter because a configuration change has to actually replicate to
// each node before that node's own GetConfiguration() reflects it --
// AddVoter's future completing only guarantees the *leader* has
// committed the change, not that every follower has caught up yet.
func waitForConfigSize(t *testing.T, cm *ClusterManager, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		future := cm.raft.GetConfiguration()
		if err := future.Error(); err == nil {
			if got := len(future.Configuration().Servers); got == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s: configuration did not reach %d servers within %s", cm.nodeID, want, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func currentLeader(t *testing.T, nodes []*Engine) *Engine {
	t.Helper()
	var leaders []*Engine
	for _, n := range nodes {
		if n.cluster.IsLeader() {
			leaders = append(leaders, n)
		}
	}
	if len(leaders) != 1 {
		t.Fatalf("expected exactly 1 leader among %d nodes, found %d", len(nodes), len(leaders))
	}
	return leaders[0]
}

func TestCluster_LeaderFailover_DataSurvives(t *testing.T) {
	node1 := newClusterNode(t)
	node2 := newClusterNode(t)
	node3 := newClusterNode(t)
	// Cleanup order doesn't matter here: Engine.Shutdown -> cluster.Stop
	// -> raft.Shutdown() is documented/known-safe to call even on a node
	// whose raft was already separately shut down mid-test (Shutdown is
	// idempotent in hashicorp/raft).
	t.Cleanup(node1.Shutdown)
	t.Cleanup(node2.Shutdown)
	t.Cleanup(node3.Shutdown)

	if err := node1.cluster.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("node1 never became leader of its own single-node cluster: %v", err)
	}
	if !node1.cluster.IsLeader() {
		t.Fatalf("node1 should be the leader of its freshly-bootstrapped single-node cluster")
	}

	for _, n := range []*Engine{node2, node3} {
		future := node1.cluster.raft.AddVoter(
			raft.ServerID(n.cluster.nodeID),
			raft.ServerAddress(n.cluster.advertise),
			0, raftTimeout)
		if err := future.Error(); err != nil {
			t.Fatalf("AddVoter(%s) failed: %v", n.cluster.nodeID, err)
		}
	}

	nodes := []*Engine{node1, node2, node3}
	for _, n := range nodes {
		waitForConfigSize(t, n.cluster, 3, 10*time.Second)
		n.cluster.refreshSingleNodeStatus()
		if n.cluster.IsSingleNode() {
			t.Fatalf("node %s still reports IsSingleNode() after joining a 3-node cluster", n.cluster.nodeID)
		}
	}

	leader := currentLeader(t, nodes)
	t.Logf("cluster formed, leader is %s", leader.cluster.nodeID)

	const dbName = "raft_failover_test"
	const block = "block1"
	if err := leader.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB via leader failed: %v", err)
	}
	if err := leader.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock via leader failed: %v", err)
	}

	const preFailoverDocs = 100
	docs := make([]map[string]any, preFailoverDocs)
	for i := range docs {
		docs[i] = map[string]any{"seq": i, "phase": "pre_failover"}
	}
	inserted, errs := leader.InsertBatch(dbName, block, docs)
	if errs != 0 {
		t.Fatalf("InsertBatch (pre-failover) reported %d errors", errs)
	}
	if inserted != preFailoverDocs {
		t.Fatalf("InsertBatch (pre-failover) inserted %d, want %d", inserted, preFailoverDocs)
	}

	// raft.Apply() (inside InsertBatch, above) only guarantees the
	// pre-failover writes are committed to a majority and applied to
	// the LEADER's own FSM before it returns -- hashicorp/raft applies
	// committed entries to every OTHER node's FSM asynchronously, as
	// each one processes the AppendEntries/heartbeat stream in the
	// background. A node can therefore be a fully caught-up member of
	// the committed majority (its log has the entries durably) while
	// its FSM -- and so Count(), which reads local Badger state -- is
	// still a beat behind.

	// The actual "node caído": stop the leader's Raft participation
	// directly. See the file comment for why this, not an OS-level kill,
	// is the right primitive for what this test checks.
	if err := leader.cluster.raft.Shutdown().Error(); err != nil {
		t.Fatalf("leader raft.Shutdown() failed: %v", err)
	}
	deadNodeID := leader.cluster.nodeID

	var survivors []*Engine
	for _, n := range nodes {
		if n.cluster.nodeID != deadNodeID {
			survivors = append(survivors, n)
		}
	}
	if len(survivors) != 2 {
		t.Fatalf("expected 2 survivors, got %d", len(survivors))
	}

	// electionTimeout is 2s; give a generous multiple of that for a new
	// leader to be elected among the two survivors.
	var newLeader *Engine
	deadline := time.Now().Add(6 * electionTimeout)
	for newLeader == nil {
		for _, n := range survivors {
			if n.cluster.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no new leader elected among survivors within %s of the old leader (%s) going down", 6*electionTimeout, deadNodeID)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader.cluster.nodeID == deadNodeID {
		t.Fatalf("elected leader is the node that was supposedly shut down")
	}
	t.Logf("new leader after failover: %s (old leader %s)", newLeader.cluster.nodeID, deadNodeID)

	// Election only guarantees the new leader's own log/FSM is at
	// least as current as a majority's -- it says nothing about how
	// far the OTHER survivor's FSM has caught up applying entries it
	// already had committed durably before the old leader died.
	//
	// hashicorp/raft has no public, safe way to wait for a specific
	// FOLLOWER's FSM to finish applying a given index:
	// raft.AppliedIndex() (confirmed by reading its doc comment in
	// api.go in the installed module) only reflects the last index
	// *handed off* to the FSM's apply channel, not one the FSM has
	// finished processing -- so waiting on it can still observe a
	// follower whose insert_batch was dispatched but hasn't actually
	// it reproduced as a real, intermittent test failure here (Count()
	// still 0 right after AppliedIndex() caught up), roughly 1 in 6-8
	// runs under `-race` where the FSM goroutine's scheduling delay
	// widens the window enough to observe it -- confirmed by temporarily
	// logging each node's insert_batch completion inside the FSM (see
	// git history if that debug logging is ever needed again). And
	// raft.Barrier(), which genuinely does wait for the FSM to drain,
	// can only be issued on the leader ("This must be run on the
	// leader, or it will fail" -- api.go) -- useless for a survivor
	// that isn't the new leader.
	//
	// So: stop trying to infer FSM-apply completion from Raft's
	// internals, and poll the actual thing this test cares about --
	// Count() itself -- until it converges or times out. A survivor
	// that's genuinely missing replicated data (a real bug, not just
	// apply lag) will never converge and the deadline below turns that
	// into a Fatalf, so this still can't mask real data loss.
	pollDeadline := time.Now().Add(6 * electionTimeout)
	for _, n := range survivors {
		for {
			c, err := n.Count(dbName, block, nil, nil)
			if err != nil {
				t.Fatalf("Count on survivor %s while waiting for FSM to catch up failed: %v", n.cluster.nodeID, err)
			}
			if c >= preFailoverDocs {
				break
			}
			if time.Now().After(pollDeadline) {
				t.Fatalf("survivor %s still reports %d/%d documents %s after the new leader was elected -- this is real replication lag/loss, not just an unlucky timing window",
					n.cluster.nodeID, c, preFailoverDocs, 6*electionTimeout)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// The real question: did the documents committed before the old
	// leader died survive on a node that never had its own local Badger
	// written to directly by us -- proving they got there through Raft
	// replication, not just local durability.
	var countAfterFailover int64
	var err error
	for _, n := range survivors {
		countAfterFailover, err = n.Count(dbName, block, nil, nil)
		if err != nil {
			t.Fatalf("Count on survivor %s after failover failed: %v", n.cluster.nodeID, err)
		}
		if countAfterFailover != preFailoverDocs {
			t.Errorf("survivor %s reports %d documents after failover, want %d (pre-failover writes must have replicated, not just landed on the old leader's local disk)",
				n.cluster.nodeID, countAfterFailover, preFailoverDocs)
		}
	}

	// Cluster must still be able to serve writes after failover, not
	// just have survived with old data.
	const postFailoverDocs = 20
	moreDocs := make([]map[string]any, postFailoverDocs)
	for i := range moreDocs {
		moreDocs[i] = map[string]any{"seq": i, "phase": "post_failover"}
	}
	insertedAfter, errsAfter := newLeader.InsertBatch(dbName, block, moreDocs)
	if errsAfter != 0 {
		t.Fatalf("InsertBatch via new leader (post-failover) reported %d errors", errsAfter)
	}
	if insertedAfter != postFailoverDocs {
		t.Fatalf("InsertBatch via new leader (post-failover) inserted %d, want %d", insertedAfter, postFailoverDocs)
	}

	finalCount, err := newLeader.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("final Count on new leader failed: %v", err)
	}
	if want := int64(preFailoverDocs + postFailoverDocs); finalCount != want {
		t.Errorf("final Count on new leader = %d, want %d", finalCount, want)
	}

	// And the other survivor (not the new leader) must have caught up too.
	for _, n := range survivors {
		if n.cluster.nodeID == newLeader.cluster.nodeID {
			continue
		}
		deadline := time.Now().Add(5 * time.Second)
		var c int64
		for {
			c, err = n.Count(dbName, block, nil, nil)
			if err != nil {
				t.Fatalf("Count on remaining follower %s failed: %v", n.cluster.nodeID, err)
			}
			if c == int64(preFailoverDocs+postFailoverDocs) || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if c != int64(preFailoverDocs+postFailoverDocs) {
			t.Errorf("remaining follower %s has %d documents, want %d (post-failover writes must replicate to every surviving node, not just the new leader)",
				n.cluster.nodeID, c, preFailoverDocs+postFailoverDocs)
		}
	}
}
