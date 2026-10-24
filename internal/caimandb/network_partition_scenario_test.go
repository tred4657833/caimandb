package caimandb

// TestCluster_NetworkPartition_MinorityCannotCommit_MajorityContinues is
// the "particiones de red" row of the reliability checklist: unlike
// cluster_failover_test.go (which stops a node's raft.Shutdown()
// entirely -- a clean, permanent departure) or crash_recovery_test.go
// (a real OS process death), this simulates the harder case: the old
// leader's process keeps running, still believes for a brief window that
// it might be leader, and can still be sent writes directly -- it just
// can no longer reach a majority of the cluster. This is what a real
// asymmetric network split (a bad top-of-rack switch, a firewall rule,
// a partial AZ outage) looks like from each node's point of view, and
// it's a meaningfully different failure mode from a node cleanly leaving
// or dying: split-brain protection has to come from Raft's own quorum
// requirement, actively enforced while the isolated node is still alive
// and reachable by a client that doesn't yet know it's partitioned.
import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestCluster_NetworkPartition_MinorityCannotCommit_MajorityContinues(t *testing.T) {
	ctrl := newPartitionController()

	var node1, node2, node3 *Engine
	withPartitionableCluster(t, ctrl, func() {
		node1 = newPartitionableClusterNode(t, ctrl)
		node2 = newPartitionableClusterNode(t, ctrl)
		node3 = newPartitionableClusterNode(t, ctrl)
	})
	t.Cleanup(node1.Shutdown)
	t.Cleanup(node2.Shutdown)
	t.Cleanup(node3.Shutdown)

	if err := node1.cluster.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("node1 never became leader of its own single-node cluster: %v", err)
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
		// Required after AddVoter: IsSingleNode() is a cached flag, only
		// refreshed on membership changes -- without this, every node
		// (leader included) keeps reading the stale "I'm still a
		// single-node cluster" value, which makes every write below take
		// the local-only fallback path instead of going through Raft at
		// all. See the matching call in cluster_failover_test.go.
		n.cluster.refreshSingleNodeStatus()
	}

	leader := currentLeader(t, nodes)
	var followers []*Engine
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	if len(followers) != 2 {
		t.Fatalf("expected 2 followers, got %d", len(followers))
	}
	t.Logf("cluster formed, leader is %s", leader.cluster.nodeID)

	const dbName = "partition_test"
	const block = "block1"
	if err := leader.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB via leader failed: %v", err)
	}
	if err := leader.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock via leader failed: %v", err)
	}

	const prePartitionDocs = 50
	docs := make([]map[string]any, prePartitionDocs)
	for i := range docs {
		docs[i] = map[string]any{"seq": i, "phase": "pre_partition"}
	}
	inserted, errs := leader.InsertBatch(dbName, block, docs)
	if errs != 0 || inserted != prePartitionDocs {
		t.Fatalf("pre-partition InsertBatch: inserted=%d errs=%d, want %d/0", inserted, errs, prePartitionDocs)
	}

	// Settle before injecting any fault: a successful Apply() future only
	// guarantees the entry is committed to a majority's Raft *log* --
	// followers apply it to their own FSM asynchronously as their commit
	// index catches up. Partitioning before that catch-up finishes would
	// test a different (also real, but separate) scenario -- a network
	// fault landing mid-replication -- and would make the split-brain
	// assertion below unreliable for reasons unrelated to it. So wait for
	// full convergence first, the same way a real chaos test establishes
	// a clean steady state before injecting a fault.
	for _, n := range nodes {
		deadline := time.Now().Add(60 * time.Second)
		for {
			c, err := n.Count(dbName, block, nil, nil)
			if err != nil {
				t.Fatalf("Count on %s while settling failed: %v", n.cluster.nodeID, err)
			}
			if c == prePartitionDocs {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s never settled to %d documents before the partition (stuck at %d) -- replication itself is broken, unrelated to partitioning", n.cluster.nodeID, prePartitionDocs, c)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Logf("all 3 nodes settled at %d documents before injecting the partition", prePartitionDocs)

	// The actual partition: isolate the leader from BOTH followers, but
	// leave the two followers able to talk to each other. This is a
	// clean 1-vs-2 split -- the leader is now in the minority.
	for _, f := range followers {
		ctrl.Partition(leader.cluster.advertise, f.cluster.advertise)
	}
	t.Logf("partitioned: %s isolated from [%s, %s]", leader.cluster.nodeID, followers[0].cluster.nodeID, followers[1].cluster.nodeID)

	// The majority side (the two followers) must elect a new leader
	// among themselves, same as a real failover.
	var newLeader *Engine
	deadline := time.Now().Add(6 * electionTimeout)
	for newLeader == nil {
		for _, n := range followers {
			if n.cluster.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("majority side never elected a new leader within %s of the partition", 6*electionTimeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("majority elected new leader: %s", newLeader.cluster.nodeID)

	// The old (now-isolated) leader must NOT be able to commit a write,
	// even though it may still locally believe it's leader for a brief
	// lease window -- this is the actual split-brain protection under
	// test. insertWithIDLocal-style silent local acceptance here would
	// be a real correctness bug: data a client believes was written
	// would be invisible to the rest of the cluster and would never
	// replicate anywhere, even after the partition heals.
	isolatedInserted, isolatedErrs := leader.InsertBatch(dbName, block, []map[string]any{
		{"phase": "attempted_during_partition_on_isolated_leader"},
	})
	if isolatedInserted != 0 || isolatedErrs == 0 {
		t.Errorf("isolated former leader %s accepted a write during the partition (inserted=%d errs=%d) -- "+
			"a minority-side node must refuse writes, not silently apply them locally where the rest of the "+
			"cluster will never see them", leader.cluster.nodeID, isolatedInserted, isolatedErrs)
	} else {
		t.Logf("isolated former leader correctly refused the write (inserted=%d errs=%d)", isolatedInserted, isolatedErrs)
	}

	// The cluster must still be usable: majority side keeps serving
	// writes during the partition.
	const duringPartitionDocs = 20
	moreDocs := make([]map[string]any, duringPartitionDocs)
	for i := range moreDocs {
		moreDocs[i] = map[string]any{"seq": i, "phase": "during_partition"}
	}
	insertedDuring, errsDuring := newLeader.InsertBatch(dbName, block, moreDocs)
	if errsDuring != 0 || insertedDuring != duringPartitionDocs {
		t.Fatalf("InsertBatch via majority leader during partition: inserted=%d errs=%d, want %d/0", insertedDuring, errsDuring, duringPartitionDocs)
	}

	// Heal the partition and confirm the formerly-isolated node rejoins
	// as a follower and catches up to the majority's state -- no
	// lingering divergence, no split-brain leftovers.
	for _, f := range followers {
		ctrl.Heal(leader.cluster.advertise, f.cluster.advertise)
	}
	t.Logf("partition healed")

	wantFinal := int64(prePartitionDocs + duringPartitionDocs)
	for _, n := range nodes {
		deadline := time.Now().Add(10 * time.Second)
		var c int64
		var err error
		for {
			c, err = n.Count(dbName, block, nil, nil)
			if err != nil {
				t.Fatalf("Count on %s after heal failed: %v", n.cluster.nodeID, err)
			}
			if c == wantFinal || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if c != wantFinal {
			t.Errorf("node %s has %d documents after heal+catch-up, want %d (must converge to the majority's state, including the writes it missed while partitioned)",
				n.cluster.nodeID, c, wantFinal)
		} else {
			t.Logf("node %s converged to %d documents after heal", n.cluster.nodeID, c)
		}
	}
}
