package caimandb

// TestCluster_AsymmetricPartition_LeaderStaysStableNoSplitBrain covers
// the "particiones de red" case a symmetric cut doesn't exercise: a
// one-directional fault (the leader can no longer dial one follower, but
// that follower can still dial everyone else -- a routing/firewall
// asymmetry, not a full link failure). The Raft paper calls this out as
// the harder case for exactly the reason this test verifies: the
// cut-off follower can still reach the rest of the cluster to try to
// call disruptive elections, which -- without protection -- could force
// needless leadership churn on an otherwise perfectly healthy majority.
//
// What actually happens, confirmed by running this against the real
// hashicorp/raft v1.7.3 dependency (not assumed from reading the code):
//   - The leader retains contact with a majority (itself + the other,
//     unaffected follower), so it keeps its lease and never needs to
//     step down -- Raft's quorum rule doesn't require *every* follower,
//     only a majority.
//   - The cut-off follower's heartbeats stop arriving, so its own
//     election timer fires and it repeatedly tries to campaign. Its
//     pre-vote requests reach the leader and the other follower fine
//     (the open direction), but both refuse ("pre-vote campaign failed
//     ... refused=2") because they're still within their own election
//     timeout, i.e. they know a leader is alive. This is raft's built-in
//     protection against exactly this scenario -- a partitioned node
//     cannot force term/leadership churn onto a cluster that's still
//     healthy, which is what this test exists to confirm rather than
//     assume.
//
// So the real assertions here are: the leader never changes, no two
// nodes ever simultaneously believe they're leader, the cluster keeps
// accepting writes throughout, and the cut-off follower -- despite never
// disrupting anything -- is left stale until the partition heals, at
// which point it catches back up.
import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestCluster_AsymmetricPartition_LeaderStaysStableNoSplitBrain(t *testing.T) {
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
		n.cluster.refreshSingleNodeStatus()
	}

	leader := currentLeader(t, nodes)
	var others []*Engine
	for _, n := range nodes {
		if n != leader {
			others = append(others, n)
		}
	}
	target := others[0]    // loses the leader's outbound path to it
	bystander := others[1] // fully connected to both the whole time
	t.Logf("cluster formed: leader=%s, asymmetric target=%s, bystander=%s",
		leader.cluster.nodeID, target.cluster.nodeID, bystander.cluster.nodeID)

	const dbName = "asym_partition_test"
	const block = "block1"
	if err := leader.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := leader.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock failed: %v", err)
	}
	const prePartitionDocs = 30
	docs := make([]map[string]any, prePartitionDocs)
	for i := range docs {
		docs[i] = map[string]any{"seq": i, "phase": "pre_partition"}
	}
	if inserted, errs := leader.InsertBatch(dbName, block, docs); errs != 0 || inserted != prePartitionDocs {
		t.Fatalf("pre-partition InsertBatch: inserted=%d errs=%d, want %d/0", inserted, errs, prePartitionDocs)
	}
	waitForCount(t, nodes, dbName, block, prePartitionDocs, 20*time.Second)
	t.Logf("all 3 nodes settled at %d documents before injecting the asymmetric partition", prePartitionDocs)

	// The actual fault: only the leader->target direction is cut.
	// target->leader (and target->bystander) stay open, so target can
	// still reach everyone to campaign once it stops hearing heartbeats
	// -- it just can't win, per the file comment above.
	origLeaderID := leader.cluster.nodeID
	ctrl.PartitionOneWay(leader.cluster.advertise, target.cluster.advertise)
	t.Logf("asymmetric partition injected: %s cannot dial %s (reverse direction still open)",
		leader.cluster.nodeID, target.cluster.nodeID)

	// Watch continuously through several election-timeout cycles (long
	// enough to see target's repeated, refused campaign attempts) for
	// two things: split-brain (more than one simultaneous leader, which
	// must never happen regardless of what target attempts) and any
	// actual leadership change (which this specific topology -- leader
	// still holds a majority -- should NOT produce, per the file
	// comment; asserting that is the point of this test, not an
	// afterthought).
	stop := make(chan struct{})
	splitBrainDetected := make(chan struct{}, 1)
	leaderChanged := make(chan string, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				var leaders []string
				for _, n := range nodes {
					if n.cluster.IsLeader() {
						leaders = append(leaders, n.cluster.nodeID)
					}
				}
				if len(leaders) > 1 {
					select {
					case splitBrainDetected <- struct{}{}:
					default:
					}
				}
				if len(leaders) == 1 && leaders[0] != origLeaderID {
					select {
					case leaderChanged <- leaders[0]:
					default:
					}
				}
			}
		}
	}()

	time.Sleep(6 * electionTimeout) // long enough for several of target's refused campaign attempts
	close(stop)

	select {
	case <-splitBrainDetected:
		t.Errorf("split-brain: more than one node reported IsLeader()==true at the same time during the asymmetric partition")
	default:
	}
	select {
	case newID := <-leaderChanged:
		t.Errorf("leadership changed to %s during the asymmetric partition -- the original leader (%s) still held a majority (itself + %s) and should never have needed to step down; if this now fails, raft's pre-vote protection against disruptive elections from a partially-connected node may have regressed",
			newID, origLeaderID, bystander.cluster.nodeID)
	default:
		t.Logf("leader remained stable throughout: %s", origLeaderID)
	}

	// The cluster must stay available: a write through the (still)
	// leader must succeed the whole time, with no forwarding/retry
	// needed since it never lost leadership.
	const duringDocs = 10
	moreDocs := make([]map[string]any, duringDocs)
	for i := range moreDocs {
		moreDocs[i] = map[string]any{"seq": i, "phase": "during_asym_partition"}
	}
	insertedDuring, errsDuring := leader.InsertBatch(dbName, block, moreDocs)
	if errsDuring != 0 || insertedDuring != duringDocs {
		t.Fatalf("InsertBatch on still-leader %s during asymmetric partition: inserted=%d errs=%d, want %d/0",
			leader.cluster.nodeID, insertedDuring, errsDuring, duringDocs)
	}

	ctrl.Heal(leader.cluster.advertise, target.cluster.advertise)
	t.Logf("asymmetric partition healed")

	wantFinal := int64(prePartitionDocs + duringDocs)
	waitForCount(t, nodes, dbName, block, wantFinal, 15*time.Second)
	t.Logf("all nodes (including the never-disruptive %s) converged to %d documents after heal", target.cluster.nodeID, wantFinal)
}

// waitForCount polls Count on every node until they all report want, or
// fails the test after timeout. Shared by the partition scenario tests.
func waitForCount(t *testing.T, nodes []*Engine, dbName, block string, want int64, timeout time.Duration) {
	t.Helper()
	for _, n := range nodes {
		deadline := time.Now().Add(timeout)
		var c int64
		var err error
		for {
			c, err = n.Count(dbName, block, nil, nil)
			if err != nil {
				t.Fatalf("Count on %s failed: %v", n.cluster.nodeID, err)
			}
			if c == want || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if c != want {
			t.Errorf("node %s has %d documents, want %d", n.cluster.nodeID, c, want)
		}
	}
}
