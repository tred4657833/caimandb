package caimandb

// Network-partition test infrastructure: a raft.StreamLayer that behaves
// exactly like the real TCP one (same net.Listen/net.DialTimeout calls
// initRaftWithCustomLogStore's default path uses) except that it can be
// told, at runtime, to start or stop dropping traffic between specific
// node pairs -- simulating a real network partition (both processes stay
// alive, nothing is killed) rather than a node going down (see
// cluster_failover_test.go and crash_recovery_test.go for that).
//
// Design: partitioning only needs to be enforced on Dial, not Accept.
// Every raft RPC in this transport is request/response over a connection
// that the *requester* dials (a candidate dials followers for
// RequestVote, a leader dials followers for AppendEntries); nothing here
// requires the accepting side to independently dial back. So blocking a
// node's outbound Dial() to another node, in both directions, is
// sufficient to fully sever traffic between that pair -- and is done at
// two points: (1) refusing a brand-new Dial while partitioned, and (2)
// wrapping the returned net.Conn so that a connection dialed *before* the
// partition began also starts failing on the next Read/Write once the
// partition activates, instead of continuing to carry traffic over an
// already-established pooled connection (raft.NetworkTransport pools and
// reuses connections, so without this a partition wouldn't affect
// in-flight or idle-but-open connections until they happened to be
// closed for unrelated reasons).
import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// partitionController tracks which directed node pairs (identified by
// their raft advertise address, e.g. "127.0.0.1:38213") are currently
// partitioned from each other. Shared by every node in a test cluster via
// closures captured in testStreamLayerHook.
type partitionController struct {
	mu      sync.RWMutex
	blocked map[[2]string]bool
}

func newPartitionController() *partitionController {
	return &partitionController{blocked: make(map[[2]string]bool)}
}

// Partition cuts traffic between a and b in both directions.
func (p *partitionController) Partition(a, b string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked[[2]string{a, b}] = true
	p.blocked[[2]string{b, a}] = true
}

// PartitionOneWay cuts traffic only in the from->to direction, leaving
// to->from open -- an asymmetric partition (a bad routing rule, a
// misconfigured firewall ACL, an AZ link that fails in only one
// direction), as opposed to Partition's symmetric cut. Real and worth
// testing separately from a symmetric partition: Raft's own paper notes
// asymmetric partitions are the case most likely to cause a stale node to
// keep forcing costly term/election churn onto a cluster that is
// otherwise healthy, even when they don't cause any correctness issue by
// themselves (safety still comes from the term/quorum rules, not from
// the network being symmetric).
func (p *partitionController) PartitionOneWay(from, to string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked[[2]string{from, to}] = true
}

// Heal restores traffic between a and b in both directions, undoing
// either Partition or PartitionOneWay.
func (p *partitionController) Heal(a, b string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.blocked, [2]string{a, b})
	delete(p.blocked, [2]string{b, a})
}

func (p *partitionController) isBlocked(from, to string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.blocked[[2]string{from, to}]
}

// partitionConn wraps a real net.Conn (either side of a Dial) and checks
// the controller on every Read/Write, not just at Dial time -- see the
// file comment for why that's needed to affect already-open connections.
type partitionConn struct {
	net.Conn
	ctrl       *partitionController
	self, peer string
}

func (c *partitionConn) Read(b []byte) (int, error) {
	if c.ctrl.isBlocked(c.self, c.peer) {
		return 0, fmt.Errorf("simulated network partition: %s cannot reach %s", c.self, c.peer)
	}
	return c.Conn.Read(b)
}

func (c *partitionConn) Write(b []byte) (int, error) {
	if c.ctrl.isBlocked(c.self, c.peer) {
		return 0, fmt.Errorf("simulated network partition: %s cannot reach %s", c.self, c.peer)
	}
	return c.Conn.Write(b)
}

// partitionStreamLayer implements raft.StreamLayer. Accept is the plain
// real-TCP behavior (see file comment for why partitioning doesn't need
// to touch it); Dial is where partitioning is enforced.
type partitionStreamLayer struct {
	ln   net.Listener
	self string // this node's own advertise address
	ctrl *partitionController
}

func newPartitionStreamLayer(bindAddr, selfAdvertise string, ctrl *partitionController) (*partitionStreamLayer, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return &partitionStreamLayer{ln: ln, self: selfAdvertise, ctrl: ctrl}, nil
}

func (p *partitionStreamLayer) Accept() (net.Conn, error) { return p.ln.Accept() }
func (p *partitionStreamLayer) Close() error               { return p.ln.Close() }
func (p *partitionStreamLayer) Addr() net.Addr              { return p.ln.Addr() }

func (p *partitionStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	peer := string(address)
	if p.ctrl.isBlocked(p.self, peer) {
		return nil, fmt.Errorf("simulated network partition: %s cannot reach %s", p.self, peer)
	}
	conn, err := net.DialTimeout("tcp", peer, timeout)
	if err != nil {
		return nil, err
	}
	return &partitionConn{Conn: conn, ctrl: p.ctrl, self: p.self, peer: peer}, nil
}

// newPartitionableClusterNode is newClusterNode (cluster_failover_test.go)
// plus wiring testStreamLayerHook to route this node's raft traffic
// through ctrl. Must be called with testStreamLayerHook already set for
// the duration of NewEngine/InitCluster -- see its one caller below,
// which sets the hook, builds all nodes, then clears it (the hook is
// package-global, so tests using it can't run t.Parallel() against each
// other, same constraint as any other shared-mutable-package-state test
// seam).
func newPartitionableClusterNode(t *testing.T, ctrl *partitionController) *Engine {
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
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64

	e := NewEngine(cfg)
	if err := e.InitCluster(); err != nil {
		t.Fatalf("InitCluster failed (raft port %d): %v", cfg.RaftPort, err)
	}
	return e
}

func withPartitionableCluster(t *testing.T, ctrl *partitionController, fn func()) {
	t.Helper()
	prev := testStreamLayerHook
	testStreamLayerHook = func(cm *ClusterManager) (raft.StreamLayer, error) {
		return newPartitionStreamLayer(cm.bindAddr, cm.advertise, ctrl)
	}
	defer func() { testStreamLayerHook = prev }()
	fn()
}
