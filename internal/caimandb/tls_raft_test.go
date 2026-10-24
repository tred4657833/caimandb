package caimandb

// tls_raft_test.go proves the mTLS Raft stream layer (tls_raft.go)
// carries real Raft RPC traffic end to end: two real hashicorp/raft
// nodes, joined via AddVoter (same pattern as
// TestCluster_LeaderFailover_DataSurvives in cluster_failover_test.go),
// with Config.RaftTLSEnabled -- every AppendEntries/vote RPC between
// them travels over a real TLS connection requiring a client
// certificate signed by the shared cluster CA. A document inserted on
// the leader replicating to the follower is the end-to-end proof: if
// the mTLS stream layer only handshakes but doesn't actually carry
// Raft's traffic correctly, replication would never complete and this
// test would time out.
//
// It also directly proves the negative case at the tls_raft.go level
// (not by trying to coax a full Raft cluster into rejecting a node,
// which Raft's own retry/backoff behavior makes slow and awkward to
// assert on): a client presenting a certificate from an unrelated CA
// cannot complete a TLS handshake against a newTLSStreamLayer server.
import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// writeRaftMTLSCerts generates a cluster CA plus one leaf certificate
// per requested node name, all signed by that CA, and writes them to
// files under t.TempDir(). Returns the CA file path and a map of node
// name -> (certFile, keyFile).
func writeRaftMTLSCerts(t *testing.T, nodeNames ...string) (caFile string, nodeCerts map[string][2]string) {
	t.Helper()
	dir := t.TempDir()
	caCertPEM, _, caCert, caKey := genCert(t, "caimandb-raft-cluster-ca", true, nil, nil)
	caFile = filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, caCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	nodeCerts = make(map[string][2]string, len(nodeNames))
	for _, name := range nodeNames {
		certPEM, keyPEM, _, _ := genCert(t, name, false, caCert, caKey)
		certFile := filepath.Join(dir, name+".crt")
		keyFile := filepath.Join(dir, name+".key")
		if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		nodeCerts[name] = [2]string{certFile, keyFile}
	}
	return caFile, nodeCerts
}

func newClusterNodeWithRaftTLS(t *testing.T, caFile, certFile, keyFile string) *Engine {
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
	cfg.RaftTLSEnabled = true
	cfg.RaftTLSCAFile = caFile
	cfg.RaftTLSCertFile = certFile
	cfg.RaftTLSKeyFile = keyFile
	cfg.TLSCertReloadInterval = 0 // no rotation needed for this test

	e := NewEngine(cfg)
	if err := e.InitCluster(); err != nil {
		t.Fatalf("InitCluster with RaftTLSEnabled failed (raft port %d): %v", cfg.RaftPort, err)
	}
	return e
}

func TestRaftMTLS_TwoNodeClusterReplicatesOverTLS(t *testing.T) {
	caFile, certs := writeRaftMTLSCerts(t, "node1", "node2")

	node1 := newClusterNodeWithRaftTLS(t, caFile, certs["node1"][0], certs["node1"][1])
	node2 := newClusterNodeWithRaftTLS(t, caFile, certs["node2"][0], certs["node2"][1])
	t.Cleanup(node1.Shutdown)
	t.Cleanup(node2.Shutdown)

	if err := node1.cluster.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("node1 never became leader of its own single-node cluster: %v", err)
	}

	future := node1.cluster.raft.AddVoter(
		raft.ServerID(node2.cluster.nodeID),
		raft.ServerAddress(node2.cluster.advertise),
		0, raftTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("AddVoter(node2) over mTLS failed: %v", err)
	}

	for _, n := range []*Engine{node1, node2} {
		waitForConfigSize(t, n.cluster, 2, 10*time.Second)
		n.cluster.refreshSingleNodeStatus()
		if n.cluster.IsSingleNode() {
			t.Fatalf("node %s still reports IsSingleNode() after joining a 2-node cluster", n.cluster.nodeID)
		}
	}

	leader := currentLeader(t, []*Engine{node1, node2})

	const dbName, block = "raft_mtls_test", "docs"
	if err := leader.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := leader.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	if _, err := leader.Insert(dbName, block, map[string]any{"name": "over-mtls"}); err != nil {
		t.Fatalf("Insert on leader: %v", err)
	}

	var follower *Engine
	if leader == node1 {
		follower = node2
	} else {
		follower = node1
	}

	deadline := time.Now().Add(5 * time.Second)
	var lastCount int64 = -1
	for time.Now().Before(deadline) {
		c, err := follower.Count(dbName, block, nil, nil)
		if err == nil && c == 1 {
			return // replicated successfully over the mTLS transport
		}
		if err == nil {
			lastCount = c
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("document inserted on leader %s never replicated to follower over mTLS Raft transport (last count seen: %d)", leader.cluster.nodeID, lastCount)
}

// TestRaftMTLS_UntrustedClientCertRejected checks the tls_raft.go
// building blocks directly (raftTLSConfig + newTLSStreamLayer) rather
// than through a full Raft cluster: a client presenting a certificate
// from a CA the server doesn't trust must fail the TLS handshake.
func TestRaftMTLS_UntrustedClientCertRejected(t *testing.T) {
	caFile, certs := writeRaftMTLSCerts(t, "server-node")
	serverCfg := DefaultConfig()
	serverCfg.RaftTLSCAFile = caFile
	serverCfg.RaftTLSCertFile = certs["server-node"][0]
	serverCfg.RaftTLSKeyFile = certs["server-node"][1]
	serverCfg.TLSCertReloadInterval = 0

	stop := make(chan struct{})
	defer close(stop)
	tlsConfig, err := raftTLSConfig(serverCfg, stop)
	if err != nil {
		t.Fatalf("raftTLSConfig: %v", err)
	}
	port := freeTCPPort(t)
	addr := "127.0.0.1:" + itoa(port)
	stream, err := newTLSStreamLayer(addr, tlsConfig)
	if err != nil {
		t.Fatalf("newTLSStreamLayer: %v", err)
	}
	defer stream.Close()
	safeGo("test_raft_tls_accept", func() {
		for {
			conn, err := stream.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	})

	// An unrelated CA + client cert, not signed by caFile.
	_, untrustedCerts := writeRaftMTLSCerts(t, "untrusted-node")
	untrustedCert, err := tls.LoadX509KeyPair(untrustedCerts["untrusted-node"][0], untrustedCerts["untrusted-node"][1])
	if err != nil {
		t.Fatalf("loading untrusted cert: %v", err)
	}

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
		Certificates:       []tls.Certificate{untrustedCert},
		InsecureSkipVerify: true,
	})
	if err == nil {
		conn.Close()
		t.Fatal("expected TLS handshake to fail for a client certificate from an unrelated CA, but it succeeded")
	}
}
