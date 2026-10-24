package caimandb

import (
	"bytes"
	"caimandb/internal/caimandb/metrics"
	caimanraft "caimandb/internal/caimandb/raft"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterManager struct {
	raft      *raft.Raft
	fsm       *RaftFSM
	engine    *Engine
	nodeID    string
	raftDir   string
	bindAddr  string
	advertise string
	mu        sync.RWMutex
	peers     map[string]string
	started   bool
	// singleNode caches whether the Raft configuration currently has at
	// most one voter. It's updated only on membership changes (join/
	// leave), not read from raft.GetConfiguration() on every write --
	// that call round-trips through Raft's internal main-loop channel,
	// so calling it per-insert would reintroduce the very overhead
	// IsSingleNode() exists to avoid. See IsSingleNode() below.
	singleNode   atomic.Bool
	stopChan     chan struct{}
	stopOnce     sync.Once
	gossipTicker *time.Ticker
	discoverer   *NodeDiscoverer
	autoJoin     bool
	config       *Config
	ctx          context.Context
	cancel       context.CancelFunc

	// raftLogStore and raftStableStore back Raft's log/stable storage
	// (BadgerDB and BoltDB respectively). Raft's own Shutdown() stops
	// the consensus algorithm but does not own or close the storage it
	// was handed, so these are closed explicitly in Stop() -- otherwise
	// the BadgerDB log store's memory-mapped .vlog file stays open past
	// shutdown. That's harmless on Linux/macOS (an unlinked-but-open
	// file just lingers until the last handle closes), but on Windows a
	// still-mapped file can't be deleted, which surfaced as spurious
	// TempDir cleanup failures in tests that shut a cluster down and
	// then remove its directory in the same test.
	raftLogStore    *caimanraft.BadgerLogStore
	raftStableStore *raftboltdb.BoltStore

	// raftTLSStop stops the Raft mTLS certificate-rotation goroutine
	// (tls_raft.go) when the cluster manager shuts down. nil unless
	// Config.RaftTLSEnabled was set at init time.
	raftTLSStop chan struct{}
}

type NodeDiscoverer struct {
	mu            sync.RWMutex
	knownNodes    map[string]string
	discoverPort  int
	discoveryChan chan NodeDiscovery
	mux           *http.ServeMux
	server        *http.Server
	stopChan      chan struct{}
}

type NodeDiscovery struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Action  string `json:"action"`
}

func NewNodeDiscoverer(port int) *NodeDiscoverer {
	nd := &NodeDiscoverer{
		knownNodes:    make(map[string]string),
		discoverPort:  port,
		discoveryChan: make(chan NodeDiscovery, 10000),
		mux:           http.NewServeMux(),
		stopChan:      make(chan struct{}),
	}
	nd.mux.HandleFunc("/discover", nd.handleDiscovery)
	nd.mux.HandleFunc("/discover/health", nd.handleHealth)
	return nd
}

func (nd *NodeDiscoverer) StartDiscovery() {
	addr := fmt.Sprintf(":%d", nd.discoverPort)
	nd.server = &http.Server{
		Addr:         addr,
		Handler:      nd.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	safeGo("cluster_discovery_server", func() {
		log().Info("Discovery service started", zap.Int("port", nd.discoverPort))
		if err := nd.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log().Error("Discovery server failed", zap.Error(err))
		}
	})
	safeGoLoop("cluster_broadcast_heartbeat", nd.broadcastHeartbeat)
}

func (nd *NodeDiscoverer) Stop() {
	close(nd.stopChan)
	if nd.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		nd.server.Shutdown(ctx)
	}
}

func (nd *NodeDiscoverer) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	var discovery NodeDiscovery
	if err := json.NewDecoder(r.Body).Decode(&discovery); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	nd.mu.Lock()
	nd.knownNodes[discovery.NodeID] = discovery.Address
	nd.mu.Unlock()

	nd.discoveryChan <- discovery
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (nd *NodeDiscoverer) handleHealth(w http.ResponseWriter, r *http.Request) {
	nd.mu.RLock()
	nodes := len(nd.knownNodes)
	nd.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"nodes":  nodes,
	})
}

func (nd *NodeDiscoverer) broadcastHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nd.mu.RLock()
			nodes := make(map[string]string)
			for id, addr := range nd.knownNodes {
				nodes[id] = addr
			}
			nd.mu.RUnlock()

			for nodeID, addr := range nodes {
				id, address := nodeID, addr
				safeGo("cluster_heartbeat_send", func() {
					client := &http.Client{Timeout: 2 * time.Second}
					data, _ := json.Marshal(NodeDiscovery{
						NodeID:  id,
						Address: address,
						Action:  "heartbeat",
					})
					client.Post(fmt.Sprintf("http://%s/discover", address), "application/json", bytes.NewReader(data))
				})
			}
		case <-nd.stopChan:
			return
		}
	}
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	var preferredPrivate []string
	var otherPrivate []string
	var publicIPs []string
	var globalIPv6 []string

	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}

		if ipnet.IP.IsLoopback() {
			continue
		}
		if ipnet.IP.IsLinkLocalUnicast() {
			continue
		}
		if ipnet.IP.IsMulticast() {
			continue
		}

		if ipv4 := ipnet.IP.To4(); ipv4 != nil {
			ipStr := ipv4.String()
			if ipv4.IsPrivate() {
				if strings.HasPrefix(ipStr, "192.168.") {
					preferredPrivate = append(preferredPrivate, ipStr)
				} else {
					otherPrivate = append(otherPrivate, ipStr)
				}
			} else {
				publicIPs = append(publicIPs, ipStr)
			}
		} else {
			if ipnet.IP.To16() != nil {
				if ipnet.IP.IsGlobalUnicast() && !ipnet.IP.IsPrivate() {
					globalIPv6 = append(globalIPv6, ipnet.IP.String())
				}
			}
		}
	}

	if len(preferredPrivate) > 0 {
		for _, ip := range preferredPrivate {
			if !isBridgeOrDocker(ip) {
				return ip, nil
			}
		}
		ip := preferredPrivate[0]
		return ip, nil
	}

	if len(otherPrivate) > 0 {
		for _, ip := range otherPrivate {
			if !isBridgeOrDocker(ip) {
				return ip, nil
			}
		}
		ip := otherPrivate[0]
		return ip, nil
	}

	if len(publicIPs) > 0 {
		ip := publicIPs[0]
		return ip, nil
	}

	if len(globalIPv6) > 0 {
		ip := globalIPv6[0]
		return ip, nil
	}

	return "127.0.0.1", nil
}

func isBridgeOrDocker(ip string) bool {
	if strings.HasPrefix(ip, "172.17.") ||
		strings.HasPrefix(ip, "172.18.") ||
		strings.HasPrefix(ip, "172.19.") ||
		strings.HasPrefix(ip, "172.20.") ||
		strings.HasPrefix(ip, "172.21.") ||
		strings.HasPrefix(ip, "172.22.") ||
		strings.HasPrefix(ip, "172.23.") ||
		strings.HasPrefix(ip, "172.24.") ||
		strings.HasPrefix(ip, "172.25.") ||
		strings.HasPrefix(ip, "172.26.") ||
		strings.HasPrefix(ip, "172.27.") ||
		strings.HasPrefix(ip, "172.28.") ||
		strings.HasPrefix(ip, "172.29.") ||
		strings.HasPrefix(ip, "172.30.") ||
		strings.HasPrefix(ip, "172.31.") {
		parts := strings.Split(ip, ".")
		if len(parts) == 4 {
			lastOctet, err := strconv.Atoi(parts[3])
			if err == nil && (lastOctet == 0 || lastOctet == 1) {
				return true
			}
		}
	}

	if strings.HasPrefix(ip, "10.88.") {
		parts := strings.Split(ip, ".")
		if len(parts) == 4 {
			lastOctet, err := strconv.Atoi(parts[3])
			if err == nil && (lastOctet == 0 || lastOctet == 1) {
				return true
			}
		}
	}

	return false
}

// testStreamLayerHook is a seam for this package's tests to inject a
// raft.StreamLayer that can simulate network partitions (dropping traffic
// between specific nodes while keeping the process itself alive), instead
// of the real TCP transport initRaftWithCustomLogStore uses by default.
// nil (its zero value, and the only value it ever has outside a test that
// explicitly sets and then restores it) means "use the real TCP stream
// layer" -- production code never touches this and its behavior is
// unchanged whether or not this variable exists.
var testStreamLayerHook func(cm *ClusterManager) (raft.StreamLayer, error)

func (cm *ClusterManager) initRaftWithCustomLogStore() error {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cm.nodeID)
	raftConfig.HeartbeatTimeout = heartbeatInterval
	raftConfig.ElectionTimeout = electionTimeout
	raftConfig.LeaderLeaseTimeout = leaderLeaseTimeout
	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 2000
	raftConfig.MaxAppendEntries = 256
	raftConfig.TrailingLogs = 100000
	raftConfig.LogOutput = io.Discard

	advertiseAddr, err := net.ResolveTCPAddr("tcp", cm.advertise)
	if err != nil {
		return fmt.Errorf("resolve advertise address: %w", err)
	}

	log().Info("Initializing Raft with custom log store",
		zap.String("bind", cm.bindAddr),
		zap.String("advertise", cm.advertise),
	)

	var transport *raft.NetworkTransport
	if testStreamLayerHook != nil {
		stream, err := testStreamLayerHook(cm)
		if err != nil {
			return fmt.Errorf("create test stream layer: %w", err)
		}
		transport = raft.NewNetworkTransport(stream, raftMaxPool, raftTimeout, io.Discard)
	} else if cm.config != nil && cm.config.RaftTLSEnabled {
		// Node-to-node Raft traffic under mutual TLS (tls_raft.go)
		// instead of the plain raft.NewTCPTransport below. Reuses the
		// same raft.NewNetworkTransport(stream, ...) constructor the
		// test-partition stream layer above already goes through, so
		// this doesn't need its own bespoke wiring into raft.NewRaft.
		if cm.raftTLSStop == nil {
			cm.raftTLSStop = make(chan struct{})
		}
		tlsConfig, err := raftTLSConfig(cm.config, cm.raftTLSStop)
		if err != nil {
			return fmt.Errorf("raft TLS setup: %w", err)
		}
		stream, err := newTLSStreamLayer(cm.bindAddr, tlsConfig)
		if err != nil {
			return fmt.Errorf("create raft TLS stream layer: %w", err)
		}
		transport = raft.NewNetworkTransport(stream, raftMaxPool, raftTimeout, io.Discard)
	} else {
		transport, err = raft.NewTCPTransport(
			cm.bindAddr,
			advertiseAddr,
			raftMaxPool,
			raftTimeout,
			io.Discard,
		)
		if err != nil {
			return fmt.Errorf("create transport: %w", err)
		}
	}

	logStore, err := caimanraft.NewBadgerLogStore(filepath.Join(cm.raftDir, "raft-log-badger"))
	if err != nil {
		return fmt.Errorf("create badger log store: %w", err)
	}
	cm.raftLogStore = logStore

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cm.raftDir, "raft-stable.db"))
	if err != nil {
		return fmt.Errorf("create stable store: %w", err)
	}
	cm.raftStableStore = stableStore

	snapshotStore, err := raft.NewFileSnapshotStore(cm.raftDir, raftSnapshotRetain, io.Discard)
	if err != nil {
		return fmt.Errorf("create snapshot store: %w", err)
	}

	cm.fsm = NewRaftFSM(cm.engine)

	raftInstance, err := raft.NewRaft(
		raftConfig,
		cm.fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return fmt.Errorf("create raft: %w", err)
	}

	cm.raft = raftInstance

	hasExisting, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		log().Warn("Failed to check existing raft state, assuming no state",
			zap.Error(err))
		hasExisting = false
	}

	if !hasExisting {
		log().Info("No existing cluster state found, bootstrapping...",
			zap.String("node_id", cm.nodeID))

		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cm.nodeID),
					Address: raft.ServerAddress(cm.advertise),
				},
			},
		}

		future := cm.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			if err == raft.ErrCantBootstrap {
				log().Info("Cluster already bootstrapped (race condition)",
					zap.String("node_id", cm.nodeID))
				return nil
			}
			return fmt.Errorf("bootstrap cluster: %w", err)
		}

		log().Info("Cluster bootstrapped successfully",
			zap.String("node_id", cm.nodeID),
			zap.String("advertise", cm.advertise))
	} else {
		log().Info("Existing cluster state detected",
			zap.String("node_id", cm.nodeID))
	}

	return nil
}

func NewClusterManager(engine *Engine, config *Config) (*ClusterManager, error) {
	if engine == nil {
		return nil, fmt.Errorf("engine is nil")
	}

	nodeID := engine.nodeID

	raftPort := config.RaftPort
	raftBind := config.RaftBind

	bindAddr := fmt.Sprintf("%s:%d", raftBind, raftPort)
	advertise := bindAddr

	if raftBind == "0.0.0.0" || raftBind == "" {
		localIP, err := getLocalIP()
		if err != nil {
			return nil, fmt.Errorf("failed to get local IP: %w", err)
		}
		advertise = fmt.Sprintf("%s:%d", localIP, raftPort)
		bindAddr = fmt.Sprintf("0.0.0.0:%d", raftPort)
		log().Info("Auto-detected IP for advertise",
			zap.String("ip", localIP),
			zap.Int("port", raftPort))
	}

	raftDir := filepath.Join(engine.dataRoot, raftDBName)
	if err := os.MkdirAll(raftDir, 0750); err != nil {
		return nil, fmt.Errorf("create raft directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cm := &ClusterManager{
		engine:    engine,
		nodeID:    nodeID,
		raftDir:   raftDir,
		bindAddr:  bindAddr,
		advertise: advertise,
		peers:     make(map[string]string),
		stopChan:  make(chan struct{}),
		autoJoin:  true,
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
	}

	cm.discoverer = NewNodeDiscoverer(raftPort + 1000)

	if err := cm.initRaftWithCustomLogStore(); err != nil {
		return nil, fmt.Errorf("initialize raft: %w", err)
	}

	// Read the actual configuration once at startup (cheap here -- it's
	// a one-time call, not per-write) since a restarting node may load
	// persisted Raft state from a cluster that already has more than
	// one voter, not just the fresh single-server bootstrap case.
	cm.refreshSingleNodeStatus()

	cm.discoverer.StartDiscovery()
	safeGoLoop("cluster_auto_discovery_loop", cm.autoDiscoveryLoop)
	safeGoLoop("cluster_auto_join_loop", cm.autoJoinLoop)
	safeGoLoop("cluster_health_check_loop", cm.healthCheckLoop)

	log().Info("Cluster manager initialized (auto-mode)",
		zap.String("node_id", nodeID),
		zap.String("bind_addr", bindAddr),
		zap.String("advertise", advertise),
		zap.Int("max_nodes", config.MaxClusterNodes))

	return cm, nil
}

func (cm *ClusterManager) healthCheckLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.checkNodeHealth()
		case <-cm.stopChan:
			return
		}
	}
}

func (cm *ClusterManager) checkNodeHealth() {
	cm.mu.RLock()
	peers := make(map[string]string)
	for id, addr := range cm.peers {
		peers[id] = addr
	}
	cm.mu.RUnlock()

	for nodeID, addr := range peers {
		id, address := nodeID, addr
		safeGo("cluster_health_check", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Get(fmt.Sprintf("http://%s/health", address))
			if err != nil || resp.StatusCode != 200 {
				log().Warn("Node health check failed",
					zap.String("node", id),
					zap.Error(err))
				cm.engine.shardMgr.HandleNodeFailure(id)
			}
			if resp != nil {
				resp.Body.Close()
			}
		})
	}
}

func (cm *ClusterManager) autoDiscoveryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.discoverNodes()
		case <-cm.stopChan:
			return
		}
	}
}

func (cm *ClusterManager) autoJoinLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cm.raft.State() == raft.Leader {
				cm.handleAutoJoin()
			}
		case <-cm.stopChan:
			return
		}
	}
}

func (cm *ClusterManager) discoverNodes() {
	cm.mu.RLock()
	knownNodes := make(map[string]string)
	for id, addr := range cm.peers {
		knownNodes[id] = addr
	}
	cm.mu.RUnlock()

	for nodeID, addr := range knownNodes {
		if nodeID == cm.nodeID {
			continue
		}
		go cm.sendDiscovery(nodeID, addr)
	}
}

func (cm *ClusterManager) sendDiscovery(nodeID, addr string) {
	client := &http.Client{Timeout: 2 * time.Second}
	data, _ := json.Marshal(NodeDiscovery{
		NodeID:  cm.nodeID,
		Address: cm.advertise,
		Action:  "heartbeat",
	})
	client.Post(fmt.Sprintf("http://%s/discover", addr), "application/json", bytes.NewReader(data))
}

func (cm *ClusterManager) handleAutoJoin() {
	cm.mu.RLock()
	knownNodes := make(map[string]string)
	for id, addr := range cm.peers {
		knownNodes[id] = addr
	}
	cm.mu.RUnlock()

	for nodeID, addr := range knownNodes {
		if nodeID == cm.nodeID {
			continue
		}
		future := cm.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			continue
		}
		config := future.Configuration()
		found := false
		for _, server := range config.Servers {
			if string(server.ID) == nodeID {
				found = true
				break
			}
		}
		if !found {
			log().Info("Auto-joining discovered node",
				zap.String("node", nodeID),
				zap.String("addr", addr))
			// Force a fresh snapshot first: while this node was running
			// as a single-node "cluster" (IsSingleNode() == true),
			// writes bypassed raft.Apply() entirely (see ops_insert.go
			// et al.), so the Raft log doesn't reflect them. A snapshot
			// taken now captures the real, current FSM state so the
			// joining node gets it via InstallSnapshot instead of an
			// incomplete log replay.
			if snapFuture := cm.raft.Snapshot(); snapFuture.Error() != nil {
				log().Warn("Pre-join snapshot failed, continuing anyway",
					zap.String("node", nodeID), zap.Error(snapFuture.Error()))
			}
			future := cm.raft.AddVoter(
				raft.ServerID(nodeID),
				raft.ServerAddress(addr),
				0, raftTimeout)
			if err := future.Error(); err != nil {
				log().Warn("Failed to auto-join node", zap.String("node", nodeID), zap.Error(err))
			} else {
				cm.engine.shardMgr.AddNodeToShards(nodeID)
				cm.refreshSingleNodeStatus()
				log().Info("Node auto-joined successfully", zap.String("node", nodeID))
			}
		}
	}
}

func (cm *ClusterManager) HandleJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.Address == "" {
		http.Error(w, "node_id and address are required", http.StatusBadRequest)
		return
	}

	if cm.raft.State() != raft.Leader {
		leader := string(cm.raft.Leader())
		if leader != "" {
			w.Header().Set("Location", leader)
			http.Error(w, fmt.Sprintf(`{"error":"not leader","leader":%q}`, leader), http.StatusTemporaryRedirect)
		} else {
			http.Error(w, `{"error":"no leader elected yet"}`, http.StatusServiceUnavailable)
		}
		return
	}

	// See the matching comment in handleAutoJoin: force a snapshot
	// before adding the voter so it reflects any writes made while
	// this node bypassed Raft as a single-node deployment.
	if snapFuture := cm.raft.Snapshot(); snapFuture.Error() != nil {
		log().Warn("Pre-join snapshot failed, continuing anyway",
			zap.String("node", req.NodeID), zap.Error(snapFuture.Error()))
	}

	future := cm.raft.AddVoter(
		raft.ServerID(req.NodeID),
		raft.ServerAddress(req.Address),
		0,
		raftTimeout,
	)
	if err := future.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cm.mu.Lock()
	cm.peers[req.NodeID] = req.Address
	cm.mu.Unlock()

	cm.engine.shardMgr.AddNodeToShards(req.NodeID)
	cm.refreshSingleNodeStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
}

func (cm *ClusterManager) HandleLeave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if cm.raft.State() != raft.Leader {
		leader := string(cm.raft.Leader())
		http.Error(w, fmt.Sprintf(`{"error":"not leader","leader":%q}`, leader), http.StatusServiceUnavailable)
		return
	}

	future := cm.raft.RemoveServer(raft.ServerID(req.NodeID), 0, raftTimeout)
	if err := future.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cm.mu.Lock()
	delete(cm.peers, req.NodeID)
	cm.mu.Unlock()

	cm.engine.shardMgr.RemoveNodeFromShards(req.NodeID)
	cm.refreshSingleNodeStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "left"})
}

func (cm *ClusterManager) HandleStatus(w http.ResponseWriter, r *http.Request) {
	stats := cm.raft.Stats()
	state := cm.raft.State().String()

	cm.mu.RLock()
	peers := make([]string, 0, len(cm.peers))
	for id := range cm.peers {
		peers = append(peers, id)
	}
	cm.mu.RUnlock()

	status := map[string]any{
		"node_id":      cm.nodeID,
		"state":        state,
		"is_leader":    cm.raft.State() == raft.Leader,
		"peers":        peers,
		"stats":        stats,
		"started":      cm.started,
		"bind":         cm.bindAddr,
		"advertise":    cm.advertise,
		"leader":       string(cm.raft.Leader()),
		"auto_join":    cm.autoJoin,
		"max_nodes":    cm.config.MaxClusterNodes,
		"total_peers":  len(peers),
		"total_shards": cm.engine.shardMgr.shardCount,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (cm *ClusterManager) Start() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.started = true
	log().Info("Cluster manager started (auto-mode)", zap.String("node_id", cm.nodeID))
}

func (cm *ClusterManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.started {
		return
	}

	cm.stopOnce.Do(func() {
		close(cm.stopChan)
	})
	if cm.raftTLSStop != nil {
		close(cm.raftTLSStop)
		cm.raftTLSStop = nil
	}
	if cm.raft != nil {
		future := cm.raft.Shutdown()
		if err := future.Error(); err != nil {
			log().Warn("Raft shutdown error", zap.Error(err))
		}
	}
	// Raft's own Shutdown() above stops the consensus algorithm, but it
	// doesn't own the log/stable stores it was constructed with -- close
	// them explicitly so their file handles (in particular the BadgerDB
	// log store's memory-mapped .vlog) are actually released before
	// Stop() returns. See the doc comment on these fields for why this
	// matters on Windows specifically.
	if cm.raftLogStore != nil {
		if err := cm.raftLogStore.Close(); err != nil {
			log().Warn("Raft log store close error", zap.Error(err))
		}
	}
	if cm.raftStableStore != nil {
		if err := cm.raftStableStore.Close(); err != nil {
			log().Warn("Raft stable store close error", zap.Error(err))
		}
	}
	if cm.discoverer != nil {
		cm.discoverer.Stop()
	}
	cm.cancel()
	cm.started = false
	log().Info("Cluster manager stopped", zap.String("node_id", cm.nodeID))
}

func (cm *ClusterManager) IsLeader() bool {
	return cm.raft != nil && cm.raft.State() == raft.Leader
}

// IsSingleNode reports whether the Raft configuration currently has at
// most one voter. It reads a cached value (updated on membership
// changes via refreshSingleNodeStatus) rather than calling
// raft.GetConfiguration() directly, since that call is serviced by
// Raft's internal main-loop goroutine and would add a channel
// round-trip to every write if checked per-operation.
//
// Callers use this to skip going through Raft consensus entirely for
// single-node deployments (see insertWithID/InsertBatch/etc. in the
// ops_*.go files): with only one voter, raft.Apply() still does the
// real work through the FSM (which just calls straight back into the
// same *Local functions the bypass calls directly), so it adds
// serialization + log-append + round-trip overhead without buying any
// actual replication safety. The moment a second node joins, writes
// go back through Raft as normal.
func (cm *ClusterManager) IsSingleNode() bool {
	if cm.raft == nil {
		return true
	}
	return cm.singleNode.Load()
}

// reportRaftLagMetric sets MetricRaftReplicationLag for this node: 0 on
// the leader (and on a node with no Raft instance at all, i.e. a
// standalone non-cluster deployment -- nothing to be "behind"), or
// seconds since raft.LastContact() otherwise. Meant to be called
// periodically (see (e *Engine) startObservabilityLoop in app.go) --
// each node only ever reports its own value, since LastContact() (like
// the rest of hashicorp/raft's public API) exposes no cross-node view.
func (cm *ClusterManager) reportRaftLagMetric() {
	if cm == nil || cm.raft == nil {
		return
	}
	cm.reportClusterStatusMetric()
	if cm.raft.State() == raft.Leader {
		metrics.MetricRaftReplicationLag.WithLabelValues(cm.nodeID).Set(0)
		return
	}
	last := cm.raft.LastContact()
	if last.IsZero() {
		// Never had contact yet (e.g. still forming, or genuinely a
		// standalone single-node "cluster" that has never had a
		// remote leader) -- report 0 rather than a huge, meaningless
		// "seconds since the Unix epoch" number.
		metrics.MetricRaftReplicationLag.WithLabelValues(cm.nodeID).Set(0)
		return
	}
	metrics.MetricRaftReplicationLag.WithLabelValues(cm.nodeID).Set(time.Since(last).Seconds())
}

// reportClusterStatusMetric sets MetricClusterStatus for this node's
// current Raft role: 1 for whichever of "leader"/"follower"/
// "candidate"/"shutdown" cm.raft.State() currently reports, 0 for the
// other three -- so a Grafana panel can graph "how many nodes think
// they're the leader right now" (which should always be at most 1;
// briefly seeing more than 1 during an election is expected, a
// sustained >1 is a split-brain signal). This metric was defined and
// registered from early in this project's history but never actually
// Set() anywhere until now -- see the CHANGELOG entry that first
// flagged it as a silent gap.
func (cm *ClusterManager) reportClusterStatusMetric() {
	current := strings.ToLower(cm.raft.State().String())
	for _, role := range []string{"leader", "follower", "candidate", "shutdown"} {
		value := 0.0
		if role == current {
			value = 1.0
		}
		metrics.MetricClusterStatus.WithLabelValues(cm.nodeID, role).Set(value)
	}
}

// ErrNotRaftLeader is returned by every write operation (Insert,
// InsertBatch, Update, Delete, CreateDB, DropDB, CreateBlock, DropBlock,
// RenameBlock, and the Turbo equivalents) when called on a node that is
// part of a multi-node Raft cluster but is not currently its leader.
//
// Before this existed, those call sites all shared a single condition to
// decide between two paths -- "go through raft.Apply()" vs. "write
// straight to this node's local storage" -- and a non-leader member of a
// real cluster fell into the *local* branch by accident: the condition
// was written to select the raft path, with local storage as the
// catch-all "everything else" case, which is only correct for the
// genuinely-no-cluster and genuinely-single-node cases. A non-leader
// follower is a third case that condition never distinguished, so it
// silently wrote to its own on-disk storage -- data no other node would
// ever see, never present in the Raft log, and never reconciled even
// after the write path exit condition (a follower losing contact with
// the leader, e.g. during a network partition) resolves. See
// network_partition_scenario_test.go for the reproduction.
var ErrNotRaftLeader = errors.New("not the raft leader: this node is a member of a multi-node cluster but is not currently its leader; retry against the current leader")

// clusterWriteGate is the single decision point every write operation
// must call before choosing between the raft.Apply() path and the local
// *Local() path -- see ErrNotRaftLeader for why a shared helper exists
// instead of each call site repeating its own condition.
//
//	viaRaft == true            -> this node is the leader of a real (multi-node) cluster; go through raft.Apply()
//	viaRaft == false, err == nil -> no cluster, or a genuine single-node cluster; write directly to local storage
//	err == ErrNotRaftLeader     -> this node is a non-leader member of a multi-node cluster; the write must be rejected, not applied locally
func (e *Engine) clusterWriteGate() (viaRaft bool, err error) {
	if e.cluster == nil || e.cluster.IsSingleNode() {
		return false, nil
	}
	if e.cluster.IsLeader() {
		return true, nil
	}
	return false, ErrNotRaftLeader
}

// refreshSingleNodeStatus re-reads the real Raft configuration and
// updates the cached singleNode flag. Called only on startup and after
// membership changes (join/leave) -- infrequent events where the cost
// of GetConfiguration() doesn't matter -- never from a hot write path.
func (cm *ClusterManager) refreshSingleNodeStatus() {
	if cm.raft == nil {
		cm.singleNode.Store(true)
		return
	}
	future := cm.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		// Unknown state: fail safe by keeping/assuming a real cluster,
		// which just means writes still go through Raft (correct, if
		// slower) rather than risking an unreplicated bypass.
		cm.singleNode.Store(false)
		return
	}
	cm.singleNode.Store(len(future.Configuration().Servers) <= 1)
}

func (cm *ClusterManager) GetLeader() string {
	if cm.raft == nil {
		return ""
	}
	return string(cm.raft.Leader())
}

func (cm *ClusterManager) GetPeers() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	peers := make([]string, 0, len(cm.peers))
	for id := range cm.peers {
		peers = append(peers, id)
	}
	return peers
}

func (cm *ClusterManager) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cm.raft != nil && cm.raft.Leader() != "" {
				log().Info("Leader elected",
					zap.String("leader", string(cm.raft.Leader())))
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for leader after %s", timeout)
			}
		case <-cm.stopChan:
			return fmt.Errorf("cluster stopped while waiting for leader")
		case <-cm.ctx.Done():
			return cm.ctx.Err()
		}
	}
}

type TransactionHelper struct{}

var txHelper = &TransactionHelper{}

func (th *TransactionHelper) SafeUpdate(db *badger.DB, fn func(*badger.Txn) error) (err error) {
	var txn *badger.Txn

	defer func() {
		if r := recover(); r != nil {
			if txn != nil {
				txn.Discard()
			}
			log().Error("Panic in transaction",
				zap.Any("recover", r),
				zap.String("stack", string(debug.Stack())))
			err = fmt.Errorf("panic in transaction: %v", r)
		}
	}()

	txn = db.NewTransaction(true)
	if txn == nil {
		return fmt.Errorf("failed to create transaction")
	}

	defer txn.Discard()

	err = fn(txn)
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (th *TransactionHelper) SafeView(db *badger.DB, fn func(*badger.Txn) error) (err error) {
	var txn *badger.Txn

	defer func() {
		if r := recover(); r != nil {
			if txn != nil {
				txn.Discard()
			}
			log().Error("Panic in read transaction",
				zap.Any("recover", r),
				zap.String("stack", string(debug.Stack())))
			err = fmt.Errorf("panic in read transaction: %v", r)
		}
	}()

	txn = db.NewTransaction(false)
	if txn == nil {
		return fmt.Errorf("failed to create read transaction")
	}

	defer txn.Discard()

	err = fn(txn)
	return err
}
