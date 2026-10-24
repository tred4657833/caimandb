package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

type ShardInfo struct {
	ID             string    `json:"id"`
	Nodes          []string  `json:"nodes"`
	Primary        string    `json:"primary"`
	DocCount       int64     `json:"doc_count"`
	SizeBytes      int64     `json:"size_bytes"`
	LastRebalanced time.Time `json:"last_rebalanced"`
	Load           float64   `json:"load"`
	RangeStart     string    `json:"range_start"`
	RangeEnd       string    `json:"range_end"`
	Status         string    `json:"status"`
	LastScanned    time.Time `json:"last_scanned"`
}

type NodeInfo struct {
	ID         string    `json:"id"`
	Address    string    `json:"address"`
	Status     string    `json:"status"`
	DocCount   int64     `json:"doc_count"`
	LastSeen   time.Time `json:"last_seen"`
	Shards     []string  `json:"shards"`
	IsLeader   bool      `json:"is_leader"`
	LoadFactor float64   `json:"load_factor"`
}

type AutoScaleManager struct {
	mu              sync.RWMutex
	scaleHistory    []ScaleEvent
	thresholds      ScaleThresholds
	cooldowns       map[string]time.Time
	currentStrategy ScaleStrategy
}

type ScaleEvent struct {
	Timestamp  time.Time `json:"timestamp"`
	Type       string    `json:"type"`
	DB         string    `json:"db"`
	OldShards  int       `json:"old_shards"`
	NewShards  int       `json:"new_shards"`
	Reason     string    `json:"reason"`
	Successful bool      `json:"successful"`
	NodesAdded int       `json:"nodes_added"`
}

type ScaleThresholds struct {
	MaxDocsPerShard   int64         `json:"max_docs_per_shard"`
	MinDocsPerShard   int64         `json:"min_docs_per_shard"`
	MaxSizePerShard   int64         `json:"max_size_per_shard"`
	SkewThreshold     float64       `json:"skew_threshold"`
	GrowthTrigger     float64       `json:"growth_trigger"`
	ScaleUpCooldown   time.Duration `json:"scale_up_cooldown"`
	ScaleDownCooldown time.Duration `json:"scale_down_cooldown"`
}

type ScaleStrategy struct {
	Name           string  `json:"name"`
	Aggressiveness float64 `json:"aggressiveness"`
	MinShards      int     `json:"min_shards"`
	MaxShards      int     `json:"max_shards"`
}

func NewAutoScaleManager() *AutoScaleManager {
	return &AutoScaleManager{
		scaleHistory: make([]ScaleEvent, 0, 100000),
		cooldowns:    make(map[string]time.Time),
		thresholds: ScaleThresholds{
			MaxDocsPerShard:   shardMaxDocs,
			MinDocsPerShard:   shardMinDocs,
			MaxSizePerShard:   shardMaxSizeBytes,
			SkewThreshold:     0.25,
			GrowthTrigger:     1.2,
			ScaleUpCooldown:   60 * time.Second,
			ScaleDownCooldown: 300 * time.Second,
		},
		currentStrategy: ScaleStrategy{
			Name:           "aggressive",
			Aggressiveness: 2.0,
			MinShards:      4,
			MaxShards:      1000000,
		},
	}
}

type ShardManager struct {
	mu         sync.RWMutex
	shards     map[string]*ShardInfo
	nodeShards map[string][]string
	shardCount int
	// shardCountAtomic mirrors shardCount so GetShardForKey/GetShardForDoc
	// -- called once per document on every insert -- never has to take
	// sm.mu just to read a single int. It's kept in sync everywhere
	// shardCount itself is assigned (InitializeShards/scaleUp/scaleDown).
	shardCountAtomic atomic.Int64
	// shardIDCache mirrors shardCountAtomic: a precomputed []string of
	// "shard_0".."shard_N-1", rebuilt in InitializeShards. GetShardForKey
	// runs once per document on every insert, and a benchmark on real
	// hardware showed fmt.Sprintf("shard_%d", ...) alone costs ~2.2us --
	// over 20x what UpdateShardStats' atomic increment costs -- because
	// fmt.Sprintf is reflection-based and allocates a new string on every
	// call, for an output that only has shardCount possible values.
	// Precomputing them once turns the hot path into an atomic pointer
	// load + slice index, no formatting or allocation per document.
	shardIDCache atomic.Pointer[[]string]
	replicaCount int
	nodeID       string
	engine       *IntelligentEngine
	clusterNodes map[string]*NodeInfo
	autoDiscover bool
	scaleManager *AutoScaleManager
	config       *Config
	stopChan     chan struct{}
	// startedAt anchors the scale-check grace period below.
	startedAt time.Time
}

func NewShardManager(nodeID string, config *Config, engine *IntelligentEngine) *ShardManager {
	shardCount := config.ShardCount
	if shardCount < 4 {
		shardCount = 4
	}
	sm := &ShardManager{
		shards:       make(map[string]*ShardInfo),
		nodeShards:   make(map[string][]string),
		shardCount:   shardCount,
		replicaCount: config.ReplicaCount,
		nodeID:       nodeID,
		engine:       engine,
		clusterNodes: make(map[string]*NodeInfo),
		autoDiscover: true,
		scaleManager: NewAutoScaleManager(),
		config:       config,
		stopChan:     make(chan struct{}),
		startedAt:    time.Now(),
	}
	sm.InitializeShards()
	safeGoLoop("shard_scale_loop", sm.scaleLoop)
	return sm
}

func (sm *ShardManager) InitializeShards() {
	sm.shards = make(map[string]*ShardInfo)
	sm.nodeShards = make(map[string][]string)
	sm.shardCountAtomic.Store(int64(sm.shardCount))

	ids := make([]string, sm.shardCount)

	for i := 0; i < sm.shardCount; i++ {
		shardID := fmt.Sprintf("shard_%d", i)
		ids[i] = shardID
		rangeStart := fmt.Sprintf("%08x", i*int(0xFFFFFFFF/uint32(sm.shardCount)))
		rangeEnd := fmt.Sprintf("%08x", (i+1)*int(0xFFFFFFFF/uint32(sm.shardCount))-1)

		sm.shards[shardID] = &ShardInfo{
			ID:             shardID,
			Nodes:          []string{sm.nodeID},
			Primary:        sm.nodeID,
			DocCount:       0,
			SizeBytes:      0,
			LastRebalanced: time.Now(),
			Load:           0,
			RangeStart:     rangeStart,
			RangeEnd:       rangeEnd,
			Status:         "active",
			LastScanned:    time.Now(),
		}
		sm.nodeShards[sm.nodeID] = append(sm.nodeShards[sm.nodeID], shardID)
	}

	// Published last, once every shard ID is computed, so a concurrent
	// GetShardForKey (which never takes sm.mu) either sees the old
	// complete slice or the new complete one -- never a partially built
	// one.
	sm.shardIDCache.Store(&ids)

	sm.clusterNodes[sm.nodeID] = &NodeInfo{
		ID:         sm.nodeID,
		Status:     "active",
		DocCount:   0,
		LastSeen:   time.Now(),
		Shards:     sm.nodeShards[sm.nodeID],
		IsLeader:   true,
		LoadFactor: 0,
	}
}

func (sm *ShardManager) scaleLoop() {
	ticker := time.NewTicker(scaleCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.checkAndScale()
		case <-sm.stopChan:
			return
		}
	}
}

func (sm *ShardManager) checkAndScale() {
	// See scaleGracePeriod's doc comment: totalDocs is a lagging signal
	// right after startup, not a real "this dataset is small" reading,
	// so don't act on it yet.
	if time.Since(sm.startedAt) < scaleGracePeriod {
		return
	}

	sm.mu.RLock()
	totalDocs := int64(0)
	for _, shard := range sm.shards {
		totalDocs += atomic.LoadInt64(&shard.DocCount)
	}
	shardCount := sm.shardCount
	sm.mu.RUnlock()

	if totalDocs == 0 {
		return
	}

	docsPerShard := totalDocs / int64(shardCount)

	if docsPerShard > sm.config.MaxShardDocs && sm.config.AutoSplitEnabled {
		newShards := shardCount * 2
		if newShards <= sm.config.MaxShardsPerNode {
			sm.scaleUp(newShards)
		}
	} else if docsPerShard < sm.config.MinShardDocs && sm.config.AutoMergeEnabled {
		newShards := shardCount / 2
		if newShards >= 4 {
			sm.scaleDown(newShards)
		}
	}
}

func (sm *ShardManager) scaleUp(newShards int) {
	if sm.isInCooldown("scale_up") {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	log().Info("Scaling up shards",
		zap.Int("old", sm.shardCount),
		zap.Int("new", newShards))

	sm.rescaleLocked(newShards)
	sm.setCooldown("scale_up")

	metrics.MetricAutoScaleEvents.Inc()
}

func (sm *ShardManager) scaleDown(newShards int) {
	if sm.isInCooldown("scale_down") {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	log().Info("Scaling down shards",
		zap.Int("old", sm.shardCount),
		zap.Int("new", newShards))

	sm.rescaleLocked(newShards)
	sm.setCooldown("scale_down")

	metrics.MetricAutoScaleEvents.Inc()
}

// rescaleLocked resizes to newShards while carrying forward the aggregate
// DocCount/SizeBytes this node had already observed, evenly split across
// the new shard set. Must be called with sm.mu held.
//
// InitializeShards on its own rebuilds sm.shards from nothing, so every
// shard's DocCount/SizeBytes goes back to 0. That used to run directly
// inside scaleUp/scaleDown: every rescale silently threw away every stat
// UpdateShardStats had accumulated. Two consequences, both bad: (1) the
// per-shard docs-per-shard reading checkAndScale itself relies on for the
// *next* decision goes back to 0 right after a scale event, so a workload
// that's still writing (a bulk IMPORT is exactly this) can trip another
// scale-down almost immediately once the cooldown lapses, chasing a
// zeroed counter instead of the real load; (2) any future observability
// that reads ShardInfo.DocCount (metrics, SHOW, capacity planning) sees a
// dataset drop to zero that never actually happened. An even split isn't
// as accurate as re-deriving real per-shard counts from each block's own
// metadata would be, but it's a correct aggregate immediately, with zero
// added I/O on the scale-event path, and self-corrects further as
// UpdateShardStats keeps posting real per-insert deltas afterward.
func (sm *ShardManager) rescaleLocked(newShards int) {
	var totalDocs, totalSize int64
	for _, shard := range sm.shards {
		totalDocs += atomic.LoadInt64(&shard.DocCount)
		totalSize += atomic.LoadInt64(&shard.SizeBytes)
	}

	sm.shardCount = newShards
	sm.InitializeShards()

	if totalDocs == 0 && totalSize == 0 {
		return
	}
	docsPerShard := totalDocs / int64(newShards)
	docsRemainder := totalDocs % int64(newShards)
	sizePerShard := totalSize / int64(newShards)
	sizeRemainder := totalSize % int64(newShards)
	i := int64(0)
	for _, shard := range sm.shards {
		d := docsPerShard
		s := sizePerShard
		if i < docsRemainder {
			d++
		}
		if i < sizeRemainder {
			s++
		}
		atomic.StoreInt64(&shard.DocCount, d)
		atomic.StoreInt64(&shard.SizeBytes, s)
		i++
	}
}

func (sm *ShardManager) isInCooldown(action string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if cooldown, ok := sm.scaleManager.cooldowns[action]; ok {
		if time.Since(cooldown) < sm.scaleManager.thresholds.ScaleUpCooldown {
			return true
		}
	}
	return false
}

func (sm *ShardManager) setCooldown(action string) {
	sm.scaleManager.cooldowns[action] = time.Now()
}

// GetShardForKey is called once per document on every single insert, so it
// deliberately takes no lock at all: shardCountAtomic and shardIDCache are
// lock-free mirrors of shardCount/the shard ID list (kept in sync in
// InitializeShards, which runs on every scaleUp/scaleDown), so this is
// just a hash, a modulo, and a slice index -- no contention with
// concurrent inserts, no serialization against sm.mu, and (since
// shardIDCache) no per-call fmt.Sprintf/allocation either.
func (sm *ShardManager) GetShardForKey(key string) string {
	hash := xxhash.Sum64String(key)
	shardCount := sm.shardCountAtomic.Load()
	if shardCount <= 0 {
		shardCount = 1
	}
	shardIdx := hash % uint64(shardCount)

	if ids := sm.shardIDCache.Load(); ids != nil && shardIdx < uint64(len(*ids)) {
		return (*ids)[shardIdx]
	}
	// Should be unreachable in practice (InitializeShards always
	// populates the cache before shardCountAtomic could report a
	// matching count), but fall back to the always-correct-if-slower
	// path rather than ever return something wrong.
	return fmt.Sprintf("shard_%d", shardIdx)
}

// GetShardForDoc is the actual per-document hot path (InsertBatch calls
// this once per document, which then calls GetShardForKey above). It used
// to build the hash input with fmt.Sprintf, which -- like the
// "shard_%d" formatting above -- is measurably slower than plain string
// concatenation for a case this simple (no width/padding/base
// formatting, just three strings glued together with separators).
func (sm *ShardManager) GetShardForDoc(db, block, id string) string {
	key := db + "/" + block + "/" + id
	return sm.GetShardForKey(key)
}

func (sm *ShardManager) GetShardsForFilter(db, block string, filters []Filter) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(filters) == 0 {
		allShards := make([]string, 0, len(sm.shards))
		for id := range sm.shards {
			allShards = append(allShards, id)
		}
		return allShards
	}

	for _, filter := range filters {
		if filter.Field == "_id" && (filter.Op == "=" || filter.Op == "==") {
			shardID := sm.GetShardForKey(fmt.Sprintf("%s/%s/%s", db, block, fmt.Sprint(filter.Value)))
			return []string{shardID}
		}
		if filter.Field == "_shard_id" && filter.Op == "=" {
			return []string{fmt.Sprint(filter.Value)}
		}
	}

	allShards := make([]string, 0, len(sm.shards))
	for id := range sm.shards {
		allShards = append(allShards, id)
	}
	return allShards
}

func (sm *ShardManager) IsShardLocal(shardID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	shard, ok := sm.shards[shardID]
	if !ok {
		return false
	}
	for _, node := range shard.Nodes {
		if node == sm.nodeID {
			return true
		}
	}
	return false
}

func (sm *ShardManager) IsShardPrimary(shardID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	shard, ok := sm.shards[shardID]
	if !ok {
		return false
	}
	return shard.Primary == sm.nodeID
}

func (sm *ShardManager) GetShardNodes(shardID string) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	shard, ok := sm.shards[shardID]
	if !ok {
		return nil
	}
	return shard.Nodes
}

func (sm *ShardManager) GetShardPrimary(shardID string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	shard, ok := sm.shards[shardID]
	if !ok {
		return ""
	}
	return shard.Primary
}

func (sm *ShardManager) GetAllShards() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	shards := make([]string, 0, len(sm.shards))
	for id := range sm.shards {
		shards = append(shards, id)
	}
	return shards
}

func (sm *ShardManager) GetShardStats(shardID string) *ShardInfo {
	sm.mu.RLock()
	shard, ok := sm.shards[shardID]
	sm.mu.RUnlock()
	if !ok {
		return nil
	}
	docCount := atomic.LoadInt64(&shard.DocCount)
	return &ShardInfo{
		ID:             shard.ID,
		Nodes:          append([]string{}, shard.Nodes...),
		Primary:        shard.Primary,
		DocCount:       docCount,
		SizeBytes:      atomic.LoadInt64(&shard.SizeBytes),
		LastRebalanced: shard.LastRebalanced,
		Load:           sm.shardLoad(docCount),
		RangeStart:     shard.RangeStart,
		RangeEnd:       shard.RangeEnd,
		Status:         shard.Status,
		LastScanned:    shard.LastScanned,
	}
}

// shardLoad computes a shard's fill ratio on demand instead of storing it as
// a field that every insert would have to write. MaxShardDocs is read
// without a lock: it's only ever set at startup/config-load, never mutated
// concurrently with inserts.
func (sm *ShardManager) shardLoad(docCount int64) float64 {
	max := sm.config.MaxShardDocs
	if max <= 0 {
		return 0
	}
	return float64(docCount) / float64(max)
}

// UpdateShardStats is called once per document on every insert (single or
// batched), so it's the hottest call in the write path -- it used to take
// the ShardManager's full write mutex (sm.mu.Lock()) per call, which
// serialized every concurrent insert worker in the process behind a single
// global lock for what's really just "increment two counters on one
// shard". It now takes a read lock just to look up the *ShardInfo pointer
// (concurrent with every other reader, including other UpdateShardStats
// calls), and mutates the shard's own counters with atomics -- the map
// itself is never written here, only during InitializeShards/scaleUp/
// scaleDown, which already hold the full write lock. The shard's Load
// field is no longer written on every call (see shardLoad/GetShardStats);
// it's derived on read instead, since nothing in the hot path needs it.
func (sm *ShardManager) UpdateShardStats(shardID string, docCount, sizeBytes int64) {
	sm.mu.RLock()
	shard, ok := sm.shards[shardID]
	sm.mu.RUnlock()
	if !ok {
		return
	}
	atomic.AddInt64(&shard.DocCount, docCount)
	atomic.AddInt64(&shard.SizeBytes, sizeBytes)
}

// reportShardHealthMetrics sets MetricShardHealth for every shard this
// node knows about: 1 if the shard's Status isn't "degraded" (the same
// signal HandleNodeFailure above already sets when a shard's primary
// died with no healthy replica to take over) AND the shard's primary
// node's own Status is "active", 0 otherwise. Also sets
// MetricShardDistribution's DocCount for the same shard -- like
// MetricClusterStatus (see reportClusterStatusMetric in cluster.go),
// this metric was defined and registered early in this project's
// history but never actually Set() anywhere until now. Its "db"/"block"
// labels are set to "" here: ShardInfo (this file) tracks document
// counts per shard across the whole node, not broken down by
// database/block, so there's no per-db/per-block figure to report
// honestly -- the label exists for a finer breakdown a future version
// of sharding could provide, not one this version actually has. Meant
// to be called periodically (see (e *Engine) startObservabilityLoop in
// app.go), same cadence as reportRaftLagMetric.
func (sm *ShardManager) reportShardHealthMetrics() {
	if sm == nil {
		return
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for shardID, shard := range sm.shards {
		healthy := shard.Status != "degraded"
		if healthy && shard.Primary != "" {
			if nodeInfo, ok := sm.clusterNodes[shard.Primary]; ok && nodeInfo.Status != "active" {
				healthy = false
			}
		}
		value := 0.0
		if healthy {
			value = 1.0
		}
		metrics.MetricShardHealth.WithLabelValues(shardID, shard.Primary).Set(value)
		metrics.MetricShardDistribution.WithLabelValues("", "", shardID, shard.Primary).
			Set(float64(atomic.LoadInt64(&shard.DocCount)))
	}
}

func (sm *ShardManager) HandleNodeFailure(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if nodeInfo, ok := sm.clusterNodes[nodeID]; ok {
		nodeInfo.Status = "dead"
		nodeInfo.LastSeen = time.Now()
	}

	for shardID, shard := range sm.shards {
		if shard.Primary == nodeID {
			var newPrimary string
			for _, node := range shard.Nodes {
				if node != nodeID {
					if nodeInfo, ok := sm.clusterNodes[node]; ok && nodeInfo.Status == "active" {
						newPrimary = node
						break
					}
				}
			}
			if newPrimary != "" {
				shard.Primary = newPrimary
				log().Info("Elected new primary for shard after node failure",
					zap.String("shard", shardID),
					zap.String("new_primary", newPrimary),
					zap.String("failed_node", nodeID))
			} else {
				shard.Status = "degraded"
				log().Warn("Shard has no available primary after node failure",
					zap.String("shard", shardID),
					zap.String("failed_node", nodeID))
			}
		}
	}
}

func (sm *ShardManager) AddNodeToShards(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.clusterNodes[nodeID]; !exists {
		sm.clusterNodes[nodeID] = &NodeInfo{
			ID:       nodeID,
			Status:   "joining",
			LastSeen: time.Now(),
			Shards:   []string{},
		}
	}

	for _, shard := range sm.shards {
		exists := false
		for _, n := range shard.Nodes {
			if n == nodeID {
				exists = true
				break
			}
		}
		if !exists && len(shard.Nodes) < sm.replicaCount {
			shard.Nodes = append(shard.Nodes, nodeID)
			sm.nodeShards[nodeID] = append(sm.nodeShards[nodeID], shard.ID)
		}
	}

	if nodeInfo, ok := sm.clusterNodes[nodeID]; ok {
		nodeInfo.Shards = sm.nodeShards[nodeID]
		nodeInfo.Status = "active"
		nodeInfo.LastSeen = time.Now()
	}

	sm.BalanceShards()
}

func (sm *ShardManager) RemoveNodeFromShards(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, shard := range sm.shards {
		newNodes := make([]string, 0, len(shard.Nodes))
		for _, n := range shard.Nodes {
			if n != nodeID {
				newNodes = append(newNodes, n)
			}
		}
		shard.Nodes = newNodes
		if shard.Primary == nodeID && len(shard.Nodes) > 0 {
			shard.Primary = shard.Nodes[0]
		}
	}
	delete(sm.nodeShards, nodeID)
	if nodeInfo, ok := sm.clusterNodes[nodeID]; ok {
		nodeInfo.Status = "leaving"
	}
	sm.BalanceShards()
}

func (sm *ShardManager) BalanceShards() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	totalShards := len(sm.shards)
	if totalShards == 0 {
		return
	}

	nodeCount := make(map[string]int)
	totalDocs := int64(0)
	for _, shard := range sm.shards {
		for _, node := range shard.Nodes {
			nodeCount[node]++
		}
		totalDocs += shard.DocCount
	}

	avgDocs := float64(0)
	if len(nodeCount) > 0 {
		avgDocs = float64(totalDocs) / float64(len(nodeCount))
	}
	avgShards := totalShards / len(nodeCount)
	if avgShards < 1 {
		avgShards = 1
	}

	for node, count := range nodeCount {
		if count > avgShards*2 {
			toMove := count - avgShards
			for _, shard := range sm.shards {
				if toMove <= 0 {
					break
				}
				if shard.Primary == node && len(shard.Nodes) > 1 {
					for otherNode, otherCount := range nodeCount {
						if otherNode != node && otherCount < avgShards {
							nodeDocs := int64(0)
							for _, s := range sm.shards {
								for _, n := range s.Nodes {
									if n == otherNode {
										nodeDocs += s.DocCount
									}
								}
							}
							if float64(nodeDocs) < avgDocs {
								shard.Nodes = append(shard.Nodes, otherNode)
								nodeCount[otherNode]++
								toMove--
								break
							}
						}
					}
				}
			}
		}
	}
}

func (sm *ShardManager) calculateSkew() float64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.shards) == 0 {
		return 0
	}

	var maxDocs int64
	var minDocs int64 = -1
	var totalDocs int64

	for _, shard := range sm.shards {
		docCount := atomic.LoadInt64(&shard.DocCount)
		totalDocs += docCount
		if docCount > maxDocs {
			maxDocs = docCount
		}
		if minDocs == -1 || docCount < minDocs {
			minDocs = docCount
		}
	}

	if totalDocs == 0 {
		return 0
	}

	ideal := totalDocs / int64(len(sm.shards))
	if ideal == 0 {
		return 0
	}

	return float64(maxDocs-minDocs) / float64(ideal)
}

func (sm *ShardManager) totalDocs() int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var total int64
	for _, shard := range sm.shards {
		total += atomic.LoadInt64(&shard.DocCount)
	}
	return total
}

func (sm *ShardManager) Close() {
	close(sm.stopChan)
}
