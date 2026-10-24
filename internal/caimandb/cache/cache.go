// Package cache implements CaimanDB's in-process L1 (document) and
// L2 (secondary-index result) caches on top of Ristretto
// (github.com/dgraph-io/ristretto/v2): a TinyLFU-admission,
// SampledLFU-eviction, cost-based concurrent cache. Ristretto handles the
// actual hot-path Get/Set/eviction/TTL machinery (lock-striped internals,
// probabilistic admission so a scan-flood of one-shot keys can't wash out
// the working set); this package layers CaimanDB's existing public API
// (byte-value L1 doc cache, structured L2 index-result cache, prefix
// deletion, human-readable Stats()) on top of it, so every caller
// elsewhere in the engine keeps working unchanged.
package cache

import (
	"caimandb/internal/caimandb/logging"
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/storage"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto/v2"
)

// fmtBytes is duplicated (rather than imported) from the root package's
// misc_utils.go to keep this package dependency-free.
func fmtBytes(n int64) string {
	const (
		KB int64 = 1024
		MB       = 1024 * KB
		GB       = 1024 * MB
		TB       = 1024 * GB
		PB       = 1024 * TB
	)
	switch {
	case n >= PB:
		return fmt.Sprintf("%.3f PB", float64(n)/float64(PB))
	case n >= TB:
		return fmt.Sprintf("%.3f TB", float64(n)/float64(TB))
	case n >= GB:
		return fmt.Sprintf("%.3f GB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.3f MB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.3f KB", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// keyHash is the KeyToHash function every cache in this package uses, so
// OnEvict callbacks (which only get handed the hashed uint64, never the
// original string -- that's a Ristretto property, not a CaimanDB choice)
// can be matched back to the string key via the hash->key registries
// below. xxhash is already a project dependency and is what Badger itself
// uses internally, so this keeps the hashing strategy consistent across
// the storage stack instead of pulling in Ristretto's default hasher too.
func keyHash(key string) (uint64, uint64) {
	return xxhash.Sum64String(key), 0
}

type registryShard struct {
	mu     sync.RWMutex
	byKey  map[string]struct{}
	byHash map[uint64]string
}

// registry tracks the set of live string keys for a Ristretto cache.
// Ristretto is deliberately not iterable and its eviction callback hands
// back only a hashed uint64, not the original key -- so prefix deletion
// and entry counts (both part of CaimanDB's existing cache API) need a
// small side index. It's sharded for the same reason the old hand-rolled
// cache was: DeleteByPrefix/Stats shouldn't serialize against every Get.
type registry struct {
	shards [32]registryShard
}

func newRegistry() *registry {
	r := &registry{}
	for i := range r.shards {
		r.shards[i].byKey = make(map[string]struct{}, 256)
		r.shards[i].byHash = make(map[uint64]string, 256)
	}
	return r
}

func (r *registry) shardFor(key string) *registryShard {
	return &r.shards[xxhash.Sum64String(key)&31]
}

func (r *registry) add(key string) {
	s := r.shardFor(key)
	h, _ := keyHash(key)
	s.mu.Lock()
	s.byKey[key] = struct{}{}
	s.byHash[h] = key
	s.mu.Unlock()
}

func (r *registry) removeByKey(key string) {
	s := r.shardFor(key)
	h, _ := keyHash(key)
	s.mu.Lock()
	delete(s.byKey, key)
	delete(s.byHash, h)
	s.mu.Unlock()
}

// removeByHash is what Ristretto's OnEvict calls back with -- it only
// knows the item's hash, so this is the only way to reconcile eviction
// with the registry. shardIdx must be the same (hash & 31) scheme used by
// shardFor, since the hash alone doesn't tell us which key produced it.
func (r *registry) removeByHash(shardIdx int, h uint64) {
	s := &r.shards[shardIdx]
	s.mu.Lock()
	if key, ok := s.byHash[h]; ok {
		delete(s.byKey, key)
		delete(s.byHash, h)
	}
	s.mu.Unlock()
}

func (r *registry) count() int {
	n := 0
	for i := range r.shards {
		r.shards[i].mu.RLock()
		n += len(r.shards[i].byKey)
		r.shards[i].mu.RUnlock()
	}
	return n
}

// keysWithPrefix returns every registered key starting with prefix.
func (r *registry) keysWithPrefix(prefix string) []string {
	var out []string
	for i := range r.shards {
		r.shards[i].mu.RLock()
		for k := range r.shards[i].byKey {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				out = append(out, k)
			}
		}
		r.shards[i].mu.RUnlock()
	}
	return out
}

// ---------------------------------------------------------------------
// L1Cache: document-body byte cache.
// ---------------------------------------------------------------------

type L1Cache struct {
	rc         *ristretto.Cache[string, []byte]
	reg        *registry
	maxBytes   int64
	compressed bool
	ttl        time.Duration
	hits       atomic.Int64
	misses     atomic.Int64
	evictions  atomic.Int64
	preloaded  atomic.Bool
}

// MaxBytes returns the cache's configured total byte budget.
func (c *L1Cache) MaxBytes() int64 { return c.maxBytes }

func NewL1Cache(maxMB int64, compressed bool, ttl time.Duration) *L1Cache {
	total := maxMB * 1024 * 1024
	if total <= 0 {
		total = 1 << 20
	}

	c := &L1Cache{
		maxBytes:   total,
		compressed: compressed,
		ttl:        ttl,
		reg:        newRegistry(),
	}

	// NumCounters ~= 10x the expected number of items is Ristretto's own
	// sizing guidance for accurate frequency estimation; ~200 bytes/item
	// is a conservative average for cached document bodies.
	estimatedItems := total / 200
	if estimatedItems < 10000 {
		estimatedItems = 10000
	}

	rc, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: estimatedItems * 10,
		MaxCost:     total,
		BufferItems: 64,
		Metrics:     true,
		KeyToHash:   keyHash,
		OnEvict: func(item *ristretto.Item[[]byte]) {
			shardIdx := int(item.Key & 31)
			c.reg.removeByHash(shardIdx, item.Key)
			c.evictions.Add(1)
		},
	})
	if err != nil {
		// Config is static and validated above; a NewCache failure here
		// means a programming error (e.g. NumCounters/MaxCost <= 0), not
		// a runtime condition callers can recover from.
		logging.Log().Fatal("failed to initialize L1 ristretto cache: " + err.Error())
	}
	c.rc = rc
	return c
}

// CleanupExpired is kept for API compatibility with callers that used to
// drive the old hand-rolled cache's TTL sweep explicitly. Ristretto sweeps
// its own TTL internally (TtlTickerDurationInSec), so this is now a no-op;
// it's safe for existing periodic-maintenance callers to keep calling it.
func (c *L1Cache) CleanupExpired() {}

func (c *L1Cache) Get(key string) ([]byte, bool) {
	val, ok := c.rc.Get(key)
	if !ok {
		c.misses.Add(1)
		metrics.MetricCacheMisses.Inc()
		return nil, false
	}
	c.hits.Add(1)
	metrics.MetricCacheHits.Inc()
	return val, true
}

func (c *L1Cache) Set(key string, value []byte) {
	if c.compressed && len(value) > 1024 {
		if compressed, ok, err := storage.CompressData(value, storage.CompressionZstd); err == nil && ok {
			value = compressed
		}
	}

	cost := int64(len(key) + len(value) + 64)
	c.reg.add(key)
	if c.ttl > 0 {
		c.rc.SetWithTTL(key, value, cost, c.ttl)
	} else {
		c.rc.Set(key, value, cost)
	}
}

// Wait blocks until every L1Cache.Set/Del call made before it returns has
// actually landed in the cache's backing store.
//
// Ristretto's Set (unlike Get and Del) does not update its backing store
// synchronously: it pushes the item onto an internal buffered channel that
// a background goroutine drains, running the admission policy and only
// then writing it into the map Get() reads from. A single Set followed
// immediately by a Get for that same key can therefore miss (an
// acceptable, self-correcting cache miss -- the caller falls through to
// Badger and gets the fresh value). But when the SAME key is Set twice in
// quick succession -- exactly what WAL recovery does for any document an
// "insert" entry and a later "update"/"tx_commit" entry both touch -- nothing
// guarantees the two buffered Set operations are drained in the order
// they were issued relative to each other under contention: the second
// (newer) Set can be superseded by the first (older) one finishing its
// admission processing later, leaving Get() returning stale data
// indefinitely instead of merely missing. That is not a performance
// hiccup, it is a wrong answer.
//
// Wait forces the buffer to drain fully (it's the same primitive
// Ristretto's own test suite uses to make cache state deterministic
// after a burst of Sets), so callers that need read-your-writes
// consistency for a batch of mutations -- like WAL recovery finishing a
// batch of replayed entries right before the engine is declared ready
// for reads -- can call this once to guarantee it, without paying the
// cost on every individual Set on the normal hot write path.
func (c *L1Cache) Wait() {
	c.rc.Wait()
}

func (c *L1Cache) Del(key string) {
	c.reg.removeByKey(key)
	c.rc.Del(key)
}

// DeleteByPrefix removes every cached entry whose key starts with prefix.
func (c *L1Cache) DeleteByPrefix(prefix string) int {
	keys := c.reg.keysWithPrefix(prefix)
	for _, k := range keys {
		c.reg.removeByKey(k)
		c.rc.Del(k)
	}
	return len(keys)
}

func (c *L1Cache) Stats() map[string]any {
	h, m := c.hits.Load(), c.misses.Load()
	ratio := 0.0
	if tot := h + m; tot > 0 {
		ratio = float64(h) / float64(tot) * 100
	}
	var used int64
	if met := c.rc.Metrics; met != nil {
		used = int64(met.CostAdded()) - int64(met.CostEvicted())
		if used < 0 {
			used = 0
		}
	}
	return map[string]any{
		"entries":       c.reg.count(),
		"used":          fmtBytes(used),
		"max":           fmtBytes(c.maxBytes),
		"used_bytes":    used,
		"max_bytes":     c.maxBytes,
		"hits":          h,
		"misses":        m,
		"hit_ratio":     fmt.Sprintf("%.2f%%", ratio),
		"hit_ratio_pct": ratio,
		"evictions":     c.evictions.Load(),
		"compressed":    c.compressed,
		"preloaded":     c.preloaded.Load(),
		"ttl":           c.ttl.String(),
		"engine":        "ristretto",
	}
}

func (c *L1Cache) Close() {
	c.rc.Close()
}

// ---------------------------------------------------------------------
// L2IndexCache: secondary-index lookup result cache.
// ---------------------------------------------------------------------

type IndexResult struct {
	DocIDs    []string
	Page      int
	Total     int64
	HasMore   bool
	NextToken string
	ExpiresAt time.Time
	LastUsed  time.Time
	Frequency int
	Size      int
	Loaded    bool
}

type L2IndexCache struct {
	rc          *ristretto.Cache[string, *IndexResult]
	reg         *registry
	max         int
	evicted     atomic.Int64
	hit         atomic.Int64
	miss        atomic.Int64
	lazyLoading bool
	ttl         time.Duration
}

func NewL2IndexCache(maxEntries int, lazyLoading bool, ttl time.Duration) *L2IndexCache {
	if maxEntries < 1 {
		maxEntries = 1
	}
	c := &L2IndexCache{
		max:         maxEntries,
		lazyLoading: lazyLoading,
		ttl:         ttl,
		reg:         newRegistry(),
	}

	rc, err := ristretto.NewCache(&ristretto.Config[string, *IndexResult]{
		NumCounters: int64(maxEntries) * 10,
		MaxCost:     int64(maxEntries), // one entry == one unit of cost: a count-based cache, same semantics as the old maxEntries limit
		BufferItems: 64,
		Metrics:     true,
		KeyToHash:   keyHash,
		OnEvict: func(item *ristretto.Item[*IndexResult]) {
			shardIdx := int(item.Key & 31)
			c.reg.removeByHash(shardIdx, item.Key)
			c.evicted.Add(1)
		},
	})
	if err != nil {
		logging.Log().Fatal("failed to initialize L2 ristretto cache: " + err.Error())
	}
	c.rc = rc
	return c
}

func l2Key(block, field, value string) string {
	return block + "/" + field + "/" + value
}

// CleanupExpired is kept for API compatibility; Ristretto sweeps its own
// TTL internally now.
func (c *L2IndexCache) CleanupExpired() {}

func (c *L2IndexCache) Get(block, field, value string) (*IndexResult, bool) {
	key := l2Key(block, field, value)
	res, ok := c.rc.Get(key)
	if !ok {
		c.miss.Add(1)
		return nil, false
	}
	if c.lazyLoading && !res.Loaded {
		c.miss.Add(1)
		return nil, false
	}
	if !res.ExpiresAt.IsZero() && time.Now().After(res.ExpiresAt) {
		c.reg.removeByKey(key)
		c.rc.Del(key)
		c.miss.Add(1)
		return nil, false
	}
	// Mutating fields on the returned pointer is safe and intentional --
	// Ristretto stores *IndexResult by pointer, same aliasing behavior
	// the old map[string]*IndexResult cache had.
	res.LastUsed = time.Now()
	res.Frequency++
	c.hit.Add(1)
	return res, true
}

func (c *L2IndexCache) Set(block, field, value string, docIDs []string, total int64, hasMore bool, nextToken string) {
	key := l2Key(block, field, value)
	res := &IndexResult{
		DocIDs:    docIDs,
		Total:     total,
		HasMore:   hasMore,
		NextToken: nextToken,
		LastUsed:  time.Now(),
		Frequency: 1,
		Size:      len(docIDs),
		Loaded:    true,
	}
	if c.ttl > 0 {
		res.ExpiresAt = time.Now().Add(c.ttl)
	}
	c.reg.add(key)
	if c.ttl > 0 {
		c.rc.SetWithTTL(key, res, 1, c.ttl)
	} else {
		c.rc.Set(key, res, 1)
	}
}

func (c *L2IndexCache) Stats() map[string]any {
	hit, miss := c.hit.Load(), c.miss.Load()
	return map[string]any{
		"entries":      c.reg.count(),
		"max":          c.max,
		"hit":          hit,
		"miss":         miss,
		"evicted":      c.evicted.Load(),
		"hit_rate":     fmt.Sprintf("%.2f%%", float64(hit)/float64(hit+miss+1)*100),
		"lazy_loading": c.lazyLoading,
		"engine":       "ristretto",
	}
}

func (c *L2IndexCache) Close() {
	c.rc.Close()
}
