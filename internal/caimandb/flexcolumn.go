package caimandb

import (
	"caimandb/internal/caimandb/query"
	"crypto/rand"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// flexShardCount matches the 32-way striping already used by the L1/L2
// cache's key registry (see cache.registry) -- same idea applied here: a
// single global mutex protecting every database/block's columnar index was
// exactly the shard_manager-style bottleneck (see UpdateShardStats), just
// one layer deeper. InsertWithColumnar/UpdateWithColumnar/DeleteWithColumnar
// run once per document on every write when FLEX-COLUMN is enabled (the
// default), so concurrent turbo workers writing to *different* blocks used
// to serialize behind the same f.mu regardless of which block they were
// touching. Striping by (db, block) lets unrelated blocks proceed fully in
// parallel; only writers to the same block (or an unlucky hash collision
// with another block) ever contend.
const flexShardCount = 32

// flexShard holds the columnar index/doc-fields/field-stats for whichever
// (db, block) pairs hash to it. Still nested by db/block internally (a
// shard can hold more than one block if their hashes collide), so existing
// per-block logic (ensureMapsLocked/addIndexLocked/removeDocIndexLocked)
// carries over unchanged -- only the lock and the map's home moved.
type flexShard struct {
	mu sync.RWMutex
	// index[db][block][field][value] = set of document IDs
	index map[string]map[string]map[string]map[string]map[string]struct{}
	// docFields[db][block][docID] = field -> indexed value (string form)
	docFields map[string]map[string]map[string]map[string]string
	// fieldStats[db][block][field] = number of writes touching that field
	fieldStats map[string]map[string]map[string]int64
}

func newFlexShard() *flexShard {
	return &flexShard{
		index:      make(map[string]map[string]map[string]map[string]map[string]struct{}),
		docFields:  make(map[string]map[string]map[string]map[string]string),
		fieldStats: make(map[string]map[string]map[string]int64),
	}
}

type FlexColumnEngine struct {
	engine *Engine
	config *FlexColumnConfig

	shards [flexShardCount]*flexShard

	queries atomic.Int64
	hits    atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// shardFor picks the stripe a given (db, block) pair lives in. xxhash is
// already this project's hashing choice for exactly this kind of key
// (shard_manager.GetShardForKey, cache.keyHash), kept consistent here.
func (f *FlexColumnEngine) shardFor(dbName, block string) *flexShard {
	h := xxhash.Sum64String(dbName + "\x00" + block)
	return f.shards[h%flexShardCount]
}

func NewFlexColumnEngine(engine *Engine, config *FlexColumnConfig) *FlexColumnEngine {
	if config == nil {
		config = DefaultFlexColumnConfig()
	}
	f := &FlexColumnEngine{
		engine: engine,
		config: config,
		stopCh: make(chan struct{}),
	}
	for i := range f.shards {
		f.shards[i] = newFlexShard()
	}
	if config.Enabled {
		f.wg.Add(1)
		safeGo("flexcolumn_maintenance_loop", f.maintenanceLoop)
	}
	return f
}

func (f *FlexColumnEngine) maintenanceLoop() {
	defer f.wg.Done()
	interval := f.config.ViewRefreshInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Periodic housekeeping hook (hot-field promotion, view refresh,
			// etc.). Intentionally a no-op placeholder: the index is kept
			// consistent synchronously on every write, so there is nothing
			// that strictly needs to happen here today.
		case <-f.stopCh:
			return
		}
	}
}

func (f *FlexColumnEngine) Stop() {
	if f == nil {
		return
	}
	select {
	case <-f.stopCh:
		// already closed
	default:
		close(f.stopCh)
	}
	f.wg.Wait()
}

// indexableFields returns up to config.MaxColumnarFields scalar field names
// from a document, sorted for determinism.
func (f *FlexColumnEngine) indexableFields(data map[string]any) []string {
	if len(data) == 0 {
		return nil
	}
	fields := make([]string, 0, len(data))
	for k, v := range data {
		switch v.(type) {
		case string, float64, int, int64, bool:
			fields = append(fields, k)
		}
	}
	sort.Strings(fields)
	if f.config.MaxColumnarFields > 0 && len(fields) > f.config.MaxColumnarFields {
		fields = fields[:f.config.MaxColumnarFields]
	}
	return fields
}

func (s *flexShard) ensureMapsLocked(dbName, block string) {
	if s.index[dbName] == nil {
		s.index[dbName] = make(map[string]map[string]map[string]map[string]struct{})
	}
	if s.index[dbName][block] == nil {
		s.index[dbName][block] = make(map[string]map[string]map[string]struct{})
	}
	if s.docFields[dbName] == nil {
		s.docFields[dbName] = make(map[string]map[string]map[string]string)
	}
	if s.docFields[dbName][block] == nil {
		s.docFields[dbName][block] = make(map[string]map[string]string)
	}
	if s.fieldStats[dbName] == nil {
		s.fieldStats[dbName] = make(map[string]map[string]int64)
	}
	if s.fieldStats[dbName][block] == nil {
		s.fieldStats[dbName][block] = make(map[string]int64)
	}
}

func (s *flexShard) addIndexLocked(dbName, block, field, val, id string) {
	fieldMap := s.index[dbName][block]
	if fieldMap[field] == nil {
		fieldMap[field] = make(map[string]map[string]struct{})
	}
	if fieldMap[field][val] == nil {
		fieldMap[field][val] = make(map[string]struct{})
	}
	fieldMap[field][val][id] = struct{}{}
}

func (s *flexShard) removeDocIndexLocked(dbName, block, id string) {
	blockDocs, ok := s.docFields[dbName][block]
	if !ok {
		return
	}
	fields, ok := blockDocs[id]
	if !ok {
		return
	}
	fieldMap := s.index[dbName][block]
	for field, val := range fields {
		if vals, ok := fieldMap[field]; ok {
			if ids, ok := vals[val]; ok {
				delete(ids, id)
				if len(ids) == 0 {
					delete(vals, val)
				}
			}
		}
	}
	delete(blockDocs, id)
}

// bulkModeActive reports whether the owning Engine currently has BULK MODE
// on (explicit BULK MODE ON, an IMPORT in progress via BulkImportReader, or
// GENERATE's auto-bulk threshold -- see Engine.SetBulkMode and its
// callers). Nil-safe throughout: a FlexColumnEngine built without an owning
// *Engine, as the concurrency/benchmark tests in this package do, never
// reads as being in bulk mode.
//
// un-evicted, in-process RAM with no cap tied to document count (see the
// architecture note on flexShard) -- indexing every document of a large
// IMPORT synchronously, one map insert per scalar field per document, is
// what actually OOM-kills the process on a multi-hundred-thousand-row
// load, confirmed by direct A/B (see CHANGELOG). Skipping the index while
// bulk mode is on turns that crash into an explicit, visible, resumable
// step: run `FLEX REINDEX <db> <block>` (or `REBUILD BLOCK <db> <block>`,
// which also does it) once the load finishes.
func (f *FlexColumnEngine) bulkModeActive() bool {
	if f == nil || f.engine == nil || f.engine.turbo == nil || f.engine.turbo.Bulk == nil {
		return false
	}
	return f.engine.turbo.Bulk.Enabled()
}

// indexDoc does the actual columnar-index write for one document,
// unconditionally -- no Enabled/bulk-mode gating. InsertWithColumnar is the
// gated hot-path entry point; ReindexBlock (an explicit, administrator-
// triggered backfill) calls this directly so REINDEX still does its job
// even if BULK MODE happens to still be on when it's run.
func (f *FlexColumnEngine) indexDoc(dbName, block string, doc *Document) {
	fields := f.indexableFields(doc.Data)
	if len(fields) == 0 {
		return
	}

	// fmt.Sprint-ing every field's value used to happen while holding the
	// lock -- originally a single engine-wide mutex shared by every
	// block/database, taken once per document on every insert; now a
	// per-shard lock (see flexShardCount), but the same principle still
	// applies one level down: this is pure CPU work with no shared state
	// (it only reads doc.Data, which nothing else mutates concurrently),
	// so it parallelizes for free across insert workers when done before
	// the lock; doing it after would instead serialize every worker's
	// string conversions behind the shard's mutex on top of the map
	// mutations that actually need it.
	stored := make(map[string]string, len(fields))
	for _, field := range fields {
		stored[field] = fmt.Sprint(doc.Data[field])
	}

	shard := f.shardFor(dbName, block)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.ensureMapsLocked(dbName, block)

	// Idempotency guard: applyMutations (and therefore this call) can
	// legitimately run more than once for the exact same document --
	// Commit()'s own inline retries, and the live tx reconciler
	// (tx_reconciler.go) both re-run applyMutations after a failure
	// without any way to know whether an earlier attempt got partway
	// through before failing. addIndexLocked's sets are naturally
	// idempotent (re-adding the same id is a no-op), but fieldStats is a
	// plain counter -- without this check, a retried indexDoc would
	// double-count every field of every retried document, and any
	// planner/estimation logic reading fieldStats would drift further
	// out of sync with reality on every retry.
	if existing, ok := shard.docFields[dbName][block][doc.ID]; ok && flexFieldsEqual(existing, stored) {
		return
	}

	for _, field := range fields {
		val := stored[field]
		shard.addIndexLocked(dbName, block, field, val, doc.ID)
		shard.fieldStats[dbName][block][field]++
	}
	shard.docFields[dbName][block][doc.ID] = stored
}

// flexFieldsEqual reports whether two indexed-field snapshots are
// identical, used by indexDoc to detect a retried call for the same
// document (see the idempotency guard there).
func flexFieldsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// InsertWithColumnar indexes the scalar fields of a newly inserted document.
// A no-op while BULK MODE is active -- see bulkModeActive's doc comment.
func (f *FlexColumnEngine) InsertWithColumnar(dbName, block string, doc *Document) {
	if f == nil || f.config == nil || !f.config.Enabled || doc == nil {
		return
	}
	if f.bulkModeActive() {
		return
	}
	f.indexDoc(dbName, block, doc)
}

// UpdateWithColumnar re-indexes a document after an update by removing its
// previous index entries and inserting fresh ones based on the new data.
// Also a no-op while BULK MODE is active (see bulkModeActive) -- skipping
// the removal too, rather than only the re-insert, avoids paying for a
// locked map walk on every updated document just to leave a hole that
// REINDEX will overwrite anyway.
func (f *FlexColumnEngine) UpdateWithColumnar(dbName, block string, oldDoc, newDoc *Document) {
	if f == nil || f.config == nil || !f.config.Enabled || newDoc == nil {
		return
	}
	if f.bulkModeActive() {
		return
	}
	if oldDoc != nil {
		shard := f.shardFor(dbName, block)
		shard.mu.Lock()
		shard.ensureMapsLocked(dbName, block)
		shard.removeDocIndexLocked(dbName, block, oldDoc.ID)
		shard.mu.Unlock()
	}
	f.indexDoc(dbName, block, newDoc)
}

// resetBlock clears whatever FLEX-COLUMN index entries currently exist for
// (dbName, block), used by ReindexBlock right before rebuilding from disk
// so a REINDEX run doesn't leave orphaned entries from before it started
// (or from documents deleted since the last time the block was indexed).
func (f *FlexColumnEngine) resetBlock(dbName, block string) {
	if f == nil {
		return
	}
	shard := f.shardFor(dbName, block)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.ensureMapsLocked(dbName, block)
	shard.index[dbName][block] = make(map[string]map[string]map[string]struct{})
	shard.docFields[dbName][block] = make(map[string]map[string]string)
	shard.fieldStats[dbName][block] = make(map[string]int64)
}

// DeleteWithColumnar removes a document's entries from the columnar index.
func (f *FlexColumnEngine) DeleteWithColumnar(dbName, block, id string) {
	if f == nil || f.config == nil || !f.config.Enabled {
		return
	}
	shard := f.shardFor(dbName, block)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.docFields[dbName] == nil || shard.docFields[dbName][block] == nil {
		return
	}
	shard.removeDocIndexLocked(dbName, block, id)
}

// QueryWithColumnar attempts to accelerate a single-field equality query
// using the in-memory columnar index. It returns (nil, nil) whenever it
// cannot help, signaling the caller to fall back to the normal scan/index
// path in engine.Find.
func (f *FlexColumnEngine) QueryWithColumnar(dbName, block string, q *Query) (*QueryResult, error) {
	if f == nil || f.config == nil || !f.config.Enabled || q == nil {
		return nil, nil
	}
	if len(q.Filters) != 1 {
		return nil, nil
	}
	if len(q.GroupBy) > 0 || q.Having != nil {
		// GROUP BY / HAVING aren't implemented by this fast path; defer
		// to the normal scan/index flow in engine.Find, which runs
		// postProcessDocs and therefore groupDocs.
		return nil, nil
	}
	if len(q.Distinct) > 0 || q.DistinctAll {
		// implemented in postProcessDocs (distinctByFields/distinctByRow),
		// which this fast path skips entirely.
		return nil, nil
	}
	for _, it := range q.SelectItems {
		if it.Kind != query.SelectPlain {
			// COUNT(...)/arithmetic SELECT items are computed in
			// postProcessDocs, which this fast path skips entirely.
			return nil, nil
		}
	}
	filter := q.Filters[0]
	if filter.Op != "=" && filter.Op != "==" {
		return nil, nil
	}

	start := time.Now()

	shard := f.shardFor(dbName, block)
	shard.mu.RLock()
	blockIndex, ok := shard.index[dbName][block]
	if !ok {
		shard.mu.RUnlock()
		return nil, nil
	}
	valsIdx, ok := blockIndex[filter.Field]
	if !ok {
		shard.mu.RUnlock()
		return nil, nil
	}
	valKey := fmt.Sprint(filter.Value)
	idSet := valsIdx[valKey]
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	shard.mu.RUnlock()

	f.queries.Add(1)
	f.hits.Add(1)

	docs := make([]Document, 0, len(ids))
	for _, id := range ids {
		if doc, ok := f.engine.FindByID(dbName, block, id); ok {
			docs = append(docs, *doc)
		}
	}

	if len(q.Sort) > 0 {
		sortDocs(docs, q.Sort)
	}

	total := int64(len(docs))

	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > len(docs) {
		offset = len(docs)
	}
	docs = docs[offset:]
	if q.Limit > 0 && q.Limit < len(docs) {
		docs = docs[:q.Limit]
	}

	if len(q.Select) > 0 || len(q.Exclude) > 0 {
		docs = f.engine.resolveRelations(dbName, block, docs, q)
	}

	return &QueryResult{
		Docs:      docs,
		Total:     total,
		Scanned:   int64(len(ids)),
		IndexUsed: "flex-column:" + filter.Field,
		Took:      time.Since(start),
	}, nil
}

// HotFields reports write counts per indexed field, used by FLEX HOT.
// If block is empty, it aggregates across every block in the database --
// which now means scanning every shard (a rare admin-path call, unlike the
// per-document hot path this sharding was introduced for), since different
// blocks of the same database can live in different shards.
func (f *FlexColumnEngine) HotFields(dbName, block string) map[string]int64 {
	if f == nil {
		return nil
	}
	result := make(map[string]int64)
	if block != "" {
		shard := f.shardFor(dbName, block)
		shard.mu.RLock()
		for field, count := range shard.fieldStats[dbName][block] {
			result[field] = count
		}
		shard.mu.RUnlock()
		return result
	}
	for _, shard := range f.shards {
		shard.mu.RLock()
		for blk, stats := range shard.fieldStats[dbName] {
			for field, count := range stats {
				result[blk+"."+field] = count
			}
		}
		shard.mu.RUnlock()
	}
	return result
}

// Stats returns a snapshot of the FLEX-COLUMN engine for STATUS/HEALTH/FLEX STATS.
func (f *FlexColumnEngine) Stats() map[string]any {
	if f == nil {
		return map[string]any{"enabled": false}
	}

	indexedBlocks := 0
	indexedFields := 0
	indexedDocs := 0
	for _, shard := range f.shards {
		shard.mu.RLock()
		for _, blocks := range shard.index {
			for _, fields := range blocks {
				indexedBlocks++
				indexedFields += len(fields)
			}
		}
		for _, blocks := range shard.docFields {
			for _, docs := range blocks {
				indexedDocs += len(docs)
			}
		}
		shard.mu.RUnlock()
	}

	return map[string]any{
		"enabled":              f.config.Enabled,
		"columnar_index":       f.config.ColumnarIndexEnabled,
		"materialized_views":   f.config.MaterializedViews,
		"columnar_compression": f.config.ColumnarCompression,
		"max_columnar_fields":  f.config.MaxColumnarFields,
		"indexed_blocks":       indexedBlocks,
		"indexed_fields":       indexedFields,
		"indexed_documents":    indexedDocs,
		"queries":              f.queries.Load(),
		"hits":                 f.hits.Load(),
	}
}

func generateRandomPassword() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()"
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		log().Fatal("Failed to generate secure random password (crypto/rand failed)",
			zap.Error(err))
	}
	for i := range buf {
		buf[i] = charset[int(buf[i])%len(charset)]
	}
	return string(buf)
}
