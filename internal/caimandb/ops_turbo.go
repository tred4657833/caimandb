package caimandb

import (
	"fmt"

	"caimandb/internal/caimandb/turbo"
	"caimandb/internal/caimandb/wal"
)

// newTurboCoordinator builds the turbo engine infrastructure (adaptive
// worker pool, priority scheduler, bulk-mode flag) and the auto-batching
// front doors that sit in front of the engine's existing write paths. It's
// deliberately a thin layer: all it does is coalesce what look like
// independent single-document requests into fewer calls to the storage
// paths that already exist (insertBatchLocalDetailed / InsertBatch), which
// already do one lock acquisition, one WriteBatch and (in cluster mode)
// one Raft round-trip per group instead of per document.
func newTurboCoordinator(e *Engine) *Engine {
	e.turbo = turbo.New(turbo.Config{
		MinWorkers: e.config.TurboMinWorkers,
		MaxWorkers: e.config.TurboMaxWorkers,

		BatchMaxSize: e.config.TurboBatchMaxSize,
		BatchMaxWait: e.config.TurboBatchMaxWait,

		BulkBatchMaxSize: e.config.TurboBulkBatchMaxSize,
		BulkBatchMaxWait: e.config.TurboBulkBatchMaxWait,
	})

	maxSize, maxWait := e.turbo.BatchThresholds()
	e.turboInserts = turbo.NewBatcherGroup[map[string]any](maxSize, maxWait, e.flushTurboInsertBatch)

	return e
}

// flushTurboInsertBatch is the BatcherGroup flush callback for
// e.turboInserts: it's handed every document that landed in the same
// (db, block) batch window and must insert all of them and report one
// error per document, in the same order.
func (e *Engine) flushTurboInsertBatch(key string, docs []map[string]any) []error {
	dbName, block, ok := splitTurboKey(key)
	if !ok {
		errs := make([]error, len(docs))
		for i := range errs {
			errs[i] = fmt.Errorf("invalid turbo batch key: %q", key)
		}
		return errs
	}

	_, errs := e.insertBatchLocalDetailed(dbName, block, docs)
	return errs
}

func turboKey(dbName, block string) string {
	return dbName + "\x00" + block
}

func splitTurboKey(key string) (dbName, block string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], key[i+1:], true
		}
	}
	return "", "", false
}

// InsertFast is a drop-in alternative to Insert for standalone (non-Raft)
// installations. It resolves the document's ID exactly like Insert does,
// then hands it off to LineDat (see linedat.go) instead of writing it
// immediately on its own: the document is durably staged and acknowledged
// right away, and reaches CaimanDB's permanent storage shortly after via
// LineDat's background drain loop. FindByID/Find both consult LineDat, so
// a read immediately following an accepted InsertFast still sees it.
//
// If LineDat isn't available (disabled, or failed to initialize), this
// falls back to the previous behavior: handing the document to the turbo
// auto-batcher, which coalesces concurrent InsertFast calls landing on the
// same (db, block) within a short window (see TurboBatchMaxWait /
// TurboBulkBatchMaxWait) into a single insertBatchLocalDetailed call, but
// -- unlike LineDat -- still blocks the caller until that commit finishes.
//
// Raft-clustered installations don't use either fast path: InsertFast falls
// back to Insert (which already routes through Raft) whenever the engine
// has an active cluster, since both LineDat and the turbo batcher write
// directly to local storage and do not go through consensus.
//
// Known trade-off: because insertBatchLocalDetailed reports one error per
// document, a rare storage-level failure affecting only one document in a
// multi-document batch is reported only for that document -- callers whose
// requests happen to land in the same batch as a failing one are
// unaffected by it. This mirrors the aggregate-result trade-off the
// existing InsertBatch/IMPORT paths already make for bulk loads.
func (e *Engine) InsertFast(dbName, block string, data map[string]any) (*Document, error) {
	if e.cluster != nil && !e.cluster.IsSingleNode() {
		// Real multi-node cluster: always defer to the regular,
		// raft-aware Insert path -- whether this node is leader or not.
		// insertWithID's clusterWriteGate makes that call correctly,
		// including rejecting on a non-leader instead of silently
		// applying locally, which the LineDat/Turbo fast paths below
		// cannot do (they write straight to local storage and never go
		// through consensus, so on a non-leader they'd otherwise accept
		// writes the rest of the cluster would never see).
		return e.Insert(dbName, block, data)
	}

	if e.lineDat != nil && e.config.LineDatEnabled {
		docs, errs := e.lineDat.EnqueueDocs(dbName, block, []map[string]any{data})
		if errs[0] != nil {
			return nil, errs[0]
		}
		return docs[0], nil
	}

	if e.turbo == nil || !e.config.TurboEnabled {
		return e.Insert(dbName, block, data)
	}

	id := resolveDocID(data)
	if id == "" {
		id = newID()
	}
	data["id"] = id

	if err := e.turboInserts.Submit(turboKey(dbName, block), data); err != nil {
		return nil, err
	}

	shardID := e.shardMgr.GetShardForDoc(dbName, block, id)
	return &Document{ID: id, Data: data, ShardID: shardID}, nil
}

// InsertBatchViaLineDat is InsertBatch's LineDat-accelerated counterpart,
// meant for large loads (INSERT ... GENERATE, IMPORT) where the caller
// wants CaimanDB to keep accepting documents at full speed while the
// actual commit to permanent storage catches up progressively in the
// background, instead of blocking until the whole batch is durably
// written to Badger. This is exactly LineDat's purpose: loading, say,
// 1,000,000 documents can hand them all to LineDat and return almost
// immediately, letting the caller move on while CaimanDB finishes writing
// them in the background.
//
// The returned (inserted, errors) counts mean "durably accepted by
// LineDat, guaranteed to eventually reach CaimanDB" for "inserted" --
// not "already committed to Badger". A document that is accepted here but
// then genuinely fails to commit (e.g. exceeds MaxDocumentSize) is retried
// a bounded number of times by LineDat's drain loop and, if it keeps
// failing, is logged and dropped -- it does not come back through this
// call's return value, the same trade-off InsertFast already makes.
//
// Falls back to the fully synchronous InsertBatch when LineDat is
// disabled/unavailable, or when this node is part of a real multi-node
// Raft cluster (leader or not) -- same restriction InsertFast has, since
// LineDat's drain writes directly to local storage and does not go
// through consensus. InsertBatch itself decides whether that means "go
// through raft.Apply()" (leader) or "reject" (non-leader) -- see
// clusterWriteGate.
func (e *Engine) InsertBatchViaLineDat(dbName, block string, docs []map[string]any) (int64, int64) {
	if e.lineDat == nil || !e.config.LineDatEnabled || (e.cluster != nil && !e.cluster.IsSingleNode()) {
		return e.InsertBatch(dbName, block, docs)
	}
	return e.lineDat.EnqueueBatch(dbName, block, docs)
}

// LineDatStats returns a snapshot of LineDat activity (pending documents,
// committed/dropped totals, journal size) for STATUS/diagnostics.
func (e *Engine) LineDatStats() map[string]any {
	if e.lineDat == nil {
		return map[string]any{"enabled": false}
	}
	stats := e.lineDat.Stats()
	stats["enabled"] = e.config.LineDatEnabled
	return stats
}

// SetBulkMode turns turbo bulk-load mode on or off. While on, the turbo
// insert batcher uses wider (TurboBulkBatchMaxSize/TurboBulkBatchMaxWait)
// thresholds for bigger, less frequent batches, and the WAL's fsync policy
// is relaxed to WALSyncInterval for the duration (restored to whatever it
// was before when bulk mode is turned back off) so a long bulk load isn't
// paying for an fsync per small group the whole way through.
func (e *Engine) SetBulkMode(on bool) {
	if e.turbo == nil {
		return
	}
	if on {
		e.turbo.Bulk.Enable()
		if e.wal != nil {
			e.bulkModePrevWALPolicy = e.wal.SyncPolicy()
			e.wal.SetSyncPolicy(wal.WALSyncInterval)
		}
	} else {
		e.turbo.Bulk.Disable()
		if e.wal != nil {
			e.wal.SetSyncPolicy(e.bulkModePrevWALPolicy)
		}
	}
	if e.pool != nil {
		// Only affects the ONE-TIME tier decision for blocks opened while
		// bulk mode is on (see pickTierLocked's doc comment) -- never
		// touches a block that's already open.
		e.pool.SetBulkModeHint(on)
	}
	if e.changeBus != nil {
		// See ChangeBus.Publish's doc comment: only ever skips work that
		// would otherwise be unobservable (no subscriber connected), and
		// is re-checked on every single Publish call, so this is safe to
		// flip at any time without coordinating with in-flight inserts.
		e.changeBus.SetBulkModeHint(on)
	}
	maxSize, maxWait := e.turbo.BatchThresholds()
	e.turboInserts.SetThresholds(maxSize, maxWait)
}

// TurboStats returns a snapshot of turbo engine activity (worker pool,
// scheduler, bulk mode, insert batcher) for STATUS/diagnostics.
func (e *Engine) TurboStats() map[string]any {
	if e.turbo == nil {
		return map[string]any{"enabled": false}
	}
	stats := e.turbo.Stats()
	stats["enabled"] = e.config.TurboEnabled
	stats["insert_batcher"] = e.turboInserts.Stats()
	return stats
}
