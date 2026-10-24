package caimandb

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestEngine_ShardAutoSplit_UnderLoad covers the "shard lleno" row of the
// a hard rejection ("full", refuse further writes) -- there's no such
// path; MaxDocsPerShard is set on ShardManager's config but never
// actually read anywhere else, so it's inert. The real behavior is
// transparent auto-splitting: ShardManager.checkAndScale, on its
// background ticker, doubles the shard count once docsPerShard crosses
// config.MaxShardDocs (see shard_manager.go). This test exercises that
// real trigger end-to-end -- real InsertBatch load driving real
// UpdateShardStats calls past the threshold -- rather than
// shard_manager_concurrency_test.go's existing coverage, which calls
// sm.scaleUp directly and never goes through the threshold-crossing
// decision itself.
//
// Two things needed backdating/direct triggering rather than waiting on
// real wall-clock time, both safe here because nothing else touches
// ShardManager concurrently on this specific field/call before the
// insert goroutines start:
//   - scaleGracePeriod (60s) holds off any auto-scale decision for that
//     long after ShardManager startup, precisely so a fresh node doesn't
//     scale on a misleadingly low startup reading. Backdating
//     sm.startedAt is the standard way to exercise time-gated logic
//     without an actual 60+ second sleep in the test suite.
//   - checkAndScale itself only runs off a background ticker
//     (scaleCheckInterval) in production; calling it directly makes the
//     scale decision happen deterministically at a known point instead
//     of racing an arbitrary ticker interval.
func TestEngine_ShardAutoSplit_UnderLoad(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataRoot = t.TempDir()
	cfg.AutoCluster = false
	cfg.AutoMergeEnabled = false
	cfg.WALSyncPolicy = "off"
	cfg.LargeDocThreshold = 0
	cfg.ShardCount = 2
	cfg.MaxShardsPerNode = 64
	cfg.AutoSplitEnabled = true
	cfg.MaxShardDocs = 20 // low on purpose: real docsPerShard must cross this from real inserts, not a contrived huge dataset
	// See the matching comment in newConcurrencyTestEngine
	// (ops_insert_concurrency_test.go).
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64

	e := NewEngine(cfg)
	t.Cleanup(e.Shutdown)

	const dbName = "shard_full_test"
	const block = "block1"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock failed: %v", err)
	}

	e.shardMgr.mu.Lock()
	shardCountBefore := e.shardMgr.shardCount
	e.shardMgr.startedAt = time.Now().Add(-2 * scaleGracePeriod)
	e.shardMgr.mu.Unlock()

	const writers = 4
	const batchesPerWriter = 10
	const docsPerBatch = 25
	const totalDocs = int64(writers * batchesPerWriter * docsPerBatch) // 1000, well past 2 shards * 20 = 40

	var wg sync.WaitGroup
	var totalInserted, totalInsertErrors int64

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for b := 0; b < batchesPerWriter; b++ {
				docs := make([]map[string]any, docsPerBatch)
				for i := range docs {
					docs[i] = map[string]any{"worker": w, "batch": b, "i": i, "tag": "shard_full_test"}
				}
				ins, errs := e.InsertBatch(dbName, block, docs)
				atomic.AddInt64(&totalInserted, ins)
				atomic.AddInt64(&totalInsertErrors, errs)
			}
			// Force the scale decision partway through each writer's
			// work, not just once at the very end -- a real "filling
			// up" scenario has the split happening while more writes
			// are still landing, which is the actually risky interleaving
			// (rescaleLocked rebuilding the shard map concurrently with
			// UpdateShardStats/GetShardForDoc calls from in-flight
			// inserts), not a split neatly after all writes are done.
			e.shardMgr.checkAndScale()
		}(w)
	}
	wg.Wait()

	// One more check after everything settles, in case docsPerShard only
	// crossed the threshold once the later batches landed.
	e.shardMgr.checkAndScale()

	if totalInsertErrors != 0 {
		t.Errorf("InsertBatch reported %d errors during shard auto-split load", totalInsertErrors)
	}
	if totalInserted != totalDocs {
		t.Errorf("InsertBatch reported %d documents inserted, want %d", totalInserted, totalDocs)
	}

	e.shardMgr.mu.RLock()
	shardCountAfter := e.shardMgr.shardCount
	e.shardMgr.mu.RUnlock()

	if shardCountAfter <= shardCountBefore {
		t.Errorf("shard count after load = %d, want more than %d (MaxShardDocs=%d should have triggered at least one auto-split under %d total documents)",
			shardCountAfter, shardCountBefore, cfg.MaxShardDocs, totalDocs)
	}

	// The real correctness question: did rescaleLocked (run concurrently
	// with in-flight inserts above) leave anything corrupted for reads.
	countAfter, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("Count after auto-split failed: %v", err)
	}
	if countAfter != totalDocs {
		t.Errorf("Count after auto-split = %d, want %d (no document should be lost or double-counted across a live split)", countAfter, totalDocs)
	}

	found, err := e.Find(dbName, block, &Query{Limit: int(totalDocs) + 10})
	if err != nil {
		t.Fatalf("Find after auto-split failed: %v", err)
	}
	if len(found.Docs) != int(totalDocs) {
		t.Errorf("Find after auto-split returned %d docs, want %d", len(found.Docs), totalDocs)
	}
	seen := make(map[string]bool, len(found.Docs))
	for _, d := range found.Docs {
		if seen[d.ID] {
			t.Errorf("duplicate document ID %s returned after auto-split", d.ID)
		}
		seen[d.ID] = true
	}
}
