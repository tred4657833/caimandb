package caimandb

// These tests exercise the real, end-to-end InsertBatch path -- WAL
// append, Badger WriteBatch commit, ShardManager.UpdateShardStats,
// FlexColumnEngine.InsertWithColumnar, and the L1 cache -- against a real
// (temp-directory) Badger instance, instead of unit-testing any single
// piece in isolation like shard_manager_concurrency_test.go and
// flexcolumn_concurrency_test.go do. It's the closest a Go test in this
// package gets to a real bulk INSERT ... GENERATE ... WORKERS run.
//
// Run with -race; a plain `go test` only catches gross data loss, not the
// synchronization bugs these tests actually exist to guard against:
//
//	go test ./internal/caimandb/ -run TestEngine_InsertBatch -race -count=1 -v
//
// IMPORTANT on memory: each test here spins up a *real* Engine (real
// BadgerDB, mmap'd files, real WAL) rather than a lightweight in-memory
// fake, and the race detector's shadow-memory accounting multiplies real
// memory use several times over -- mmap-backed storage engines are a
// known worst case for it, since it has to shadow the whole mapped range
// regardless of how much of it is actually touched. Two of these engines
// back-to-back in one `go test` process (as -run TestEngine_InsertBatch
// runs both by default) can be enough to exhaust a constrained VM's
// commit limit on Windows -- that's a TSan allocation failure
// ("ThreadSanitizer failed to allocate ... error code: 1455"), not a data
// race and not a bug in this package. If you hit that:
//   - Run the two Engine-level tests as separate `go test` invocations
//     (each gets a fresh process, so TSan's accounting resets):
//     go test ./internal/caimandb/ -run TestEngine_InsertBatch_ConcurrentAcrossBlocks -race -v
//     go test ./internal/caimandb/ -run TestEngine_InsertBatch_ConcurrentSameBlock -race -v
//   - And/or increase the Windows page file (Settings -> System -> About
//     -> Advanced system settings -> Performance -> Advanced -> Virtual
//     memory) -- TSan wants a lot of address space and commit, especially
//     alongside a real IDE/browser already using RAM.
//
// Each test builds its own *Engine against t.TempDir() with the heavier,
// test-irrelevant machinery (autoscaling, external large-doc storage,
// WAL fsync) trimmed down -- see newConcurrencyTestEngine -- specifically
// to keep this real-engine variant's memory/time footprint as low as
// correctness-testing allows under -race.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// newConcurrencyTestEngine builds a standalone (no raft/cluster) Engine
// rooted at a fresh t.TempDir(), and registers its cleanup so callers
// don't have to remember to call Shutdown themselves.
//
// A few defaults are deliberately trimmed down from DefaultConfig() here,
// none of which affect what these tests are checking (InsertBatch
// concurrency correctness):
//   - AutoSplitEnabled/AutoMergeEnabled off, so a real (but here
//     irrelevant) auto-scale event can't fire mid-test and add several
//     seconds plus log noise -- shard_manager_concurrency_test.go already
//     covers scaling concurrency specifically.
//   - WALSyncPolicy "off" skips real fsync on every WAL append; these
//     tests write to a throwaway t.TempDir(), so durability across a
//     crash is not what's being tested.
//   - LargeDocThreshold 0 skips initializing the external large-document
//     store, which none of these documents are big enough to need.
//
// All three exist purely to reduce this test's real (and, under -race,
// multiplied) resource footprint -- see the memory note above.
func newConcurrencyTestEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataRoot = t.TempDir()
	// AutoCluster only matters if InitCluster() is called explicitly --
	// NewEngine itself never calls it -- but set it false anyway so this
	// test can never accidentally start binding raft/network ports if
	// that ever changes.
	cfg.AutoCluster = false
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.WALSyncPolicy = "off"
	cfg.LargeDocThreshold = 0
	// DefaultConfig's L1/L2 cache sizes (defaultCacheMB = 2048) are sized
	// for a production server, not for a test process that creates many
	// short-lived engines in the same run. Ristretto pre-allocates its
	// count-min sketch up front based on this size, so leaving it at the
	// production default here means every single test engine commits
	// memory for a 2GB cache regardless of how few documents it actually
	// touches -- across the dozens of engines this package's test suite
	// creates, that adds up to a lot of committed-but-unused virtual
	// memory, which is what was pushing Windows test runs into
	// ERROR_COMMIT_LIMIT ("VirtualAlloc ... fatal error: out of
	// memory") under a constrained pagefile. 64 is Config.Validate's own
	// enforced minimum -- plenty for what these tests actually cache.
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64
	e := NewEngine(cfg)
	t.Cleanup(e.Shutdown)
	return e
}

// TestEngine_InsertBatch_ConcurrentAcrossBlocks drives real InsertBatch
// calls from many goroutines into several different blocks at once -- the
// actual shape of a multi-block bulk load, and the scenario today's
// shard_manager/flexcolumn sharding fixes were made for. It checks three
// independent things: InsertBatch itself reports no errors, the reported
// insert counts add up, and (the strongest check) Count() -- which reads
// back through the real query path -- agrees with what was supposedly
// inserted. That last check is what would catch a subtler bug than a
// lost counter: a document that InsertBatch reported as successful but
// that didn't actually end up durably readable.
func TestEngine_InsertBatch_ConcurrentAcrossBlocks(t *testing.T) {
	e := newConcurrencyTestEngine(t)

	const dbName = "concurrency_across_blocks"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	const blocks = 4
	const goroutinesPerBlock = 4
	const batchesPerGoroutine = 5
	const docsPerBatch = 50

	blockNames := make([]string, blocks)
	for b := 0; b < blocks; b++ {
		blockNames[b] = fmt.Sprintf("block_%d", b)
		if err := e.CreateBlock(dbName, blockNames[b]); err != nil {
			t.Fatalf("CreateBlock(%s) failed: %v", blockNames[b], err)
		}
	}

	var wg sync.WaitGroup
	var totalInserted, totalErrors int64

	for _, block := range blockNames {
		for g := 0; g < goroutinesPerBlock; g++ {
			wg.Add(1)
			go func(block string, g int) {
				defer wg.Done()
				for batch := 0; batch < batchesPerGoroutine; batch++ {
					docs := make([]map[string]any, docsPerBatch)
					for i := range docs {
						docs[i] = map[string]any{
							"g":     g,
							"batch": batch,
							"i":     i,
							"tag":   "concurrency",
						}
					}
					ins, errs := e.InsertBatch(dbName, block, docs)
					atomic.AddInt64(&totalInserted, ins)
					atomic.AddInt64(&totalErrors, errs)
				}
			}(block, g)
		}
	}
	wg.Wait()

	wantTotal := int64(blocks * goroutinesPerBlock * batchesPerGoroutine * docsPerBatch)
	if totalErrors != 0 {
		t.Errorf("InsertBatch reported %d errors during concurrent inserts", totalErrors)
	}
	if totalInserted != wantTotal {
		t.Errorf("InsertBatch reported %d documents inserted, want %d", totalInserted, wantTotal)
	}

	var sumCounted int64
	for _, block := range blockNames {
		n, err := e.Count(dbName, block, nil, nil)
		if err != nil {
			t.Errorf("Count(%s) failed: %v", block, err)
			continue
		}
		sumCounted += n
	}
	if sumCounted != wantTotal {
		t.Errorf("Count() across blocks = %d, want %d (InsertBatch reported success but documents are missing)", sumCounted, wantTotal)
	}
}

// TestEngine_InsertBatch_ConcurrentSameBlock is the same shape of test as
// above, but every goroutine writes to the *same* block -- guaranteeing
// contention on that one block's lockManager entry and its one
// ShardManager/FlexColumnEngine shard, instead of spreading the work out.
// It exists to confirm today's changes didn't weaken correctness for the
// case sharding doesn't parallelize away.
func TestEngine_InsertBatch_ConcurrentSameBlock(t *testing.T) {
	e := newConcurrencyTestEngine(t)

	const dbName = "concurrency_same_block"
	const block = "hot_block"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock failed: %v", err)
	}

	const goroutines = 8
	const batchesPerGoroutine = 10
	const docsPerBatch = 25

	var wg sync.WaitGroup
	var totalInserted, totalErrors int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for batch := 0; batch < batchesPerGoroutine; batch++ {
				docs := make([]map[string]any, docsPerBatch)
				for i := range docs {
					docs[i] = map[string]any{"g": g, "batch": batch, "i": i}
				}
				ins, errs := e.InsertBatch(dbName, block, docs)
				atomic.AddInt64(&totalInserted, ins)
				atomic.AddInt64(&totalErrors, errs)
			}
		}(g)
	}
	wg.Wait()

	wantTotal := int64(goroutines * batchesPerGoroutine * docsPerBatch)
	if totalErrors != 0 {
		t.Errorf("InsertBatch reported %d errors during concurrent inserts", totalErrors)
	}
	if totalInserted != wantTotal {
		t.Errorf("InsertBatch reported %d documents inserted, want %d", totalInserted, wantTotal)
	}

	n, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if n != wantTotal {
		t.Errorf("Count() = %d, want %d (InsertBatch reported success but documents are missing)", n, wantTotal)
	}
}
