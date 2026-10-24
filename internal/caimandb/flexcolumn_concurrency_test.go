package caimandb

// These tests target today's FlexColumnEngine change: replacing a single
// engine-wide f.mu with 32 hash-striped shards (see flexShardCount in
// flexcolumn.go), each with its own lock, so concurrent writers to
// different blocks no longer serialize behind one mutex.
//
// As with the shard_manager tests, these are only meaningful under the
// race detector:
//
//	go test ./internal/caimandb/ -run TestFlexColumnEngine -race -count=1 -v
//
// The count assertions are a second, independent correctness check for
// lost updates, on top of whatever -race does or doesn't catch in a given
// run.

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestFlexColumnEngine_ConcurrentInsertAcrossBlocks is the scenario the
// sharding fix exists for: many goroutines inserting into many *different*
// blocks at once (the normal shape of a multi-block bulk load). Every
// block hashes to some shard's lock, and different blocks landing on
// different shards is exactly what lets them proceed in parallel instead
// of queuing behind one global mutex. This test checks that spreading the
// work across shards didn't lose or cross-contaminate any writes.
func TestFlexColumnEngine_ConcurrentInsertAcrossBlocks(t *testing.T) {
	f := NewFlexColumnEngine(nil, DefaultFlexColumnConfig())
	defer f.Stop()

	const dbName = "testdb_flex_across_blocks"
	const blocks = 6
	const goroutinesPerBlock = 8
	const perGoroutine = 60 // -> perBlock = 480

	var wg sync.WaitGroup
	for b := 0; b < blocks; b++ {
		block := fmt.Sprintf("block_%d", b)
		for g := 0; g < goroutinesPerBlock; g++ {
			wg.Add(1)
			go func(block string, g int) {
				defer wg.Done()
				for i := 0; i < perGoroutine; i++ {
					doc := &Document{
						ID:   fmt.Sprintf("%s-g%d-i%d", block, g, i),
						Data: map[string]any{"status": "active", "n": i},
					}
					f.InsertWithColumnar(dbName, block, doc)
				}
			}(block, g)
		}
	}
	wg.Wait()

	wantPerBlock := int64(goroutinesPerBlock * perGoroutine)
	for b := 0; b < blocks; b++ {
		block := fmt.Sprintf("block_%d", b)
		got := f.HotFields(dbName, block)["status"]
		if got != wantPerBlock {
			t.Errorf("block %s: field 'status' write count = %d, want %d", block, got, wantPerBlock)
		}
	}

	stats := f.Stats()
	wantTotal := int(int64(blocks) * wantPerBlock)
	if docs, _ := stats["indexed_documents"].(int); docs != wantTotal {
		t.Errorf("indexed_documents = %v, want %d", stats["indexed_documents"], wantTotal)
	}
}

// TestFlexColumnEngine_ConcurrentInsertSameBlock hammers a single (db,
// block) pair from many goroutines -- guaranteed to land on the same shard
// every time, so unlike the across-blocks test above, this one exercises
// the shard's own lock under real contention rather than the benefit of
// striping. It exists to confirm the refactor didn't weaken correctness
// for the case sharding *doesn't* help.
func TestFlexColumnEngine_ConcurrentInsertSameBlock(t *testing.T) {
	f := NewFlexColumnEngine(nil, DefaultFlexColumnConfig())
	defer f.Stop()

	const dbName = "testdb_flex_same_block"
	const block = "hot_block"
	const goroutines = 32
	const perGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				doc := &Document{
					ID:   fmt.Sprintf("g%d-i%d", g, i),
					Data: map[string]any{"kind": "x"},
				}
				f.InsertWithColumnar(dbName, block, doc)
			}
		}(g)
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if got := f.HotFields(dbName, block)["kind"]; got != want {
		t.Errorf("field 'kind' write count = %d, want %d", got, want)
	}
	stats := f.Stats()
	if docs, _ := stats["indexed_documents"].(int); docs != int(want) {
		t.Errorf("indexed_documents = %v, want %d", stats["indexed_documents"], want)
	}
}

// TestFlexColumnEngine_ConcurrentMixedOpsNoRace mixes Insert/Update/Delete
// across several blocks with concurrent HotFields/Stats/QueryWithColumnar
// reads -- the shape of a bulk load running alongside a dashboard or admin
// query. Each writer goroutine owns a private doc ID within its block, so
// its own sequence of insert/update/delete stays internally consistent;
// this test's job is purely to be race-detector clean and to never panic
// or deadlock, not to assert a specific final count (which a live mix of
// updates/deletes makes awkward to pin down).
//
// QueryWithColumnar is exercised here only via its early-exit path (a
// filter on a field that's never indexed), since the match path calls
// back into engine.FindByID, which needs a real *Engine/Badger instance
// this unit test deliberately doesn't set up -- that path is better
// covered by an integration test.
func TestFlexColumnEngine_ConcurrentMixedOpsNoRace(t *testing.T) {
	f := NewFlexColumnEngine(nil, DefaultFlexColumnConfig())
	defer f.Stop()

	const dbName = "testdb_flex_mixed"
	blocks := []string{"b0", "b1", "b2", "b3"}
	noMatchQuery := &Query{Filters: []Filter{{Field: "never_indexed", Op: "=", Value: "x"}}}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for _, block := range blocks {
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(block string, w int) {
				defer wg.Done()
				id := fmt.Sprintf("%s-w%d-doc", block, w)
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					doc := &Document{ID: id, Data: map[string]any{"v": i}}
					if i == 0 {
						f.InsertWithColumnar(dbName, block, doc)
					} else {
						old := &Document{ID: id, Data: map[string]any{"v": i - 1}}
						f.UpdateWithColumnar(dbName, block, old, doc)
					}
					i++
					if i%10 == 0 {
						f.DeleteWithColumnar(dbName, block, id)
						i = 0
					}
				}
			}(block, w)
		}
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, block := range blocks {
					f.HotFields(dbName, block)
					if _, err := f.QueryWithColumnar(dbName, block, noMatchQuery); err != nil {
						t.Errorf("QueryWithColumnar returned an error: %v", err)
					}
				}
				f.HotFields(dbName, "")
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.Stats()
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}
