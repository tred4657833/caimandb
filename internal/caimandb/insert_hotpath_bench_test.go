package caimandb

// Benchmarks for the two hot-path changes made today (see
// shard_manager.go's UpdateShardStats/GetShardForKey and flexcolumn.go's
// shard striping). These measure the *current* code only -- there's no
// "before" binary to compare against in this checkout. To see the actual
// before/after delta:
//
//  1. Run these now and save the output:
//     go test ./internal/caimandb/ -run=^$ -bench . -benchtime=2s -cpu=1,2,4,8 ./internal/caimandb/ > new.txt
//  2. git stash (or checkout the commit before today's changes)
//  3. Run the same command again into old.txt
//  4. git stash pop (restore today's changes)
//  5. Compare: go run golang.org/x/perf/cmd/benchstat@latest old.txt new.txt
//
// The -cpu=1,2,4,8 sweep matters more than any single number: a global
// lock shows up as throughput that stops scaling (or gets worse) past
// -cpu=1, while a properly sharded/atomic path should keep scaling up
// until real per-key contention takes over.

import (
	"fmt"
	"testing"
)

// BenchmarkShardManager_UpdateShardStats measures the exact call that used
// to take sm.mu.Lock() (full write lock) once per document; it's now
// sm.mu.RLock() + atomic increments (see UpdateShardStats's doc comment).
func BenchmarkShardManager_UpdateShardStats(b *testing.B) {
	cfg := DefaultConfig()
	cfg.ShardCount = 8
	sm := NewShardManager("bench-node", cfg, nil)
	defer sm.Close()
	shardIDs := sm.GetAllShards()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			shardID := shardIDs[i%len(shardIDs)]
			sm.UpdateShardStats(shardID, 1, 128)
			i++
		}
	})
}

// BenchmarkShardManager_GetShardForKey measures the other fix: GetShardForKey
// went from an unsynchronized field read (fast but racy) to a lock-free
// atomic load (fast and correct). A benchmark run on real hardware found
// this call costing ~2.2us -- over 20x UpdateShardStats' cost -- almost
// entirely from fmt.Sprintf("shard_%d", ...) building a string every call
// for an output that only has shardCount possible values. It now reads a
// precomputed []string (shardIDCache) instead, so this benchmark should
// look dramatically faster than that first run.
func BenchmarkShardManager_GetShardForKey(b *testing.B) {
	cfg := DefaultConfig()
	sm := NewShardManager("bench-node", cfg, nil)
	defer sm.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.GetShardForKey(fmt.Sprintf("doc-%d", i))
			i++
		}
	})
}

// BenchmarkShardManager_GetShardForDoc measures the *actual* per-document
// hot path (InsertBatch calls GetShardForDoc, not GetShardForKey
// directly): it builds a db/block/id composite key and then does
// everything GetShardForKey does. It used to add a second fmt.Sprintf on
// top of GetShardForKey's own; that's now plain string concatenation.
func BenchmarkShardManager_GetShardForDoc(b *testing.B) {
	cfg := DefaultConfig()
	sm := NewShardManager("bench-node", cfg, nil)
	defer sm.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.GetShardForDoc("bench_db", "bench_block", fmt.Sprintf("doc-%d", i))
			i++
		}
	})
}

// BenchmarkFlexColumnEngine_InsertSingleBlock is the contention floor:
// every parallel goroutine writes to the SAME block, so they all land on
// the same shard lock regardless of sharding -- this number should be
// close to what the old single-global-mutex code would have measured.
func BenchmarkFlexColumnEngine_InsertSingleBlock(b *testing.B) {
	f := NewFlexColumnEngine(nil, DefaultFlexColumnConfig())
	defer f.Stop()
	const dbName, block = "bench_db", "bench_block"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			doc := &Document{
				ID:   fmt.Sprintf("single-%d-%d", b.N, i),
				Data: map[string]any{"status": "active", "n": i},
			}
			f.InsertWithColumnar(dbName, block, doc)
			i++
		}
	})
}

// BenchmarkFlexColumnEngine_InsertMultiBlock is the same workload spread
// across 32 different blocks (matching flexShardCount in flexcolumn.go),
// so parallel goroutines mostly land on different shards' locks instead of
// queuing behind one. The gap between this result and
// BenchmarkFlexColumnEngine_InsertSingleBlock's, at a given -cpu, is the
// measured benefit of today's sharding change; run with -cpu set to
// (or near) your actual core count to see it.
func BenchmarkFlexColumnEngine_InsertMultiBlock(b *testing.B) {
	f := NewFlexColumnEngine(nil, DefaultFlexColumnConfig())
	defer f.Stop()
	const dbName = "bench_db"
	const numBlocks = 32

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			block := fmt.Sprintf("bench_block_%d", i%numBlocks)
			doc := &Document{
				ID:   fmt.Sprintf("multi-%d-%d", b.N, i),
				Data: map[string]any{"status": "active", "n": i},
			}
			f.InsertWithColumnar(dbName, block, doc)
			i++
		}
	})
}
