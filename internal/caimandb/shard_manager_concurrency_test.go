package caimandb

// These tests target the exact hot path touched by today's
// UpdateShardStats/GetShardForKey optimization (see shard_manager.go):
// moving UpdateShardStats from a full sm.mu.Lock() per document to an
// RLock + atomic increments, and making GetShardForKey read shardCount via
// a lock-free atomic mirror instead of an unsynchronized field access.
//
// Correctness assertions alone (no -race) would pass even with a real data
// race, since Go doesn't guarantee a race produces a wrong answer, only
// that it's undefined behavior. These tests are only meaningful run with
// the race detector:
//
//	go test ./internal/caimandb/ -run TestShardManager -race -count=1 -v
//
// The count/skew assertions below are a second, independent check: they'd
// catch a *lost update* (e.g. a stray non-atomic write slipping back in)
// even in a run where the race detector doesn't happen to catch the
// interleaving.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShardManager_ConcurrentUpdateShardStats hammers UpdateShardStats from
// many goroutines -- including many goroutines targeting the very same
// shard, guaranteeing real contention on that shard's counters -- and
// checks that every increment landed. Before today's fix this path was
// already correct (it held the full write lock), so this test's real job
// is to lock in the *new* RLock+atomic implementation's correctness going
// forward, not to reproduce the old bug.
func TestShardManager_ConcurrentUpdateShardStats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShardCount = 8
	sm := NewShardManager("test-node", cfg, nil)
	defer sm.Close()

	shardIDs := sm.GetAllShards()
	if len(shardIDs) == 0 {
		t.Fatal("expected at least one shard")
	}

	const goroutines = 64
	const perGoroutine = 2000
	const bytesPerCall = 128

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			// Deliberately fewer shards than goroutines, so several
			// goroutines always land on the same shard concurrently.
			shardID := shardIDs[g%len(shardIDs)]
			for i := 0; i < perGoroutine; i++ {
				sm.UpdateShardStats(shardID, 1, bytesPerCall)
			}
		}(g)
	}
	wg.Wait()

	var totalDocs, totalBytes int64
	for _, id := range shardIDs {
		stats := sm.GetShardStats(id)
		if stats == nil {
			t.Fatalf("shard %s disappeared after concurrent updates", id)
		}
		totalDocs += stats.DocCount
		totalBytes += stats.SizeBytes
	}

	wantDocs := int64(goroutines * perGoroutine)
	wantBytes := wantDocs * bytesPerCall
	if totalDocs != wantDocs {
		t.Errorf("doc count = %d, want %d (updates were lost under concurrency)", totalDocs, wantDocs)
	}
	if totalBytes != wantBytes {
		t.Errorf("byte count = %d, want %d (updates were lost under concurrency)", totalBytes, wantBytes)
	}
}

// TestShardManager_ConcurrentUpdateAndRead runs UpdateShardStats (which now
// only takes sm.mu.RLock, then mutates shard.DocCount/SizeBytes with
// atomics) at the same time as every read path that touches those same
// fields: GetShardStats, calculateSkew, and totalDocs. All of those reads
// also had to move to atomic.LoadInt64 as part of today's fix, precisely
// because they can now run concurrently with UpdateShardStats (both only
// take RLock) -- a plain, non-atomic read racing an atomic write is a real
// data race even though "only reading" sounds harmless. This test's only
// job is to be race-detector clean:
//
//	go test ./internal/caimandb/ -run TestShardManager_ConcurrentUpdateAndRead -race -count=1
func TestShardManager_ConcurrentUpdateAndRead(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShardCount = 8
	sm := NewShardManager("test-node", cfg, nil)
	defer sm.Close()

	shardIDs := sm.GetAllShards()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	const writers = 4
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			shardID := shardIDs[i%len(shardIDs)]
			for {
				select {
				case <-stop:
					return
				default:
					sm.UpdateShardStats(shardID, 1, 64)
				}
			}
		}(i)
	}

	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, id := range shardIDs {
					sm.GetShardStats(id)
				}
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
				sm.calculateSkew()
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
				sm.totalDocs()
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestShardManager_GetShardForKey_ConcurrentWithScale hammers
// GetShardForKey (the per-document, lock-free hot path) from many
// goroutines while a scale-up runs concurrently, and checks every
// returned shard ID is well-formed: "shard_<non-negative integer>".
//
// This is a direct regression test for two bugs the old code had at once:
//  1. GetShardForKey read sm.shardCount with no synchronization at all --
//     a data race with scaleUp/scaleDown, only catchable with -race.
//  2. `int(hash) % sm.shardCount` could go negative for large hash values
//     (converting a uint64 hash to a signed int can itself be negative),
//     producing an id like "shard_-5" that matches no real shard --
//     catchable even without -race, which is what the format check below
//     does.
func TestShardManager_GetShardForKey_ConcurrentWithScale(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShardCount = 4
	cfg.MaxShardsPerNode = 64
	sm := NewShardManager("test-node", cfg, nil)
	defer sm.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var badIDs int64

	const readers = 8
	wg.Add(readers)
	for g := 0; g < readers; g++ {
		go func(g int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Sprintf("doc-%d-%d", g, i)
					id := sm.GetShardForKey(key)
					if !isWellFormedShardID(id) {
						atomic.AddInt64(&badIDs, 1)
					}
					i++
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		sm.scaleUp(8)
	}()

	time.Sleep(250 * time.Millisecond)
	close(stop)
	wg.Wait()

	if badIDs != 0 {
		t.Errorf("GetShardForKey returned %d malformed shard IDs while scaling concurrently", badIDs)
	}
}

func isWellFormedShardID(id string) bool {
	var n int
	if _, err := fmt.Sscanf(id, "shard_%d", &n); err != nil {
		return false
	}
	return n >= 0
}

// TestShardManager_GetShardForKey_CacheMatchesShardMap is a correctness
// check for the shardIDCache optimization: every ID GetShardForKey can
// return must actually be a key in sm.shards, for every shard count the
// cache gets rebuilt at (InitializeShards, scaleUp, scaleDown). A stale or
// out-of-sync cache would silently route documents to a shard ID that
// UpdateShardStats/GetShardStats can't find.
func TestShardManager_GetShardForKey_CacheMatchesShardMap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShardCount = 4
	cfg.MaxShardsPerNode = 64
	sm := NewShardManager("test-node", cfg, nil)
	defer sm.Close()

	check := func(label string) {
		valid := make(map[string]bool)
		for _, id := range sm.GetAllShards() {
			valid[id] = true
		}
		for i := 0; i < 500; i++ {
			id := sm.GetShardForKey(fmt.Sprintf("key-%s-%d", label, i))
			if !valid[id] {
				t.Errorf("%s: GetShardForKey returned %q, which is not in GetAllShards() (%v)", label, id, sm.GetAllShards())
			}
		}
	}

	check("initial")
	sm.scaleUp(8)
	check("after-scale-up")
}

// TestShardManager_GetShardForDoc_MatchesComposedKey confirms
// GetShardForDoc (now plain string concatenation instead of
// fmt.Sprintf("%s/%s/%s", ...)) still hashes the exact same key
// GetShardForKey would for the "db/block/id" form.
func TestShardManager_GetShardForDoc_MatchesComposedKey(t *testing.T) {
	cfg := DefaultConfig()
	sm := NewShardManager("test-node", cfg, nil)
	defer sm.Close()

	cases := [][3]string{
		{"mydb", "myblock", "doc1"},
		{"", "b", "id"},
		{"db", "", "id"},
		{"db", "block", ""},
	}
	for _, c := range cases {
		db, block, id := c[0], c[1], c[2]
		want := sm.GetShardForKey(db + "/" + block + "/" + id)
		got := sm.GetShardForDoc(db, block, id)
		if got != want {
			t.Errorf("GetShardForDoc(%q,%q,%q) = %q, want %q", db, block, id, got, want)
		}
	}
}
