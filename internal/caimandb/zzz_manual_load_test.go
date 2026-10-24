package caimandb

// zzz_manual_load_test.go is a manual, opt-in mixed-workload
// concurrency test run against the exact production entrypoint
// (parseAndExec) under real goroutine concurrency, not a synthetic
// single-threaded loop. It reports real throughput/latency numbers on
// whatever hardware it's run on, and checks for goroutine leaks by
// comparing goroutine counts before and after a sustained run.
//
// Skipped unless CAIMANDB_MANUAL_BENCH=1 (same gate as
// TestManualImportBenchmark1M), since it's slow and not meant to run
// as part of the normal `go test ./...` suite.
import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestManualConcurrentCRUDLoad runs CAIMANDB_LOAD_WORKERS (default 50)
// goroutines for CAIMANDB_LOAD_DURATION (default 20s), each repeatedly
// picking one of FIND/INSERT/UPDATE/DELETE at random and issuing it
// through parseAndExec against a shared block that's pre-seeded with
// documents -- a realistic mixed read/write workload, not an
// insert-only benchmark. Reports ops/sec and p50/p95/p99 latency per
// operation type, and confirms the goroutine count returns to
// baseline after the load stops (a real, if short, leak check).
func TestManualConcurrentCRUDLoad(t *testing.T) {
	if os.Getenv("CAIMANDB_MANUAL_BENCH") != "1" {
		t.Skip("set CAIMANDB_MANUAL_BENCH=1 to run")
	}

	workers := 50
	if v := os.Getenv("CAIMANDB_LOAD_WORKERS"); v != "" {
		fmt.Sscanf(v, "%d", &workers)
	}
	duration := 20 * time.Second
	if v := os.Getenv("CAIMANDB_LOAD_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			duration = d
		}
	}

	cfg := DefaultConfig()
	cfg.DataRoot = t.TempDir()
	cfg.AutoCluster = false
	// ~3.9GB RAM total, DefaultConfig's cache sizes are for a real
	// server and OOM here on their own.
	cfg.CacheMB = 256
	cfg.L2CacheMB = 512
	// DefaultConfig's RateLimit (10,000 req/min, keyed per authUser --
	// see ratelimit.go/dsl_parser.go) is a per-client production safety
	// limit, not a measurement of the engine's own capacity, and every
	// worker below shares one bucket (authUser "admin"). Before the
	// singleIDEquality fast path (ops_local.go), UPDATE/DELETE's
	// whole-block-scan cost kept this workload's real throughput well
	// under 10,000/min on its own, so the limiter was never the
	// constraint being measured. It very much is now (a fixed engine
	// can exceed 10,000 ops in well under a second), so it's raised
	// here to keep this test measuring engine/lock/scan latency, the
	// thing docs/load-testing-findings.md is about -- not an unrelated
	// per-client throttle a real deployment would apply deliberately.
	cfg.RateLimit = 1 << 30

	eng := NewEngine(cfg)
	defer eng.Shutdown()
	sess := &Session{authUser: "admin", authRole: "admin", authDatabase: "loaddb"}

	if out := parseAndExec(eng, sess, "CREATE DB loaddb"); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(eng, sess, "USE loaddb"); wantsError(out) {
		t.Fatalf("USE: %s", out)
	}
	if out := parseAndExec(eng, sess, "CREATE BLOCK loaddb (items)"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}

	// Seed 5,000 documents so FIND/UPDATE/DELETE have something real
	// to work against from the start, not an empty block.
	const seedCount = 5000
	for i := 0; i < seedCount; i++ {
		if _, err := eng.insertWithID("loaddb", "items", fmt.Sprintf("seed_%d", i),
			map[string]any{"n": i, "status": "active"}); err != nil {
			t.Fatalf("seeding doc %d: %v", i, err)
		}
	}

	t.Logf("GOMAXPROCS=%d NumCPU=%d workers=%d duration=%s", runtime.GOMAXPROCS(0), runtime.NumCPU(), workers, duration)

	goroutinesBefore := runtime.NumGoroutine()

	type opStats struct {
		count   int64
		errs    int64
		latency []time.Duration
		mu      sync.Mutex
	}
	stats := map[string]*opStats{
		"find": {}, "insert": {}, "update": {}, "delete": {},
	}
	record := func(op string, d time.Duration, isErr bool) {
		s := stats[op]
		atomic.AddInt64(&s.count, 1)
		if isErr {
			atomic.AddInt64(&s.errs, 1)
		}
		s.mu.Lock()
		s.latency = append(s.latency, d)
		s.mu.Unlock()
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			workerSess := &Session{authUser: "admin", authRole: "admin", authDatabase: "loaddb", currentDB: "loaddb"}
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				id := fmt.Sprintf("seed_%d", rng.Intn(seedCount))
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5: // 60% reads
					start := time.Now()
					out := parseAndExec(eng, workerSess, "FIND items "+id)
					record("find", time.Since(start), wantsError(out))
				case 6, 7: // 20% inserts (new docs, unique per worker+i to avoid ID collisions)
					newID := fmt.Sprintf("load_w%d_%d", workerID, i)
					start := time.Now()
					out := parseAndExec(eng, workerSess, fmt.Sprintf(`INSERT items %s {"n": %d, "status": "new"}`, newID, i))
					record("insert", time.Since(start), wantsError(out))
				case 8: // 10% updates
					start := time.Now()
					out := parseAndExec(eng, workerSess, `UPDATE items WHERE id = "`+id+`" SET status = "touched"`)
					record("update", time.Since(start), wantsError(out))
				case 9: // 10% deletes (of worker-inserted docs, so seed data survives for the run's duration)
					delID := fmt.Sprintf("load_w%d_%d", workerID, i-1)
					start := time.Now()
					out := parseAndExec(eng, workerSess, `DELETE items WHERE id = "`+delID+`"`)
					record("delete", time.Since(start), wantsError(out))
				}
			}
		}(w)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// Give background goroutines (LineDat drain workers, WAL writers,
	// etc.) a moment to settle before checking for leaks.
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	goroutinesAfter := runtime.NumGoroutine()

	var totalOps int64
	for _, op := range []string{"find", "insert", "update", "delete"} {
		s := stats[op]
		totalOps += s.count
		if s.count == 0 {
			continue
		}
		s.mu.Lock()
		lat := append([]time.Duration(nil), s.latency...)
		s.mu.Unlock()
		p50, p95, p99 := percentiles(lat)
		t.Logf("%-6s: %7d ops, %5d errs, p50=%-10s p95=%-10s p99=%-10s",
			op, s.count, s.errs, p50, p95, p99)
	}
	t.Logf("total: %d ops over %s = %.0f ops/sec", totalOps, duration, float64(totalOps)/duration.Seconds())
	t.Logf("goroutines: %d before load, %d after (workers=%d already stopped and joined)",
		goroutinesBefore, goroutinesAfter, workers)

	// A generous margin (not an exact match) since some long-lived
	// background goroutines (metrics scraping, WAL flush timers) can
	// legitimately start during Engine startup independent of this
	// growth (e.g. one leaked goroutine per request), not exact
	// baseline parity.
	if goroutinesAfter > goroutinesBefore+20 {
		t.Errorf("possible goroutine leak: %d before load, %d after (workers=%d, all joined via wg.Wait())",
			goroutinesBefore, goroutinesAfter, workers)
	}
}

func percentiles(d []time.Duration) (p50, p95, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), d...)
	// sort.Slice (O(n log n)), not the previous insertion sort (O(n^2)):
	// this test's own "find" op count can reach the hundreds of
	// thousands in a few seconds once update/delete no longer serialize
	// behind a whole-block scan (see the singleIDEquality fast path in
	// ops_local.go / docs/load-testing-findings.md) -- at that size an
	// O(n^2) sort of the latency slice became slower than the load run
	// itself, timing out this test on exactly the improvement it exists
	// to measure.
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := func(p float64) time.Duration {
		i := int(p * float64(len(sorted)-1))
		return sorted[i]
	}
	return idx(0.50), idx(0.95), idx(0.99)
}
