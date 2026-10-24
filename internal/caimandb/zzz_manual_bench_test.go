package caimandb

// Manual, opt-in 1M-document IMPORT benchmark run against the exact
// production entrypoint (parseAndExec, the same function the HTTP admin
// and query endpoints call) instead of a synthetic reimplementation.
// Skipped unless CAIMANDB_MANUAL_BENCH=1 is set, since it's slow and
// needs an external NDJSON file, and isn't meant to run as part of the
// normal `go test ./...` suite.

import (
	"os"
	"runtime"
	"testing"
	"time"
)

func TestManualImportBenchmark1M(t *testing.T) {
	if os.Getenv("CAIMANDB_MANUAL_BENCH") != "1" {
		t.Skip("set CAIMANDB_MANUAL_BENCH=1 to run")
	}
	path := os.Getenv("CAIMANDB_BENCH_FILE")
	if path == "" {
		path = "/home/claude/bench/users_1000000.ndjson"
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("bench file not found: %v", err)
	}

	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataRoot = dataDir
	cfg.AutoCluster = false
	// This sandbox has ~3.9GB total RAM; DefaultConfig's CacheMB=2048 +
	// L2CacheMB=4096 (6GB combined) is sized for a real server and OOMs
	// here on its own, independent of anything under test. Scaled down
	// to fit this box -- not a code change, just matching the config to
	// the hardware, same as any ops team would when deploying to a
	// smaller instance.
	cfg.CacheMB = 256
	cfg.L2CacheMB = 512
	if os.Getenv("CAIMANDB_BENCH_NO_FLEXCOLUMN") == "1" {
		cfg.FlexColumn = DefaultFlexColumnConfig()
		cfg.FlexColumn.Enabled = false
	}

	eng := NewEngine(cfg)
	defer eng.Shutdown()

	sess := &Session{authUser: "admin", authRole: "admin", authDatabase: "benchdb"}

	t.Logf("GOMAXPROCS=%d NumCPU=%d", runtime.GOMAXPROCS(0), runtime.NumCPU())

	if out := parseAndExec(eng, sess, "CREATE DB benchdb"); !okOrExists(out) {
		t.Fatalf("CREATE DATABASE failed: %s", out)
	}
	if out := parseAndExec(eng, sess, "USE benchdb"); wantsError(out) {
		t.Fatalf("USE failed: %s", out)
	}

	cmd := "IMPORT users FROM FILE '" + path + "' FORMAT NDJSON BATCH 20000"
	t.Logf("running: %s", cmd)

	start := time.Now()
	out := parseAndExec(eng, sess, cmd)
	elapsed := time.Since(start)

	t.Logf("IMPORT result: %s", out)
	t.Logf("IMPORT wall time: %s", elapsed)
	if wantsError(out) {
		t.Fatalf("IMPORT reported an error: %s", out)
	}

	countOut := parseAndExec(eng, sess, "COUNT users")
	t.Logf("COUNT result: %s", countOut)

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	runtime.GC()
	runtime.ReadMemStats(&m2)
	t.Logf("heap alloc after GC: %.1f MB (before GC: %.1f MB)", float64(m2.HeapAlloc)/1e6, float64(m1.HeapAlloc)/1e6)
}

func okOrExists(out string) bool {
	if wantsError(out) {
		return len(out) >= 6 && (out[:6] == "ERROR:") && containsAny(out, []string{"already exists", "ya existe"})
	}
	return true
}

func wantsError(out string) bool {
	return len(out) >= 5 && out[:5] == "ERROR"
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if len(sub) <= len(s) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
