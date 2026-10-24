package caimandb

// TestEngine_RealCrashRecovery is the one test in this package that
// simulates an actual crash rather than an orderly Shutdown() (see
// restart_test.go's file comment, which flagged this exact gap when it
// was written) or a hand-truncated WAL segment (wal/wal_test.go, which
// is a real regression test for a real bug but simulates the *symptom*
// of a crash on a static file, not a live process actually dying
// mid-write).
//
// It works by building and running cmd/crashtestworker -- a tiny
// separate program whose only job is to insert documents forever and
// never call Shutdown -- as a genuine child OS process, then killing
// that process with Process.Kill() while it's actively writing. Kill()
// is an unconditional, immediate termination on every OS Go supports
// (SIGKILL-equivalent semantics on Windows too), which is what actually
// releases the Badger directory lock and the WAL's open file handle the
// way a real crash does -- something no amount of manipulating an
// *Engine value from inside this same test process could reproduce,
// since the OS resources stay held by this process regardless of what
// its own variables point at.
//
// This needs `go` on PATH to build the worker binary (same requirement
// `go test` itself already has) and takes noticeably longer than this
// package's other tests -- building a binary and running a real
// external process is inherently slower than in-process calls. Run it
// on its own when iterating:
//
//	go test ./internal/caimandb/ -run TestEngine_RealCrashRecovery -race -v
import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func buildCrashTestWorker(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed -- can't locate repo root to build cmd/crashtestworker")
	}
	// thisFile is .../<repoRoot>/internal/caimandb/crash_recovery_test.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	if _, err := os.Stat(filepath.Join(repoRoot, "go.mod")); err != nil {
		t.Fatalf("computed repo root %q doesn't contain go.mod (got there via runtime.Caller(0) on %q): %v", repoRoot, thisFile, err)
	}

	binName := "crashtestworker"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(t.TempDir(), binName)

	build := exec.Command("go", "build", "-o", binPath, "./cmd/crashtestworker")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building cmd/crashtestworker failed: %v\n%s", err, out)
	}
	return binPath
}

func TestEngine_RealCrashRecovery(t *testing.T) {
	binPath := buildCrashTestWorker(t)

	dataRoot := t.TempDir()
	const dbName = "crashtest"
	const block = "block1"
	const batchSize = 10

	cmd := exec.Command(binPath,
		"-dataroot", dataRoot,
		"-db", dbName,
		"-block", block,
		"-batch", strconv.Itoa(batchSize),
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe failed: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting crashtestworker failed: %v", err)
	}

	var ready atomic.Bool
	var lastProgress atomic.Int64
	scannerDone := make(chan struct{})

	go func() {
		defer close(scannerDone)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "READY":
				ready.Store(true)
			case strings.HasPrefix(line, "PROGRESS "):
				n, err := strconv.ParseInt(strings.TrimPrefix(line, "PROGRESS "), 10, 64)
				if err == nil {
					lastProgress.Store(n)
				}
			}
			// "DONE" is only ever reached if -count is hit, which the
			// worker's default is far too high for this test to reach
			// before killing it; no case needed for it here.
		}
	}()

	// Wait for the worker to finish engine startup + CreateDB/CreateBlock
	// (signaled by "READY") before expecting any inserts to happen.
	deadline := time.Now().Add(30 * time.Second)
	for !ready.Load() {
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("crashtestworker never printed READY within 30s; stderr:\n%s", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait until it has actually completed a handful of InsertBatch
	// calls -- killing it before any write has landed wouldn't be
	// exercising crash recovery, just empty-block startup.
	const minBatchesBeforeKill = 5
	deadline = time.Now().Add(30 * time.Second)
	for lastProgress.Load() < int64(minBatchesBeforeKill*batchSize) {
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("crashtestworker only reached progress=%d within 30s (wanted >= %d); stderr:\n%s",
				lastProgress.Load(), minBatchesBeforeKill*batchSize, stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The actual crash: an unconditional, immediate kill while the
	// worker is (almost certainly) still mid-loop. Exactly how far past
	// our last-observed lastProgress it got before dying is inherently
	// unknowable -- more PROGRESS lines may already be sitting unread in
	// assertions below drain the pipe fully before computing bounds,
	// line.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Process.Kill failed: %v", err)
	}

	// Per os/exec's documented contract for StdoutPipe: it's incorrect
	// to call Wait before all reads from the pipe have completed. The
	// worker is dead (or dying) after Kill(), so its stdout will reach
	// EOF on its own almost immediately -- this just waits for that
	// rather than racing it.
	<-scannerDone
	waitErr := cmd.Wait()
	if waitErr == nil {
		t.Fatalf("cmd.Wait() returned nil error after Process.Kill() -- the worker exited on its own instead of being killed, so this run didn't actually test a crash")
	}

	// This is the trustworthy upper bound: everything the worker's
	// stdout ever contained, now that the pipe has been fully drained to
	// EOF. Unlike the lastProgress value read before Kill() above, no
	// buffered-but-unread line can be hiding after this point.
	finalObservedProgress := lastProgress.Load()
	if finalObservedProgress < int64(minBatchesBeforeKill*batchSize) {
		t.Fatalf("finalObservedProgress=%d is somehow lower than the minimum we already waited for (%d) -- scanner goroutine bug",
			finalObservedProgress, minBatchesBeforeKill*batchSize)
	}

	// --- recovery: open a brand new Engine on the same, now-abandoned DataRoot ---
	cfg := DefaultConfig()
	cfg.DataRoot = dataRoot
	cfg.AutoCluster = false
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.LargeDocThreshold = 0
	cfg.WALSyncPolicy = "always" // match the worker's config
	// See the matching comment in newConcurrencyTestEngine
	// (ops_insert_concurrency_test.go).
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64

	e := NewEngine(cfg)
	defer e.Shutdown()

	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB after crash failed: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock after crash failed: %v", err)
	}

	recoveredCount, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("Count after crash recovery failed: %v", err)
	}

	// batchSize=10 is small enough that a single InsertBatch call
	// commits as one atomic wb.Flush() rather than being split into
	// multiple physical Badger commits (that chunking only kicks in for
	// batches much larger than flushChunkSize; see ops_insert.go) -- so
	// every batch either landed completely or not at all. If this ever
	// fails, it means a batch was torn: some but not all of its 10
	// documents survived the crash.
	if recoveredCount%batchSize != 0 {
		t.Errorf("recovered document count %d is not a multiple of batchSize=%d -- a batch was torn by the crash instead of being all-or-nothing",
			recoveredCount, batchSize)
	}

	// Progress prints strictly after InsertBatch returns having already
	// committed to Badger, so recoveredCount can exceed
	// finalObservedProgress by at most the one in-flight batch that may
	// have finished its Badger commit but been killed before reaching
	// its own fmt.Printf line -- never by more than that.
	maxPlausible := finalObservedProgress + batchSize
	if recoveredCount > maxPlausible {
		t.Errorf("recovered document count %d exceeds the maximum plausible %d (finalObservedProgress=%d + one in-flight batch) -- more data survived than the crash should have allowed",
			recoveredCount, maxPlausible, finalObservedProgress)
	}
	if recoveredCount == 0 {
		t.Errorf("recovered document count is 0, but the worker confirmed progress=%d before being killed -- durable writes were lost entirely", finalObservedProgress)
	}

	// The engine must be immediately usable after recovery, not just
	// report a count -- confirm a fresh write actually goes through.
	postRecoveryInsert, errs := e.InsertBatch(dbName, block, []map[string]any{{"tag": "post_recovery"}})
	if errs != 0 || postRecoveryInsert != 1 {
		t.Errorf("InsertBatch after crash recovery failed: inserted=%d errs=%d", postRecoveryInsert, errs)
	}

	t.Logf("crash recovery: worker confirmed progress=%d before being killed, recovered %d documents (+1 post-recovery insert)", finalObservedProgress, recoveredCount)
	if stderr.Len() > 0 {
		t.Logf("crashtestworker stderr:\n%s", stderr.String())
	}
}
