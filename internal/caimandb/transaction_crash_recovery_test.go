package caimandb

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestTransactionCrashRecovery_UpdateDelete kills a real child process after
// the transaction's Badger WriteBatch has flushed but before the transaction
// commit marker is written to the WAL. This covers the two mutation paths that
// a generic insert-only crash test does not exercise: UPDATE and DELETE.
//
// The important invariant is that the durable document state is internally
// consistent after restart: an update is either fully visible or not visible,
// and a delete is either fully applied or not applied. The test deliberately
// kills the process at a deterministic point rather than relying on timing.
func TestTransactionCrashRecovery_UpdateDelete(t *testing.T) {
	bin := buildTxCrashTestWorker(t)
	for _, mode := range []string{"update", "delete"} {
		t.Run(mode, func(t *testing.T) {
			runTransactionCrashScenario(t, bin, mode)
		})
	}
}

func buildTxCrashTestWorker(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	bin := filepath.Join(t.TempDir(), "txcrashtestworker")
	// On Windows, go build produces an .exe even when -o is given
	// without an extension. Keep the path we execute consistent with
	// the actual binary produced by the Go toolchain.
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.Command("go", "build", "-o", bin, "./cmd/txcrashtestworker")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building txcrashtestworker failed: %v\n%s", err, out)
	}
	return bin
}

func runTransactionCrashScenario(t *testing.T, bin, mode string) {
	const docCount = 100
	dataRoot := t.TempDir()

	cmd := exec.Command(bin, "-dataroot", dataRoot, "-mode", mode, "-count", strconv.Itoa(docCount))
	cmd.Env = append(os.Environ(), "CAIMANDB_TX_CRASH_POINT=tx_after_flush")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}

	seen := make(chan string, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			line := s.Text()
			select {
			case seen <- line:
			default:
			}
		}
	}()

	deadline := time.Now().Add(60 * time.Second)
	chaosReached := false
	for time.Now().Before(deadline) {
		select {
		case line := <-seen:
			if line == "CHAOS_POINT tx_after_flush" {
				chaosReached = true
			}
		default:
		}
		if chaosReached {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !chaosReached {
		_ = cmd.Process.Kill()
		<-done
		_ = cmd.Wait()
		t.Fatalf("worker never reached tx_after_flush; stderr:\n%s", stderr.String())
	}

	if err := cmd.Process.Kill(); err == nil {
		// expected: the worker is intentionally parked at the crash point.
	} else {
		t.Fatalf("kill worker: %v", err)
	}
	<-done
	if err := cmd.Wait(); err == nil {
		t.Fatal("worker exited cleanly; crash path was not exercised")
	}

	cfg := DefaultConfig()
	cfg.DataRoot = dataRoot
	cfg.AutoCluster = false
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.LargeDocThreshold = 0
	cfg.WALSyncPolicy = "always"
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64

	e := NewEngine(cfg)
	defer e.Shutdown()
	if err := e.CreateDB("txcrashtest"); err != nil {
		t.Fatalf("CreateDB after crash: %v", err)
	}
	if err := e.CreateBlock("txcrashtest", "block1"); err != nil {
		t.Fatalf("CreateBlock after crash: %v", err)
	}

	count, err := e.Count("txcrashtest", "block1", nil, nil)
	if err != nil {
		t.Fatalf("Count after crash: %v", err)
	}

	if mode == "update" {
		if count != docCount {
			t.Fatalf("update crash recovered count=%d, want %d", count, docCount)
		}
		res, err := e.Find("txcrashtest", "block1", &Query{Filters: []Filter{{Field: "tag", Op: "=", Value: "tx-chaos"}}, Limit: docCount + 1})
		if err != nil {
			t.Fatalf("Find after update crash: %v", err)
		}
		if len(res.Docs) != docCount {
			t.Fatalf("update crash recovered %d docs, want %d", len(res.Docs), docCount)
		}
		for _, doc := range res.Docs {
			if got := doc.Data["state"]; got != "after" {
				t.Errorf("document %s has state=%v, want after", doc.ID, got)
			}
		}
	} else {
		if count != 0 {
			t.Fatalf("delete crash recovered count=%d, want 0", count)
		}
	}

	// Recovery must leave the engine writable, not merely readable.
	inserted, errs := e.InsertBatch("txcrashtest", "block1", []map[string]any{{"tag": "post-crash", "mode": mode}})
	if inserted != 1 || errs != 0 {
		t.Fatalf("post-%s-crash write failed: inserted=%d errs=%d", mode, inserted, errs)
	}
}
