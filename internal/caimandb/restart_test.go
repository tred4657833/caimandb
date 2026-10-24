package caimandb

// TestEngine_GracefulRestart_DataSurvives covers the "restart" row of the
// reliability matrix: shut an Engine down cleanly and bring up a brand
// new *Engine instance pointed at the same on-disk DataRoot (as a real
// process restart would), then confirm the previously inserted documents
// are still there and still correctly counted.
//
// What this test does NOT cover: an *ungraceful* crash (kill -9, power
// loss) mid-operation. That needs the process to actually die with
// in-flight writes and no orderly Shutdown() -- which, from inside a
// single Go test binary, can't be faithfully simulated by just
// abandoning an *Engine value, because Shutdown() is what releases the
// Badger directory lock and flushes/closes the WAL's file handle; a
// second Engine opened against the same DataRoot while those handles are
// still live would fail to acquire Badger's lock (correctly -- that lock
// is exactly what stops two processes from touching one on-disk DB at
// once) rather than reproducing what a real crash leaves behind. Testing
// that scenario faithfully needs a subprocess harness (spawn a real
// child process, SIGKILL it mid-write, restart, inspect) rather than
// in-process test code -- a reasonable next test to add, but a
// materially bigger piece of work than this one. The WAL-level crash
// simulation in wal/wal_test.go (truncating/corrupting a segment file
// directly, then reopening) is what currently stands in for that at the
// layer where it's actually feasible to simulate honestly in-process.
//
// This test also documents a real behavioral assumption worth knowing:
// reopening a DataRoot from a fresh Engine does not implicitly
// rediscover existing databases/blocks in memory -- CreateDB/CreateBlock
// must be called again. That's safe/idempotent here (createDBLocal uses
// os.MkdirAll and overwrites only metadata, not document data; the
// equivalent block-level call in ops_block.go behaves the same way
// against the existing on-disk block layout) and mirrors what a real
// client already does on a fresh connection, but it's worth knowing
// explicitly rather than discovering it by surprise.
import (
	"testing"
)

func TestEngine_GracefulRestart_DataSurvives(t *testing.T) {
	dataRoot := t.TempDir()
	const dbName = "restart_test_db"
	const block = "restart_test_block"
	const docCount = 500

	// --- "before restart" ---
	cfg1 := DefaultConfig()
	cfg1.DataRoot = dataRoot
	cfg1.AutoCluster = false
	cfg1.AutoSplitEnabled = false
	cfg1.AutoMergeEnabled = false
	cfg1.WALSyncPolicy = "off" // Find/Count read from Badger, not the WAL -- InsertBatch already commits synchronously (see ops_insert.go), so this doesn't affect what this test checks.
	cfg1.LargeDocThreshold = 0
	// See the matching comment in newConcurrencyTestEngine
	// (ops_insert_concurrency_test.go).
	cfg1.CacheMB = 64
	cfg1.L2CacheMB = 64

	e1 := NewEngine(cfg1)

	if err := e1.CreateDB(dbName); err != nil {
		e1.Shutdown()
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := e1.CreateBlock(dbName, block); err != nil {
		e1.Shutdown()
		t.Fatalf("CreateBlock failed: %v", err)
	}

	docs := make([]map[string]any, docCount)
	for i := range docs {
		docs[i] = map[string]any{
			"idx":  i,
			"tag":  "restart",
			"note": "should survive a graceful restart",
		}
	}
	inserted, errs := e1.InsertBatch(dbName, block, docs)
	if errs != 0 {
		e1.Shutdown()
		t.Fatalf("InsertBatch reported %d errors", errs)
	}
	if inserted != int64(docCount) {
		e1.Shutdown()
		t.Fatalf("InsertBatch inserted %d documents, want %d", inserted, docCount)
	}

	countBefore, err := e1.Count(dbName, block, nil, nil)
	if err != nil {
		e1.Shutdown()
		t.Fatalf("Count before restart failed: %v", err)
	}
	if countBefore != int64(docCount) {
		e1.Shutdown()
		t.Fatalf("Count before restart = %d, want %d", countBefore, docCount)
	}

	// Orderly shutdown -- this is what releases Badger's directory lock
	// and the WAL's file handle so a second Engine can open the same
	// DataRoot below, and (per checkCleanliness's comment in wal.go) is
	// what writes the .clean marker recording that this shutdown was
	// graceful.
	e1.Shutdown()

	// --- "after restart": brand new Engine, same DataRoot ---
	cfg2 := DefaultConfig()
	cfg2.DataRoot = dataRoot
	cfg2.AutoCluster = false
	cfg2.AutoSplitEnabled = false
	cfg2.AutoMergeEnabled = false
	cfg2.WALSyncPolicy = "off"
	cfg2.LargeDocThreshold = 0
	cfg2.CacheMB = 64
	cfg2.L2CacheMB = 64

	e2 := NewEngine(cfg2)
	defer e2.Shutdown()

	// See the file-level comment: re-declaring is expected/idempotent
	// here, not a workaround for a bug.
	if err := e2.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB after restart failed: %v", err)
	}
	if err := e2.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock after restart failed: %v", err)
	}

	countAfter, err := e2.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("Count after restart failed: %v", err)
	}
	if countAfter != int64(docCount) {
		t.Errorf("Count after restart = %d, want %d (documents inserted before a graceful Shutdown must survive a restart against the same DataRoot)", countAfter, docCount)
	}

	res, err := e2.Find(dbName, block, &Query{Limit: docCount + 10})
	if err != nil {
		t.Fatalf("Find after restart failed: %v", err)
	}
	if len(res.Docs) != docCount {
		t.Errorf("Find after restart returned %d docs, want %d", len(res.Docs), docCount)
	}
}
