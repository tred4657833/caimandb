package caimandb

import "testing"

// TestAbort_AfterStagedMutations_DoesNotCorruptLocks is a regression test
// for a bug where Abort() called releaseLocks(tx), which unconditionally
// called lockManager.Unlock() for every key in tx.Mutations -- even though
// locks are only ever acquired inside applyMutations() (i.e. during
// Commit(), never during staging). Since the shard lock manager wraps raw
// sync.RWMutex values, unlocking a mutex that was never locked doesn't
// panic recoverably -- it's an unrecoverable runtime fatal error ("sync:
// Unlock of unlocked RWMutex") that takes down the whole process. Any
// transaction that staged an Insert/Update/Delete and then called Abort()
// instead of Commit() hit this on every single run.
//
// If this regresses, the test process itself will die with that fatal
// error rather than reporting a normal test failure.
func TestAbort_AfterStagedMutations_DoesNotCorruptLocks(t *testing.T) {
	e := newConcurrencyTestEngine(t)

	if err := e.CreateDB("tx_abort_locks"); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock("tx_abort_locks", "b1"); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	if _, errs := e.InsertBatch("tx_abort_locks", "b1", []map[string]any{{"n": 1}, {"n": 2}}); errs != 0 {
		t.Fatalf("seed insert had %d errors", errs)
	}

	// Insert, then abort.
	tx1, err := e.txManager.Begin("tx_abort_locks", "b1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := e.txManager.Insert(tx1, map[string]any{"n": 3}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := e.txManager.Abort(tx1); err != nil {
		t.Fatalf("Abort after Insert: %v", err)
	}

	// Update, then abort.
	tx2, err := e.txManager.Begin("tx_abort_locks", "b1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := e.txManager.Update(tx2, nil, nil, UpdateOp{Set: map[string]any{"touched": true}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := e.txManager.Abort(tx2); err != nil {
		t.Fatalf("Abort after Update: %v", err)
	}

	// Delete, then abort.
	tx3, err := e.txManager.Begin("tx_abort_locks", "b1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := e.txManager.Delete(tx3, nil, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e.txManager.Abort(tx3); err != nil {
		t.Fatalf("Abort after Delete: %v", err)
	}

	// Nothing above should have been applied: all three transactions were
	// aborted, never committed.
	count, err := e.Count("tx_abort_locks", "b1", nil, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d after three aborted transactions, want 2 (seed data untouched)", count)
	}

	// The lock manager must still be in a healthy state: a real Commit
	// right after must succeed and actually persist.
	tx4, err := e.txManager.Begin("tx_abort_locks", "b1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := e.txManager.Insert(tx4, map[string]any{"n": 4}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := e.txManager.Commit(tx4); err != nil {
		t.Fatalf("Commit after prior aborts: %v", err)
	}
	count, err = e.Count("tx_abort_locks", "b1", nil, nil)
	if err != nil {
		t.Fatalf("Count after commit: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d after a real commit, want 3 -- lock manager left in a bad state by a prior Abort", count)
	}
}

// TestTransactionExternalStore_AbortDoesNotBreakRollback is a regression
// test for two related bugs in how transactions handled large-document
// blobs in external storage:
//
//   - Delete() used to call externalStore.Delete() eagerly during staging,
//     before the transaction was known to commit. An Abort() after that
//     point could not undo it: the document's blob was already gone even
//     though the document itself was still supposed to exist.
//   - Update() saved a new blob during staging but never cleaned up the
//     old one on commit, and never cleaned up the new one on abort --
//     either path leaked a blob in external storage forever.
//
// This uses a LargeDocThreshold of a few bytes so ordinary small test
// documents are routed through external storage without needing
// multi-megabyte payloads.
func TestTransactionExternalStore_AbortDoesNotBreakRollback(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataRoot = t.TempDir()
	cfg.AutoCluster = false
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.WALSyncPolicy = "off"
	cfg.LargeDocThreshold = 1 // force every document through externalStore
	e := NewEngine(cfg)
	t.Cleanup(e.Shutdown)

	if e.externalStore == nil {
		t.Fatal("externalStore was not initialized despite LargeDocThreshold > 0")
	}

	if err := e.CreateDB("tx_abort_ext"); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock("tx_abort_ext", "b1"); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	if _, errs := e.InsertBatch("tx_abort_ext", "b1", []map[string]any{{"tag": "seed"}}); errs != 0 {
		t.Fatalf("seed insert had errors")
	}

	seedDocs, err := e.Find("tx_abort_ext", "b1", &Query{Limit: 1})
	if err != nil || len(seedDocs.Docs) != 1 {
		t.Fatalf("Find seed doc: %v", err)
	}
	seedLargeID := seedDocs.Docs[0].LargeID
	if seedLargeID == "" {
		t.Fatal("seed document was not routed through externalStore")
	}

	// Delete it inside a transaction, then abort: the document and its
	// blob must both still be intact afterward.
	tx, err := e.txManager.Begin("tx_abort_ext", "b1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := e.txManager.Delete(tx, nil, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e.txManager.Abort(tx); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	if _, err := e.externalStore.Load(seedLargeID); err != nil {
		t.Fatalf("external blob %q was removed despite the deleting transaction being aborted: %v", seedLargeID, err)
	}

	count, err := e.Count("tx_abort_ext", "b1", nil, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d after aborted delete, want 1", count)
	}

	// Now actually commit a delete: the blob must be gone afterward (no
	// permanent leak).
	tx2, err := e.txManager.Begin("tx_abort_ext", "b1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := e.txManager.Delete(tx2, nil, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e.txManager.Commit(tx2); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := e.externalStore.Load(seedLargeID); err == nil {
		t.Fatalf("external blob %q still present after its deleting transaction committed -- leaked", seedLargeID)
	}
}
