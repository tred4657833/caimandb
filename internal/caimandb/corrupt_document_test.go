package caimandb

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// TestEngine_CorruptDocument_ExcludedNotFatal covers the "datos
// corruptos" row of the reliability matrix, at the layer above Badger's
// own physical-integrity checksums: what happens when a document's
// stored value is well-formed as far as Badger is concerned (it wrote
// and re-reads those exact bytes fine, no CRC mismatch) but isn't valid
// JSON -- e.g. a bug elsewhere wrote garbage, a document from an
// incompatible/older format, or a version of the document a different,
// broken component produced. Badger's own checksum only catches bit-rot
// *after* a good write; it can't and doesn't try to catch "the
// application asked me to store nonsense", which is exactly what this
// test simulates by writing garbage directly through a raw *badger.DB
// handle -- bypassing prepareInsertDoc's json.Marshal entirely, the same
// way a hypothetical bug in some other write path could.
//
// Before this test existed, ops_find.go's decode-failure paths (both the
// by-ID fetch in Find and the full scan in scanBlock) silently dropped
// any document that failed to json.Unmarshal, indistinguishable from "no
// such document" -- zero log line, zero metric, nothing. That's the gap
// this test's assertions (and the log.Warn calls added alongside it in
// ops_find.go) target: a corrupt document must not crash or fail the
// whole query (checked here), and must now at least be observable
// (added as a log.Warn with the doc ID/key and the decode error; not
// asserted directly in this test, since intercepting the package's
// global zap logger would mean mutating shared state other tests in
// this package also read through log(), which isn't worth the fragility
// for what's ultimately a secondary, non-functional assertion here).
func TestEngine_CorruptDocument_ExcludedNotFatal(t *testing.T) {
	e := newConcurrencyTestEngine(t)

	const dbName = "corrupt_doc_test"
	const block = "block1"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock failed: %v", err)
	}

	const docCount = 20
	docs := make([]map[string]any, docCount)
	for i := range docs {
		docs[i] = map[string]any{"idx": i, "tag": "corrupt_doc_test"}
	}
	inserted, errs := e.InsertBatch(dbName, block, docs)
	if errs != 0 {
		t.Fatalf("InsertBatch reported %d errors", errs)
	}
	if inserted != docCount {
		t.Fatalf("InsertBatch inserted %d documents, want %d", inserted, docCount)
	}

	countBefore, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("Count before corruption failed: %v", err)
	}
	if countBefore != docCount {
		t.Fatalf("Count before corruption = %d, want %d", countBefore, docCount)
	}

	// Find every document's ID first (via a normal, uncorrupted Find), so
	// the corrupted one is picked from real, valid data rather than
	// guessed at.
	before, err := e.Find(dbName, block, &Query{Limit: docCount + 10})
	if err != nil {
		t.Fatalf("Find before corruption failed: %v", err)
	}
	if len(before.Docs) != docCount {
		t.Fatalf("Find before corruption returned %d docs, want %d", len(before.Docs), docCount)
	}
	corruptID := before.Docs[0].ID

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		t.Fatalf("OpenBlock failed: %v", err)
	}
	// Overwrite the document's stored value directly with bytes that
	// aren't valid JSON (or valid zstd, in case CompressLargeDocs ever
	// defaults on for this config) -- Badger itself will accept and
	// checksum this write completely normally; the corruption is at the
	// application/encoding layer, not Badger's.
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(docKey(block, corruptID), []byte("this is not valid json { at all"))
	}); err != nil {
		t.Fatalf("failed to inject corrupted value: %v", err)
	}
	// The by-ID fetch path in Find checks the L1 cache before hitting
	// Badger -- evict this document's cached (valid) copy so the query
	// below is forced to actually read the corrupted bytes back, rather
	// than serving a stale cache hit and never exercising the code this
	// test is for.
	e.l1Cache.Del(e.cacheKey(dbName, block, corruptID))

	// --- full scan path (scanBlock) ---
	afterScan, err := e.Find(dbName, block, &Query{Limit: docCount + 10})
	if err != nil {
		t.Fatalf("Find after corruption returned an error (should exclude the bad doc, not fail the whole query): %v", err)
	}
	if len(afterScan.Docs) != docCount-1 {
		t.Errorf("Find after corruption returned %d docs, want %d (exactly the corrupted one excluded)", len(afterScan.Docs), docCount-1)
	}
	for _, d := range afterScan.Docs {
		if d.ID == corruptID {
			t.Errorf("corrupted document %s was returned by Find instead of being excluded", corruptID)
		}
	}

	// --- dedicated by-ID lookup path (FindByID) ---
	if doc, ok := e.FindByID(dbName, block, corruptID); ok || doc != nil {
		t.Errorf("FindByID for the corrupted document returned ok=%v doc=%+v, want ok=false, doc=nil", ok, doc)
	}

	// Count must not itself panic or error on a corrupted document either.
	if _, err := e.Count(dbName, block, nil, nil); err != nil {
		t.Errorf("Count after corruption failed: %v", err)
	}
}
