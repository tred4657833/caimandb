package caimandb

// Covers today's fix for the FlexColumn OOM finding documented in
// CHANGELOG.md ("FlexColumn's columnar index is unbounded, un-evicted,
// in-process RAM"): InsertWithColumnar/UpdateWithColumnar now skip
// indexing entirely while BULK MODE is active (see
// FlexColumnEngine.bulkModeActive), and Engine.ReindexFlexColumn is the
// explicit backfill step documented as the fix for it. This test checks
// both halves against a real Engine/Badger instance (not a synthetic
// FlexColumnEngine in isolation, like flexcolumn_concurrency_test.go
// does): that a batch inserted under BULK MODE ends up with an empty
// FLEX-COLUMN index, and that ReindexFlexColumn (and the FLEX REINDEX
// command wrapping it) fully repopulates it from what's actually on disk.
//
//	go test ./internal/caimandb/ -run TestFlexColumn_BulkMode -race -count=1 -v

import (
	"testing"
)

func TestFlexColumn_BulkModeSkipsIndexing_ReindexBackfills(t *testing.T) {
	e := newConcurrencyTestEngine(t)

	const dbName = "flex_bulk_reindex_db"
	const block = "users"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock failed: %v", err)
	}

	const n = 500
	docs := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		docs[i] = map[string]any{
			"id":     "u" + itoaFlex(i),
			"status": "active",
			"n":      i,
		}
	}

	// Simulate what IMPORT does: BULK MODE on for the duration of the
	// load, off again after.
	e.SetBulkMode(true)
	inserted, errs := e.InsertBatch(dbName, block, docs)
	e.SetBulkMode(false)

	if errs != 0 {
		t.Fatalf("InsertBatch reported %d errors", errs)
	}
	if inserted != n {
		t.Fatalf("InsertBatch inserted %d, want %d", inserted, n)
	}

	// Documents themselves must be durably readable -- BULK MODE only
	// gates the secondary FLEX-COLUMN index, never the actual write.
	count, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != n {
		t.Fatalf("Count = %d, want %d", count, n)
	}

	// The FLEX-COLUMN index must be empty: it was skipped for every
	// document inserted while bulk mode was on.
	stats := e.flexEngine.Stats()
	if got := stats["indexed_documents"]; got != 0 {
		t.Fatalf("indexed_documents after bulk-mode insert = %v, want 0", got)
	}

	// A columnar-index query must decline to accelerate (nil, nil) --
	// engine.Find falls back to a normal scan, which still returns
	// correct results, just not via this fast path.
	q := &Query{Filters: []Filter{{Field: "status", Op: "=", Value: "active"}}}
	res, err := e.flexEngine.QueryWithColumnar(dbName, block, q)
	if err != nil {
		t.Fatalf("QueryWithColumnar returned error: %v", err)
	}
	if res != nil {
		t.Fatalf("QueryWithColumnar returned a result before REINDEX; want nil (no index yet)")
	}

	// Now the explicit backfill step.
	reindexed, err := e.ReindexFlexColumn(dbName, block)
	if err != nil {
		t.Fatalf("ReindexFlexColumn failed: %v", err)
	}
	if reindexed != n {
		t.Fatalf("ReindexFlexColumn indexed %d documents, want %d", reindexed, n)
	}

	stats = e.flexEngine.Stats()
	if got := stats["indexed_documents"]; got != n {
		t.Fatalf("indexed_documents after REINDEX = %v, want %d", got, n)
	}

	// And the columnar fast path must now actually serve the query.
	res, err = e.flexEngine.QueryWithColumnar(dbName, block, q)
	if err != nil {
		t.Fatalf("QueryWithColumnar returned error after REINDEX: %v", err)
	}
	if res == nil {
		t.Fatalf("QueryWithColumnar returned nil after REINDEX; want a result")
	}
	if res.IndexUsed != "flex-column:status" {
		t.Fatalf("IndexUsed = %q, want flex-column:status", res.IndexUsed)
	}
	if int(res.Total) != n {
		t.Fatalf("QueryWithColumnar Total = %d, want %d", res.Total, n)
	}
}

// TestFlexColumn_ReindexCommand exercises the same fix through the actual
// NQL command surface (FLEX REINDEX <db> <block>), the way an operator
// running IMPORT would actually invoke it -- rather than only calling
// Engine.ReindexFlexColumn directly as the test above does.
func TestFlexColumn_ReindexCommand(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	sess := &Session{authUser: "admin", authRole: "admin"}

	const dbName = "flex_reindex_cmd_db"
	const block = "users"
	sess.authDatabase = dbName

	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB failed: %s", out)
	}
	if out := parseAndExec(e, sess, "USE "+dbName); wantsError(out) {
		t.Fatalf("USE failed: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK "+block); wantsError(out) {
		t.Fatalf("CREATE BLOCK failed: %s", out)
	}

	e.SetBulkMode(true)
	docs := make([]map[string]any, 200)
	for i := range docs {
		docs[i] = map[string]any{"id": "u" + itoaFlex(i), "status": "active"}
	}
	inserted, errs := e.InsertBatch(dbName, block, docs)
	e.SetBulkMode(false)
	if errs != 0 || inserted != int64(len(docs)) {
		t.Fatalf("InsertBatch: inserted=%d errs=%d", inserted, errs)
	}

	if got := e.flexEngine.Stats()["indexed_documents"]; got != 0 {
		t.Fatalf("indexed_documents before REINDEX = %v, want 0", got)
	}

	out := parseAndExec(e, sess, "FLEX REINDEX "+dbName+" "+block)
	if wantsError(out) {
		t.Fatalf("FLEX REINDEX failed: %s", out)
	}
	t.Logf("FLEX REINDEX result: %s", out)

	if got := e.flexEngine.Stats()["indexed_documents"]; got != len(docs) {
		t.Fatalf("indexed_documents after REINDEX = %v, want %d", got, len(docs))
	}
}

func itoaFlex(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
