package caimandb

// find_distinct_test.go covers FIND <block> DISTINCT <fields> and the
// no-field-list "whole row" form, against a real Engine/Badger instance
// (not a synthetic Query built by hand): tokens go through
// buildFindQuery exactly as parseAndExec's FIND handler would build
// them, so this also confirms DISTINCT parsing, not just
// postProcessDocs' dedup logic in isolation.
import (
	"testing"
)

func insertDistinctFixture(t *testing.T, e *Engine, dbName, block string) {
	t.Helper()
	docs := []map[string]any{
		{"city": "CDMX", "country": "MX", "name": "a"},
		{"city": "CDMX", "country": "MX", "name": "b"}, // dup city+country
		{"city": "Monterrey", "country": "MX", "name": "c"},
		{"city": "Bogota", "country": "CO", "name": "d"},
	}
	inserted, errs := e.InsertBatch(dbName, block, docs)
	if errs != 0 || int(inserted) != len(docs) {
		t.Fatalf("fixture insert failed: inserted=%d errs=%d", inserted, errs)
	}
}

// TestFind_DistinctFields confirms "FIND <block> DISTINCT city,country"
// collapses the two CDMX/MX rows into one, keeping the other two distinct
// combinations, going through buildFindQuery's real tokenizing/parsing.
func TestFind_DistinctFields(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "distinctdb", "places"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	insertDistinctFixture(t, e, dbName, block)

	tokens := []string{"FIND", block, "DISTINCT", "city,country"}
	q, _, err := buildFindQuery(tokens, 2)
	if err != nil {
		t.Fatalf("buildFindQuery: %v", err)
	}
	if len(q.Distinct) != 2 || q.DistinctAll {
		t.Fatalf("expected Distinct=[city country], DistinctAll=false; got Distinct=%v DistinctAll=%v", q.Distinct, q.DistinctAll)
	}

	res, err := e.Find(dbName, block, q)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 distinct (city,country) rows, got %d: %+v", len(res.Docs), res.Docs)
	}
	seen := map[string]bool{}
	for _, d := range res.Docs {
		key, _ := d.Data["city"].(string)
		if seen[key] {
			t.Errorf("city %q appeared more than once in DISTINCT result", key)
		}
		seen[key] = true
	}
}

// TestFind_DistinctAll confirms bare "FIND <block> DISTINCT" (no field
// list) dedupes on the whole projected row: with SELECT city, two rows
// that only differ in "name" (not selected) should collapse to one.
func TestFind_DistinctAll(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "distinctdb2", "places"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	insertDistinctFixture(t, e, dbName, block)

	tokens := []string{"FIND", block, "SELECT", "city", "DISTINCT"}
	q, _, err := buildFindQuery(tokens, 2)
	if err != nil {
		t.Fatalf("buildFindQuery: %v", err)
	}
	if !q.DistinctAll || len(q.Distinct) != 0 {
		t.Fatalf("expected DistinctAll=true, Distinct=nil; got Distinct=%v DistinctAll=%v", q.Distinct, q.DistinctAll)
	}
	if len(q.Select) != 1 || q.Select[0] != "city" {
		t.Fatalf("expected Select=[city], got %v", q.Select)
	}

	res, err := e.Find(dbName, block, q)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 distinct projected rows (CDMX, Monterrey, Bogota), got %d: %+v", len(res.Docs), res.Docs)
	}
}

// TestFind_DistinctByID_NotConfusedWithDocumentID confirms handleFind's
// FindByID shortcut ("FIND <block> <id>") doesn't misfire on the literal
// token "DISTINCT" -- i.e. handleFind must treat DISTINCT as a query
// modifier keyword, the same way it already does for WHERE/SELECT/GROUP.
func TestFind_DistinctByID_NotConfusedWithDocumentID(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "distinctdb3", "places"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	insertDistinctFixture(t, e, dbName, block)

	sess := &Session{currentDB: dbName}
	out := parseAndExec(e, sess, "FIND "+block+" DISTINCT city")
	if len(out) >= 5 && out[:5] == "ERROR" {
		t.Fatalf("FIND ... DISTINCT was misread as FindByID(id=%q): %s", "DISTINCT", out)
	}
}
