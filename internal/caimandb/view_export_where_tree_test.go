package caimandb

// view_export_where_tree_test.go covers VIEW CREATE and EXPORT's
// migration to parseWhereClause/parse.Expr end to end: real
// Engine/Badger, through the actual command strings (parseAndExec),
// not hand-built Query structs.
import (
	"os"
	"strings"
	"testing"
)

// TestViewCreate_ParenAndOrWhere confirms a view's WHERE clause keeps
// real parenthesized/OR grouping when the view is later queried --
// the actual point of ViewDefinition gaining a Where *parse.Expr
// field alongside the legacy flat Filters.
func TestViewCreate_ParenAndOrWhere(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "viewwheredb", "items"
	sess := &Session{currentDB: dbName}

	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK "+dbName+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	// Same shape as the UPDATE paren/OR test: only "cheap_but_new" and
	// "old_but_pricey" should match category="tool" AND (price < 10 OR
	// age_days > 365). "neither" must NOT match -- it needs the OR
	// correctly grouped with the AND, not a flat left-to-right chain.
	docs := map[string]map[string]any{
		"cheap_but_new":  {"category": "tool", "price": 5.0, "age_days": 10.0},
		"old_but_pricey": {"category": "tool", "price": 50.0, "age_days": 400.0},
		"neither":        {"category": "tool", "price": 50.0, "age_days": 10.0},
		"wrong_category": {"category": "toy", "price": 5.0, "age_days": 400.0},
	}
	for id, data := range docs {
		if _, err := e.insertWithID(dbName, block, id, data); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	viewName := "cheap_or_old_tools"
	out := parseAndExec(e, sess, `VIEW CREATE `+viewName+` AS FIND `+block+` WHERE category = "tool" AND ( price < 10 OR age_days > 365 )`)
	if wantsError(out) {
		t.Fatalf("VIEW CREATE ... WHERE (parenthesized OR): %s", out)
	}

	result := parseAndExec(e, sess, "VIEW "+viewName)
	if wantsError(result) {
		t.Fatalf("querying the view: %s", result)
	}
	for id, wantMatch := range map[string]bool{
		"cheap_but_new": true, "old_but_pricey": true, "neither": false, "wrong_category": false,
	} {
		got := strings.Contains(result, `"`+id+`"`)
		if got != wantMatch {
			t.Errorf("%s: expected present in view result=%v, got %v (result: %s)", id, wantMatch, got, result)
		}
	}
}

// TestExport_ParenAndOrWhere confirms EXPORT ... WHERE respects real
// parenthesized/OR grouping. It also regression-tests the second bug
// found alongside this migration: the old guard was "if
// len(filters) > 0" before applying a WHERE filter to the export --
// but parseWhereClause's flat []Filter is empty/nil for any WHERE
// that isn't a pure AND-chain (e.g. one with an OR group), so a
// grouped WHERE like this test's would have silently been ignored,
// exporting the entire block instead of just the matches.
func TestExport_ParenAndOrWhere(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "exportwheredb", "items"
	sess := &Session{currentDB: dbName}

	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK "+dbName+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	docs := map[string]map[string]any{
		"cheap_but_new":  {"category": "tool", "price": 5.0, "age_days": 10.0},
		"old_but_pricey": {"category": "tool", "price": 50.0, "age_days": 400.0},
		"neither":        {"category": "tool", "price": 50.0, "age_days": 10.0},
		"wrong_category": {"category": "toy", "price": 5.0, "age_days": 400.0},
	}
	for id, data := range docs {
		if _, err := e.insertWithID(dbName, block, id, data); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	dest := "export_test_" + block
	out := parseAndExec(e, sess, `EXPORT `+block+` WHERE category = "tool" AND ( price < 10 OR age_days > 365 ) TO "`+dest+`"`)
	if wantsError(out) {
		t.Fatalf("EXPORT ... WHERE (parenthesized OR): %s", out)
	}

	jsonPath := e.dirMgr.BackupsPath() + "/" + dest + ".json"
	jsonBytes, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("reading exported JSON at %s: %v", jsonPath, err)
	}
	content := string(jsonBytes)
	for id, wantMatch := range map[string]bool{
		"cheap_but_new": true, "old_but_pricey": true, "neither": false, "wrong_category": false,
	} {
		got := strings.Contains(content, id)
		if got != wantMatch {
			t.Errorf("%s: expected present in export=%v, got %v", id, wantMatch, got)
		}
	}
}
