package caimandb

// update_where_tree_test.go covers UPDATE's migration to
// parseWhereClause/parse.Expr end to end: real Engine/Badger, going
// through the actual "UPDATE <block> WHERE ..." command string
// (parseAndExec / handleUpdate), not a hand-built Query or UpdateOp.
//
// It also regression-tests the parsing bug the migration fixed along
// the way: the original handleUpdate called parseFilters/
// parseWhereClause with the token index still pointing AT the "WHERE"
// token itself instead of just past it (unlike DELETE, which already
// advanced past WHERE correctly) -- nothing had ever exercised
// "UPDATE ... WHERE ..." through the real command string before, so
// this was never caught. TestUpdate_SimpleWhere below would have
// failed under the old code (parseFilters would have tried to read
// "WHERE" itself as a field name).
import (
	"testing"
)

func TestUpdate_SimpleWhere(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "updatewheredb", "users"
	sess := &Session{currentDB: dbName}

	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK "+dbName+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	if _, err := e.insertWithID(dbName, block, "u1", map[string]any{"name": "ana", "status": "pending"}); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := e.insertWithID(dbName, block, "u2", map[string]any{"name": "beto", "status": "pending"}); err != nil {
		t.Fatalf("insert u2: %v", err)
	}

	// This exact form -- WHERE immediately followed by a simple
	// equality -- is what the index-still-on-WHERE bug would have
	// broken: parseFilters would have read "WHERE" as the field name.
	out := parseAndExec(e, sess, `UPDATE `+block+` WHERE name = "ana" SET status = "active"`)
	if wantsError(out) {
		t.Fatalf("UPDATE ... WHERE (simple equality): %s", out)
	}

	u1, ok := e.FindByID(dbName, block, "u1")
	if !ok {
		t.Fatal("u1 not found after update")
	}
	if u1.Data["status"] != "active" {
		t.Fatalf("expected u1.status=active after UPDATE ... WHERE name=ana, got %v", u1.Data["status"])
	}
	u2, ok := e.FindByID(dbName, block, "u2")
	if !ok {
		t.Fatal("u2 not found after update")
	}
	if u2.Data["status"] != "pending" {
		t.Fatalf("expected u2.status to stay pending (WHERE shouldn't have matched it), got %v", u2.Data["status"])
	}
}

// TestUpdate_ParenAndOrWhere confirms UPDATE now understands real
// parenthesized/OR grouping in WHERE -- the actual point of the
// parseFilters -> parseWhereClause migration -- against a case that
// the old flat left-to-right parser would have evaluated incorrectly
// (AND/OR with no precedence or grouping at all).
func TestUpdate_ParenAndOrWhere(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "updatewheredb2", "users"
	sess := &Session{currentDB: dbName}

	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK "+dbName+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	// Only "vip" (age 17, vip=true) and "adult" (age 30) should match
	// status="pending" AND (age > 18 OR vip = true). "neither" (age
	// 17, vip=false) must NOT match: it needs the OR grouped correctly
	// with AND, not evaluated as a flat left-to-right chain.
	docs := map[string]map[string]any{
		"vip":          {"name": "vip", "status": "pending", "age": 17.0, "vip": true},
		"adult":        {"name": "adult", "status": "pending", "age": 30.0, "vip": false},
		"neither":      {"name": "neither", "status": "pending", "age": 17.0, "vip": false},
		"wrong_status": {"name": "wrong_status", "status": "done", "age": 30.0, "vip": true},
	}
	for id, data := range docs {
		if _, err := e.insertWithID(dbName, block, id, data); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	out := parseAndExec(e, sess, `UPDATE `+block+` WHERE status = "pending" AND ( age > 18 OR vip = true ) SET status = "matched"`)
	if wantsError(out) {
		t.Fatalf("UPDATE ... WHERE (parenthesized OR): %s", out)
	}

	for id, wantMatched := range map[string]bool{
		"vip": true, "adult": true, "neither": false, "wrong_status": false,
	} {
		doc, ok := e.FindByID(dbName, block, id)
		if !ok {
			t.Fatalf("%s not found after update", id)
		}
		gotMatched := doc.Data["status"] == "matched"
		if gotMatched != wantMatched {
			t.Errorf("%s: expected matched=%v, got status=%v", id, wantMatched, doc.Data["status"])
		}
	}
}
