package caimandb

import "testing"

// TestCreateBlock_ParenList covers CREATE BLOCK (a, b, c): every listed
// block must be created in the current database.
func TestCreateBlock_ParenList(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	sess := &Session{authUser: "admin", authRole: "admin"}

	const dbName = "create_block_list_db"
	sess.authDatabase = dbName
	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB failed: %s", out)
	}
	if out := parseAndExec(e, sess, "USE "+dbName); wantsError(out) {
		t.Fatalf("USE failed: %s", out)
	}

	out := parseAndExec(e, sess, "CREATE BLOCK (producto, cliente, estante)")
	if wantsError(out) {
		t.Fatalf("CREATE BLOCK (list) failed: %s", out)
	}

	blocks, err := e.ShowBlocks(dbName)
	if err != nil {
		t.Fatalf("ShowBlocks failed: %v", err)
	}
	want := map[string]bool{"producto": false, "cliente": false, "estante": false}
	for _, b := range blocks {
		if _, ok := want[b]; ok {
			want[b] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("expected block %q to have been created, blocks=%v", name, blocks)
		}
	}
}

// TestCreateBlock_ParenList_ExplicitDB covers CREATE BLOCK <db> (a, b).
func TestCreateBlock_ParenList_ExplicitDB(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	sess := &Session{authUser: "admin", authRole: "admin"}

	const dbName = "create_block_list_explicit_db"
	sess.authDatabase = dbName
	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB failed: %s", out)
	}

	out := parseAndExec(e, sess, "CREATE BLOCK "+dbName+" (alpha, beta)")
	if wantsError(out) {
		t.Fatalf("CREATE BLOCK <db> (list) failed: %s", out)
	}

	blocks, err := e.ShowBlocks(dbName)
	if err != nil {
		t.Fatalf("ShowBlocks failed: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %v", blocks)
	}
}

// TestRenameBlock_ParenList covers the parallel
// RENAME BLOCK (a, b) TO (a2, b2) form.
func TestRenameBlock_ParenList(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	sess := &Session{authUser: "admin", authRole: "admin"}

	const dbName = "rename_block_list_db"
	sess.authDatabase = dbName
	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB failed: %s", out)
	}
	if out := parseAndExec(e, sess, "USE "+dbName); wantsError(out) {
		t.Fatalf("USE failed: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK (producto, cliente)"); wantsError(out) {
		t.Fatalf("CREATE BLOCK failed: %s", out)
	}

	out := parseAndExec(e, sess, "RENAME BLOCK (producto, cliente) TO (productos, clientes)")
	if wantsError(out) {
		t.Fatalf("RENAME BLOCK (list) TO (list) failed: %s", out)
	}

	blocks, err := e.ShowBlocks(dbName)
	if err != nil {
		t.Fatalf("ShowBlocks failed: %v", err)
	}
	found := map[string]bool{}
	for _, b := range blocks {
		found[b] = true
	}
	if !found["productos"] || !found["clientes"] {
		t.Fatalf("expected renamed blocks productos/clientes, got %v", blocks)
	}
	if found["producto"] || found["cliente"] {
		t.Fatalf("old block names should no longer exist, got %v", blocks)
	}
}

// TestRenameBlock_ParenList_MismatchedLengths covers the error path
// where the old/new lists don't line up.
func TestRenameBlock_ParenList_MismatchedLengths(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	sess := &Session{authUser: "admin", authRole: "admin"}

	const dbName = "rename_block_list_mismatch_db"
	sess.authDatabase = dbName
	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB failed: %s", out)
	}
	if out := parseAndExec(e, sess, "USE "+dbName); wantsError(out) {
		t.Fatalf("USE failed: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK (a, b)"); wantsError(out) {
		t.Fatalf("CREATE BLOCK failed: %s", out)
	}

	out := parseAndExec(e, sess, "RENAME BLOCK (a, b) TO (a2)")
	if !wantsError(out) {
		t.Fatalf("expected an error for mismatched list lengths, got: %s", out)
	}
}

// TestRenameField covers RENAME FIELD <block>.<field> TO <new_field>:
// every document in the block should have the field renamed, with its
// value preserved and the secondary index updated to match.
func TestRenameField(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	sess := &Session{authUser: "admin", authRole: "admin"}

	const dbName = "rename_field_db"
	const block = "producto"
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

	if _, err := e.Insert(dbName, block, map[string]any{"id": "p1", "nombre": "Silla"}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if _, err := e.Insert(dbName, block, map[string]any{"id": "p2", "nombre": "Mesa"}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	out := parseAndExec(e, sess, "RENAME FIELD "+block+".nombre TO nombre_producto")
	if wantsError(out) {
		t.Fatalf("RENAME FIELD failed: %s", out)
	}

	result, err := e.Find(dbName, block, &Query{Filters: []Filter{{Field: "nombre_producto", Op: "=", Value: "Silla"}}})
	if err != nil {
		t.Fatalf("Find by new field failed: %v", err)
	}
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 document matching renamed field, got %d", len(result.Docs))
	}
	if _, has := result.Docs[0].Data["nombre"]; has {
		t.Fatalf("old field name should be gone, doc: %#v", result.Docs[0].Data)
	}

	byOld, err := e.Find(dbName, block, &Query{Filters: []Filter{{Field: "nombre", Op: "=", Value: "Mesa"}}})
	if err != nil {
		t.Fatalf("Find by old field failed: %v", err)
	}
	if len(byOld.Docs) != 0 {
		t.Fatalf("expected 0 documents still matching the old field name, got %d", len(byOld.Docs))
	}
}
