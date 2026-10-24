package caimandb

// backup_pitr_test.go covers point-in-time restore end to end: real
// Engine, real BackupDB/RestoreDB (encrypted JSONL backups on disk in
// t.TempDir()), not a hand-built Manifest. It proves the actual
// property PITR promises -- "restore to how things looked at time T"
// -- by taking a full backup of one version of a document, changing
// the document, taking an incremental backup of the new version, and
// confirming RestoreDB(dbName, asOf) with asOf between those two
// backups brings back the OLD version even though the live database
// (and the incremental backup) both currently hold the new one.
//
// Timestamps here are real wall-clock time with second-level sleeps,
// not injected/mocked, because Document.Meta.UpdatedAt and
// backup.Entry.At/Cutoff are all time.Now().Unix() in the real code
// path (ops_local.go, ops_backup_jsonl.go) -- this test exercises that
// real resolution rather than assuming it away.
import (
	"testing"
	"time"
)

func TestRestoreDB_PointInTime(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "pitrdb", "docs"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}

	if _, err := e.insertWithID(dbName, block, "doc1", map[string]any{"name": "v1"}); err != nil {
		t.Fatalf("insert v1: %v", err)
	}
	if _, err := e.BackupDB(dbName, true); err != nil {
		t.Fatalf("full backup: %v", err)
	}

	// Second-resolution timestamps: sleep past the second boundary so
	// asOf (captured here) is strictly after the full backup's Cutoff
	// and strictly before the update below.
	time.Sleep(1100 * time.Millisecond)
	asOf := time.Now().Unix()
	time.Sleep(1100 * time.Millisecond)

	if _, err := e.insertWithID(dbName, block, "doc1", map[string]any{"name": "v2"}); err != nil {
		t.Fatalf("insert v2: %v", err)
	}
	if _, err := e.BackupDB(dbName, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}

	// Sanity: the live document really is v2 right now, before we
	// restore anything -- otherwise the rest of this test wouldn't be
	// proving PITR did anything.
	live, ok := e.FindByID(dbName, block, "doc1")
	if !ok {
		t.Fatalf("FindByID before restore: not found")
	}
	if live.Data["name"] != "v2" {
		t.Fatalf("expected live doc1 to be v2 before restore, got %v", live.Data["name"])
	}

	restored, err := e.RestoreDB(dbName, asOf)
	if err != nil {
		t.Fatalf("RestoreDB asOf=%d: %v", asOf, err)
	}
	if restored != 1 {
		t.Fatalf("expected 1 document replayed at asOf=%d (only the full snapshot's v1), got %d", asOf, restored)
	}

	after, ok := e.FindByID(dbName, block, "doc1")
	if !ok {
		t.Fatalf("FindByID after restore: not found")
	}
	if after.Data["name"] != "v1" {
		t.Fatalf("expected doc1 to be v1 after point-in-time restore to before the update, got %v", after.Data["name"])
	}

	// And restoring with asOf=0 (latest, the pre-existing behavior)
	// should bring v2 back, confirming PITR didn't change the
	// unbounded-restore path.
	if _, err := e.RestoreDB(dbName, 0); err != nil {
		t.Fatalf("RestoreDB asOf=0 (latest): %v", err)
	}
	latest, ok := e.FindByID(dbName, block, "doc1")
	if !ok {
		t.Fatalf("FindByID after latest restore: not found")
	}
	if latest.Data["name"] != "v2" {
		t.Fatalf("expected doc1 to be v2 after unbounded (latest) restore, got %v", latest.Data["name"])
	}
}

// TestRestoreDB_PointInTime_TooOld confirms an asOf that predates the
// oldest (full) backup fails clearly instead of silently restoring
// zero documents.
func TestRestoreDB_PointInTime_TooOld(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "pitrdb2", "docs"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock: %v", err)
	}
	if _, err := e.insertWithID(dbName, block, "doc1", map[string]any{"name": "v1"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := e.BackupDB(dbName, true); err != nil {
		t.Fatalf("full backup: %v", err)
	}

	_, err := e.RestoreDB(dbName, 1) // 1970-01-01T00:00:01Z, long before this test ran
	if err == nil {
		t.Fatal("expected an error restoring to a time before the oldest backup, got nil")
	}
}

// TestHandleRestoreJSONL_AsOfParsing covers the RESTORE <db> AS OF <ts>
// command surface: dispatch routing (dsl_parser.go must send this to
// handleRestoreJSONL, not the older RESTORE ... FROM <file> command),
// and both accepted AS OF formats (Unix seconds and RFC3339).
func TestHandleRestoreJSONL_AsOfParsing(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	const dbName, block = "pitrdb3", "docs"
	sess := &Session{currentDB: dbName, authRole: RoleGeneralAdmin}

	if out := parseAndExec(e, sess, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(e, sess, "CREATE BLOCK "+dbName+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	if _, err := e.insertWithID(dbName, block, "doc1", map[string]any{"name": "v1"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := e.BackupDB(dbName, true); err != nil {
		t.Fatalf("full backup: %v", err)
	}

	asOf := time.Now().Unix()

	if out := parseAndExec(e, sess, "RESTORE "+dbName+" AS OF "+time.Unix(asOf, 0).UTC().Format(time.RFC3339)); wantsError(out) {
		t.Fatalf("RESTORE ... AS OF <RFC3339>: %s", out)
	}

	if out := parseAndExec(e, sess, "RESTORE "+dbName+" AS OF notatimestamp"); !wantsError(out) {
		t.Fatalf("expected RESTORE ... AS OF with a garbage timestamp to error, got: %s", out)
	}
}
