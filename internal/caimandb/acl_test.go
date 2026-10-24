package caimandb

// acl_test.go covers the granular ACL system (acl.go, cmd_grant.go)
// end to end: real Engine/Badger, real users created via
// CreateUserWithOpts, real command strings through parseAndExec, not
// hand-built Grant structs or direct authorizeBlockAccess calls.
//
// Scenario: "bob" is a USER scoped to db_alpha by the role hierarchy
// alone -- he can't touch db_beta at all under the fixed role system.
// A GRANT extends him read (and later write) access to one specific
// block in db_beta, without changing his role or touching db_alpha's
// isolation from anyone else.
import (
	"testing"
)

func mustCreateUserForACLTest(t *testing.T, e *Engine, username, role, database string) {
	t.Helper()
	if err := e.CreateUserWithOpts(username, "password123", CreateUserOpts{Role: role, Database: database}); err != nil {
		t.Fatalf("creating user %s: %v", username, err)
	}
}

func TestGrant_ExtendsAccessBeyondRole(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	admin := &Session{authUser: "root", authRole: RoleGeneralAdmin}

	const dbAlpha, dbBeta, block = "db_alpha", "db_beta", "secrets"
	if out := parseAndExec(e, admin, "CREATE DB "+dbAlpha); wantsError(out) {
		t.Fatalf("CREATE DB alpha: %s", out)
	}
	if out := parseAndExec(e, admin, "CREATE DB "+dbBeta); wantsError(out) {
		t.Fatalf("CREATE DB beta: %s", out)
	}
	if out := parseAndExec(e, admin, "USE "+dbBeta); wantsError(out) {
		t.Fatalf("USE beta (as admin): %s", out)
	}
	if out := parseAndExec(e, admin, "CREATE BLOCK "+dbBeta+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	if _, err := e.insertWithID(dbBeta, block, "doc1", map[string]any{"name": "classified"}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	mustCreateUserForACLTest(t, e, "bob", RoleUser, dbAlpha)
	bob := &Session{authUser: "bob", authRole: RoleUser, authDatabase: dbAlpha}

	// Before any grant: bob's role only covers db_alpha, so he can't
	// even USE into db_beta.
	if out := parseAndExec(e, bob, "USE "+dbBeta); !wantsError(out) {
		t.Fatalf("expected bob to be denied USE %s before any grant, got: %s", dbBeta, out)
	}

	// A non-admin (bob) can't GRANT anything, including to himself.
	if out := parseAndExec(e, bob, "GRANT read ON "+dbBeta+"."+block+" TO bob"); !wantsError(out) {
		t.Fatalf("expected a non-admin GRANT to be denied, got: %s", out)
	}

	// Admin grants bob read on db_beta.secrets specifically.
	if out := parseAndExec(e, admin, "GRANT read ON "+dbBeta+"."+block+" TO bob"); wantsError(out) {
		t.Fatalf("GRANT (as admin): %s", out)
	}

	// Now bob can USE into db_beta (the grant alone is enough)...
	if out := parseAndExec(e, bob, "USE "+dbBeta); wantsError(out) {
		t.Fatalf("expected bob to be admitted into %s after the grant: %s", dbBeta, out)
	}
	// ...and can read the granted block...
	findOut := parseAndExec(e, bob, "FIND "+block)
	if wantsError(findOut) {
		t.Fatalf("expected bob to read %s.%s after a read grant: %s", dbBeta, block, findOut)
	}
	// ...but still can't write to it (no write grant yet).
	insertOut := parseAndExec(e, bob, `INSERT `+block+` {"name": "bob was here"}`)
	if !wantsError(insertOut) {
		t.Fatalf("expected bob's INSERT to be denied (read-only grant), got: %s", insertOut)
	}

	// Extend the grant to include write.
	if out := parseAndExec(e, admin, "GRANT write ON "+dbBeta+"."+block+" TO bob"); wantsError(out) {
		t.Fatalf("GRANT write (as admin): %s", out)
	}
	insertOut2 := parseAndExec(e, bob, `INSERT `+block+` {"name": "bob was here"}`)
	if wantsError(insertOut2) {
		t.Fatalf("expected bob's INSERT to succeed after a write grant: %s", insertOut2)
	}

	// Revoke read: bob keeps write but loses read.
	if out := parseAndExec(e, admin, "REVOKE read ON "+dbBeta+"."+block+" FROM bob"); wantsError(out) {
		t.Fatalf("REVOKE read (as admin): %s", out)
	}
	findOut2 := parseAndExec(e, bob, "FIND "+block)
	if !wantsError(findOut2) {
		t.Fatalf("expected bob's FIND to be denied after revoking read, got: %s", findOut2)
	}
	insertOut3 := parseAndExec(e, bob, `INSERT `+block+` {"name": "bob again"}`)
	if wantsError(insertOut3) {
		t.Fatalf("expected bob's INSERT to still succeed (write grant untouched): %s", insertOut3)
	}
}

// TestGrant_DoesNotWeakenIsolationBetweenAdmins confirms that granting
// bob access to one block in db_beta has no effect on a completely
// unrelated admin's isolation from db_beta -- the grant is additive
// and scoped, not a global policy change.
func TestGrant_DoesNotWeakenIsolationBetweenAdmins(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	admin := &Session{authUser: "root", authRole: RoleGeneralAdmin}

	const dbBeta, dbGamma, block = "db_beta2", "db_gamma2", "secrets"
	for _, db := range []string{dbBeta, dbGamma} {
		if out := parseAndExec(e, admin, "CREATE DB "+db); wantsError(out) {
			t.Fatalf("CREATE DB %s: %s", db, out)
		}
	}
	if out := parseAndExec(e, admin, "USE "+dbBeta); wantsError(out) {
		t.Fatalf("USE: %s", out)
	}
	if out := parseAndExec(e, admin, "CREATE BLOCK "+dbBeta+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}

	mustCreateUserForACLTest(t, e, "bob2", RoleUser, dbGamma)
	mustCreateUserForACLTest(t, e, "carol", RoleAdmin, dbGamma)
	if out := parseAndExec(e, admin, "GRANT read ON "+dbBeta+"."+block+" TO bob2"); wantsError(out) {
		t.Fatalf("GRANT to bob2: %s", out)
	}

	carol := &Session{authUser: "carol", authRole: RoleAdmin, authDatabase: dbGamma}
	if out := parseAndExec(e, carol, "USE "+dbBeta); !wantsError(out) {
		t.Fatalf("expected carol (no grant of her own) to still be denied USE %s, got: %s", dbBeta, out)
	}
}

// TestGrant_CoversAggregatesExportView confirms authorizeBlockAccess
// now also gates COUNT/aggregations, EXPORT, and VIEW (both CREATE
// and querying an existing view) -- previously scoped to
// FIND/SEARCH/INSERT/UPDATE/DELETE only (see the CHANGELOG entry that
// first shipped granular ACL, and the follow-up that extended it to
// these four).
func TestGrant_CoversAggregatesExportView(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	admin := &Session{authUser: "root", authRole: RoleGeneralAdmin}

	const dbAlpha, dbBeta, block = "db_alpha3", "db_beta3", "nums"
	if out := parseAndExec(e, admin, "CREATE DB "+dbAlpha); wantsError(out) {
		t.Fatalf("CREATE DB alpha: %s", out)
	}
	if out := parseAndExec(e, admin, "CREATE DB "+dbBeta); wantsError(out) {
		t.Fatalf("CREATE DB beta: %s", out)
	}
	if out := parseAndExec(e, admin, "USE "+dbBeta); wantsError(out) {
		t.Fatalf("USE beta (as admin): %s", out)
	}
	if out := parseAndExec(e, admin, "CREATE BLOCK "+dbBeta+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}
	if _, err := e.insertWithID(dbBeta, block, "d1", map[string]any{"value": 10.0}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	mustCreateUserForACLTest(t, e, "frank", RoleUser, dbAlpha)
	frank := &Session{authUser: "frank", authRole: RoleUser, authDatabase: dbAlpha}

	// Before any grant: COUNT, EXPORT, and VIEW CREATE against
	// db_beta are all denied, same as FIND already was.
	if out := parseAndExec(e, frank, "USE "+dbBeta); !wantsError(out) {
		t.Fatalf("expected frank to be denied USE %s before any grant, got: %s", dbBeta, out)
	}

	if out := parseAndExec(e, admin, "GRANT read ON "+dbBeta+"."+block+" TO frank"); wantsError(out) {
		t.Fatalf("GRANT (as admin): %s", out)
	}
	if out := parseAndExec(e, frank, "USE "+dbBeta); wantsError(out) {
		t.Fatalf("expected frank to be admitted into %s after the grant: %s", dbBeta, out)
	}

	if out := parseAndExec(e, frank, "COUNT "+block); wantsError(out) {
		t.Fatalf("expected frank's COUNT to succeed after a read grant: %s", out)
	}
	if out := parseAndExec(e, frank, "SUM "+block+" value"); wantsError(out) {
		t.Fatalf("expected frank's SUM (aggregation) to succeed after a read grant: %s", out)
	}

	dest := "acl_export_test_" + block
	if out := parseAndExec(e, frank, `EXPORT `+block+` TO "`+dest+`"`); wantsError(out) {
		t.Fatalf("expected frank's EXPORT to succeed after a read grant: %s", out)
	}

	viewName := "frank_view"
	if out := parseAndExec(e, frank, "VIEW CREATE "+viewName+" AS FIND "+block); wantsError(out) {
		t.Fatalf("expected frank's VIEW CREATE to succeed after a read grant: %s", out)
	}
	if out := parseAndExec(e, frank, "VIEW "+viewName); wantsError(out) {
		t.Fatalf("expected frank to query his own view after a read grant: %s", out)
	}
}

// own username unconditionally, and requires admin to inspect
// someone else's.
func TestShowGrants_SelfVsOther(t *testing.T) {
	e := newConcurrencyTestEngine(t)
	admin := &Session{authUser: "root", authRole: RoleGeneralAdmin}
	const dbName, block = "db_showgrants", "b"
	if out := parseAndExec(e, admin, "CREATE DB "+dbName); wantsError(out) {
		t.Fatalf("CREATE DB: %s", out)
	}
	if out := parseAndExec(e, admin, "USE "+dbName); wantsError(out) {
		t.Fatalf("USE: %s", out)
	}
	if out := parseAndExec(e, admin, "CREATE BLOCK "+dbName+" ("+block+")"); wantsError(out) {
		t.Fatalf("CREATE BLOCK: %s", out)
	}

	mustCreateUserForACLTest(t, e, "dave", RoleUser, dbName)
	mustCreateUserForACLTest(t, e, "erin", RoleUser, dbName)
	if out := parseAndExec(e, admin, "GRANT read ON "+dbName+"."+block+" TO dave"); wantsError(out) {
		t.Fatalf("GRANT: %s", out)
	}

	dave := &Session{authUser: "dave", authRole: RoleUser, authDatabase: dbName}
	if out := parseAndExec(e, dave, "SHOW GRANTS FOR dave"); wantsError(out) {
		t.Fatalf("dave viewing his own grants: %s", out)
	}

	erin := &Session{authUser: "erin", authRole: RoleUser, authDatabase: dbName}
	if out := parseAndExec(e, erin, "SHOW GRANTS FOR dave"); !wantsError(out) {
		t.Fatalf("expected erin (non-admin) to be denied viewing dave's grants, got: %s", out)
	}
}
