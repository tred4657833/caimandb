// cmd_grant.go implements the DSL surface for the granular ACL system
// in acl.go:
//
//	GRANT read[,write] ON <db>.<block> TO <user>
//	REVOKE read[,write] ON <db>.<block> FROM <user>
//	SHOW GRANTS [FOR <user>]
//
// <block> may be "*" for "every block in <db>". Only an admin
// (IsAdminRole: ADMIN or ADMIN_GENERAL) may run GRANT/REVOKE, and a
// scoped ADMIN (not ADMIN_GENERAL) may only grant/revoke access to
// their own database -- the same isolation rule requireDBAccess
// already enforces elsewhere -- so a DB's own admin can hand out
// narrower access within their base, but can't reach into another
// admin's database. SHOW GRANTS FOR <user> is available to any
// authenticated caller for their own username; seeing another user's
// grants requires admin.
package caimandb

import (
	"fmt"
	"strings"
)

// parseGrantTarget splits "<db>.<block>" into (db, block). A bare
// name with no "." means "every block in that db" (block="*"), so
// "GRANT read ON mydb TO alice" and "GRANT read ON mydb.* TO alice"
// are equivalent.
func parseGrantTarget(raw string) (db, block string) {
	if idx := strings.Index(raw, "."); idx >= 0 {
		return raw[:idx], raw[idx+1:]
	}
	return raw, "*"
}

func handleGrant(eng *Engine, sess *Session, tokens []string) string {
	// GRANT <actions> ON <target> TO <user>
	if len(tokens) < 6 || !strings.EqualFold(tokens[2], "ON") || !strings.EqualFold(tokens[4], "TO") {
		return `ERROR: usage: GRANT read[,write] ON <db>.<block> TO <user>`
	}
	if sess.authRole != "" && !IsAdminRole(sess.authRole) {
		return "ERROR: solo un administrador (" + RoleAdmin + " o " + RoleGeneralAdmin + ") puede ejecutar GRANT"
	}
	actions := strings.Split(tokens[1], ",")
	db, block := parseGrantTarget(tokens[3])
	username := tokens[5]

	if sess.authRole != "" && !IsGeneralAdmin(sess.authRole) &&
		!UserCanAccessDB(User{Role: sess.authRole, Database: sess.authDatabase}, db) {
		return "ERROR: acceso denegado: " + sess.authRole + " no administra la base '" + db + "'"
	}

	grantedBy := sess.authUser
	if grantedBy == "" {
		grantedBy = "console"
	}
	if err := eng.GrantAccess(username, db, block, actions, grantedBy); err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Granted %s on %s.%s to %s", strings.Join(actions, ","), db, block, username)
}

func handleRevoke(eng *Engine, sess *Session, tokens []string) string {
	// REVOKE <actions> ON <target> FROM <user>
	if len(tokens) < 6 || !strings.EqualFold(tokens[2], "ON") || !strings.EqualFold(tokens[4], "FROM") {
		return `ERROR: usage: REVOKE read[,write] ON <db>.<block> FROM <user>`
	}
	if sess.authRole != "" && !IsAdminRole(sess.authRole) {
		return "ERROR: solo un administrador (" + RoleAdmin + " o " + RoleGeneralAdmin + ") puede ejecutar REVOKE"
	}
	actions := strings.Split(tokens[1], ",")
	db, block := parseGrantTarget(tokens[3])
	username := tokens[5]

	if sess.authRole != "" && !IsGeneralAdmin(sess.authRole) &&
		!UserCanAccessDB(User{Role: sess.authRole, Database: sess.authDatabase}, db) {
		return "ERROR: acceso denegado: " + sess.authRole + " no administra la base '" + db + "'"
	}

	if err := eng.RevokeAccess(username, db, block, actions); err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Revoked %s on %s.%s from %s", strings.Join(actions, ","), db, block, username)
}

func handleShowGrants(eng *Engine, sess *Session, tokens []string) string {
	target := sess.authUser
	if len(tokens) >= 4 && strings.EqualFold(tokens[2], "FOR") {
		target = tokens[3]
	}
	if target == "" {
		return "ERROR: usage: SHOW GRANTS FOR <user>"
	}
	if target != sess.authUser && sess.authRole != "" && !IsAdminRole(sess.authRole) {
		return "ERROR: solo un administrador puede ver los permisos de otro usuario"
	}

	grants, err := eng.ListGrants(target)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if len(grants) == 0 {
		return "No grants for " + target
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "Grants for %s:\n", target)
	for _, g := range grants {
		fmt.Fprintf(&sb, "  %s.%s: %s\n", g.DB, g.Block, strings.Join(g.Actions, ","))
	}
	return sb.String()
}
