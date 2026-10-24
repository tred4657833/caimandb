// cmd_backup_jsonl.go implements the DSL commands for the encrypted
// JSONL backup system (ops_backup_jsonl.go):
//
//	BACKUP <db>              -- incremental (or first-ever full)
//	BACKUP <db> FULL         -- force a full snapshot
//	RESTORE <db>             -- replay full + incrementals (latest state)
//	RESTORE <db> AS OF <ts>  -- point-in-time restore (see RestoreDB)
//	VERIFY BACKUP <db>       -- checksum + decrypt/decode check, read-only
//
// All three are gated to admin roles (IsAdminRole: ADMIN or
// ADMIN_GENERAL) per the requirement that only an administrator can
// trigger a backup or restore -- the automatic backups the scheduler
// runs on its own (backup_scheduler.go) aren't a session/user action at
// all, so this gate doesn't apply to them; it only applies to a human
// typing these commands.
package caimandb

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func handleBackupJSONL(eng *Engine, sess *Session, tokens []string) string {
	// sess.authRole == "" is the unauthenticated local console, which the
	// rest of the DSL (see hierarchy.go, acl.go) treats as full local
	// access -- IsAdminRole("") alone returns false, since "" isn't
	// literally ADMIN/ADMIN_GENERAL, which used to block the local
	// console from ever running this command even as the built-in
	// ADMIN_GENERAL user.
	if sess.authRole != "" && !IsAdminRole(sess.authRole) {
		return "ERROR: solo un administrador (" + RoleAdmin + " o " + RoleGeneralAdmin + ") puede ejecutar BACKUP"
	}
	if len(tokens) < 2 {
		return "ERROR: usage: BACKUP <db> [FULL]"
	}
	db := tokens[1]
	full := len(tokens) >= 3 && strings.ToUpper(tokens[2]) == "FULL"

	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}

	path, err := eng.BackupDB(db, full)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if path == "" {
		return fmt.Sprintf("No changes since the last backup of %s -- nothing written", db)
	}
	kind := "incremental"
	if full {
		kind = "full"
	}
	return fmt.Sprintf("Backup (%s) completed for %s: %s", kind, db, path)
}

func handleRestoreJSONL(eng *Engine, sess *Session, tokens []string) string {
	// sess.authRole == "" is the unauthenticated local console, which the
	// rest of the DSL (see hierarchy.go, acl.go) treats as full local
	// access -- IsAdminRole("") alone returns false, since "" isn't
	// literally ADMIN/ADMIN_GENERAL, which used to block the local
	// console from ever running this command even as the built-in
	// ADMIN_GENERAL user.
	if sess.authRole != "" && !IsAdminRole(sess.authRole) {
		return "ERROR: solo un administrador (" + RoleAdmin + " o " + RoleGeneralAdmin + ") puede ejecutar RESTORE"
	}
	if len(tokens) < 2 {
		return "ERROR: usage: RESTORE <db> [AS OF <unix-seconds>|<RFC3339>]"
	}
	db := tokens[1]
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}

	var asOf int64
	if len(tokens) >= 4 && strings.EqualFold(tokens[2], "AS") && strings.EqualFold(tokens[3], "OF") {
		if len(tokens) < 5 {
			return "ERROR: usage: RESTORE <db> AS OF <unix-seconds>|<RFC3339>"
		}
		ts, err := parseRestoreTimestamp(tokens[4])
		if err != nil {
			return "ERROR: " + err.Error()
		}
		asOf = ts
	} else if len(tokens) >= 3 {
		return "ERROR: usage: RESTORE <db> [AS OF <unix-seconds>|<RFC3339>]"
	}

	count, err := eng.RestoreDB(db, asOf)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if asOf > 0 {
		return fmt.Sprintf("Restored %d document(s) into %s as of %s", count, db, time.Unix(asOf, 0).UTC().Format(time.RFC3339))
	}
	return fmt.Sprintf("Restored %d document(s) into %s from backup", count, db)
}

// parseRestoreTimestamp accepts either raw Unix seconds ("1717000000")
// or an RFC3339 timestamp ("2024-05-29T12:00:00Z") for RESTORE ... AS
// OF, since operators reasoning about "restore to just before the
// incident" typically have a wall-clock time, not a Unix timestamp,
// on hand.
func parseRestoreTimestamp(raw string) (int64, error) {
	if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if secs <= 0 {
			return 0, fmt.Errorf("AS OF timestamp must be positive, got %d", secs)
		}
		return secs, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, fmt.Errorf("invalid AS OF timestamp %q: expected Unix seconds or RFC3339 (e.g. 2024-05-29T12:00:00Z)", raw)
	}
	return t.Unix(), nil
}

func handleVerifyBackup(eng *Engine, sess *Session, tokens []string) string {
	// sess.authRole == "" is the unauthenticated local console, which the
	// rest of the DSL (see hierarchy.go, acl.go) treats as full local
	// access -- IsAdminRole("") alone returns false, since "" isn't
	// literally ADMIN/ADMIN_GENERAL, which used to block the local
	// console from ever running this command even as the built-in
	// ADMIN_GENERAL user.
	if sess.authRole != "" && !IsAdminRole(sess.authRole) {
		return "ERROR: solo un administrador (" + RoleAdmin + " o " + RoleGeneralAdmin + ") puede ejecutar VERIFY BACKUP"
	}
	// tokens = ["VERIFY", "BACKUP", "<db>"]
	if len(tokens) < 3 {
		return "ERROR: usage: VERIFY BACKUP <db>"
	}
	db := tokens[2]
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}

	report, err := eng.VerifyBackup(db)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	var sb strings.Builder
	status := "OK"
	if ok, _ := report["ok"].(bool); !ok {
		status = "PROBLEMS FOUND"
	}
	sb.WriteString(fmt.Sprintf("Backup verification for %s: %s\n", db, status))
	entries, _ := report["entries"].([]map[string]any)
	if len(entries) == 0 {
		sb.WriteString("  No backups recorded yet\n")
	}
	for _, e := range entries {
		line := fmt.Sprintf("  %v [%s]", e["file"], e["status"])
		if docs, ok := e["docs_verified"]; ok {
			line += fmt.Sprintf(" docs=%v/%v", docs, e["docs_recorded"])
		}
		if errMsg, ok := e["error"]; ok {
			line += fmt.Sprintf(" error=%v", errMsg)
		}
		sb.WriteString(line + "\n")
	}
	return sb.String()
}
