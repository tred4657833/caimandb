// cmd_admin_extra.go implements the DSL command handlers referenced by
// dsl_parser.go that did not yet have an implementation: DROP, RENAME,
// INFO, DESCRIBE, STATS, SIZE, REBUILD, CHECK, REPAIR, FLEX and the
// transaction commands (BEGIN/COMMIT/ROLLBACK/TX).
package caimandb

import (
	"fmt"
	"strings"
)

// resolveDBBlock resolves a database/block pair for admin sub-commands that
// accept either "<db> <block>" or just "<block>" (using the session's
// current database) starting at tokens[argStart].
//
// (DROP BLOCK, INFO/DESCRIBE BLOCK, SIZE BLOCK, REBUILD BLOCK, CHECK
// BLOCK, REPAIR BLOCK, FLEX REINDEX): db is checked with
// requireDBAccess before being returned, so none of those handlers can
// forget the check -- see docs/hierarchy.md, "Alcance actual y
// siguiente fase".
func resolveDBBlock(sess *Session, tokens []string, argStart int) (db, block string, err error) {
	switch len(tokens) - argStart {
	case 1:
		block = tokens[argStart]
		db = sess.currentDB
		if db == "" {
			return "", "", fmt.Errorf("no database in use; specify: <db> <block>, or USE a database first")
		}
	case 0:
		return "", "", fmt.Errorf("block name required")
	default:
		db = tokens[argStart]
		block = tokens[argStart+1]
	}
	if db == "" {
		return "", "", fmt.Errorf("no database specified and no database in use")
	}
	if block == "" {
		return "", "", fmt.Errorf("block name required")
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "", "", err
	}
	return db, block, nil
}

func handleDrop(eng *Engine, sess *Session, tokens []string) string {
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		if len(tokens) < 3 {
			return "ERROR: DROP DB requires database name"
		}
		db := tokens[2]
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.DropDB(db); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Database dropped: " + db

	case "BLOCK":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.DropBlock(db, block); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Block dropped: " + block + " in database: " + db

	case "USER":
		if len(tokens) < 3 {
			return "ERROR: DROP USER requires username"
		}
		username := tokens[2]
		// Aislamiento (docs/hierarchy.md, "Otros pendientes razonables"):
		// un ADMIN/SUBADMIN solo puede borrar usuarios de su propia base,
		// y nunca a otro ADMIN ni al ADMIN_GENERAL -- ver
		// authorizeDropUser. Si el usuario objetivo no existe, se deniega
		// igual (fail closed) en vez de dejar pasar la operación para que
		// DropUser falle por su cuenta; así no hay forma de usar el
		// mensaje de error de "no existe" para confirmar o descartar
		// cuentas de otra base.
		if sess != nil && sess.authRole != "" && !IsGeneralAdmin(sess.authRole) {
			target, ok := eng.GetUserByUsername(username)
			if !ok {
				return "ERROR: acceso denegado: no puede borrar usuarios fuera de su propia base"
			}
			if err := authorizeDropUser(sess, target); err != nil {
				return "ERROR: " + err.Error()
			}
		}
		if err := eng.DropUser(username); err != nil {
			return "ERROR: " + err.Error()
		}
		return "User dropped: " + username

	default:
		return "ERROR: unknown DROP object: " + subCmd
	}
}

func handleRename(eng *Engine, sess *Session, tokens []string) string {
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		// RENAME DB <old> TO <new>
		if len(tokens) < 5 || strings.ToUpper(tokens[3]) != "TO" {
			return "ERROR: usage: RENAME DB <old> TO <new>"
		}
		oldName, newName := tokens[2], tokens[4]
		if err := requireDBAccess(sess, oldName); err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.RenameDB(oldName, newName); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Database renamed: " + oldName + " -> " + newName

	case "BLOCK":
		// RENAME BLOCK [<db>] (<old1>, <old2>, ...) TO (<new1>, <new2>, ...)
		// renames each pair in parallel, by position; the two lists must
		// be the same length. RENAME BLOCK [<db>] <old> TO <new> (no
		// parens) is the original single-block form, unchanged.
		if len(tokens) >= 3 && tokens[2] == "(" {
			return renameBlocks(eng, sess, sess.currentDB, tokens, 2)
		}
		if len(tokens) >= 4 && tokens[3] == "(" {
			return renameBlocks(eng, sess, tokens[2], tokens, 3)
		}

		// RENAME BLOCK <db> <old> TO <new>  (or RENAME BLOCK <old> TO <new> using current db)
		var db, oldName, newName string
		if len(tokens) >= 6 && strings.ToUpper(tokens[4]) == "TO" {
			db, oldName, newName = tokens[2], tokens[3], tokens[5]
		} else if len(tokens) >= 5 && strings.ToUpper(tokens[3]) == "TO" {
			db = sess.currentDB
			oldName, newName = tokens[2], tokens[4]
		} else {
			return "ERROR: usage: RENAME BLOCK [db] <old> TO <new>"
		}
		if db == "" {
			return "ERROR: no database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.RenameBlock(db, oldName, newName); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Block renamed: " + oldName + " -> " + newName + " in database: " + db

	case "FIELD":
		// RENAME FIELD [<db>] <block>.<field> TO <new_field>
		return renameField(eng, sess, tokens)

	default:
		return "ERROR: unknown RENAME object: " + subCmd
	}
}

// renameBlocks implements the parallel RENAME BLOCK (a, b, c) TO
// (a2, b2, c2) form: db is the target database (already resolved by the
// caller), tokens[parenStart] is "(" for the old-names list. It expects
// the token right after that list's closing ")" to be TO, followed by
// the new-names list in the same paren-list syntax. Like createBlocks,
// each rename is independent -- not one atomic operation -- so the
// result reports exactly which pairs succeeded and which didn't.
func renameBlocks(eng *Engine, sess *Session, db string, tokens []string, parenStart int) string {
	if db == "" {
		return "ERROR: no database specified and no database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	oldNames, next, err := parseParenList(tokens, parenStart)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if next >= len(tokens) || strings.ToUpper(tokens[next]) != "TO" {
		return "ERROR: usage: RENAME BLOCK [db] (<old>, ...) TO (<new>, ...)"
	}
	next++
	newNames, next, err := parseParenList(tokens, next)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if next != len(tokens) {
		return "ERROR: unexpected tokens after new-name list"
	}
	if len(oldNames) != len(newNames) {
		return fmt.Sprintf("ERROR: old-name list has %d entr%s but new-name list has %d",
			len(oldNames), pluralIes(len(oldNames)), len(newNames))
	}

	var renamed, failed []string
	for i, oldName := range oldNames {
		newName := newNames[i]
		if err := eng.RenameBlock(db, oldName, newName); err != nil {
			failed = append(failed, oldName+" -> "+newName+" ("+err.Error()+")")
		} else {
			renamed = append(renamed, oldName+" -> "+newName)
		}
	}

	msg := ""
	if len(renamed) > 0 {
		msg += "Blocks renamed: " + strings.Join(renamed, ", ") + " in database: " + db
	}
	if len(failed) > 0 {
		if msg != "" {
			msg += "; "
		}
		msg += "ERROR renaming: " + strings.Join(failed, ", ")
	}
	return msg
}

func pluralIes(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// renameField implements RENAME FIELD [<db>] <block>.<field> TO
// <new_field>: tokens[2] (or tokens[3] if a db is given explicitly
// before it) is "<block>.<field>", tokens[.. +1] must be TO, and the
// final token is the new field name.
func renameField(eng *Engine, sess *Session, tokens []string) string {
	usage := "ERROR: usage: RENAME FIELD [<db>] <block>.<field> TO <new_field>"

	var db string
	rest := tokens[2:]
	// The grammar is fully positional -- RENAME FIELD [<db>] <block>.<field>
	// TO <new_field> -- so an explicit db is present exactly when there are
	// 4 remaining tokens (db, block.field, TO, new_field) instead of 3
	// (block.field, TO, new_field). This used to be guessed from whether
	// rest[0] contained a "." (i.e. "does it look like block.field"),
	// which breaks the moment a db name itself contains a dot and is more
	// fragile than it needs to be for a fixed-arity command; counting
	// tokens matches the documented syntax exactly instead of sniffing
	// content.
	switch len(rest) {
	case 4:
		db = rest[0]
		rest = rest[1:]
	case 3:
		db = sess.currentDB
	default:
		return usage
	}
	if db == "" {
		return "ERROR: no database specified and no database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	if strings.ToUpper(rest[1]) != "TO" {
		return usage
	}

	blockField := stripQuotes(rest[0])
	newField := stripQuotes(rest[2])
	dot := strings.Index(blockField, ".")
	if dot <= 0 || dot == len(blockField)-1 {
		return usage + " (expected <block>.<field>, got: " + blockField + ")"
	}
	block := blockField[:dot]
	oldField := blockField[dot+1:]

	renamed, err := eng.RenameField(db, block, oldField, newField)
	if err != nil {
		return fmt.Sprintf("ERROR: %s (renamed %d document(s) before this)", err.Error(), renamed)
	}
	return fmt.Sprintf("Field renamed: %s.%s -> %s in database: %s (%d document(s))",
		block, oldField, newField, db, renamed)
}

func describeDBOrBlock(eng *Engine, sess *Session, tokens []string) string {
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		if len(tokens) < 3 {
			return "ERROR: requires database name"
		}
		if err := requireDBAccess(sess, tokens[2]); err != nil {
			return "ERROR: " + err.Error()
		}
		info, err := eng.DescribeDB(tokens[2])
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return jsonOut(info)

	case "BLOCK":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		info, err := eng.DescribeBlock(db, block)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return jsonOut(info)

	default:
		return "ERROR: unknown object: " + subCmd + " (expected DB or BLOCK)"
	}
}

func handleInfo(eng *Engine, sess *Session, tokens []string) string {
	return describeDBOrBlock(eng, sess, tokens)
}

func handleDescribe(eng *Engine, sess *Session, tokens []string) string {
	return describeDBOrBlock(eng, sess, tokens)
}

// handleStats implements "STATS DB [<name>]", matching the OBJECT-keyword
// syntax documented in help.go / docs/nql-reference.md and used by every
// other admin command (ANALYZE, OPTIMIZE, INFO, DESCRIBE, ...).
func handleStats(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: STATS requires object (DB)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		db := sess.currentDB
		if len(tokens) >= 3 {
			db = tokens[2]
		}
		if db == "" {
			return "ERROR: no database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.DBExists(db) {
			return "ERROR: database not found: " + db
		}
		stats, err := eng.StatsDB(db)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return jsonOut(stats)

	default:
		return "ERROR: unknown STATS object: " + subCmd + " (expected DB)"
	}
}

// handleSize implements "SIZE DB [<name>]" and "SIZE BLOCK [<db>] <name>",
// matching the OBJECT-keyword syntax documented in help.go /
// docs/nql-reference.md. SIZE BLOCK reads size_bytes via DescribeBlock so
// it reports the exact same figure as SHOW BLOCKS, instead of an
// independently-computed directory walk.
func handleSize(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: SIZE requires object (DB or BLOCK)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		db := sess.currentDB
		if len(tokens) >= 3 {
			db = tokens[2]
		}
		if db == "" {
			return "ERROR: no database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.DBExists(db) {
			return "ERROR: database not found: " + db
		}
		sz, err := dirSize(eng.dirMgr.DBPath(db))
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return "Size of " + db + ": " + fmtBytes(sz)

	case "BLOCK":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.DBExists(db) {
			return "ERROR: database not found: " + db
		}
		info, err := eng.DescribeBlock(db, block)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		var sizeBytes int64
		if v, ok := info["size_bytes"].(int64); ok {
			sizeBytes = v
		}
		return "Size of " + block + " in " + db + ": " + fmtBytes(sizeBytes)

	default:
		return "ERROR: unknown SIZE object: " + subCmd + " (expected DB or BLOCK)"
	}
}

func handleRebuild(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: REBUILD requires object (BLOCK)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "BLOCK":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.RebuildBlock(db, block); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Block indexes rebuilt: " + block + " in database: " + db
	default:
		return "ERROR: unknown REBUILD object: " + subCmd
	}
}

func handleCheck(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: CHECK requires object (BLOCK)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "BLOCK":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		report, err := eng.CheckBlock(db, block)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return jsonOut(report)
	default:
		return "ERROR: unknown CHECK object: " + subCmd
	}
}

func handleRepair(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: REPAIR requires object (BLOCK)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "BLOCK":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.RepairBlock(db, block); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Block repaired: " + block + " in database: " + db
	default:
		return "ERROR: unknown REPAIR object: " + subCmd
	}
}

func handleFlexCommand(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: FLEX requires a sub-command (STATS, HOT, REINDEX)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "STATS":
		return jsonOut(eng.flexEngine.Stats())

	case "REINDEX":
		db, block, err := resolveDBBlock(sess, tokens, 2)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.BlockExists(db, block) {
			return "ERROR: Block not found: " + block
		}
		count, err := eng.ReindexFlexColumn(db, block)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return fmt.Sprintf("FLEX-COLUMN index rebuilt: %s in database: %s (%d documents indexed)", block, db, count)

	case "HOT":
		db := sess.currentDB
		block := ""
		if len(tokens) >= 3 {
			block = tokens[2]
		}
		if db == "" {
			return "ERROR: no database in use"
		}
		hot := eng.flexEngine.HotFields(db, block)
		if len(hot) == 0 {
			return "No hot fields recorded"
		}
		return jsonOut(hot)

	default:
		return "ERROR: unknown FLEX sub-command: " + subCmd
	}
}

func handleTransaction(eng *Engine, sess *Session, tokens []string) string {
	cmd := strings.ToUpper(tokens[0])
	switch cmd {
	case "BEGIN":
		if sess.currentTx != nil && sess.currentTx.Status == "pending" {
			return "ERROR: a transaction is already active (id: " + sess.currentTx.ID + ")"
		}
		db := sess.currentDB
		block := ""
		if len(tokens) >= 2 {
			db = tokens[1]
		}
		if len(tokens) >= 3 {
			block = tokens[2]
		}
		if db == "" {
			return "ERROR: no database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		tx, err := eng.txManager.Begin(db, block)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		sess.currentTx = tx
		return "Transaction started: " + tx.ID

	case "COMMIT":
		if sess.currentTx == nil {
			return "ERROR: no active transaction"
		}
		tx := sess.currentTx
		if err := eng.txManager.Commit(tx); err != nil {
			return "ERROR: " + err.Error()
		}
		sess.currentTx = nil
		return "Transaction committed: " + tx.ID

	case "ROLLBACK", "ABORT":
		if sess.currentTx == nil {
			return "ERROR: no active transaction"
		}
		tx := sess.currentTx
		if err := eng.txManager.Abort(tx); err != nil {
			return "ERROR: " + err.Error()
		}
		sess.currentTx = nil
		return "Transaction aborted: " + tx.ID

	case "TX", "TRANSACTION":
		sub := ""
		if len(tokens) >= 2 {
			sub = strings.ToUpper(tokens[1])
		}
		switch sub {
		case "LIST":
			active := eng.txManager.GetActiveTransactions()
			if len(active) == 0 {
				return "No active transactions"
			}
			return "Active transactions:\n  " + strings.Join(active, "\n  ")

		case "STATS":
			return jsonOut(eng.txManager.Stats())

		case "ISOLATION":
			// Documented in help.go and docs/NQL_SYNTAX.en.md but never
			// implemented: it used to fall into the default branch and
			// answer with the current transaction's status instead of
			// the isolation level.
			return "Isolation level: " + eng.txManager.isolation +
				" (configured engine-wide, not selectable per statement)"

		default:
			if sess.currentTx == nil {
				return "No active transaction in this session"
			}
			return fmt.Sprintf("Current transaction: %s (db: %s, block: %s, status: %s)",
				sess.currentTx.ID, sess.currentTx.DB, sess.currentTx.Block, sess.currentTx.Status)
		}

	default:
		return "ERROR: unknown transaction command: " + cmd
	}
}
