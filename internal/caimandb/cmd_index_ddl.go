// cmd_index_ddl.go implements the DSL handlers for the named-index
// syntax added on top of CREATE/DROP/SHOW:
//
//	CREATE INDEX <name> ON <block> (<field>)
//	CREATE UNIQUE INDEX <name> ON <block> (<field>)
//	CREATE INDEX <name> ON <block> (<field>) WHERE <condición>
//	DROP INDEX <name> ON <block>
//	SHOW INDEXES ON <block>
//
// See ops_named_index.go for what CREATE/DROP actually do to engine
// state, and its doc comments for current limitations (WHERE is
// stored but not enforced; UNIQUE is only checked on plain INSERT).
package caimandb

import (
	"fmt"
	"strings"
)

// handleCreateIndex parses tokens[0] == "CREATE" with tokens[1] being
// either "INDEX" or "UNIQUE" (dsl_parser.go routes here instead of
// handleCreate specifically for those two cases).
func handleCreateIndex(eng *Engine, sess *Session, tokens []string) string {
	i := 1
	unique := false
	if strings.ToUpper(tokens[i]) == "UNIQUE" {
		unique = true
		i++
	}
	if i >= len(tokens) || strings.ToUpper(tokens[i]) != "INDEX" {
		return "ERROR: usage: CREATE [UNIQUE] INDEX <name> ON <block> (<field>) [WHERE <condición>]"
	}
	i++

	if i >= len(tokens) {
		return "ERROR: CREATE INDEX requires a name"
	}
	name := tokens[i]
	i++

	if i >= len(tokens) || strings.ToUpper(tokens[i]) != "ON" {
		return "ERROR: usage: CREATE [UNIQUE] INDEX <name> ON <block> (<field>)"
	}
	i++

	if i >= len(tokens) {
		return "ERROR: CREATE INDEX requires a block name"
	}
	block := tokens[i]
	i++

	if i >= len(tokens) || tokens[i] != "(" {
		return "ERROR: usage: CREATE [UNIQUE] INDEX <name> ON <block> (<field>)"
	}
	i++
	if i >= len(tokens) {
		return "ERROR: CREATE INDEX requires a field inside ( )"
	}
	field := strings.Trim(tokens[i], ",")
	i++
	if i >= len(tokens) || tokens[i] != ")" {
		return "ERROR: CREATE INDEX only supports a single field for now: (<field>)"
	}
	i++

	var whereExpr string
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "WHERE" {
		whereExpr = strings.Join(tokens[i+1:], " ")
		if whereExpr == "" {
			return "ERROR: WHERE requires a condition"
		}
	}

	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	if err := eng.CreateNamedIndex(db, name, block, field, unique, whereExpr); err != nil {
		return "ERROR: " + err.Error()
	}

	var note strings.Builder
	if unique {
		note.WriteString(" (UNIQUE, enforced on INSERT)")
	}
	if whereExpr != "" {
		note.WriteString(" [partial: WHERE " + whereExpr + " stored but not yet enforced -- all documents are indexed]")
	}
	return fmt.Sprintf("Index created: %s ON %s (%s)%s", name, block, field, note.String())
}

// handleDropIndex parses "DROP INDEX <name> ON <block>". dsl_parser.go
// routes here instead of handleDrop when tokens[1] == "INDEX".
func handleDropIndex(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: usage: DROP INDEX <name> ON <block>"
	}
	name := tokens[2]
	var block string
	if len(tokens) >= 5 && strings.ToUpper(tokens[3]) == "ON" {
		block = tokens[4]
	} else if len(tokens) > 3 {
		return "ERROR: usage: DROP INDEX <name> ON <block>"
	}

	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}

	if err := eng.DropNamedIndex(db, name, block); err != nil {
		return "ERROR: " + err.Error()
	}
	return "Index dropped: " + name
}

// handleShowIndexesOnBlock implements "SHOW INDEXES ON <block>":
// tokens = ["SHOW", "INDEXES", "ON", "<block>"]. This is a block-
// scoped view of the same cfg.NamedIndexes that the existing
// "SHOW INDEXES [<db>]" (cmd_create_show.go) exposes at the DB level
// as a flat field-name list; this variant lists the named
// CREATE INDEX definitions for one block instead.
func handleShowIndexesOnBlock(eng *Engine, sess *Session, block string) string {
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	cfg, ok := eng.intelEngine.GetConfig(db)
	if !ok {
		return "ERROR: Database not found: " + db
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Indexes on %s.%s:\n", db, block))
	found := false
	for _, idx := range cfg.NamedIndexes {
		if idx.Block != block {
			continue
		}
		found = true
		line := fmt.Sprintf("  %s (%s)", idx.Name, idx.Field)
		if idx.Unique {
			line += " UNIQUE"
		}
		if idx.Where != "" {
			line += " WHERE " + idx.Where
		}
		sb.WriteString(line + "\n")
	}
	if !found {
		sb.WriteString("  No named indexes defined\n")
	}
	return sb.String()
}
