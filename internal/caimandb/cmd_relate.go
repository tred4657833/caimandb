package caimandb

import (
	"fmt"
	"strings"
)

// handleRelate parses:
//
//	RELATE <block> USE <target1>[,<target2>,...];
//
// Each target is either a bare block name (same database as <block>) or
// "db.block" for a cross-database relation. Once registered, FIND queries
// against <block> can select "<targetAlias>.field" (or the bare alias for
// the whole related document) and CaimanDB resolves it automatically.
func handleRelate(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 4 {
		return "ERROR: RELATE requires: RELATE <block> USE <target1>[,<target2>,...]"
	}

	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !strings.EqualFold(tokens[2], "USE") {
		return "ERROR: Expected USE keyword. Usage: RELATE <block> USE <target1>[,<target2>,...]"
	}

	targetsRaw := strings.Join(tokens[3:], " ")
	targetsRaw = strings.TrimRight(strings.TrimSpace(targetsRaw), ";")
	if targetsRaw == "" {
		return "ERROR: RELATE requires at least one target block after USE"
	}

	var targets []string
	for _, t := range strings.Split(targetsRaw, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			targets = append(targets, t)
		}
	}
	if len(targets) == 0 {
		return "ERROR: RELATE requires at least one target block after USE"
	}

	def, err := eng.Relate(db, block, targets)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	parts := make([]string, len(def.Targets))
	for i, t := range def.Targets {
		parts[i] = fmt.Sprintf("%s.%s (fk: %s)", t.DB, t.Block, t.FK)
	}
	return fmt.Sprintf("Related %s.%s USE %s", db, block, strings.Join(parts, ", "))
}
