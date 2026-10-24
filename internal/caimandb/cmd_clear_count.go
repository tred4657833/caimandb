package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"fmt"
	"strings"
)

func handleClear(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: CLEAR requires block name"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	if isInteractive() {
		fmt.Print("Empty " + block + " (delete all documents)? (y/N): ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
			return "Clear cancelled"
		}
	}

	count, err := eng.EmptyBlock(db, block)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Cleared %d documents from %s", count, block)
}

func handleCount(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: COUNT requires block name"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	var filters []Filter
	var where *parse.Expr
	i := 2
	for i < len(tokens) {
		if strings.ToUpper(tokens[i]) == "WHERE" {
			i++
			var err error
			where, filters, err = parseWhereClause(tokens, &i)
			if err != nil {
				return "ERROR: " + err.Error()
			}
		} else {
			i++
		}
	}

	count, err := eng.Count(db, block, filters, where)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Count: %d documents in %s", count, block)
}
