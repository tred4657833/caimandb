package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"fmt"
	"strings"
)

func handleAggregate(eng *Engine, sess *Session, aggOp string, tokens []string) string {
	if len(tokens) < 3 {
		return fmt.Sprintf("ERROR: %s requires block and field", aggOp)
	}
	block := tokens[1]
	field := tokens[2]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}
	if err := eng.authorizeBlockAccess(sess, db, block, "read"); err != nil {
		return "ERROR: " + err.Error()
	}

	// Migrated to parseWhereClause/parse.Expr: real parentheses/NOT/
	// AND-OR precedence, same pattern already used for DELETE/COUNT (see
	// query_filter.go's matchesWhere). Read-only, no WAL/Raft involved.
	var where *parse.Expr
	var filters []Filter
	i := 3
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

	// MODE is meaningful on non-numeric fields (e.g. a status category),
	// so it doesn't go through Aggregate's float64-only path -- see
	// AggregateMode's doc comment.
	if strings.ToUpper(aggOp) == "MODE" {
		value, occurrences, err := eng.AggregateMode(db, block, field, where, filters)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if value == nil {
			return fmt.Sprintf("MODE of %s: no documents matched", field)
		}
		return fmt.Sprintf("MODE of %s: %v (%d occurrence(s))", field, value, occurrences)
	}

	result, err := eng.Aggregate(db, block, aggOp, field, where, filters)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("%s of %s: %v", aggOp, field, result)
}

func handleGroup(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 4 {
		return "ERROR: GROUP requires block, BY, and field"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}
	if err := eng.authorizeBlockAccess(sess, db, block, "read"); err != nil {
		return "ERROR: " + err.Error()
	}

	var byField string
	var aggOp string
	var aggField string
	var where *parse.Expr
	var filters []Filter

	i := 2
	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
		case "BY":
			i++
			if i < len(tokens) {
				byField = tokens[i]
				i++
			}
		case "SUM", "AVG", "MIN", "MAX", "COUNT":
			aggOp = strings.ToUpper(tokens[i])
			i++
			if i < len(tokens) && !strings.EqualFold(tokens[i], "WHERE") {
				aggField = tokens[i]
				i++
			}
		case "WHERE":
			// Migrated to parseWhereClause/parse.Expr -- same reasoning
			// as handleAggregate above. Read-only, no WAL/Raft involved.
			i++
			var err error
			where, filters, err = parseWhereClause(tokens, &i)
			if err != nil {
				return "ERROR: " + err.Error()
			}
		default:
			i++
		}
	}

	if byField == "" {
		return "ERROR: GROUP requires BY field"
	}

	results, err := eng.Group(db, block, byField, aggOp, aggField, where, filters)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return jsonOut(results)
}
