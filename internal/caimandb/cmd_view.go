package caimandb

import (
	"fmt"
	"strings"
	"sync"

	"caimandb/internal/caimandb/parse"
)

type ViewDefinition struct {
	Name    string
	DB      string
	Block   string
	Query   string
	Filters []Filter
	// Where holds the real parenthesized/NOT/AND-OR expression tree
	// for the view's WHERE clause when it has one, alongside the
	// legacy flat Filters (kept for JSON output / VIEW INFO
	// backward-compatibility). nil for views with no WHERE, or for
	// views created before this field existed (loaded from an older
	// in-memory/JSON representation) -- either way, the query built in
	// the "default" case below falls back to Filters via the same
	// "tree if present, else flat" rule matchesWhere already uses
	// elsewhere, since Query.Where/Query.Filters are evaluated the
	// same way FIND's own WHERE is.
	Where *parse.Expr `json:"-"`
}

var views = make(map[string]ViewDefinition)

var viewsMu sync.RWMutex

func handleViewCmd(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: VIEW requires command (CREATE, DROP, SHOW, INFO)"
	}

	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "CREATE":
		if len(tokens) < 5 {
			return "ERROR: VIEW CREATE requires: VIEW name AS FIND block WHERE condition"
		}
		viewName := tokens[2]
		if strings.ToUpper(tokens[3]) != "AS" {
			return "ERROR: Expected AS keyword"
		}

		rest := strings.Join(tokens[4:], " ")
		rest = strings.TrimSpace(rest)

		if !strings.HasPrefix(strings.ToUpper(rest), "FIND") {
			return "ERROR: View must be based on FIND query"
		}

		restTokens := parse.Tokenize(rest)
		if len(restTokens) < 2 {
			return "ERROR: Invalid view query"
		}

		block := restTokens[1]
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

		// Migrated to parseWhereClause/parse.Expr: real
		// parentheses/NOT/AND-OR precedence, same pattern already used
		// for DELETE/CLEAR/COUNT/aggregations/UPDATE (see
		// query_filter.go's matchesWhere). ViewDefinition.Where now
		// carries the tree alongside the legacy flat Filters, and the
		// "default" case below (VIEW <name>, i.e. querying the view)
		// passes both into Query the same way FIND's own WHERE does.
		var filters []Filter
		var where *parse.Expr
		i := 2
		for i < len(restTokens) {
			if strings.ToUpper(restTokens[i]) == "WHERE" {
				i++
				var err error
				where, filters, err = parseWhereClause(restTokens, &i)
				if err != nil {
					return "ERROR: " + err.Error()
				}
				break
			}
			i++
		}

		viewsMu.Lock()
		views[viewName] = ViewDefinition{
			Name:    viewName,
			DB:      db,
			Block:   block,
			Query:   rest,
			Filters: filters,
			Where:   where,
		}
		viewsMu.Unlock()

		return "View created: " + viewName

	case "DROP":
		if len(tokens) < 3 {
			return "ERROR: VIEW DROP requires view name"
		}
		viewName := tokens[2]
		viewsMu.Lock()
		delete(views, viewName)
		viewsMu.Unlock()
		return "View dropped: " + viewName

	case "SHOW":
		viewsMu.RLock()
		defer viewsMu.RUnlock()
		if len(views) == 0 {
			return "No views defined"
		}
		var sb strings.Builder
		sb.WriteString("Views:\n")
		for name, v := range views {
			sb.WriteString(fmt.Sprintf("  %s (db: %s, block: %s)\n", name, v.DB, v.Block))
		}
		return sb.String()

	case "INFO":
		if len(tokens) < 3 {
			return "ERROR: VIEW INFO requires view name"
		}
		viewName := tokens[2]
		viewsMu.RLock()
		v, ok := views[viewName]
		viewsMu.RUnlock()
		if !ok {
			return "View not found: " + viewName
		}
		return jsonOut(v)

	default:
		viewName := tokens[1]
		viewsMu.RLock()
		v, ok := views[viewName]
		viewsMu.RUnlock()
		if !ok {
			return "ERROR: View not found: " + viewName
		}

		if !eng.dirMgr.BlockExists(v.DB, v.Block) {
			return "ERROR: Block not found: " + v.Block
		}
		if err := eng.authorizeBlockAccess(sess, v.DB, v.Block, "read"); err != nil {
			return "ERROR: " + err.Error()
		}

		q := &Query{
			Filters: v.Filters,
			Where:   v.Where,
			Limit:   100,
			Offset:  0,
		}

		result, err := eng.Find(v.DB, v.Block, q)
		if err != nil {
			return "ERROR: " + err.Error()
		}

		var sb strings.Builder
		if result.Total == 0 {
			sb.WriteString("No documents in view")
		} else {
			sb.WriteString(fmt.Sprintf("View %s: %d documents\n", viewName, result.Total))
			for _, doc := range result.Docs {
				sb.WriteString(fmt.Sprintf("  _id: %s, data: %s\n", doc.ID, jsonOut(doc.Data)))
			}
		}
		return sb.String()
	}
}
