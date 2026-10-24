package caimandb

import (
	"fmt"
	"strings"
)

// handleExplain implements "EXPLAIN FIND ..." and "EXPLAIN SEARCH ...".
//
// Design choice: EXPLAIN here is closer to Postgres' EXPLAIN ANALYZE than a
// pure dry-run planner. It builds the exact same Query that FIND/SEARCH
// would build (via buildFindQuery/buildSearchQuery -- the same functions
// FIND and SEARCH themselves call, so there is no separate "explain parser"
// that could drift out of sync with the real one), actually runs it, and
// reports the real numbers the engine measured: rows scanned, rows
// matched, which index (if any) was actually used, and how long it took.
//
// It also reports what the query optimizer would have *liked* to use
// (QueryOptimizer.AnalyzeQuery, which only ever considers a single
// equality/IN condition -- see query_optimizer.go) side by side with what
// was *actually* used, since those two can differ: the planned index
// lookup can come back empty (e.g. no matching secondary index entries
// yet) and silently fall back to a full scan inside Engine.Find.
//
// "memory used" or "cost" figures) -- every line below comes from a real
// measurement or a real, inspectable decision the engine made.
func handleExplain(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: EXPLAIN requires a FIND or SEARCH query, e.g. EXPLAIN FIND movies WHERE rating > 8"
	}

	sub := strings.ToUpper(tokens[1])
	switch sub {
	case "FIND":
		return explainFind(eng, sess, tokens[1:])
	case "SEARCH":
		return explainSearch(eng, sess, tokens[1:])
	default:
		return "ERROR: EXPLAIN only supports FIND or SEARCH, got: " + tokens[1]
	}
}

func explainFind(eng *Engine, sess *Session, tokens []string) string {
	// tokens here is the FIND-only tail: tokens[0]=="FIND", tokens[1]=block, ...
	if len(tokens) < 2 {
		return "ERROR: EXPLAIN FIND requires a block name"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	q, _, err := buildFindQuery(tokens, 2)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	planned := eng.queryOpt.AnalyzeQuery(q.Filters)

	result, err := eng.Find(db, block, q)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	var sb strings.Builder
	sb.WriteString("Query Plan\n")
	sb.WriteString(strings.Repeat("-", 60) + "\n")
	sb.WriteString(fmt.Sprintf("Command       : FIND %s\n", block))
	writeWhereLine(&sb, q)
	writePlannedLine(&sb, planned)
	sb.WriteString(fmt.Sprintf("Actual Access : %s\n", result.IndexUsed))
	sb.WriteString(fmt.Sprintf("Rows Scanned  : %d\n", result.Scanned))
	sb.WriteString(fmt.Sprintf("Rows Matched  : %d\n", result.Total))
	if len(q.Sort) > 0 {
		sb.WriteString(fmt.Sprintf("Sort          : %s\n", sortFieldsString(q.Sort)))
	}
	sb.WriteString(fmt.Sprintf("Limit/Offset  : %d / %d\n", q.Limit, q.Offset))
	sb.WriteString(fmt.Sprintf("Took          : %v\n", result.Took))
	sb.WriteString(strings.Repeat("-", 60) + "\n")
	sb.WriteString("Note: this EXPLAIN executed the query for real (like EXPLAIN ANALYZE)\n")
	sb.WriteString("rather than only estimating it -- the numbers above are measured, not guessed.\n")
	return sb.String()
}

func explainSearch(eng *Engine, sess *Session, tokens []string) string {
	// tokens here is the SEARCH-only tail: tokens[0]=="SEARCH", tokens[1]=block, tokens[2]=text, ...
	if len(tokens) < 3 {
		return "ERROR: EXPLAIN SEARCH requires a block name and search text"
	}
	block := tokens[1]
	text := tokens[2]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	q, opts, err := buildSearchQuery(tokens, 3)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	// See unquoteSearchTerm's doc comment (ops_search.go): tokens[2]
	// carries its literal quote characters, so EXACT/FUZZY's own
	// markers have to wrap the bare text, not the quote characters.
	bareText := unquoteSearchTerm(text)
	if opts.searchMode == "exact" {
		text = `"` + bareText + `"`
	}
	if opts.searchMode == "fuzzy" {
		text = "~" + bareText
	}

	result, err := eng.Search(db, block, text, q)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	var sb strings.Builder
	sb.WriteString("Query Plan\n")
	sb.WriteString(strings.Repeat("-", 60) + "\n")
	sb.WriteString(fmt.Sprintf("Command       : SEARCH %s FOR %q\n", block, text))
	writeWhereLine(&sb, q)
	sb.WriteString(fmt.Sprintf("Actual Access : %s\n", result.IndexUsed))
	sb.WriteString(fmt.Sprintf("Rows Scanned  : %d\n", result.Scanned))
	sb.WriteString(fmt.Sprintf("Rows Matched  : %d\n", result.Total))
	sb.WriteString(fmt.Sprintf("Limit/Offset  : %d / %d\n", q.Limit, q.Offset))
	sb.WriteString(fmt.Sprintf("Took          : %v\n", result.Took))
	sb.WriteString(strings.Repeat("-", 60) + "\n")
	sb.WriteString("Note: this EXPLAIN executed the query for real (like EXPLAIN ANALYZE)\n")
	sb.WriteString("rather than only estimating it -- the numbers above are measured, not guessed.\n")
	return sb.String()
}

// writeWhereLine renders q.Where (the parsed AST -- see
// internal/caimandb/parse/ast.go) when present, or "none" when the query has
// no WHERE clause at all.
func writeWhereLine(sb *strings.Builder, q *Query) {
	if q.Where != nil {
		sb.WriteString(fmt.Sprintf("Where         : %s\n", q.Where.String()))
		return
	}
	sb.WriteString("Where         : none\n")
}

// writePlannedLine reports what the query optimizer would use for index
// selection, based only on the top-level flattened equality/IN conditions
// (see parse.FlattenTopLevelAnd) -- the same limited view
// QueryOptimizer.AnalyzeQuery always had, AST or not.
func writePlannedLine(sb *strings.Builder, planned QueryPlan) {
	if planned.UseIndex {
		sb.WriteString(fmt.Sprintf("Planned       : index scan on '%s' = %s (predicted)\n",
			planned.IndexField, planned.IndexValue))
		return
	}
	sb.WriteString("Planned       : full scan (no usable equality/IN index candidate)\n")
}

func sortFieldsString(sorts []SortField) string {
	parts := make([]string, 0, len(sorts))
	for _, s := range sorts {
		if s.Desc {
			parts = append(parts, s.Field+" DESC")
		} else {
			parts = append(parts, s.Field)
		}
	}
	return strings.Join(parts, ", ")
}
