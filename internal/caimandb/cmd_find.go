package caimandb

import (
	"caimandb/internal/caimandb/query"
	"fmt"
	"strconv"
	"strings"
)

// fieldListStopWords are the tokens that terminate a comma-separated field
// list (SELECT/EXCLUDE/ORDER). The tokenizer (parse.Tokenize) only splits
// on whitespace, so "SELECT title, directors.name" arrives as two tokens
// ("title," and "directors.name") once there's a space after the comma.
// collectFieldList keeps consuming tokens past i+1 until it hits one of
// these keywords (or a --flag, or the end of input) so the whole list is
// captured instead of just the first token.
var fieldListStopWords = map[string]bool{
	"WHERE": true, "LIMIT": true, "ORDER": true, "SELECT": true,
	"EXCLUDE": true, "OFFSET": true, "ONLY": true,
	// SEARCH-only keywords; harmless to include here too since they're
	// not valid bare field names in either command.
	"WITH": true, "SHOW": true, "EXACT": true, "PHRASE": true,
	"FUZZY": true, "SIMILAR": true,
	// GROUP BY / HAVING (FIND only): stop a SELECT/ORDER field list here
	// too, so e.g. "SELECT title, COUNT(actors) AS n GROUP BY title"
	// doesn't swallow "GROUP" into the field list.
	"GROUP": true, "HAVING": true,
	"DISTINCT": true,
}

// collectFieldList reads a comma-separated field list starting right after
// tokens[*i] (the keyword itself, e.g. "SELECT"), advances *i past the
// consumed tokens, and returns the cleaned, trimmed field names. It copes
// with both "a,b,c" (one token) and "a, b, c" (several tokens, since the
// tokenizer only splits on whitespace) and strips a trailing ';'.
func collectFieldList(tokens []string, i *int) []string {
	j := *i + 1
	fields := collectFieldsFrom(tokens, &j)
	*i = j
	return fields
}

// collectFieldsFrom is the same comma-separated field-list reader as
// collectFieldList, except it starts reading at *i itself instead of
// *i+1. collectFieldList exists for callers positioned on the
// introducing keyword ("SELECT", "ORDER", ...); GROUP BY needs this
// variant because by the time the field list starts, the caller has
// already advanced past both "GROUP" and "BY".
func collectFieldsFrom(tokens []string, i *int) []string {
	var raw []string
	j := *i
	for j < len(tokens) {
		t := tokens[j]
		if fieldListStopWords[strings.ToUpper(t)] || strings.HasPrefix(t, "--") {
			break
		}
		raw = append(raw, t)
		j++
	}
	*i = j

	joined := strings.TrimSuffix(strings.TrimSpace(strings.Join(raw, " ")), ";")
	if joined == "" {
		return nil
	}

	var fields []string
	for _, f := range strings.Split(joined, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			fields = append(fields, f)
		}
	}
	return fields
}

// buildFindQuery parses the FIND-style modifiers (WHERE/LIMIT/OFFSET/
// SELECT/EXCLUDE/ORDER, plus the bare "field op value" shorthand) from
// tokens starting at index start, and returns the resulting Query.
//
// shared by handleFind and handleExplain (EXPLAIN FIND ...) so EXPLAIN is
// guaranteed to analyze exactly the query FIND would actually run, never
// a hand-maintained approximation of it.
func buildFindQuery(tokens []string, start int) (q *Query, outputAsTable bool, err error) {
	q = &Query{
		Filters:  []Filter{},
		Limit:    100,
		Offset:   0,
		UseIndex: true,
	}

	i := start
	for i < len(tokens) {
		token := tokens[i]

		if token == "--type:table" || token == "--format:table" {
			outputAsTable = true
			i++
			continue
		}

		switch strings.ToUpper(token) {
		case "WHERE":
			i++
			where, flat, werr := parseWhereClause(tokens, &i)
			if werr != nil {
				return nil, false, werr
			}
			q.Where = where
			q.Filters = flat
		case "LIMIT":
			if i+1 < len(tokens) {
				limit, cerr := strconv.Atoi(tokens[i+1])
				if cerr == nil && limit > 0 {
					q.Limit = limit
				}
				i += 2
			} else {
				return nil, false, fmt.Errorf("LIMIT requires value")
			}
		case "OFFSET":
			if i+1 < len(tokens) {
				offset, cerr := strconv.Atoi(tokens[i+1])
				if cerr == nil && offset >= 0 {
					q.Offset = offset
				}
				i += 2
			} else {
				return nil, false, fmt.Errorf("OFFSET requires value")
			}
		case "SELECT", "ONLY":
			if i+1 >= len(tokens) {
				return nil, false, fmt.Errorf("SELECT requires fields")
			}
			q.Select = collectFieldList(tokens, &i)
			if len(q.Select) == 0 {
				return nil, false, fmt.Errorf("SELECT requires fields")
			}
		case "EXCLUDE":
			if i+1 >= len(tokens) {
				return nil, false, fmt.Errorf("EXCLUDE requires fields")
			}
			q.Exclude = collectFieldList(tokens, &i)
			if len(q.Exclude) == 0 {
				return nil, false, fmt.Errorf("EXCLUDE requires fields")
			}
		case "ORDER":
			if i+1 >= len(tokens) {
				return nil, false, fmt.Errorf("ORDER requires fields")
			}
			fields := collectFieldList(tokens, &i)
			if len(fields) == 0 {
				return nil, false, fmt.Errorf("ORDER requires fields")
			}
			for _, f := range fields {
				q.Sort = append(q.Sort, parseSortField(f))
			}
		case "DISTINCT":
			// "FIND <block> DISTINCT" (no fields) dedupes on the whole
			// projected row; "FIND <block> DISTINCT field1,field2"
			// dedupes only on those field values. Mirrors how GROUP BY
			// reads its field list below, via the shared helper.
			i++
			fields := collectFieldsFrom(tokens, &i)
			if len(fields) == 0 {
				q.DistinctAll = true
			} else {
				q.Distinct = fields
			}
		case "GROUP":
			if i+1 >= len(tokens) || strings.ToUpper(tokens[i+1]) != "BY" {
				return nil, false, fmt.Errorf("GROUP requires BY <fields>")
			}
			i += 2
			fields := collectFieldsFrom(tokens, &i)
			if len(fields) == 0 {
				return nil, false, fmt.Errorf("GROUP BY requires fields")
			}
			q.GroupBy = fields
		case "HAVING":
			i++
			having, _, herr := parseWhereClause(tokens, &i)
			if herr != nil {
				return nil, false, herr
			}
			q.Having = having
		default:
			if i+2 < len(tokens) {
				operator := strings.ToUpper(tokens[i+1])
				if operator == "=" || operator == "==" || operator == "!=" ||
					operator == ">" || operator == "<" || operator == ">=" || operator == "<=" ||
					operator == "LIKE" || operator == "CONTAINS" {
					fieldName := token
					fieldValue := tokens[i+2]
					var fieldFilter Filter
					fieldFilter.Field = fieldName
					fieldFilter.Op = operator
					if num, nerr := strconv.ParseFloat(fieldValue, 64); nerr == nil {
						fieldFilter.Value = num
					} else {
						fieldValue = strings.Trim(fieldValue, "\"'")
						fieldFilter.Value = fieldValue
					}
					q.Filters = append(q.Filters, fieldFilter)
					i += 3
					continue
				}
			}
			i++
		}
	}

	for _, raw := range q.Select {
		q.SelectItems = append(q.SelectItems, query.ParseSelectItem(raw))
	}

	return q, outputAsTable, nil
}

func handleFind(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: FIND requires block name"
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

	if len(tokens) >= 3 {
		nextToken := tokens[2]
		upperNext := strings.ToUpper(nextToken)
		if upperNext != "WHERE" && upperNext != "LIMIT" && upperNext != "ORDER" &&
			upperNext != "SELECT" && upperNext != "EXCLUDE" && upperNext != "OFFSET" &&
			upperNext != "ONLY" && upperNext != "GROUP" && upperNext != "HAVING" &&
			upperNext != "DISTINCT" {
			doc, ok := eng.FindByID(db, block, nextToken)
			if !ok {
				return "Document not found: " + nextToken
			}
			eng.autoRelMgr.RecordAccess(db, block, doc.ID, sess.authUser, doc.Data)
			return jsonOut(doc)
		}
	}

	q, outputAsTable, err := buildFindQuery(tokens, 2)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	result, err := eng.Find(db, block, q)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	for _, doc := range result.Docs {
		eng.autoRelMgr.RecordAccess(db, block, doc.ID, sess.authUser, doc.Data)
	}

	if outputAsTable {
		return formatResultAsTable(result, q.Select)
	}

	var sb strings.Builder
	if result.Total == 0 {
		sb.WriteString("No documents found")
	} else {
		sb.WriteString(fmt.Sprintf("Found %d documents (scanned: %d, index: %s, took: %v)\n",
			result.Total, result.Scanned, result.IndexUsed, result.Took))
		for _, doc := range result.Docs {
			sb.WriteString(fmt.Sprintf("  _id: %s, shard: %s, data: %s\n",
				doc.ID, doc.ShardID, jsonOut(doc.Data)))
		}
	}
	return sb.String()
}

func formatResultAsTable(result *QueryResult, selectFields []string) string {
	if len(result.Docs) == 0 {
		return "No documents found"
	}

	var sb strings.Builder

	var fields []string
	if len(selectFields) > 0 {
		fields = selectFields
	} else {
		doc := result.Docs[0]
		fields = make([]string, 0, len(doc.Data)+2)
		fields = append(fields, "_id", "_shard_id")
		for k := range doc.Data {
			fields = append(fields, k)
		}
	}

	colWidths := make(map[string]int)
	for _, f := range fields {
		colWidths[f] = len(f)
	}

	for _, doc := range result.Docs {
		for _, f := range fields {
			var val string
			switch f {
			case "_id":
				val = doc.ID
			case "_shard_id":
				val = doc.ShardID
			default:
				if v, ok := doc.Data[f]; ok {
					val = fmt.Sprint(v)
				} else {
					val = ""
				}
			}
			if len(val) > colWidths[f] {
				colWidths[f] = len(val)
			}
		}
	}

	maxWidth := 120
	totalWidth := 0
	for _, f := range fields {
		totalWidth += colWidths[f] + 3
	}
	if totalWidth > maxWidth {
		for _, f := range fields {
			if colWidths[f] > 30 {
				colWidths[f] = 30
			}
		}
		totalWidth = 0
		for _, f := range fields {
			totalWidth += colWidths[f] + 3
		}
	}

	sb.WriteString("+" + strings.Repeat("-", totalWidth-1) + "+\n")

	sb.WriteString("|")
	for _, f := range fields {
		width := colWidths[f]
		if width < len(f) {
			width = len(f)
		}
		sb.WriteString(fmt.Sprintf(" %-*s |", width, f))
	}
	sb.WriteString("\n")

	sb.WriteString("+" + strings.Repeat("-", totalWidth-1) + "+\n")

	for _, doc := range result.Docs {
		sb.WriteString("|")
		for _, f := range fields {
			var val string
			switch f {
			case "_id":
				val = doc.ID
			case "_shard_id":
				val = doc.ShardID
			default:
				if v, ok := doc.Data[f]; ok {
					val = fmt.Sprint(v)
					if len(val) > 100 {
						val = val[:97] + "..."
					}
				} else {
					val = ""
				}
			}
			width := colWidths[f]
			if width < len(f) {
				width = len(f)
			}
			sb.WriteString(fmt.Sprintf(" %-*s |", width, val))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("+" + strings.Repeat("-", totalWidth-1) + "+\n")

	sb.WriteString(fmt.Sprintf("Total: %d documents (scanned: %d, index: %s, took: %v)\n",
		result.Total, result.Scanned, result.IndexUsed, result.Took))

	return sb.String()
}

func handleGet(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: GET requires block name and ID"
	}
	block := tokens[1]
	// "GET <block> @ <id>" is the documented alternative spelling of
	// "GET <block> <id>" (see docs/NQL_SYNTAX.en.md, FIND/GET). The "@"
	// arrives as its own token, so without this it was taken as the id
	// itself and every such GET answered "Document not found: @".
	id := tokens[2]
	if id == "@" {
		if len(tokens) < 4 {
			return "ERROR: GET <block> @ <id> requires an ID after '@'"
		}
		id = tokens[3]
	}
	id = strings.Trim(id, "\"'")
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	doc, ok := eng.FindByID(db, block, id)
	if !ok {
		return "Document not found: " + id
	}
	return jsonOut(doc)
}

// searchOptions holds the SEARCH-only modifiers (WITH SCORE/MATCHES,
// EXACT/PHRASE, FUZZY/SIMILAR) that sit alongside the shared Query.
type searchOptions struct {
	showScore   bool
	showMatches bool
	searchMode  string // "default", "exact", "fuzzy"
}

// buildSearchQuery parses SEARCH-style modifiers (LIMIT/OFFSET/WHERE/
// ORDER/SELECT, plus the SEARCH-only WITH/SHOW/EXACT/PHRASE/FUZZY/SIMILAR
// options) from tokens starting at index start. Shared by handleSearch and
// handleExplain (EXPLAIN SEARCH ...).
func buildSearchQuery(tokens []string, start int) (q *Query, opts searchOptions, err error) {
	q = &Query{
		Filters:    []Filter{},
		Limit:      100,
		Offset:     0,
		UseIndex:   false,
		PreferScan: true,
	}
	opts.searchMode = "default"

	i := start
	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
		case "LIMIT":
			if i+1 < len(tokens) {
				limit, cerr := strconv.Atoi(tokens[i+1])
				if cerr == nil && limit > 0 {
					q.Limit = limit
				}
				i += 2
			} else {
				return nil, opts, fmt.Errorf("LIMIT requires value")
			}
		case "OFFSET":
			if i+1 < len(tokens) {
				offset, cerr := strconv.Atoi(tokens[i+1])
				if cerr == nil && offset >= 0 {
					q.Offset = offset
				}
				i += 2
			} else {
				return nil, opts, fmt.Errorf("OFFSET requires value")
			}
		case "WHERE":
			i++
			where, flat, werr := parseWhereClause(tokens, &i)
			if werr != nil {
				return nil, opts, werr
			}
			q.Where = where
			q.Filters = flat
		case "ORDER":
			if i+1 >= len(tokens) {
				return nil, opts, fmt.Errorf("ORDER requires fields")
			}
			fields := collectFieldList(tokens, &i)
			if len(fields) == 0 {
				return nil, opts, fmt.Errorf("ORDER requires fields")
			}
			for _, f := range fields {
				q.Sort = append(q.Sort, parseSortField(f))
			}
		case "ONLY", "SELECT":
			if i+1 >= len(tokens) {
				return nil, opts, fmt.Errorf("SELECT requires fields")
			}
			q.Select = collectFieldList(tokens, &i)
			if len(q.Select) == 0 {
				return nil, opts, fmt.Errorf("SELECT requires fields")
			}
		case "WITH", "SHOW":
			if i+1 < len(tokens) {
				opt := strings.ToUpper(tokens[i+1])
				if opt == "SCORE" || opt == "RELEVANCE" {
					opts.showScore = true
					i += 2
				} else if opt == "MATCHES" {
					opts.showMatches = true
					i += 2
				} else {
					i++
				}
			} else {
				i++
			}
		case "EXACT", "PHRASE":
			opts.searchMode = "exact"
			i++
		case "FUZZY", "SIMILAR":
			opts.searchMode = "fuzzy"
			i++
		default:
			i++
		}
	}
	return q, opts, nil
}

func handleSearch(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: SEARCH requires block name and search text"
	}
	block := tokens[1]
	text := tokens[2]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	dbs, err := eng.ShowDBs()
	if err != nil {
		return "ERROR: " + err.Error()
	}
	dbExists := false
	for _, d := range dbs {
		if d == db {
			dbExists = true
			break
		}
	}
	if !dbExists {
		return "ERROR: Database not found: " + db
	}

	blocks, err := eng.ShowBlocks(db)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	blockExists := false
	for _, b := range blocks {
		if b == block {
			blockExists = true
			break
		}
	}
	if !blockExists {
		return "ERROR: Block not found: " + block
	}
	if err := eng.authorizeBlockAccess(sess, db, block, "read"); err != nil {
		return "ERROR: " + err.Error()
	}

	q, opts, err := buildSearchQuery(tokens, 3)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	// tokens[2] arrives with the tokenizer's literal quote characters
	// still attached (see unquoteSearchTerm's doc comment) -- EXACT and
	// FUZZY each add their own marker (`"..."` / `~...`) around the
	// user's actual words, so that marker has to wrap the *unquoted*
	// text or it ends up wrapping the quote characters themselves
	// (FUZZY on "elefant" produced the literal text ~"elefant" --
	// tilde, then quote, then the word -- which matched nothing).
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
	if result.Total == 0 {
		sb.WriteString("No documents found matching search")
	} else {
		sb.WriteString(fmt.Sprintf("[SEARCH] Found %d documents matching '%s' (scanned: %d, took: %v)\n",
			result.Total, text, result.Scanned, result.Took))

		for idx, doc := range result.Docs {
			if opts.showScore {
				// Score against the same bare words Search() itself
				// matched on -- recomputing with the quoted/prefixed
				// text (the bug this replaces) compared literal
				// document values against a string that still had
				// stray "/~ characters in it, so strings.Contains
				// never matched and every result showed Score: 0.00
				// regardless of how good the match actually was.
				searchText := strings.ToLower(strings.TrimSpace(bareText))
				words := strings.Fields(searchText)
				score, matches := calculateRelevance(doc, searchText, words)
				sb.WriteString(fmt.Sprintf("  %d. Score: %.2f | _id: %s, shard: %s, data: %s\n",
					idx+1, score, doc.ID, doc.ShardID, jsonOut(doc.Data)))
				if opts.showMatches && len(matches) > 0 {
					sb.WriteString(fmt.Sprintf("     Matches: %s\n", strings.Join(matches, ", ")))
				}
			} else {
				sb.WriteString(fmt.Sprintf("  %d. _id: %s, shard: %s, data: %s\n",
					idx+1, doc.ID, doc.ShardID, jsonOut(doc.Data)))
			}
		}
	}
	return sb.String()
}
