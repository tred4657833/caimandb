package caimandb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"caimandb/internal/caimandb/parse"
)

// autoRelModifierStopWords are the keywords that introduce (or end) a
// SHOW AUTORELATIONS modifier -- used the same way fieldListStopWords
// is used for FIND field lists, to know where a free-form token span
// (WHERE/FILTER's expression) ends.
var autoRelModifierStopWords = map[string]bool{
	"FROM": true, "TO": true, "DEPTH": true, "DIRECTION": true,
	"FORMAT": true, "ORDER": true, "BY": true, "LIMIT": true, "OFFSET": true,
	"FILTER": true, "WHERE": true, "STATS": true, "PATHS": true, "ORPHANS": true,
	"CYCLES": true, "BROKEN": true, "SUMMARY": true, "VERBOSE": true,
}

// autoRelOrderFields are the valid ORDER BY fields for SHOW AUTORELATIONS.
// DEGREE/ID/NAME are graph/display properties computed on the fly; the
// rest map straight onto AutoRelation's own fields.
var autoRelOrderFields = map[string]bool{
	"DEGREE": true, "ID": true, "NAME": true,
	"ACCESS_COUNT": true, "RELEVANCE": true,
	"LAST_SEEN": true, "FIRST_SEEN": true,
}

// autoRelShowQuery is the parsed form of:
//
//	SHOW AUTORELATIONS <block>
//	    [FROM <id>] [TO <id>] [DEPTH <n>] [DIRECTION IN|OUT|BOTH]
//	    [FORMAT TABLE|TREE|GRAPH|JSON]
//	    [WHERE|FILTER <expression>]
//	    [ORDER BY DEGREE|ID|NAME|ACCESS_COUNT|RELEVANCE|LAST_SEEN|FIRST_SEEN [ASC|DESC]]
//	    [LIMIT <n>] [OFFSET <n>]
//	    [STATS] [PATHS] [ORPHANS] [CYCLES] [BROKEN] [SUMMARY] [VERBOSE];
//
// WHERE and FILTER are interchangeable spellings of the same modifier --
// WHERE matches the condition syntax used everywhere else (FIND, SEARCH,
// VIEW), FILTER is kept for backward compatibility.
type autoRelShowQuery struct {
	Block     string
	From      string
	To        string
	Depth     int
	Direction string // BOTH (default), IN, OUT
	Format    string // TABLE (default), TREE, GRAPH, JSON
	OrderBy   string // "" (relevance, the manager's default), DEGREE, ID, NAME, ACCESS_COUNT, RELEVANCE, LAST_SEEN, FIRST_SEEN
	OrderDir  string // "" (field-specific default), ASC, DESC
	Limit     int
	Offset    int
	Filter    *parse.Expr

	Stats   bool
	Paths   bool
	Orphans bool
	Cycles  bool
	Broken  bool
	Summary bool
	Verbose bool
}

// parseAutoRelShowQuery parses everything after "SHOW AUTORELATIONS":
// args[0] is the block name, args[1:] are the optional modifiers.
func parseAutoRelShowQuery(args []string) (*autoRelShowQuery, error) {
	if len(args) == 0 || autoRelModifierStopWords[strings.ToUpper(strings.TrimSuffix(args[0], ";"))] {
		return nil, fmt.Errorf("SHOW AUTORELATIONS requires a block name: SHOW AUTORELATIONS <block>")
	}

	q := &autoRelShowQuery{
		Block:     strings.TrimSuffix(args[0], ";"),
		Direction: "BOTH",
		Format:    "TABLE",
		Depth:     1,
	}

	i := 1
	for i < len(args) {
		raw := strings.TrimSuffix(args[i], ";")
		tok := strings.ToUpper(raw)

		switch tok {
		case "FROM":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("FROM requires an id")
			}
			q.From = strings.Trim(strings.TrimSuffix(args[i+1], ";"), "\"'")
			i += 2

		case "TO":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("TO requires an id")
			}
			q.To = strings.Trim(strings.TrimSuffix(args[i+1], ";"), "\"'")
			i += 2

		case "DEPTH":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("DEPTH requires a number")
			}
			n, err := strconv.Atoi(strings.TrimSuffix(args[i+1], ";"))
			if err != nil || n < 1 {
				return nil, fmt.Errorf("DEPTH requires a positive integer")
			}
			q.Depth = n
			i += 2

		case "DIRECTION":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("DIRECTION requires IN, OUT, or BOTH")
			}
			dir := strings.ToUpper(strings.TrimSuffix(args[i+1], ";"))
			if dir != "IN" && dir != "OUT" && dir != "BOTH" {
				return nil, fmt.Errorf("DIRECTION must be IN, OUT, or BOTH")
			}
			q.Direction = dir
			i += 2

		case "FORMAT":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("FORMAT requires TABLE, TREE, GRAPH, or JSON")
			}
			f := strings.ToUpper(strings.TrimSuffix(args[i+1], ";"))
			if f != "TABLE" && f != "TREE" && f != "GRAPH" && f != "JSON" {
				return nil, fmt.Errorf("FORMAT must be TABLE, TREE, GRAPH, or JSON")
			}
			q.Format = f
			i += 2

		case "ORDER":
			if i+1 >= len(args) || strings.ToUpper(strings.TrimSuffix(args[i+1], ";")) != "BY" {
				return nil, fmt.Errorf("ORDER requires BY <DEGREE|ID|NAME|ACCESS_COUNT|RELEVANCE|LAST_SEEN|FIRST_SEEN>")
			}
			if i+2 >= len(args) {
				return nil, fmt.Errorf("ORDER BY requires DEGREE, ID, NAME, ACCESS_COUNT, RELEVANCE, LAST_SEEN, or FIRST_SEEN")
			}
			obRaw := strings.TrimSuffix(args[i+2], ";")
			ob := strings.ToUpper(obRaw)
			if !autoRelOrderFields[ob] {
				return nil, fmt.Errorf("ORDER BY must be one of DEGREE, ID, NAME, ACCESS_COUNT, RELEVANCE, LAST_SEEN, FIRST_SEEN")
			}
			q.OrderBy = ob
			i += 3

			// Optional trailing ASC|DESC direction, e.g. ORDER BY ACCESS_COUNT DESC.
			if i < len(args) {
				dirTok := strings.ToUpper(strings.TrimSuffix(args[i], ";"))
				if dirTok == "ASC" || dirTok == "DESC" {
					q.OrderDir = dirTok
					i++
				}
			}

		case "LIMIT":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("LIMIT requires a number")
			}
			n, err := strconv.Atoi(strings.TrimSuffix(args[i+1], ";"))
			if err != nil || n < 0 {
				return nil, fmt.Errorf("LIMIT requires a non-negative integer")
			}
			q.Limit = n
			i += 2

		case "OFFSET":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("OFFSET requires a number")
			}
			n, err := strconv.Atoi(strings.TrimSuffix(args[i+1], ";"))
			if err != nil || n < 0 {
				return nil, fmt.Errorf("OFFSET requires a non-negative integer")
			}
			q.Offset = n
			i += 2

		case "FILTER", "WHERE":
			j := i + 1
			var exprTokens []string
			for j < len(args) {
				stripped := strings.TrimSuffix(args[j], ";")
				if autoRelModifierStopWords[strings.ToUpper(stripped)] {
					break
				}
				exprTokens = append(exprTokens, stripped)
				j++
			}
			if len(exprTokens) == 0 {
				return nil, fmt.Errorf("%s requires a condition, e.g. %s access_count > 10", tok, tok)
			}
			expr, consumed, err := parse.ParseWhere(exprTokens)
			if err != nil {
				return nil, fmt.Errorf("invalid %s expression: %w", tok, err)
			}
			if consumed != len(exprTokens) {
				return nil, fmt.Errorf("invalid %s expression near %q", tok, exprTokens[consumed])
			}
			q.Filter = expr
			i = j

		case "STATS":
			q.Stats = true
			i++
		case "PATHS":
			q.Paths = true
			i++
		case "ORPHANS":
			q.Orphans = true
			i++
		case "CYCLES":
			q.Cycles = true
			i++
		case "BROKEN":
			q.Broken = true
			i++
		case "SUMMARY":
			q.Summary = true
			i++
		case "VERBOSE":
			q.Verbose = true
			i++

		default:
			return nil, fmt.Errorf("unexpected token in SHOW AUTORELATIONS: %q", args[i])
		}
	}

	return q, nil
}

// asFilterDoc exposes an AutoRelation's fields as a synthetic Document
// so FILTER can reuse the exact same WHERE-clause evaluator FIND uses
// (matchesQuery / matchFilter in query_filter.go), instead of a
// second, parallel condition evaluator.
func autoRelFilterDoc(ar *AutoRelation) Document {
	return Document{
		ID: ar.DocID,
		Data: map[string]any{
			"doc_id":       ar.DocID,
			"user_id":      ar.UserID,
			"access_count": float64(ar.AccessCount),
			"relevance":    ar.Relevance,
			"last_seen":    ar.LastSeen.Unix(),
			"first_seen":   ar.FirstSeen.Unix(),
		},
	}
}

// autoRelDisplayName returns the friendliest available label for a
// relation's document: its sampled name/title if one was captured,
// otherwise the bare doc id.
func autoRelDisplayName(ar *AutoRelation) string {
	for _, key := range []string{"name", "title", "label"} {
		if v, ok := ar.KeyMetadata[key]; ok {
			return fmt.Sprint(v)
		}
	}
	return ar.DocID
}

func handleShowAutoRelations(eng *Engine, sess *Session, args []string) string {
	q, err := parseAutoRelShowQuery(args)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, q.Block) {
		return "ERROR: Block not found: " + q.Block
	}

	all := eng.autoRelMgr.List(db, q.Block)
	if len(all) == 0 {
		return "No auto-relations for " + db + "." + q.Block +
			" yet (they appear once a user reads the same document repeatedly)"
	}

	working := all
	var pathOrder []*bfsNode // populated only when FROM is given, for TREE/PATHS rendering

	if q.From != "" {
		g := buildAutoRelGraph(all)
		kind, ok := g.resolveNode(q.From)
		if !ok {
			return fmt.Sprintf("No auto-relations found starting at id %q in %s.%s", q.From, db, q.Block)
		}
		order := g.bfs(kind, q.From, q.Direction, q.Depth)
		pathOrder = order

		seen := make(map[*AutoRelation]bool)
		var rels []*AutoRelation
		for _, n := range order {
			if n.via != nil && !seen[n.via] {
				seen[n.via] = true
				rels = append(rels, n.via)
			}
		}
		working = rels
		if len(working) == 0 {
			return fmt.Sprintf("%q has no auto-relations within DEPTH %d / DIRECTION %s in %s.%s",
				q.From, q.Depth, q.Direction, db, q.Block)
		}
	}

	if q.To != "" {
		var filtered []*AutoRelation
		for _, r := range working {
			if r.DocID == q.To || r.UserID == q.To {
				filtered = append(filtered, r)
			}
		}
		working = filtered
	}

	if q.Filter != nil {
		var filtered []*AutoRelation
		for _, r := range working {
			if evalExpr(autoRelFilterDoc(r), q.Filter) {
				filtered = append(filtered, r)
			}
		}
		working = filtered
	}

	if len(working) == 0 {
		return "No auto-relations match those conditions in " + db + "." + q.Block
	}

	g := buildAutoRelGraph(working)
	brokenList := findBrokenAutoRelations(eng, db, q.Block, working)
	brokenSet := make(map[*AutoRelation]bool, len(brokenList))
	for _, r := range brokenList {
		brokenSet[r] = true
	}

	switch {
	case q.Broken:
		working = brokenList
	case q.Cycles:
		working = findCycles(working)
	case q.Orphans:
		working = findOrphans(g)
	}

	if len(working) == 0 {
		return "No matches for that view in " + db + "." + q.Block
	}

	sortAutoRelations(working, g, q.OrderBy, q.OrderDir)

	if q.Offset > 0 && q.Offset < len(working) {
		working = working[q.Offset:]
	} else if q.Offset >= len(working) {
		working = nil
	}
	if q.Limit > 0 && len(working) > q.Limit {
		working = working[:q.Limit]
	}

	var sb strings.Builder

	if q.Stats {
		stats := computeAutoRelStats(g, len(brokenList))
		sb.WriteString(renderAutoRelStats(db, q.Block, stats))
		sb.WriteString("\n")
	}
	if q.Summary {
		stats := computeAutoRelStats(g, len(brokenList))
		sb.WriteString(renderAutoRelSummary(db, q.Block, stats))
		sb.WriteString("\n")
	}

	if len(working) == 0 {
		if sb.Len() > 0 {
			return strings.TrimRight(sb.String(), "\n")
		}
		return "No auto-relations to display for " + db + "." + q.Block
	}

	switch {
	case q.Format == "JSON":
		sb.WriteString(jsonOut(working))
	case (q.Format == "TREE" || q.Paths) && q.From != "":
		sb.WriteString(renderAutoRelTree(pathOrder, q.Verbose))
	case q.Format == "GRAPH":
		sb.WriteString(renderAutoRelGraph(g))
	default:
		sb.WriteString(renderAutoRelTable(db, q.Block, working, brokenSet, q.Verbose))
	}

	return strings.TrimRight(sb.String(), "\n")
}

// findBrokenAutoRelations returns the relations in rels whose document
// no longer exists in the block -- i.e. it was deleted after the
// auto-relation was created but before it expired.
func findBrokenAutoRelations(eng *Engine, db, block string, rels []*AutoRelation) []*AutoRelation {
	checked := make(map[string]bool)
	var broken []*AutoRelation
	for _, r := range rels {
		if checked[r.DocID] {
			continue
		}
		checked[r.DocID] = true
		if _, ok := eng.FindByID(db, block, r.DocID); !ok {
			for _, dup := range rels {
				if dup.DocID == r.DocID {
					broken = append(broken, dup)
				}
			}
		}
	}
	return broken
}

// sortAutoRelations orders rels in place by orderBy. Each field has a
// sensible default direction (DEGREE/ACCESS_COUNT/RELEVANCE/LAST_SEEN/
// FIRST_SEEN default to descending -- "busiest/most recent first";
// ID/NAME default to ascending, i.e. alphabetical). orderDir, when
// ASC or DESC, overrides that default explicitly.
func sortAutoRelations(rels []*AutoRelation, g *autoRelGraph, orderBy, orderDir string) {
	wantDesc := func(defaultDesc bool) bool {
		switch orderDir {
		case "ASC":
			return false
		case "DESC":
			return true
		default:
			return defaultDesc
		}
	}

	switch orderBy {
	case "DEGREE":
		desc := wantDesc(true)
		sort.Slice(rels, func(i, j int) bool {
			di, ui := g.degree(rels[i])
			dj, uj := g.degree(rels[j])
			if desc {
				return (di + ui) > (dj + uj)
			}
			return (di + ui) < (dj + uj)
		})
	case "ID":
		desc := wantDesc(false)
		sort.Slice(rels, func(i, j int) bool {
			if desc {
				return rels[i].DocID > rels[j].DocID
			}
			return rels[i].DocID < rels[j].DocID
		})
	case "NAME":
		desc := wantDesc(false)
		sort.Slice(rels, func(i, j int) bool {
			ni, nj := autoRelDisplayName(rels[i]), autoRelDisplayName(rels[j])
			if desc {
				return ni > nj
			}
			return ni < nj
		})
	case "ACCESS_COUNT":
		desc := wantDesc(true)
		sort.Slice(rels, func(i, j int) bool {
			if desc {
				return rels[i].AccessCount > rels[j].AccessCount
			}
			return rels[i].AccessCount < rels[j].AccessCount
		})
	case "RELEVANCE":
		desc := wantDesc(true)
		sort.Slice(rels, func(i, j int) bool {
			if desc {
				return rels[i].Relevance > rels[j].Relevance
			}
			return rels[i].Relevance < rels[j].Relevance
		})
	case "LAST_SEEN":
		desc := wantDesc(true)
		sort.Slice(rels, func(i, j int) bool {
			if desc {
				return rels[i].LastSeen.After(rels[j].LastSeen)
			}
			return rels[i].LastSeen.Before(rels[j].LastSeen)
		})
	case "FIRST_SEEN":
		desc := wantDesc(true)
		sort.Slice(rels, func(i, j int) bool {
			if desc {
				return rels[i].FirstSeen.After(rels[j].FirstSeen)
			}
			return rels[i].FirstSeen.Before(rels[j].FirstSeen)
		})
	default:
		// No ORDER BY given: already relevance-sorted (descending) by
		// AutoRelationManager.List.
	}
}
