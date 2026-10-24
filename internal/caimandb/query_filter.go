package caimandb

import (
	"fmt"
	"regexp"
	"strings"

	"caimandb/internal/caimandb/parse"
	"caimandb/internal/caimandb/query"
)

// matchesQuery decides whether doc satisfies q. When q.Where is set
// (currently: queries built by handleFind/handleSearch) it evaluates
// the full WHERE-clause expression tree, which correctly supports
// parentheses and AND/OR/NOT precedence. Otherwise it falls back to
// matchesFilters, the historical flat left-to-right evaluation, so
// every other command that still builds a bare []Filter (JOIN,
// transactions, CLEAR/COUNT/aggregate, admin commands) keeps behaving
// exactly as it did before this file changed.
func matchesQuery(doc Document, q *Query) bool {
	if q.Where != nil {
		return evalExpr(doc, q.Where)
	}
	return matchesFilters(doc, q.Filters)
}

// evalExpr walks a parse.Expr tree (see internal/caimandb/parse/ast.go)
// and evaluates it against doc. Leaf nodes reuse matchFilter, the same
// single-condition matcher the flat filter list already used, so a
// leaf behaves identically whether it arrived via the old []Filter
// path or the new AST.
func evalExpr(doc Document, e *parse.Expr) bool {
	if e == nil {
		return true
	}
	switch e.Kind {
	case parse.KindAnd:
		return evalExpr(doc, e.Left) && evalExpr(doc, e.Right)
	case parse.KindOr:
		return evalExpr(doc, e.Left) || evalExpr(doc, e.Right)
	case parse.KindNot:
		return !evalExpr(doc, e.Child)
	default:
		return matchFilter(doc, Filter{
			Field:  e.Field,
			Op:     e.Op,
			Value:  e.Value,
			Value2: e.Value2,
			Values: e.Values,
		})
	}
}

// matchesWhere is the Delete/Count/CLEAR counterpart of matchesQuery
// above: when where is set (a command that went through
// parseWhereClause, e.g. the migrated DELETE/COUNT) it evaluates the
// full expression tree -- parentheses, NOT, and correct AND/OR
// precedence included. Otherwise it falls back to matchesFilters, the
// flat left-to-right evaluation, for the remaining callers that only
// ever have a []Filter and no tree: EmptyBlock's "match everything"
// empty filter set, and WAL entries written before this migration
// (see wal_recovery.go).
func matchesWhere(doc Document, where *parse.Expr, filters []Filter) bool {
	if where != nil {
		return evalExpr(doc, where)
	}
	return matchesFilters(doc, filters)
}

func matchesFilters(doc Document, filters []Filter) bool {
	if len(filters) == 0 {
		return true
	}
	result := false
	first := true
	for _, f := range filters {
		m := matchFilter(doc, f)
		if first {
			result = m
			first = false
		} else if strings.EqualFold(f.Logic, "OR") {
			result = result || m
		} else {
			result = result && m
		}
	}
	return result
}

// matchFilter evaluates a single "field op value" condition against doc.
// f.Field is resolved via getNestedValues rather than a plain single-value
// lookup: for an ordinary scalar/nested-object path this behaves exactly
// like before (one value, matched once), but when the path passes through
// an array of objects -- e.g. "genres.name" after a one-to-many RELATE
// alias has been attached to doc.Data by attachRelationsForFilter, or any
// plain array-of-objects field -- it matches if *any* element satisfies
// the condition, the usual join semantics for a one-to-many relationship
// in a WHERE clause.
func matchFilter(doc Document, f Filter) bool {
	if fc, ok := query.ParseFuncCall(f.Field); ok {
		// A function-call field (e.g. "YEAR(fecha)", from
		// parse.ast.go's tryParseFunctionField) always yields exactly
		// one value per document, unlike a plain path through an
		// array of objects -- so it skips the multi-value fan-out
		// below entirely.
		val, err := query.EvalFuncCall(fc, doc.Data)
		if err != nil {
			return false
		}
		return matchValue(val, f)
	}
	vals := getNestedValues(doc.Data, f.Field)
	if len(vals) <= 1 {
		var val any
		if len(vals) == 1 {
			val = vals[0]
		}
		return matchValue(val, f)
	}
	for _, val := range vals {
		if matchValue(val, f) {
			return true
		}
	}
	return false
}

func matchValue(val any, f Filter) bool {
	switch strings.ToUpper(f.Op) {
	case "=", "==":
		return fmt.Sprint(val) == fmt.Sprint(f.Value)
	case "!=", "<>":
		return fmt.Sprint(val) != fmt.Sprint(f.Value)
	case ">":
		return compareNumbers(val, f.Value) > 0
	case "<":
		return compareNumbers(val, f.Value) < 0
	case ">=":
		return compareNumbers(val, f.Value) >= 0
	case "<=":
		return compareNumbers(val, f.Value) <= 0
	case "LIKE":
		s := strings.ToLower(fmt.Sprint(val))
		p := strings.ToLower(fmt.Sprint(f.Value))
		if strings.HasPrefix(p, "%") && strings.HasSuffix(p, "%") {
			return strings.Contains(s, strings.Trim(p, "%"))
		} else if strings.HasPrefix(p, "%") {
			return strings.HasSuffix(s, strings.TrimPrefix(p, "%"))
		} else if strings.HasSuffix(p, "%") {
			return strings.HasPrefix(s, strings.TrimSuffix(p, "%"))
		}
		return s == p
	case "NOT LIKE":
		s := strings.ToLower(fmt.Sprint(val))
		p := strings.ToLower(fmt.Sprint(f.Value))
		if strings.HasPrefix(p, "%") && strings.HasSuffix(p, "%") {
			return !strings.Contains(s, strings.Trim(p, "%"))
		} else if strings.HasPrefix(p, "%") {
			return !strings.HasSuffix(s, strings.TrimPrefix(p, "%"))
		} else if strings.HasSuffix(p, "%") {
			return !strings.HasPrefix(s, strings.TrimSuffix(p, "%"))
		}
		return s != p
	case "CONTAINS":
		// TODO(pendiente-motor): CONTAINS solo hace substring sobre
		// fmt.Sprint(val) -- si val es un array (etiquetas: [...]), lo
		// convierte a su representación de texto en vez de revisar
		// membresía. Falta: (a) detectar val.([]any) aquí y comparar
		// elemento por elemento en vez de con fmt.Sprint; (b) agregar
		// los operadores nuevos CONTAINS ALL / CONTAINS ANY (recorren
		// f.Values contra el array, con AND/OR respectivamente en vez
		// de un solo f.Value); (c) el caso inverso "valor" IN etiquetas
		// (membresía invertida) necesita su propio operador porque hoy
		// IN (más abajo) asume que val es escalar y f.Values es la
		// lista, no al revés.
		return strings.Contains(strings.ToLower(fmt.Sprint(val)), strings.ToLower(fmt.Sprint(f.Value)))
	case "NOT CONTAINS":
		return !strings.Contains(strings.ToLower(fmt.Sprint(val)), strings.ToLower(fmt.Sprint(f.Value)))
	case "EXISTS":
		return val != nil
	case "IN":
		sv := fmt.Sprint(val)
		for _, v := range f.Values {
			if fmt.Sprint(v) == sv {
				return true
			}
		}
		return false
	case "NOT IN":
		sv := fmt.Sprint(val)
		for _, v := range f.Values {
			if fmt.Sprint(v) == sv {
				return false
			}
		}
		return true
	case "BETWEEN":
		return compareNumbers(val, f.Value) >= 0 && compareNumbers(val, f.Value2) <= 0
	case "STARTS WITH":
		return strings.HasPrefix(fmt.Sprint(val), fmt.Sprint(f.Value))
	case "ENDS WITH":
		return strings.HasSuffix(fmt.Sprint(val), fmt.Sprint(f.Value))
	case "IS NULL":
		return val == nil || fmt.Sprint(val) == ""
	case "IS NOT NULL":
		return val != nil && fmt.Sprint(val) != ""
	case "REGEXP", "RLIKE":
		pattern := fmt.Sprint(f.Value)
		re, err := regexp.Compile(pattern)
		if err != nil {
			// An invalid pattern never matches, rather than erroring the
			// whole query -- consistent with LIKE/CONTAINS above, which
			// also degrade to "no match" instead of failing the request.
			return false
		}
		return re.MatchString(fmt.Sprint(val))
	}
	return false
}

func compareNumbers(a, b any) int {
	fa := toF64(a)
	fb := toF64(b)

	if fa != 0 || fb != 0 {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	sa := fmt.Sprint(a)
	sb := fmt.Sprint(b)
	return strings.Compare(sa, sb)
}

// singleIDEquality reports whether the given filter set -- as either the
// legacy flat []Filter list or a parsed WHERE expression tree -- reduces
// to exactly one condition, "id = <literal>", with nothing else AND'd,
// OR'd, or NOT'd around it. ("_id" is normalized to "id" by both parsers
// before this ever sees it -- see parseCondition in parse/ast.go and
// parseFilters in cmd_filters_util.go.)
//
// This is logically the same "find one document by its key" operation
// FindByID already serves in O(1) via a direct txn.Get(docKey(...)).
// updateLocal/deleteLocal (ops_local.go) use this to route that exact
// case through the same kind of direct key lookup instead of taking the
// block-wide lock and then scanning every document in the block -- see
// docs/load-testing-findings.md for the concurrent-write-load latency
// gap this closes (measured there at ~14,000x-19,000x FIND's p50 for a
// query that, like this one, only ever matches a single document).
//
// A tree or filter list that ANDs another condition onto the id
// equality, ORs it with anything, or wraps it in NOT does not qualify:
// parse.FlattenTopLevelAnd already rejects OR/NOT (returning ok=false),
// and the len(leaves) != 1 check below additionally rejects a pure
// AND-chain like "id = X AND other = Y" -- FlattenTopLevelAnd would
// otherwise happily flatten that into two leaves, but matching only on
// the id half would be wrong: it's no longer a single-key lookup, it's
// a filtered lookup that happens to include the id.
func singleIDEquality(filters []Filter, where *parse.Expr) (id string, ok bool) {
	if where != nil {
		leaves, flatOK := parse.FlattenTopLevelAnd(where)
		if !flatOK || len(leaves) != 1 {
			return "", false
		}
		leaf := leaves[0]
		if leaf.Kind != parse.KindCondition {
			return "", false
		}
		return idEqualityLiteral(leaf.Field, leaf.Op, leaf.Value)
	}
	if len(filters) != 1 {
		return "", false
	}
	return idEqualityLiteral(filters[0].Field, filters[0].Op, filters[0].Value)
}

// idEqualityLiteral checks a single "field op value" condition against
// the id-equality shape singleIDEquality looks for, and renders value
// the same way matchValue's "=" case already would (fmt.Sprint on both
// sides) so a bare numeric literal (WHERE id = 123, coerced to
// float64(123) by both parsers) resolves to the same key string
// ("123") that a quoted WHERE id = "123" -- or the id as originally
// stored -- would.
func idEqualityLiteral(field, op string, value any) (string, bool) {
	if field != "id" {
		return "", false
	}
	if op != "=" && op != "==" {
		return "", false
	}
	if value == nil {
		return "", false
	}
	return fmt.Sprint(value), true
}
