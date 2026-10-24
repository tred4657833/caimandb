// Package parse: this file adds a real expression tree (AST) for NQL
// WHERE clauses, on top of the existing Tokenize().
//
// Before this file, WHERE was parsed into a *flat* []Filter list with a
// per-item Logic ("AND"/"OR") tag and evaluated strictly left-to-right
// (see matchesFilters/matchFilter in the main caimandb package). That
// representation cannot express operator precedence or grouping at
// all: "a=1 OR b=2 AND c=3" and "(a=1 OR b=2) AND c=3" were literally
// the same query, and there was no way to write the first one on
// purpose. It is also not a tree, so nothing else (an optimizer, an
// EXPLAIN command, a different storage backend) can walk or rewrite it.
//
// ParseWhere below builds a proper binary expression tree: leaves are
// single "field op value" conditions, interior nodes are AND/OR/NOT.
// Parentheses in the input become real nesting in the tree, and AND
// binds tighter than OR, matching standard SQL precedence.
//
// encoding/json from the standard library, no reference to Engine,
// Document, Config, Session, Filter or Transaction), for the same
// reason tokenizer.go was moved into this package: see
// docs/known-limitations.md.
package parse

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Kind identifies the shape of an Expr node.
type Kind int

const (
	// KindCondition is a leaf: a single "field op value" test.
	KindCondition Kind = iota
	// KindAnd/KindOr/KindNot are interior nodes combining sub-expressions.
	KindAnd
	KindOr
	KindNot
)

func (k Kind) String() string {
	switch k {
	case KindCondition:
		return "CONDITION"
	case KindAnd:
		return "AND"
	case KindOr:
		return "OR"
	case KindNot:
		return "NOT"
	default:
		return "UNKNOWN"
	}
}

// Expr is a node in the WHERE-clause expression tree produced by
// ParseWhere. Exactly one of the two shapes below is populated,
// depending on Kind:
//
//   - KindCondition: Field, Op, and one of Value/Value2/Values.
//   - KindAnd/KindOr: Left and Right.
//   - KindNot: Child.
type Expr struct {
	Kind Kind

	// Leaf condition fields (Kind == KindCondition).
	Field  string
	Op     string
	Value  any
	Value2 any
	Values []any

	// Binary node fields (Kind == KindAnd || Kind == KindOr).
	Left  *Expr
	Right *Expr

	// Unary node field (Kind == KindNot).
	Child *Expr
}

// Walk calls fn for every node in the tree rooted at e, in pre-order
// (the node itself before its children). It's the hook an optimizer,
// an EXPLAIN command, or a rewrite pass would use to inspect or
// transform the AST without needing to know about matchFilter/
// matchesFilters in the main package.
func (e *Expr) Walk(fn func(*Expr)) {
	if e == nil {
		return
	}
	fn(e)
	e.Left.Walk(fn)
	e.Right.Walk(fn)
	e.Child.Walk(fn)
}

// String renders the tree back into a parenthesized, human-readable
// form. Mostly useful for debugging/EXPLAIN-style output and tests.
func (e *Expr) String() string {
	if e == nil {
		return ""
	}
	switch e.Kind {
	case KindAnd:
		return "(" + e.Left.String() + " AND " + e.Right.String() + ")"
	case KindOr:
		return "(" + e.Left.String() + " OR " + e.Right.String() + ")"
	case KindNot:
		return "NOT " + e.Child.String()
	default:
		switch {
		case e.Op == "BETWEEN":
			return fmt.Sprintf("%s BETWEEN %v AND %v", e.Field, e.Value, e.Value2)
		case e.Values != nil:
			return fmt.Sprintf("%s %s %v", e.Field, e.Op, e.Values)
		default:
			return fmt.Sprintf("%s %s %v", e.Field, e.Op, e.Value)
		}
	}
}

// stopWords are tokens that end a WHERE clause when they appear
// outside any parentheses -- the rest of the command (LIMIT, ORDER BY,
// SET ..., etc.) is parsed by the caller starting there. Kept in sync
// with the historical flat-filter parser (parseFilters in
// cmd_filters_util.go), which callers that haven't been migrated to
// ParseWhere yet still use.
var stopWords = map[string]bool{
	"LIMIT": true, "OFFSET": true, "SELECT": true, "ORDER": true,
	"SET": true, "INC": true, "DEC": true, "PUSH": true, "PULL": true,
	"TO": true, "FROM": true, "BY": true, "ON": true, "WITH": true,
	"AS": true, "ALL": true, "EXCLUDE": true, "ONLY": true,
	"SHOW": true, "EXACT": true, "PHRASE": true, "FUZZY": true, "SIMILAR": true,
	"GROUP": true, "HAVING": true,
}

// IsStopWord reports whether tok ends a WHERE clause at the top level.
// Exported so callers building the token list (e.g. deciding where a
// WHERE clause ends before ever calling ParseWhere) can reuse the same
// definition instead of duplicating it. A leading "--" (e.g.
// "--type:table") also ends the clause: these are CLI-style flags, not
// filter conditions, and previously would have made the flat filter
// parser fail with a confusing "incomplete filter" error instead of
// being left for the caller to handle.
func IsStopWord(tok string) bool {
	if strings.HasPrefix(tok, "--") {
		return true
	}
	return stopWords[strings.ToUpper(tok)]
}

var validOps = map[string]bool{
	"=": true, "==": true, "!=": true, "<>": true,
	">": true, "<": true, ">=": true, "<=": true,
	"LIKE": true, "NOT LIKE": true,
	"CONTAINS": true, "NOT CONTAINS": true,
	"EXISTS": true,
	"IN":     true, "NOT IN": true,
	"BETWEEN":     true,
	"STARTS WITH": true, "ENDS WITH": true,
	"IS NULL": true, "IS NOT NULL": true,
	"REGEXP": true, "RLIKE": true,
}

// whereParser is a small recursive-descent parser over the token slice
// produced by Tokenize. It never looks at raw text, only at tokens, so
// it composes cleanly with the existing tokenizer (which already
// handles quoting and bracket/brace nesting).
type whereParser struct {
	tokens []string
	pos    int
}

// ParseWhere parses a WHERE clause starting at tokens[0] and returns
// the resulting expression tree plus the number of tokens consumed,
// so the caller can resume parsing the rest of the command (LIMIT,
// ORDER BY, ...) from tokens[consumed:]. It stops at the first
// unparenthesized stop word (see IsStopWord) or at the end of tokens.
//
// Grammar (highest to lowest precedence):
//
//	primary := '(' expr ')' | condition
//	notExpr := 'NOT' notExpr | primary
//	andExpr := notExpr ( 'AND' notExpr )*
//	expr    := andExpr ( 'OR' andExpr )*
//
// AND binds tighter than OR, matching standard SQL/most languages, and
// '(' ... ')' can always override precedence explicitly.
func ParseWhere(tokens []string) (*Expr, int, error) {
	if len(tokens) == 0 {
		return nil, 0, fmt.Errorf("empty WHERE clause")
	}
	p := &whereParser{tokens: tokens}
	expr, err := p.parseOr()
	if err != nil {
		return nil, p.pos, err
	}
	if expr == nil {
		return nil, p.pos, fmt.Errorf("no valid filters found")
	}
	return expr, p.pos, nil
}

func (p *whereParser) atEnd() bool { return p.pos >= len(p.tokens) }

func (p *whereParser) peekUpper() string {
	if p.atEnd() {
		return ""
	}
	return strings.ToUpper(p.tokens[p.pos])
}

func (p *whereParser) parseOr() (*Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peekUpper() == "OR" {
		p.pos++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &Expr{Kind: KindOr, Left: left, Right: right}
	}
	return left, nil
}

func (p *whereParser) parseAnd() (*Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peekUpper() == "AND" {
		p.pos++
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &Expr{Kind: KindAnd, Left: left, Right: right}
	}
	return left, nil
}

func (p *whereParser) parseNot() (*Expr, error) {
	if p.peekUpper() == "NOT" {
		p.pos++
		child, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &Expr{Kind: KindNot, Child: child}, nil
	}
	return p.parsePrimary()
}

func (p *whereParser) parsePrimary() (*Expr, error) {
	if p.atEnd() {
		return nil, fmt.Errorf("unexpected end of WHERE clause")
	}
	if p.tokens[p.pos] == "(" {
		p.pos++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.atEnd() || p.tokens[p.pos] != ")" {
			return nil, fmt.Errorf("missing closing ')' in WHERE clause")
		}
		p.pos++ // consume ')'
		return inner, nil
	}
	if IsStopWord(p.tokens[p.pos]) || p.tokens[p.pos] == ")" {
		return nil, fmt.Errorf("unexpected '%s' in WHERE clause", p.tokens[p.pos])
	}
	return p.parseCondition()
}

// parseCondition parses a single leaf "field op value" test. It
// mirrors the operator/value grammar the project already used in its
// flat filter parser (quoted strings, JSON arrays/objects, BETWEEN
// x AND y, IS [NOT] NULL, multi-word operators) so existing WHERE
// clauses keep working exactly as before -- the only new thing is that
// these leaves can now be combined with real precedence and grouping.
func (p *whereParser) parseCondition() (*Expr, error) {
	if p.atEnd() {
		return nil, fmt.Errorf("expected a filter condition")
	}
	field := p.tokens[p.pos]
	if field == "_id" {
		field = "id"
	}
	if fnField, ok := p.tryParseFunctionField(); ok {
		field = fnField
	} else {
		p.pos++
	}

	if p.atEnd() {
		return nil, fmt.Errorf("missing operator for field %s", field)
	}
	op := strings.ToUpper(p.tokens[p.pos])

	switch {
	case op == "NOT" && p.nextIs("IN"):
		op = "NOT IN"
		p.pos += 2
	case op == "STARTS" && p.nextIs("WITH"):
		op = "STARTS WITH"
		p.pos += 2
	case op == "ENDS" && p.nextIs("WITH"):
		op = "ENDS WITH"
		p.pos += 2
	case op == "NOT" && p.nextIs("LIKE"):
		op = "NOT LIKE"
		p.pos += 2
	case op == "NOT" && p.nextIs("CONTAINS"):
		op = "NOT CONTAINS"
		p.pos += 2
	case op == "IS" && p.nextIs("NOT") && p.nextNIs(2, "NULL"):
		p.pos += 3
		return &Expr{Kind: KindCondition, Field: field, Op: "IS NOT NULL", Value: true}, nil
	case op == "IS" && p.nextIs("NULL"):
		p.pos += 2
		return &Expr{Kind: KindCondition, Field: field, Op: "IS NULL", Value: true}, nil
	default:
		p.pos++
	}

	if !validOps[op] {
		// Unrecognized operator token: treat the field as a bare
		// existence check, same fallback the original flat parser used.
		return &Expr{Kind: KindCondition, Field: field, Op: "EXISTS", Value: true}, nil
	}

	if p.atEnd() {
		return nil, fmt.Errorf("missing value for field %s with operator %s", field, op)
	}

	// TODO(pendiente-motor): subconsultas -- WHERE precio > (SELECT
	// AVG(precio) FROM productos) -- no existen. p.parseValue de abajo
	// no reconoce un "(" en esta posición como el inicio de un FIND
	// anidado, así que ahora mismo lo interpretaría como un JSON array
	// mal formado o fallaría. Implementarlo requeriría: (a) detectar
	// aquí que el valor empieza con "(" seguido de un keyword de
	// consulta (FIND/SELECT) en vez de un literal; (b) parsear esa
	// subconsulta con el mismo tokenizer/parser que usa el resto del
	// motor (dsl_parser.go) y ejecutarla contra el Engine ANTES de
	// evaluar el WHERE exterior -- lo cual acopla parse.Expr (que hoy
	// deliberadamente no depende de Engine, ver el comentario de
	// paquete en query/functions.go) con el motor de ejecución, o
	// alternativamente resolver la subconsulta en una capa aparte
	// (cmd_find.go) antes de construir el árbol, sustituyendo el
	// "(SELECT ...)" por su valor ya calculado.
	return p.parseValue(field, op)
}

func (p *whereParser) nextIs(word string) bool {
	return p.pos+1 < len(p.tokens) && strings.ToUpper(p.tokens[p.pos+1]) == word
}

// tryParseFunctionField recognizes a "NAME ( arg1 , arg2 , ... )" span
// starting at p.pos as a function-call field for WHERE conditions like
// "YEAR(fecha) = 2024" or "UPPER(nombre) = \"TECLADO\"" -- see
// query.EvalFuncCall (evaluated in the root package's matchFilter,
// which recognizes the same "NAME(...)" shape via query.ParseFuncCall
// on Expr.Field instead of doing a plain getNested lookup). On success
// it advances p.pos past the whole call and returns the reassembled
// "NAME(args...)" text; on any mismatch it leaves p.pos untouched so
// the caller falls back to treating tokens[p.pos] as a plain field.
func (p *whereParser) tryParseFunctionField() (string, bool) {
	if p.atEnd() || p.pos+1 >= len(p.tokens) || p.tokens[p.pos+1] != "(" {
		return "", false
	}
	name := p.tokens[p.pos]
	for _, r := range name {
		if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return "", false
		}
	}
	start := p.pos
	pos := p.pos + 2 // skip name and '('
	depth := 1
	var argToks []string
	for pos < len(p.tokens) {
		tok := p.tokens[pos]
		switch tok {
		case "(":
			depth++
			argToks = append(argToks, tok)
			pos++
		case ")":
			depth--
			pos++
			if depth == 0 {
				p.pos = pos
				return name + "(" + strings.Join(argToks, " ") + ")", true
			}
			argToks = append(argToks, tok)
		default:
			argToks = append(argToks, tok)
			pos++
		}
	}
	// Unbalanced parens: leave p.pos untouched, let the normal
	// single-token field path handle (and likely error on) this.
	p.pos = start
	return "", false
}

func (p *whereParser) nextNIs(n int, word string) bool {
	return p.pos+n < len(p.tokens) && strings.ToUpper(p.tokens[p.pos+n]) == word
}

// parseValue mirrors the exact branch structure the original flat
// filter parser used: array/object/BETWEEN values build their own
// typed Filter and return immediately, while quoted-string and plain
// values both fall through to the same generic coercion step at the
// bottom (numeric/bool/null detection via coerce). That last part
// looks slightly odd -- a quoted `"123"` still becomes the number 123
// -- but it's the original, intentional behavior (lets a quoted value
// compare equal against a numeric document field), so it's preserved
// here rather than "fixed" into a silent behavior change.
func (p *whereParser) parseValue(field, op string) (*Expr, error) {
	valStr := p.tokens[p.pos]

	switch {
	case strings.HasPrefix(valStr, "[") && strings.HasSuffix(valStr, "]"):
		p.pos++
		return &Expr{Kind: KindCondition, Field: field, Op: op, Values: parseArrayLiteral(valStr)}, nil

	case strings.HasPrefix(valStr, "{") && strings.HasSuffix(valStr, "}"):
		p.pos++
		var obj map[string]any
		if err := json.Unmarshal([]byte(valStr), &obj); err == nil {
			return &Expr{Kind: KindCondition, Field: field, Op: op, Value: obj}, nil
		}
		return &Expr{Kind: KindCondition, Field: field, Op: op, Value: valStr}, nil

	case op == "BETWEEN":
		v1 := coerce(strings.Trim(valStr, "\"'"))
		if p.pos+2 < len(p.tokens) && strings.ToUpper(p.tokens[p.pos+1]) == "AND" {
			v2 := coerce(strings.Trim(p.tokens[p.pos+2], "\"'"))
			p.pos += 3
			return &Expr{Kind: KindCondition, Field: field, Op: "BETWEEN", Value: v1, Value2: v2}, nil
		}
		p.pos++
		return nil, fmt.Errorf("BETWEEN requires 'value1 AND value2' for field %s", field)

	default:
		// Covers both quoted strings ("foo", 'foo') and bare values --
		// same generic coercion for both, matching the legacy parser.
		p.pos++
		raw := valStr
		if isQuoted(raw) {
			raw = unquote(raw)
		} else {
			raw = strings.Trim(raw, "\"'")
		}
		value := coerce(raw)
		switch op {
		case "IN", "NOT IN":
			return &Expr{Kind: KindCondition, Field: field, Op: op, Values: []any{value}}, nil
		case "STARTS WITH", "ENDS WITH":
			return &Expr{Kind: KindCondition, Field: field, Op: op, Value: fmt.Sprint(value)}, nil
		default:
			return &Expr{Kind: KindCondition, Field: field, Op: op, Value: value}, nil
		}
	}
}

func isQuoted(s string) bool {
	return (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) && len(s) >= 2) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2)
}

func unquote(s string) string {
	return s[1 : len(s)-1]
}

// parseArrayLiteral parses a `[...]` token into a slice of typed
// values, trying JSON first and falling back to a permissive
// comma-split (so `[a, b, c]` works without quotes, not just
// `["a","b","c"]`).
func parseArrayLiteral(valStr string) []any {
	var arr []any
	if err := json.Unmarshal([]byte(valStr), &arr); err == nil {
		return arr
	}
	inner := strings.Trim(valStr, "[]")
	if inner == "" {
		return []any{}
	}
	parts := strings.Split(inner, ",")
	values := make([]any, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		values[i] = coerce(strings.Trim(part, "\"'"))
	}
	return values
}

// coerce converts a raw token into a number, bool, nil, or string --
// the same implicit-typing rules the flat filter parser used, so
// existing WHERE clauses produce identical values.
func coerce(s string) any {
	if num, err := strconv.ParseFloat(s, 64); err == nil {
		return num
	}
	if strings.EqualFold(s, "true") {
		return true
	}
	if strings.EqualFold(s, "false") {
		return false
	}
	if strings.EqualFold(s, "null") {
		return nil
	}
	return s
}

// FlattenTopLevelAnd walks the outermost chain of AND nodes (the
// common case: no OR, no NOT, no parentheses) and returns its leaves
// as a flat slice, in left-to-right order. It returns ok=false for any
// tree that isn't a pure top-level AND-chain of conditions (an OR, a
// NOT, or an explicitly grouped subexpression appears somewhere at the
// top).
//
// This lets callers keep feeding a flat condition list to code that
// doesn't need full boolean semantics -- today, index selection in
// QueryOptimizer.AnalyzeQuery, which only ever looks for a single
// equality condition to satisfy from an index and safely ignores
// anything it doesn't recognize. When ok is false, callers should
// simply not pass anything to that fast path and let it fall back to
// a full scan; correctness comes from evaluating the Expr tree itself,
// never from the flattened list.
func FlattenTopLevelAnd(e *Expr) (leaves []*Expr, ok bool) {
	if e == nil {
		return nil, true
	}
	switch e.Kind {
	case KindCondition:
		return []*Expr{e}, true
	case KindAnd:
		l, ok1 := FlattenTopLevelAnd(e.Left)
		r, ok2 := FlattenTopLevelAnd(e.Right)
		if !ok1 || !ok2 {
			return nil, false
		}
		return append(l, r...), true
	default:
		return nil, false
	}
}
