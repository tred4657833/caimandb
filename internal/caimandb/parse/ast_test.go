package parse

import "testing"

// helper: parse a WHERE clause given as already-tokenized strings and
// fail the test immediately on any parse error.
func mustParse(t *testing.T, tokens []string) (*Expr, int) {
	t.Helper()
	expr, consumed, err := ParseWhere(tokens)
	if err != nil {
		t.Fatalf("ParseWhere(%v) returned error: %v", tokens, err)
	}
	return expr, consumed
}

func TestParseWhere_SimpleCondition(t *testing.T) {
	expr, consumed := mustParse(t, Tokenize(`age > 18`))
	if consumed != 3 {
		t.Fatalf("expected to consume 3 tokens, consumed %d", consumed)
	}
	if expr.Kind != KindCondition || expr.Field != "age" || expr.Op != ">" {
		t.Fatalf("unexpected expr: %+v", expr)
	}
	if expr.Value != 18.0 {
		t.Fatalf("expected numeric 18, got %#v", expr.Value)
	}
}

func TestParseWhere_AndChain(t *testing.T) {
	expr, _ := mustParse(t, Tokenize(`age > 18 AND status = "active"`))
	if expr.Kind != KindAnd {
		t.Fatalf("expected top-level AND, got %v", expr.Kind)
	}
	leaves, ok := FlattenTopLevelAnd(expr)
	if !ok {
		t.Fatalf("expected a pure AND-chain to flatten cleanly")
	}
	if len(leaves) != 2 {
		t.Fatalf("expected 2 leaves, got %d", len(leaves))
	}
	if leaves[0].Field != "age" || leaves[1].Field != "status" {
		t.Fatalf("unexpected leaf order: %+v", leaves)
	}
}

func TestParseWhere_AndBindsTighterThanOr(t *testing.T) {
	// a = 1 OR b = 2 AND c = 3  must parse as  a=1 OR (b=2 AND c=3)
	expr, _ := mustParse(t, Tokenize(`a = 1 OR b = 2 AND c = 3`))
	if expr.Kind != KindOr {
		t.Fatalf("expected top-level OR, got %v", expr.Kind)
	}
	if expr.Left.Kind != KindCondition || expr.Left.Field != "a" {
		t.Fatalf("expected left side to be the bare condition a=1, got %+v", expr.Left)
	}
	if expr.Right.Kind != KindAnd {
		t.Fatalf("expected right side to be an AND node, got %v", expr.Right.Kind)
	}
	// A pure top-level OR is not a pure AND-chain, so it must not flatten.
	if _, ok := FlattenTopLevelAnd(expr); ok {
		t.Fatalf("expected FlattenTopLevelAnd to refuse a top-level OR")
	}
}

func TestParseWhere_ParenthesesOverridePrecedence(t *testing.T) {
	// (a = 1 OR b = 2) AND c = 3  must parse as  (a=1 OR b=2) AND c=3
	expr, _ := mustParse(t, Tokenize(`( a = 1 OR b = 2 ) AND c = 3`))
	if expr.Kind != KindAnd {
		t.Fatalf("expected top-level AND, got %v", expr.Kind)
	}
	if expr.Left.Kind != KindOr {
		t.Fatalf("expected left side to be the parenthesized OR, got %v", expr.Left.Kind)
	}
	if expr.Right.Kind != KindCondition || expr.Right.Field != "c" {
		t.Fatalf("expected right side to be c=3, got %+v", expr.Right)
	}
}

func TestParseWhere_ParenthesesGluedToTokens(t *testing.T) {
	// No spaces around the parens -- the tokenizer must still split
	// them into their own tokens, even though "status=\"active\""
	// itself stays glued (Tokenize only ever splits on whitespace,
	// quotes, and bracket/brace/paren nesting -- '=' isn't special).
	toks := Tokenize(`(status="active")`)
	if len(toks) != 3 {
		t.Fatalf("expected 3 tokens ('(', the glued condition, ')'), got %v", toks)
	}
	if toks[0] != "(" || toks[2] != ")" {
		t.Fatalf("expected parens to be split into their own tokens, got %v", toks)
	}
}

func TestParseWhere_Not(t *testing.T) {
	expr, _ := mustParse(t, Tokenize(`NOT ( status = "banned" OR status = "suspended" )`))
	if expr.Kind != KindNot {
		t.Fatalf("expected top-level NOT, got %v", expr.Kind)
	}
	if expr.Child.Kind != KindOr {
		t.Fatalf("expected NOT's child to be the OR group, got %v", expr.Child.Kind)
	}
}

func TestParseWhere_Between(t *testing.T) {
	expr, consumed := mustParse(t, Tokenize(`age BETWEEN 18 AND 65`))
	if expr.Kind != KindCondition || expr.Op != "BETWEEN" {
		t.Fatalf("unexpected expr: %+v", expr)
	}
	if expr.Value != 18.0 || expr.Value2 != 65.0 {
		t.Fatalf("unexpected BETWEEN bounds: %#v / %#v", expr.Value, expr.Value2)
	}
	if consumed != 5 {
		t.Fatalf("expected to consume 5 tokens (field BETWEEN v1 AND v2), consumed %d", consumed)
	}
}

func TestParseWhere_BetweenInsideAndChain(t *testing.T) {
	// The literal "AND" inside BETWEEN must not be mistaken for the
	// logical AND connecting the next condition.
	expr, _ := mustParse(t, Tokenize(`age BETWEEN 18 AND 65 AND status = "active"`))
	if expr.Kind != KindAnd {
		t.Fatalf("expected top-level AND, got %v", expr.Kind)
	}
	if expr.Left.Op != "BETWEEN" {
		t.Fatalf("expected left side to be the BETWEEN condition, got %+v", expr.Left)
	}
	if expr.Right.Field != "status" {
		t.Fatalf("expected right side to be status=active, got %+v", expr.Right)
	}
}

func TestParseWhere_IsNullVariants(t *testing.T) {
	expr, _ := mustParse(t, Tokenize(`deleted_at IS NULL`))
	if expr.Op != "IS NULL" {
		t.Fatalf("expected IS NULL, got %q", expr.Op)
	}
	expr2, _ := mustParse(t, Tokenize(`deleted_at IS NOT NULL`))
	if expr2.Op != "IS NOT NULL" {
		t.Fatalf("expected IS NOT NULL, got %q", expr2.Op)
	}
}

func TestParseWhere_MultiWordOperators(t *testing.T) {
	cases := map[string]string{
		`tags NOT IN ["x","y"]`:       "NOT IN",
		`name STARTS WITH "Jo"`:       "STARTS WITH",
		`name ENDS WITH "hn"`:         "ENDS WITH",
		`bio NOT LIKE "%spam%"`:       "NOT LIKE",
		`bio NOT CONTAINS "spam"`:     "NOT CONTAINS",
	}
	for input, wantOp := range cases {
		expr, _ := mustParse(t, Tokenize(input))
		if expr.Op != wantOp {
			t.Errorf("input %q: expected op %q, got %q", input, wantOp, expr.Op)
		}
	}
}

func TestParseWhere_StopsAtStopWord(t *testing.T) {
	toks := Tokenize(`age > 18 LIMIT 10`)
	expr, consumed := mustParse(t, toks)
	if expr.Field != "age" {
		t.Fatalf("unexpected expr: %+v", expr)
	}
	if toks[consumed] != "LIMIT" {
		t.Fatalf("expected parser to stop right before LIMIT, stopped at %q", toks[consumed])
	}
}

func TestParseWhere_StopsAtDashFlag(t *testing.T) {
	toks := Tokenize(`age > 18 --type:table`)
	_, consumed := mustParse(t, toks)
	if toks[consumed] != "--type:table" {
		t.Fatalf("expected parser to stop right before the flag, stopped at %q", toks[consumed])
	}
}

func TestParseWhere_UnclosedParenIsAnError(t *testing.T) {
	_, _, err := ParseWhere(Tokenize(`( age > 18 AND status = "active"`))
	if err == nil {
		t.Fatal("expected an error for an unclosed '('")
	}
}

func TestParseWhere_EmptyIsAnError(t *testing.T) {
	_, _, err := ParseWhere(nil)
	if err == nil {
		t.Fatal("expected an error for an empty WHERE clause")
	}
}

func TestFlattenTopLevelAnd_RejectsNot(t *testing.T) {
	expr, _ := mustParse(t, Tokenize(`NOT status = "banned"`))
	if _, ok := FlattenTopLevelAnd(expr); ok {
		t.Fatal("expected FlattenTopLevelAnd to refuse a top-level NOT")
	}
}
