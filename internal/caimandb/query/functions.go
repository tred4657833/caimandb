// functions.go implements the "esencial" scalar function library from
// the NQL syntax reference (text/date/math/JSON functions), as a single
// shared evaluator used from three call sites:
//
//   - SELECT projections (select_expr.go: SelectFunc)
//   - WHERE conditions (root package's query_filter.go, on a Field that
//     parse.ParseWhere recognized as a function call -- see ast.go)
//   - anywhere else in the root package that wants "resolve this raw
//     SELECT/WHERE-style text against a document" without depending on
//     the Engine (this package deliberately has no such dependency; see
//     the package doc comment in select_expr.go).
//
// TODO(pendiente-motor): It's intentionally NOT wired into
// UPDATE ... SET <field> = JSON_SET(...)
// yet: cmd_update.go builds SET values at eight different call sites
// (flat "k=v" parsing duplicated across UPDATE/UPDATE ALL/ID/WHERE
// variants), and threading function-call detection through all eight
// without breaking any of them needs its own careful pass. JSON_SET and
// JSON_REMOVE are implemented and covered by EvalFuncCall here (and by
// SELECT/WHERE), just not reachable from UPDATE SET yet.
package query

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// FuncCall is a parsed "NAME(arg1, arg2, ...)" call.
type FuncCall struct {
	Name string
	Args []string // raw, untrimmed-of-quotes argument text
}

// ParseFuncCall recognizes text as a function call. text is expected to
// already have had paren spacing normalized (see normalizeCallText) --
// tokenizing "YEAR ( campo )" and rejoining with spaces, which is how
// text arrives here from both the WHERE parser and the SELECT field
// list, otherwise separates "YEAR" and "(" with a space.
func ParseFuncCall(text string) (FuncCall, bool) {
	s := normalizeCallText(strings.TrimSpace(text))
	if !strings.HasSuffix(s, ")") {
		return FuncCall{}, false
	}
	open := strings.Index(s, "(")
	if open <= 0 {
		return FuncCall{}, false
	}
	name := s[:open]
	for _, r := range name {
		if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return FuncCall{}, false
		}
	}
	inner := s[open+1 : len(s)-1]
	var args []string
	if strings.TrimSpace(inner) != "" {
		for _, a := range splitTopLevelComma(inner) {
			args = append(args, strings.TrimSpace(a))
		}
	}
	return FuncCall{Name: strings.ToUpper(name), Args: args}, true
}

// normalizeCallText collapses the whitespace that Tokenize's
// always-standalone '(' / ')' tokens (see tokenizer.go) introduce
// around parens once a token list is rejoined with spaces, e.g.
// "COUNT ( actors )" -> "COUNT(actors)". Safe to run on arbitrary
// SELECT/WHERE field text: it never touches parens that were already
// tight, and select_expr.go's arithmetic parsing only inspects the
// outermost characters of the trimmed string, which this doesn't move.
func normalizeCallText(s string) string {
	s = strings.ReplaceAll(s, " (", "(")
	s = strings.ReplaceAll(s, "( ", "(")
	s = strings.ReplaceAll(s, " )", ")")
	return s
}

// splitTopLevelComma splits s on commas that aren't inside a quoted
// string, so a string-literal argument (JSON path, text to CONCAT,
// etc.) can safely contain no commas of its own -- which matches every
// documented example -- while still not being split if it did contain
// simple punctuation. Commas inside nested "(" ")" are also protected,
// for a function argument that is itself a function call.
func splitTopLevelComma(s string) []string {
	var parts []string
	var buf strings.Builder
	inStr := false
	var strCh rune
	depth := 0
	for _, r := range s {
		switch {
		case !inStr && (r == '"' || r == '\''):
			inStr = true
			strCh = r
			buf.WriteRune(r)
		case inStr && r == strCh:
			inStr = false
			buf.WriteRune(r)
		case !inStr && r == '(':
			depth++
			buf.WriteRune(r)
		case !inStr && r == ')':
			depth--
			buf.WriteRune(r)
		case !inStr && depth == 0 && r == ',':
			parts = append(parts, buf.String())
			buf.Reset()
		default:
			buf.WriteRune(r)
		}
	}
	parts = append(parts, buf.String())
	return parts
}

// ResolveArg resolves one raw function argument against a document: a
// quoted string literal is unquoted, a bare numeric literal becomes a
// float64, a nested function call is evaluated recursively, and
// anything else is treated as a dotted field path (getNested).
func ResolveArg(raw string, data map[string]any) any {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		if (raw[0] == '"' && raw[len(raw)-1] == '"') || (raw[0] == '\'' && raw[len(raw)-1] == '\'') {
			return raw[1 : len(raw)-1]
		}
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	if strings.EqualFold(raw, "true") {
		return true
	}
	if strings.EqualFold(raw, "false") {
		return false
	}
	if strings.EqualFold(raw, "null") {
		return nil
	}
	if fc, ok := ParseFuncCall(raw); ok {
		v, _ := EvalFuncCall(fc, data)
		return v
	}
	return getNested(data, raw)
}

func argF64(args []string, i int, data map[string]any) float64 {
	if i >= len(args) {
		return 0
	}
	return toF64(ResolveArg(args[i], data))
}

func argStr(args []string, i int, data map[string]any) string {
	if i >= len(args) {
		return ""
	}
	v := ResolveArg(args[i], data)
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// EvalFuncCall evaluates fc against one document's data, implementing
// the "FUNCIONES ESENCIALES" section of the NQL reference:
//
//	Date:  YEAR MONTH DATE NOW DATE_DIFF
//	Text:  UPPER LOWER CONCAT LENGTH SUBSTRING TRIM
//	Math:  ROUND CEIL FLOOR ABS POWER SQRT MOD RANDOM
//	Array: ARRAY_LENGTH
//	JSON:  JSON_EXTRACT JSON_CONTAINS JSON_SET JSON_REMOVE JSON_KEYS JSON_LENGTH
//
// Unknown function names return an error rather than a zero value, so a
// typo surfaces as "ERROR: ..." instead of silently evaluating to
// null/0/false.
func EvalFuncCall(fc FuncCall, data map[string]any) (any, error) {
	switch fc.Name {
	// ---- date ----
	case "YEAR", "MONTH", "DATE":
		if len(fc.Args) < 1 {
			return nil, fmt.Errorf("%s requires 1 argument", fc.Name)
		}
		t, ok := parseFlexibleTime(ResolveArg(fc.Args[0], data))
		if !ok {
			return nil, nil
		}
		switch fc.Name {
		case "YEAR":
			return float64(t.Year()), nil
		case "MONTH":
			return float64(t.Month()), nil
		default: // DATE
			return t.Format("2006-01-02"), nil
		}
	case "NOW":
		return time.Now().UTC().Format(time.RFC3339), nil
	case "DATE_DIFF":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("DATE_DIFF requires 2 arguments")
		}
		t1, ok1 := parseFlexibleTime(ResolveArg(fc.Args[0], data))
		t2, ok2 := parseFlexibleTime(ResolveArg(fc.Args[1], data))
		if !ok1 || !ok2 {
			return nil, nil
		}
		return t1.Sub(t2).Hours() / 24, nil

	// ---- text ----
	case "UPPER":
		return strings.ToUpper(argStr(fc.Args, 0, data)), nil
	case "LOWER":
		return strings.ToLower(argStr(fc.Args, 0, data)), nil
	case "TRIM":
		return strings.TrimSpace(argStr(fc.Args, 0, data)), nil
	case "LENGTH":
		v := ResolveArg(orEmpty(fc.Args, 0), data)
		if arr, ok := v.([]any); ok {
			return float64(len(arr)), nil
		}
		return float64(len([]rune(fmt.Sprint(v)))), nil
	case "CONCAT":
		var sb strings.Builder
		for _, a := range fc.Args {
			sb.WriteString(argStrRaw(a, data))
		}
		return sb.String(), nil
	case "SUBSTRING":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("SUBSTRING requires at least 2 arguments")
		}
		s := []rune(argStr(fc.Args, 0, data))
		start := int(argF64(fc.Args, 1, data))
		if start < 1 {
			start = 1
		}
		if start > len(s) {
			return "", nil
		}
		sliceEnd := len(s)
		if len(fc.Args) >= 3 {
			n := int(argF64(fc.Args, 2, data))
			if n < 0 {
				n = 0
			}
			if start-1+n < sliceEnd {
				sliceEnd = start - 1 + n
			}
		}
		return string(s[start-1 : sliceEnd]), nil

	// ---- math ----
	case "ROUND":
		x := argF64(fc.Args, 0, data)
		decimals := 0
		if len(fc.Args) >= 2 {
			decimals = int(argF64(fc.Args, 1, data))
		}
		mult := math.Pow(10, float64(decimals))
		return math.Round(x*mult) / mult, nil
	case "CEIL":
		return math.Ceil(argF64(fc.Args, 0, data)), nil
	case "FLOOR":
		return math.Floor(argF64(fc.Args, 0, data)), nil
	case "ABS":
		return math.Abs(argF64(fc.Args, 0, data)), nil
	case "POWER":
		return math.Pow(argF64(fc.Args, 0, data), argF64(fc.Args, 1, data)), nil
	case "SQRT":
		return math.Sqrt(argF64(fc.Args, 0, data)), nil
	case "MOD":
		divisor := argF64(fc.Args, 1, data)
		if divisor == 0 {
			return nil, nil
		}
		return math.Mod(argF64(fc.Args, 0, data), divisor), nil
	case "RANDOM":
		return rand.Float64(), nil

	// ---- arrays ----
	case "ARRAY_LENGTH":
		v := ResolveArg(orEmpty(fc.Args, 0), data)
		if arr, ok := v.([]any); ok {
			return float64(len(arr)), nil
		}
		return float64(0), nil

	// ---- JSON ----
	case "JSON_EXTRACT":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("JSON_EXTRACT requires 2 arguments")
		}
		obj := jsonObjectArg(fc.Args[0], data)
		return getNested(obj, jsonPathToDotted(argStr(fc.Args, 1, data))), nil
	case "JSON_CONTAINS":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("JSON_CONTAINS requires 3 arguments")
		}
		obj := jsonObjectArg(fc.Args[0], data)
		got := getNested(obj, jsonPathToDotted(argStr(fc.Args, 1, data)))
		want := ResolveArg(fc.Args[2], data)
		return fmt.Sprint(got) == fmt.Sprint(want), nil
	case "JSON_KEYS":
		obj := jsonObjectArg(orEmpty(fc.Args, 0), data)
		keys := make([]any, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		return keys, nil
	case "JSON_LENGTH":
		obj := jsonObjectArg(orEmpty(fc.Args, 0), data)
		return float64(len(obj)), nil
	case "JSON_SET":
		if len(fc.Args) < 3 {
			return nil, fmt.Errorf("JSON_SET requires 3 arguments")
		}
		obj := cloneMap(jsonObjectArg(fc.Args[0], data))
		setNestedLocal(obj, jsonPathToDotted(argStr(fc.Args, 1, data)), ResolveArg(fc.Args[2], data))
		return obj, nil
	case "JSON_REMOVE":
		if len(fc.Args) < 2 {
			return nil, fmt.Errorf("JSON_REMOVE requires 2 arguments")
		}
		obj := cloneMap(jsonObjectArg(fc.Args[0], data))
		deleteNestedLocal(obj, jsonPathToDotted(argStr(fc.Args, 1, data)))
		return obj, nil
	}
	return nil, fmt.Errorf("unknown function: %s", fc.Name)
}

func orEmpty(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}

// argStrRaw is like argStr but for CONCAT, where an unquoted, non-numeric,
// non-field-path argument (e.g. a literal separator someone forgot to
// quote) should still degrade to its own text rather than an empty
// string -- ResolveArg's field-path fallback already does exactly that
// via getNested returning nil -> fmt.Sprint(nil) == "<nil>", which we
// don't want, so this treats a nil resolution as the raw text itself.
func argStrRaw(raw string, data map[string]any) string {
	v := ResolveArg(raw, data)
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// jsonPathToDotted turns a "$.a.b.c" JSON-path argument into the plain
// dotted "a.b.c" form getNested/setNested already understand. A path
// without the "$." prefix is accepted as-is (already dotted).
func jsonPathToDotted(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "$.")
	p = strings.TrimPrefix(p, "$")
	return strings.TrimPrefix(p, ".")
}

// jsonObjectArg resolves a JSON_* function's first argument (a field
// path, almost always) into a map[string]any: if the field already
// holds a decoded object (the common case -- documents are stored as
// already-parsed maps), it's used directly; if it holds a JSON string,
// it's decoded first. Anything else (missing field, non-object value)
// yields an empty map so callers get a clean "not found" (nil) rather
// than a panic or a confusing error.
func jsonObjectArg(raw string, data map[string]any) map[string]any {
	v := ResolveArg(raw, data)
	switch t := v.(type) {
	case map[string]any:
		return t
	case string:
		var obj map[string]any
		if err := json.Unmarshal([]byte(t), &obj); err == nil {
			return obj
		}
	}
	return map[string]any{}
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// setNestedLocal/deleteNestedLocal mirror the root package's
// setNested/deleteNested (nested_fields.go) exactly, duplicated here
// for the same reason getNested already is in nested.go: this package
// can't import the root package back without a cycle.
func setNestedLocal(data map[string]any, path string, value any) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		data[parts[0]] = value
		return
	}
	sub, ok := data[parts[0]].(map[string]any)
	if !ok {
		sub = make(map[string]any)
		data[parts[0]] = sub
	}
	setNestedLocal(sub, parts[1], value)
}

func deleteNestedLocal(data map[string]any, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		delete(data, parts[0])
		return
	}
	sub, ok := data[parts[0]].(map[string]any)
	if !ok {
		return
	}
	deleteNestedLocal(sub, parts[1])
}

// parseFlexibleTime tries a handful of layouts documents commonly store
// dates in, plus a bare unix-seconds number, since NQL documents have
// no fixed schema/types (see the syntax reference's own "sin tipos, sin
// esquema" framing) and this project doesn't otherwise standardize on
// one date representation.
func parseFlexibleTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case float64:
		return time.Unix(int64(t), 0).UTC(), true
	case string:
		layouts := []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed.UTC(), true
			}
		}
	}
	return time.Time{}, false
}
