package caimandb

import (
	"caimandb/internal/caimandb/query"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func getNested(data map[string]any, path string) any {
	parts := strings.SplitN(path, ".", 2)
	v, ok := data[parts[0]]
	if !ok {
		return nil
	}
	if len(parts) == 1 {
		return v
	}
	sub, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return getNested(sub, parts[1])
}

// getNestedValues resolves path against data like getNested, but never
// fails on an array partway through the path: instead of requiring every
// intermediate to be a map[string]any, an []any of objects fans out into
// one result per matching element. A plain scalar/nested-map path still
// returns exactly one value, same as getNested. This is what WHERE/HAVING
// matching (matchFilter) uses, so a dotted path can walk through a
// resolved one-to-many RELATE alias (e.g. "genres.name" once "genres" has
// been attached as an array of related docs) or any plain array-of-object
// field, with any-of-the-elements semantics.
//
// It also special-cases a leading "COUNT(field)" pseudo-field, evaluating
// to the element count of field (see countValue in select_expr.go), so
// FIND ... HAVING COUNT(actors) >= 5 can be evaluated the same way any
// other condition is, via matchFilter/evalExpr, without HAVING needing
// its own separate matcher.
func getNestedValues(data map[string]any, path string) []any {
	if strings.HasPrefix(path, "COUNT(") && strings.HasSuffix(path, ")") {
		inner := strings.TrimSpace(path[len("COUNT(") : len(path)-1])
		return []any{query.CountValue(inner, data)}
	}

	parts := strings.SplitN(path, ".", 2)
	v, ok := data[parts[0]]
	if !ok {
		return nil
	}
	if len(parts) == 1 {
		return []any{v}
	}
	switch sub := v.(type) {
	case map[string]any:
		return getNestedValues(sub, parts[1])
	case []any:
		var out []any
		for _, item := range sub {
			if m, ok := item.(map[string]any); ok {
				out = append(out, getNestedValues(m, parts[1])...)
			}
		}
		return out
	default:
		return nil
	}
}

func setNested(data map[string]any, path string, value any) {
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
	setNested(sub, parts[1], value)
}

// hasNested reports whether path resolves to a present key (even if its
// value is nil/false/0) rather than simply being absent. Used by ADD
// FIELD ... DEFAULT to decide which documents are missing the field
// and therefore need the default applied.
func hasNested(data map[string]any, path string) bool {
	parts := strings.SplitN(path, ".", 2)
	v, ok := data[parts[0]]
	if !ok {
		return false
	}
	if len(parts) == 1 {
		return true
	}
	sub, ok := v.(map[string]any)
	if !ok {
		return false
	}
	return hasNested(sub, parts[1])
}

// deleteNested removes the key at path from data (used by DROP FIELD).
// It's a no-op if any part of the path doesn't exist; it never creates
// intermediate maps the way setNested does.
func deleteNested(data map[string]any, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		delete(data, parts[0])
		return
	}
	sub, ok := data[parts[0]].(map[string]any)
	if !ok {
		return
	}
	deleteNested(sub, parts[1])
}

func getNestedF64(data map[string]any, path string) float64 { return toF64(getNested(data, path)) }

func getNestedArr(data map[string]any, path string) []any {
	if arr, ok := getNested(data, path).([]any); ok {
		return arr
	}
	return nil
}

func project(data map[string]any, include, exclude []string) map[string]any {
	out := make(map[string]any, len(data))
	if len(include) > 0 {
		for _, f := range include {
			out[f] = getNested(data, f)
		}
		return out
	}
	for k, v := range data {
		out[k] = v
	}
	for _, f := range exclude {
		delete(out, f)
	}
	return out
}

func toF64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	}
	f, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
	return f
}
