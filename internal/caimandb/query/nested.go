package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// getNested resolves a dotted field path against a document's data map.
// This is a local copy of the root package's getNested (nested_fields.go):
// query is imported by the root caimandb package, so importing back would
// create a cycle. Keep this in sync if the root implementation changes.
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

// getNestedF64 resolves path like getNested, coercing the result to a
// float64 (0 if missing/non-numeric).
func getNestedF64(data map[string]any, path string) float64 { return toF64(getNested(data, path)) }

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
