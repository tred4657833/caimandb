package caimandb

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"caimandb/internal/caimandb/parse"
)

// parseWhereClause parses the WHERE clause starting at tokens[*idx]
// using the full expression-tree parser (internal/caimandb/parse/ast.go),
// advances *idx past the consumed tokens, and returns both:
//
//   - the expression tree itself (where), which is what actually gets
//     evaluated -- see matchesQuery/evalExpr in query_filter.go -- and
//     correctly supports parentheses and AND/OR/NOT precedence;
//   - a best-effort flattened []Filter (flat), containing the
//     top-level leaves *only* when the tree is a pure AND-chain with
//     no OR/NOT/grouping. flat exists solely so the query optimizer's
//     existing index-selection heuristic (which only ever looks for a
//     single equality condition in a flat list) still has something to
//     look at; it is never used for correctness. When the WHERE clause
//     isn't a pure AND-chain, flat is nil and index selection simply
//     falls back to a full scan, which is always correct, just
//     unoptimized -- matching how AnalyzeQuery already behaves whenever
//     it can't find a usable index.
//
// This is currently used by FIND and SEARCH (handleFind/handleSearch
// in cmd_find.go). Other commands that take a WHERE clause (UPDATE,
// DELETE, CLEAR/COUNT, aggregates, VIEW, admin) still call the older
// parseFilters below, which only ever produces a flat AND/OR chain
// evaluated left-to-right with no grouping -- migrating them to
// parseWhereClause is mechanical (same token-cursor shape) and is
// tracked as follow-up work in docs/known-limitations.md.
func parseWhereClause(tokens []string, idx *int) (where *parse.Expr, flat []Filter, err error) {
	if *idx >= len(tokens) {
		return nil, nil, fmt.Errorf("WHERE requires filter conditions")
	}
	expr, consumed, err := parse.ParseWhere(tokens[*idx:])
	if err != nil {
		return nil, nil, err
	}
	*idx += consumed

	leaves, ok := parse.FlattenTopLevelAnd(expr)
	if ok {
		flat = make([]Filter, 0, len(leaves))
		for _, leaf := range leaves {
			flat = append(flat, Filter{
				Field:  leaf.Field,
				Op:     leaf.Op,
				Value:  leaf.Value,
				Value2: leaf.Value2,
				Values: leaf.Values,
			})
		}
	}
	return expr, flat, nil
}

func parseFilters(tokens []string, idx *int) ([]Filter, error) {
	var filters []Filter
	var logic string

	for *idx < len(tokens) {
		tok := strings.ToUpper(tokens[*idx])

		if tok == "AND" || tok == "OR" {
			logic = tok
			(*idx)++
			continue
		}

		if tok == "LIMIT" || tok == "OFFSET" || tok == "SELECT" || tok == "ORDER" ||
			tok == "SET" || tok == "INC" || tok == "DEC" || tok == "PUSH" || tok == "PULL" ||
			tok == "TO" || tok == "FROM" || tok == "BY" || tok == "ON" || tok == "WITH" ||
			tok == "AS" || tok == "ALL" || tok == "EXCLUDE" || tok == "ONLY" {
			break
		}

		if *idx+1 >= len(tokens) {
			return nil, fmt.Errorf("incomplete filter at %s", tokens[*idx])
		}

		field := tokens[*idx]

		// CORRECCIÓN: Convertir _id a id para que coincida con el almacenamiento
		if field == "_id" {
			field = "id"
		}

		// Avanzar al operador
		(*idx)++

		if *idx >= len(tokens) {
			return nil, fmt.Errorf("missing operator for field %s", field)
		}

		op := strings.ToUpper(tokens[*idx])

		// Manejar operadores de múltiples palabras
		if op == "NOT" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "IN" {
			op = "NOT IN"
			(*idx)++ // Saltar "NOT"
			(*idx)++ // Saltar "IN"
		} else if op == "STARTS" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "WITH" {
			op = "STARTS WITH"
			(*idx)++ // Saltar "STARTS"
			(*idx)++ // Saltar "WITH"
		} else if op == "ENDS" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "WITH" {
			op = "ENDS WITH"
			(*idx)++ // Saltar "ENDS"
			(*idx)++ // Saltar "WITH"
		} else if op == "NOT" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "LIKE" {
			op = "NOT LIKE"
			(*idx)++ // Saltar "NOT"
			(*idx)++ // Saltar "LIKE"
		} else if op == "NOT" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "CONTAINS" {
			op = "NOT CONTAINS"
			(*idx)++ // Saltar "NOT"
			(*idx)++ // Saltar "CONTAINS"
		} else if op == "IS" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "NOT" &&
			*idx+2 < len(tokens) && strings.ToUpper(tokens[*idx+2]) == "NULL" {
			op = "IS NOT NULL"
			(*idx)++ // Saltar "IS"
			(*idx)++ // Saltar "NOT"
			(*idx)++ // Saltar "NULL"

			// IS NOT NULL no necesita valor, agregar filtro y continuar
			filters = append(filters, Filter{
				Field: field,
				Op:    op,
				Value: true,
				Logic: logic,
			})
			logic = ""
			continue
		} else if op == "IS" && *idx+1 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "NULL" {
			op = "IS NULL"
			(*idx)++ // Saltar "IS"
			(*idx)++ // Saltar "NULL"

			// IS NULL no necesita valor, agregar filtro y continuar
			filters = append(filters, Filter{
				Field: field,
				Op:    op,
				Value: true,
				Logic: logic,
			})
			logic = ""
			continue
		} else {
			// Operador simple, ya estamos posicionados en él
			(*idx)++
		}

		// ============================================================
		// AHORA verificamos si hay valor (después de manejar IS NULL/IS NOT NULL)
		// ============================================================
		if *idx >= len(tokens) {
			return nil, fmt.Errorf("missing value for field %s with operator %s", field, op)
		}

		validOps := map[string]bool{
			"=": true, "==": true, "!=": true, "<>": true,
			">": true, "<": true, ">=": true, "<=": true,
			"LIKE": true, "NOT LIKE": true,
			"CONTAINS": true, "NOT CONTAINS": true,
			"EXISTS": true,
			"IN":     true, "NOT IN": true,
			"BETWEEN":     true,
			"STARTS WITH": true, "ENDS WITH": true,
			"IS NULL": true, "IS NOT NULL": true,
		}

		if !validOps[op] {
			// Si el operador no es válido, tratarlo como EXISTS
			filters = append(filters, Filter{
				Field: field,
				Op:    "EXISTS",
				Value: true,
				Logic: logic,
			})
			logic = ""
			continue
		}

		// OBTENER EL VALOR
		valStr := tokens[*idx]

		// Si es un string entre comillas
		if (strings.HasPrefix(valStr, "\"") && strings.HasSuffix(valStr, "\"")) ||
			(strings.HasPrefix(valStr, "'") && strings.HasSuffix(valStr, "'")) {
			valStr = valStr[1 : len(valStr)-1]
			(*idx)++
		} else if strings.HasPrefix(valStr, "[") && strings.HasSuffix(valStr, "]") {
			// Array
			(*idx)++
			var arr []any
			if err := json.Unmarshal([]byte(valStr), &arr); err == nil {
				filters = append(filters, Filter{
					Field:  field,
					Op:     op,
					Values: arr,
					Logic:  logic,
				})
				logic = ""
				continue
			}
			inner := strings.Trim(valStr, "[]")
			if inner == "" {
				filters = append(filters, Filter{
					Field:  field,
					Op:     op,
					Values: []any{},
					Logic:  logic,
				})
				logic = ""
				continue
			}
			parts := strings.Split(inner, ",")
			values := make([]any, len(parts))
			for i, p := range parts {
				p = strings.TrimSpace(p)
				if num, err := strconv.ParseFloat(p, 64); err == nil {
					values[i] = num
				} else {
					values[i] = strings.Trim(p, "\"'")
				}
			}
			filters = append(filters, Filter{
				Field:  field,
				Op:     op,
				Values: values,
				Logic:  logic,
			})
			logic = ""
			continue
		} else if strings.HasPrefix(valStr, "{") && strings.HasSuffix(valStr, "}") {
			// Object
			(*idx)++
			var obj map[string]any
			if err := json.Unmarshal([]byte(valStr), &obj); err == nil {
				filters = append(filters, Filter{
					Field: field,
					Op:    op,
					Value: obj,
					Logic: logic,
				})
				logic = ""
				continue
			}
		} else if op == "BETWEEN" {
			valStr = strings.Trim(valStr, "\"'")
			if *idx+2 < len(tokens) && strings.ToUpper(tokens[*idx+1]) == "AND" {
				val2Str := tokens[*idx+2]
				val2Str = strings.Trim(val2Str, "\"'")
				var v1, v2 any
				if num, err := strconv.ParseFloat(valStr, 64); err == nil {
					v1 = num
				} else {
					v1 = valStr
				}
				if num, err := strconv.ParseFloat(val2Str, 64); err == nil {
					v2 = num
				} else {
					v2 = val2Str
				}
				filters = append(filters, Filter{
					Field:  field,
					Op:     "BETWEEN",
					Value:  v1,
					Value2: v2,
					Logic:  logic,
				})
				*idx += 3
				logic = ""
				continue
			}
			(*idx)++
		} else {
			// Valor simple
			valStr = strings.Trim(valStr, "\"'")
			(*idx)++
		}

		// Parsear valor
		var value any
		if num, err := strconv.ParseFloat(valStr, 64); err == nil {
			value = num
		} else if strings.EqualFold(valStr, "true") {
			value = true
		} else if strings.EqualFold(valStr, "false") {
			value = false
		} else if strings.EqualFold(valStr, "null") {
			value = nil
		} else {
			value = valStr
		}

		if op == "IN" || op == "NOT IN" {
			filters = append(filters, Filter{
				Field:  field,
				Op:     op,
				Values: []any{value},
				Logic:  logic,
			})
			logic = ""
			continue
		}

		if op == "STARTS WITH" || op == "ENDS WITH" {
			filters = append(filters, Filter{
				Field: field,
				Op:    op,
				Value: fmt.Sprint(value),
				Logic: logic,
			})
			logic = ""
			continue
		}

		filters = append(filters, Filter{
			Field: field,
			Op:    op,
			Value: value,
			Logic: logic,
		})

		logic = ""
	}

	if len(filters) == 0 {
		return nil, fmt.Errorf("no valid filters found")
	}

	return filters, nil
}

func isInteractive() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}
