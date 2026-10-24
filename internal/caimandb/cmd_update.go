package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"caimandb/internal/caimandb/query"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// resolveSetLiteral turns the raw right-hand-side text of a SET
// "field = <value>" assignment into a typed value. It recognizes
// zero/no-field-arg function calls such as now() (see
// query.EvalFuncCall's NOW case) by evaluating them immediately with a
// nil document, so "SET last_reviewed = now()" stores the current
// timestamp instead of the literal string "now()". Field-referencing
// functions (e.g. UPPER(name)) aren't resolvable here since SET values
// are computed once, before the per-document update loop, not per
// document -- ResolveArg/EvalFuncCall still won't panic on a nil map
// (a nil map read just returns the zero value), so unresolvable calls
// simply fall through unset args rather than erroring.
// consumeParenGroup returns the raw "( ... )" text (tokens rejoined with
// single spaces, matching what ParseFuncCall/normalizeCallText expect)
// starting at tokens[idx] == "(", along with the index of the token just
// past the matching ")". The tokenizer always splits "(" and ")" into
// their own standalone tokens (see parse/tokenizer.go), which is why a
// call like now() shows up as three tokens ["now", "(", ")"] instead of
// one -- this walks paren depth to find where such a call actually ends.
// Returns ok=false if there's no balanced close.
func consumeParenGroup(tokens []string, idx int) (text string, next int, ok bool) {
	if idx >= len(tokens) || tokens[idx] != "(" {
		return "", idx, false
	}
	depth := 0
	var buf strings.Builder
	for j := idx; j < len(tokens); j++ {
		if tokens[j] == "(" {
			depth++
		} else if tokens[j] == ")" {
			depth--
		}
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(tokens[j])
		if depth == 0 {
			return buf.String(), j + 1, true
		}
	}
	return "", idx, false
}

func resolveSetLiteral(valStr string) any {
	trimmed := strings.TrimSpace(valStr)
	if fc, ok := query.ParseFuncCall(trimmed); ok {
		if v, err := query.EvalFuncCall(fc, nil); err == nil {
			return v
		}
	}
	if num, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return num
	}
	return strings.Trim(trimmed, "\"'")
}

// stripTrailingComma removes one trailing "," (and the whitespace
// around it) from a SET/INC/DEC/PUSH/PULL value token before it's
// parsed as JSON, a number, or a quoted literal.
//
// The tokenizer (parse/tokenizer.go) only splits tokens on whitespace,
// brackets and parentheses -- a comma is ordinary text to it. So
// "SET name = "Simba the Great", weight_kg = 195" tokenizes with the
// separating comma glued onto whatever precedes it with no space:
// the value token comes through as `"Simba the Great",`, not
// `"Simba the Great"`. Every downstream literal parser here
// (strings.Trim(s, `"'`), json.Unmarshal, strconv.ParseFloat) only
// strips or expects quote characters, not a trailing comma, so
// without this the comma -- and the closing quote it stuck itself
// after -- ended up baked into the stored value (`Simba the Great",`)
// while later fields silently lost their leading quote. Every value
// token in this file needs this before it's parsed.
func stripTrailingComma(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, ",") {
		return strings.TrimSpace(strings.TrimSuffix(s, ","))
	}
	return s
}

func handleUpdate(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: UPDATE requires block name"
	}

	// ============================================================
	// DETECTAR "UPDATE ALL <block> SET ..."
	// (documented syntax: ALL comes right after UPDATE, before the
	// block name -- see docs/NQL_SYNTAX.en.md and nql-reference.md.
	// This used to check tokens[2] ("UPDATE <block> ALL ..."), which
	// doesn't match the docs or the CLI's own examples: for
	// "UPDATE ALL animals SET ...", tokens[1] is "ALL" and tokens[2]
	// is "animals", so the old check missed the branch entirely and
	// fell through to the generic path, which then treated "ALL"
	// itself as the block name -- hence "Block not found: ALL".)
	// ============================================================
	if len(tokens) >= 3 && strings.ToUpper(tokens[1]) == "ALL" {
		block := tokens[2]
		db := sess.currentDB
		if db == "" {
			return "ERROR: No database in use"
		}

		if !eng.dirMgr.BlockExists(db, block) {
			return "ERROR: Block not found: " + block
		}
		if err := eng.authorizeBlockAccess(sess, db, block, "write"); err != nil {
			return "ERROR: " + err.Error()
		}

		setPos := -1
		for j := 3; j < len(tokens); j++ {
			if strings.ToUpper(tokens[j]) == "SET" {
				setPos = j
				break
			}
		}

		if setPos == -1 || setPos+1 >= len(tokens) {
			return "ERROR: UPDATE ALL requires SET with values"
		}

		var op UpdateOp
		op.Set = make(map[string]any)

		jsonStr := strings.Join(tokens[setPos+1:], " ")
		if strings.HasPrefix(jsonStr, "{") {
			var setData map[string]any
			if err := json.Unmarshal([]byte(jsonStr), &setData); err == nil {
				for k, v := range setData {
					op.Set[k] = v
				}
			}
		} else {
			parts := strings.Split(jsonStr, ",")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if strings.Contains(part, "=") {
					kv := strings.SplitN(part, "=", 2)
					if len(kv) == 2 {
						key := strings.TrimSpace(kv[0])
						op.Set[key] = resolveSetLiteral(kv[1])
					}
				}
			}
		}

		count, err := execUpdate(eng, sess, db, block, []Filter{}, nil, op)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return fmt.Sprintf("Updated %d documents", count)
	}

	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}
	if err := eng.authorizeBlockAccess(sess, db, block, "write"); err != nil {
		return "ERROR: " + err.Error()
	}

	var filters []Filter
	var where *parse.Expr
	var op UpdateOp
	op.Set = make(map[string]any)
	op.Inc = make(map[string]float64)
	op.Dec = make(map[string]float64)
	op.Push = make(map[string]any)
	op.Pull = make(map[string]any)

	i := 2
	var idValue string
	var hasID bool
	var fieldName string
	var fieldValue string
	var hasField bool
	var hasWhere bool
	var isIDFilter bool

	// ============================================================
	// ANALIZAR EL TOKEN INICIAL PARA DETECTAR ID O CAMPO
	// ============================================================
	if i < len(tokens) {
		firstToken := tokens[i]
		upperFirst := strings.ToUpper(firstToken)

		if upperFirst == "ID" && i+1 < len(tokens) {
			idValue = tokens[i+1]
			hasID = true
			isIDFilter = true
			i += 2
		} else if upperFirst == "WHERE" {
			hasWhere = true
			i++
		} else if upperFirst == "_ID" && i+2 < len(tokens) {
			if tokens[i+1] == "=" {
				idValue = tokens[i+2]
				hasID = true
				isIDFilter = true
				i += 3
			}
		} else if upperFirst == "ID" && i+2 < len(tokens) && tokens[i+1] == "=" {
			idValue = tokens[i+2]
			hasID = true
			isIDFilter = true
			i += 3
		} else {
			if i+2 < len(tokens) {
				operator := strings.ToUpper(tokens[i+1])
				if operator == "=" || operator == "==" || operator == "!=" ||
					operator == ">" || operator == "<" || operator == ">=" || operator == "<=" ||
					operator == "LIKE" || operator == "CONTAINS" {
					fieldName = firstToken
					fieldValue = tokens[i+2]
					hasField = true
					i += 3
				}
			}

			if !hasField && !hasWhere && !hasID {
				if i+1 < len(tokens) {
					nextUpper := strings.ToUpper(tokens[i+1])
					if nextUpper == "SET" || nextUpper == "INC" || nextUpper == "DEC" ||
						nextUpper == "PUSH" || nextUpper == "PULL" {
						idValue = firstToken
						hasID = true
						isIDFilter = true
						i++
					}
				} else {
					if i+1 < len(tokens) {
						idValue = firstToken
						hasID = true
						isIDFilter = true
						i++
					}
				}
			}
		}
	}

	// ============================================================
	// SI ES UN FILTRO POR ID, USAR id (NO _id)
	// ============================================================
	if isIDFilter && idValue != "" {
		idValue = strings.Trim(idValue, "\"'")

		// IMPORTANTE: Usar el campo "id" (no "_id")
		var idFilter Filter
		idFilter.Field = "id" // <- SIEMPRE "id", nunca "_id"
		idFilter.Op = "="
		if num, err := strconv.ParseFloat(idValue, 64); err == nil {
			idFilter.Value = num
		} else {
			idFilter.Value = idValue
		}
		filters = []Filter{idFilter}
	}

	// ============================================================
	// PROCESAR WHERE (convertir _id a id)
	// ============================================================
	// Migrated to parseWhereClause/parse.Expr: real parentheses/NOT/
	// AND-OR precedence, same pattern already used for
	// DELETE/CLEAR/COUNT/aggregations (see query_filter.go's
	// matchesWhere). UPDATE writes and replicates, but its WAL entry
	// is already the fully-computed new document body (not a filter
	// description to re-match at recovery time the way DELETE's WAL
	// entry is -- see ops_local.go/wal_recovery.go's "update" case),
	// so unlike DELETE this didn't need any WAL-shape change, only
	// threading `where` through Engine.Update/updateLocal and
	// RaftCommand.Where (which already existed, added for DELETE).
	//
	// A second, pre-existing bug found and fixed while doing this
	// migration: this block used to always re-scan tokens[i:] looking
	// for a literal "WHERE" token before parsing. But for the by far
	// most common form -- "UPDATE <block> WHERE ..." -- the initial
	// token-classification block above (search "ANALIZAR EL TOKEN
	// INICIAL") already detects "WHERE" as tokens[i] itself, sets
	// hasWhere=true, and advances i PAST it. That left this block
	// scanning for a "WHERE" that no longer existed in the remaining
	// tokens, so it silently found nothing, leaving filters/where both
	// nil -- which matchesWhere/matchesFilters treat as "no
	// restriction, match every document in the block". In other
	// words, "UPDATE <block> WHERE <condition> SET ..." was silently
	// updating the ENTIRE block, ignoring the WHERE condition
	// entirely, for any WHERE clause that didn't hit the isIDFilter
	// shortcut. This never surfaced before because nothing exercised
	// "UPDATE ... WHERE ..." through the real command string (see
	// update_where_tree_test.go). Fixed by parsing from `i` directly
	// when hasWhere is already true, instead of re-searching for the
	// token that's already been consumed.
	if !isIDFilter && hasWhere {
		j := i
		var err error
		where, filters, err = parseWhereClause(tokens, &j)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		i = j
		for idx := range filters {
			if filters[idx].Field == "_id" {
				filters[idx].Field = "id"
			}
		}
	} else if !isIDFilter {
		for j := i; j < len(tokens); j++ {
			if strings.ToUpper(tokens[j]) == "WHERE" {
				j++
				var err error
				where, filters, err = parseWhereClause(tokens, &j)
				if err != nil {
					return "ERROR: " + err.Error()
				}

				// Convertir cualquier filtro _id a id
				for idx := range filters {
					if filters[idx].Field == "_id" {
						filters[idx].Field = "id"
					}
				}
				break
			}
		}
	}

	if hasField && fieldName != "" && fieldValue != "" {
		fieldValue = strings.Trim(fieldValue, "\"'")
		var fieldFilter Filter
		fieldFilter.Field = fieldName
		fieldFilter.Op = "="
		if num, err := strconv.ParseFloat(fieldValue, 64); err == nil {
			fieldFilter.Value = num
		} else {
			fieldFilter.Value = fieldValue
		}
		filters = []Filter{fieldFilter}
	}

	// ============================================================
	// SI NO HAY WHERE NI ID Y HAY SET, ACTUALIZAR TODOS
	// ============================================================
	if !hasWhere && !hasID && !hasField && !isIDFilter {
		setPos := -1
		for j := i; j < len(tokens); j++ {
			if strings.ToUpper(tokens[j]) == "SET" {
				setPos = j
				break
			}
		}

		if setPos != -1 && setPos+1 < len(tokens) {
			jsonStr := strings.Join(tokens[setPos+1:], " ")
			if strings.HasPrefix(jsonStr, "{") {
				var setData map[string]any
				if err := json.Unmarshal([]byte(jsonStr), &setData); err == nil {
					for k, v := range setData {
						op.Set[k] = v
					}
				}
			} else {
				parts := strings.Split(jsonStr, ",")
				for _, part := range parts {
					part = strings.TrimSpace(part)
					if strings.Contains(part, "=") {
						kv := strings.SplitN(part, "=", 2)
						if len(kv) == 2 {
							key := strings.TrimSpace(kv[0])
							op.Set[key] = resolveSetLiteral(kv[1])
						}
					}
				}
			}

			count, err := execUpdate(eng, sess, db, block, []Filter{}, nil, op)
			if err != nil {
				return "ERROR: " + err.Error()
			}
			return fmt.Sprintf("Updated %d documents", count)
		}
	}

	// ============================================================
	// PROCESAR SET, INC, DEC, PUSH, PULL
	// ============================================================
	for i < len(tokens) {
		token := strings.ToUpper(tokens[i])

		switch token {
		case "SET":
			i++
			if i < len(tokens) {
				jsonStr := strings.Join(tokens[i:], " ")
				if strings.HasPrefix(jsonStr, "{") {
					jsonEnd := strings.Index(jsonStr, "}")
					if jsonEnd != -1 {
						jsonStr = jsonStr[:jsonEnd+1]
						var setData map[string]any
						if err := json.Unmarshal([]byte(jsonStr), &setData); err == nil {
							for k, v := range setData {
								op.Set[k] = v
							}
							i += len(strings.Split(jsonStr, " "))
							break
						}
					}
				}
				for i < len(tokens) {
					if strings.ToUpper(tokens[i]) == "WHERE" ||
						strings.ToUpper(tokens[i]) == "INC" ||
						strings.ToUpper(tokens[i]) == "DEC" ||
						strings.ToUpper(tokens[i]) == "PUSH" ||
						strings.ToUpper(tokens[i]) == "PULL" {
						break
					}
					if strings.Contains(tokens[i], "=") {
						parts := strings.SplitN(tokens[i], "=", 2)
						if len(parts) == 2 {
							key := strings.TrimSpace(parts[0])
							valStr := stripTrailingComma(parts[1])
							nextI := i + 1
							// A call like now() tokenizes as this token
							// ("...=now") plus separate "(" ")" tokens --
							// pull those in so it isn't left as the bare
							// string "now" (see consumeParenGroup).
							if group, after, ok := consumeParenGroup(tokens, nextI); ok {
								valStr = valStr + " " + group
								nextI = after
							}
							var val any
							if err := json.Unmarshal([]byte(valStr), &val); err != nil {
								val = resolveSetLiteral(valStr)
							}
							op.Set[key] = val
							i = nextI
							continue
						}
					} else if strings.Contains(tokens[i], ":") {
						parts := strings.SplitN(tokens[i], ":", 2)
						if len(parts) == 2 {
							key := strings.TrimSpace(parts[0])
							valStr := stripTrailingComma(parts[1])
							nextI := i + 1
							if group, after, ok := consumeParenGroup(tokens, nextI); ok {
								valStr = valStr + " " + group
								nextI = after
							}
							var val any
							if err := json.Unmarshal([]byte(valStr), &val); err != nil {
								val = resolveSetLiteral(valStr)
							}
							op.Set[key] = val
							i = nextI
							continue
						}
					}
					if i+2 < len(tokens) && tokens[i+1] == "=" {
						key := tokens[i]
						valStr := stripTrailingComma(tokens[i+2])
						nextI := i + 3
						if group, after, ok := consumeParenGroup(tokens, nextI); ok {
							valStr = valStr + " " + group
							nextI = after
						}
						var val any
						if err := json.Unmarshal([]byte(valStr), &val); err != nil {
							val = resolveSetLiteral(valStr)
						}
						op.Set[key] = val
						i = nextI
						continue
					}
					i++
				}
			}
		case "INC":
			i++
			for i < len(tokens) {
				if strings.ToUpper(tokens[i]) == "WHERE" ||
					strings.ToUpper(tokens[i]) == "SET" ||
					strings.ToUpper(tokens[i]) == "DEC" ||
					strings.ToUpper(tokens[i]) == "PUSH" ||
					strings.ToUpper(tokens[i]) == "PULL" {
					break
				}
				if strings.Contains(tokens[i], "=") {
					parts := strings.SplitN(tokens[i], "=", 2)
					if len(parts) == 2 {
						val, err := strconv.ParseFloat(stripTrailingComma(parts[1]), 64)
						if err == nil {
							op.Inc[parts[0]] = val
						}
					}
				} else if strings.Contains(tokens[i], ":") {
					parts := strings.SplitN(tokens[i], ":", 2)
					if len(parts) == 2 {
						val, err := strconv.ParseFloat(stripTrailingComma(parts[1]), 64)
						if err == nil {
							op.Inc[parts[0]] = val
						}
					}
				} else if i+2 < len(tokens) && tokens[i+1] == "=" {
					val, err := strconv.ParseFloat(stripTrailingComma(tokens[i+2]), 64)
					if err == nil {
						op.Inc[tokens[i]] = val
					}
					i += 2
				}
				i++
			}
		case "DEC":
			i++
			for i < len(tokens) {
				if strings.ToUpper(tokens[i]) == "WHERE" ||
					strings.ToUpper(tokens[i]) == "SET" ||
					strings.ToUpper(tokens[i]) == "INC" ||
					strings.ToUpper(tokens[i]) == "PUSH" ||
					strings.ToUpper(tokens[i]) == "PULL" {
					break
				}
				if strings.Contains(tokens[i], "=") {
					parts := strings.SplitN(tokens[i], "=", 2)
					if len(parts) == 2 {
						val, err := strconv.ParseFloat(stripTrailingComma(parts[1]), 64)
						if err == nil {
							op.Dec[parts[0]] = val
						}
					}
				} else if strings.Contains(tokens[i], ":") {
					parts := strings.SplitN(tokens[i], ":", 2)
					if len(parts) == 2 {
						val, err := strconv.ParseFloat(stripTrailingComma(parts[1]), 64)
						if err == nil {
							op.Dec[parts[0]] = val
						}
					}
				} else if i+2 < len(tokens) && tokens[i+1] == "=" {
					val, err := strconv.ParseFloat(stripTrailingComma(tokens[i+2]), 64)
					if err == nil {
						op.Dec[tokens[i]] = val
					}
					i += 2
				}
				i++
			}
		case "PUSH":
			i++
			for i < len(tokens) {
				if strings.ToUpper(tokens[i]) == "WHERE" ||
					strings.ToUpper(tokens[i]) == "SET" ||
					strings.ToUpper(tokens[i]) == "INC" ||
					strings.ToUpper(tokens[i]) == "DEC" ||
					strings.ToUpper(tokens[i]) == "PULL" {
					break
				}
				if strings.Contains(tokens[i], "=") {
					parts := strings.SplitN(tokens[i], "=", 2)
					if len(parts) == 2 {
						v := stripTrailingComma(parts[1])
						var val any
						if err := json.Unmarshal([]byte(v), &val); err != nil {
							val = strings.Trim(v, "\"'")
						}
						op.Push[parts[0]] = val
					}
				} else if strings.Contains(tokens[i], ":") {
					parts := strings.SplitN(tokens[i], ":", 2)
					if len(parts) == 2 {
						v := stripTrailingComma(parts[1])
						var val any
						if err := json.Unmarshal([]byte(v), &val); err != nil {
							val = strings.Trim(v, "\"'")
						}
						op.Push[parts[0]] = val
					}
				} else if i+2 < len(tokens) && tokens[i+1] == "=" {
					v := stripTrailingComma(tokens[i+2])
					var val any
					if err := json.Unmarshal([]byte(v), &val); err != nil {
						val = strings.Trim(v, "\"'")
					}
					op.Push[tokens[i]] = val
					i += 2
				}
				i++
			}
		case "PULL":
			i++
			for i < len(tokens) {
				if strings.ToUpper(tokens[i]) == "WHERE" ||
					strings.ToUpper(tokens[i]) == "SET" ||
					strings.ToUpper(tokens[i]) == "INC" ||
					strings.ToUpper(tokens[i]) == "DEC" ||
					strings.ToUpper(tokens[i]) == "PUSH" {
					break
				}
				if strings.Contains(tokens[i], "=") {
					parts := strings.SplitN(tokens[i], "=", 2)
					if len(parts) == 2 {
						v := stripTrailingComma(parts[1])
						var val any
						if err := json.Unmarshal([]byte(v), &val); err != nil {
							val = strings.Trim(v, "\"'")
						}
						op.Pull[parts[0]] = val
					}
				} else if strings.Contains(tokens[i], ":") {
					parts := strings.SplitN(tokens[i], ":", 2)
					if len(parts) == 2 {
						v := stripTrailingComma(parts[1])
						var val any
						if err := json.Unmarshal([]byte(v), &val); err != nil {
							val = strings.Trim(v, "\"'")
						}
						op.Pull[parts[0]] = val
					}
				} else if i+2 < len(tokens) && tokens[i+1] == "=" {
					v := stripTrailingComma(tokens[i+2])
					var val any
					if err := json.Unmarshal([]byte(v), &val); err != nil {
						val = strings.Trim(v, "\"'")
					}
					op.Pull[tokens[i]] = val
					i += 2
				}
				i++
			}
		default:
			if i+2 < len(tokens) {
				operator := strings.ToUpper(tokens[i+1])
				if operator == "=" || operator == "==" {
					fieldName := tokens[i]
					fieldValue := tokens[i+2]
					fieldValue = strings.Trim(fieldValue, "\"'")
					var fieldFilter Filter
					fieldFilter.Field = fieldName
					fieldFilter.Op = "="
					if num, err := strconv.ParseFloat(fieldValue, 64); err == nil {
						fieldFilter.Value = num
					} else {
						fieldFilter.Value = fieldValue
					}
					filters = []Filter{fieldFilter}
					i += 3
					continue
				}
			}
			i++
		}
	}

	if len(filters) == 0 && !hasID && !hasField && !hasWhere && !isIDFilter {
		return "ERROR: UPDATE requires WHERE clause, ID, field condition"
	}

	// ============================================================
	// VERIFICAR QUE EL FILTRO USARÁ EL CAMPO "id" CORRECTO
	// ============================================================
	for idx := range filters {
		if filters[idx].Field == "_id" {
			filters[idx].Field = "id"
		}
	}

	count, err := execUpdate(eng, sess, db, block, filters, where, op)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	if count == 0 {
		return "Updated 0 documents (verify your condition)"
	}
	return fmt.Sprintf("Updated %d documents", count)
}
