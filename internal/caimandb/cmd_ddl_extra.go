// cmd_ddl_extra.go implements the DSL handlers for four new commands
// layered on top of existing engine primitives (Update/Insert/Find),
// so none of them need their own storage-level logic:
//
//	TRUNCATE <block>
//	UPSERT <block> <id> SET <field>=<value>[, ...]
//	UPSERT <block> <id> <json>
//	ADD FIELD <block>.<field> DEFAULT <value>[, <field2> DEFAULT <value2> ...]
//	DROP FIELD <block>.<field>[, <field2> ...] [WHERE <condición>]
package caimandb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"caimandb/internal/caimandb/parse"
)

// handleTruncate empties a block unconditionally (no confirmation
// prompt, unlike CLEAR/EMPTY in cmd_clear_count.go) -- matching the
// classic SQL TRUNCATE semantics the requested syntax is modeled on.
// It's a thin wrapper over the same EmptyBlock the existing CLEAR
// command already uses.
func handleTruncate(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: TRUNCATE requires a block name"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}
	count, err := eng.EmptyBlock(db, block)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Truncated %s (%d document(s) removed)", block, count)
}

// coerceDDLValue turns a raw token into a string/float64/bool/nil,
// the same implicit typing the WHERE-clause parser and the existing
// "UPDATE ALL SET field=value" flat parser (cmd_update.go) already
// use, kept local here since parse.coerce is unexported.
func coerceDDLValue(raw string) any {
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
	return raw
}

// normalizeCommaTokens re-tokenizes a token slice after forcing every
// "," to stand as its own token (it otherwise sticks to whichever
// token it's adjacent to, since parse.Tokenize only splits on
// whitespace/parens -- see tokenizer.go). This lets ADD FIELD/DROP
// FIELD walk a clean, comma-delimited token stream regardless of
// whether the input had spaces around each comma.
//
// Limitation shared with the existing "UPDATE ALL SET a=1,b=2" flat
// parser (cmd_update.go), which has the same issue: a comma inside a
// quoted default value (DEFAULT "a,b") would be split too. Not
// handled here for the same reason it isn't there -- an edge case
// this DSL's simple non-JSON assignment syntax doesn't try to solve.
func normalizeCommaTokens(tokens []string) []string {
	joined := strings.Join(tokens, " ")
	joined = strings.ReplaceAll(joined, ",", " , ")
	return parse.Tokenize(joined)
}

// handleAddField implements "ADD FIELD <block>.<field> DEFAULT <value>
// [, <field2> DEFAULT <value2> ...]". Only the first field carries the
// "<block>." prefix; every field after a comma is assumed to belong to
// the same block. Existing documents that already have a field (even
// with a falsy value) are left untouched -- see UpdateOp.SetDefault
// in ops_update.go.
func handleAddField(eng *Engine, sess *Session, tokens []string) string {
	// tokens[0] == "ADD", tokens[1] == "FIELD"
	if len(tokens) < 5 {
		return "ERROR: usage: ADD FIELD <block>.<field> DEFAULT <value> [, <field2> DEFAULT <value2> ...]"
	}
	norm := normalizeCommaTokens(tokens[2:])

	block := ""
	defaults := map[string]any{}
	first := true
	i := 0
	for i < len(norm) {
		if norm[i] == "," {
			i++
			continue
		}
		fieldTok := norm[i]
		i++
		if i >= len(norm) || strings.ToUpper(norm[i]) != "DEFAULT" {
			return "ERROR: ADD FIELD requires 'DEFAULT <value>' after field '" + fieldTok + "'"
		}
		i++
		if i >= len(norm) {
			return "ERROR: missing default value for field '" + fieldTok + "'"
		}
		valTok := norm[i]
		i++

		var fieldName string
		if first {
			dot := strings.Index(fieldTok, ".")
			if dot < 0 {
				return "ERROR: ADD FIELD requires <block>.<field> for the first field"
			}
			block = fieldTok[:dot]
			fieldName = fieldTok[dot+1:]
			first = false
		} else {
			fieldName = fieldTok
		}
		if fieldName == "" {
			return "ERROR: ADD FIELD: empty field name"
		}
		defaults[fieldName] = coerceDDLValue(valTok)
	}
	if block == "" || len(defaults) == 0 {
		return "ERROR: ADD FIELD requires at least one <block>.<field> DEFAULT <value>"
	}

	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	count, err := eng.Update(db, block, []Filter{}, nil, UpdateOp{SetDefault: defaults})
	if err != nil {
		return "ERROR: " + err.Error()
	}
	names := make([]string, 0, len(defaults))
	for name := range defaults {
		names = append(names, name)
	}
	return fmt.Sprintf("Added field(s) %s to %s (checked %d document(s); existing values were left untouched)",
		strings.Join(names, ", "), block, count)
}

// handleDropField implements "DROP FIELD <block>.<field>[, <field2>
// ...] [WHERE <condición>]". Without WHERE it's a single bulk Update
// across the whole block. With WHERE, correctness for arbitrary
// expressions (not just a top-level AND-chain) matters more than
// doing it in one pass, so it resolves matches once via Find (which
// evaluates the full parse.Expr tree, ORs/NOTs/parens included) and
// then issues one targeted per-document Update -- eng.Update's own
// signature only takes a flat []Filter, which can't safely represent
// an arbitrary WHERE tree.
func handleDropField(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: usage: DROP FIELD <block>.<field> [, <field2> ...] [WHERE <condición>]"
	}

	whereIdx := -1
	for i := 2; i < len(tokens); i++ {
		if strings.ToUpper(tokens[i]) == "WHERE" {
			whereIdx = i
			break
		}
	}
	fieldTokens := tokens[2:]
	if whereIdx != -1 {
		fieldTokens = tokens[2:whereIdx]
	}

	norm := normalizeCommaTokens(fieldTokens)
	block := ""
	var fields []string
	first := true
	for _, tok := range norm {
		if tok == "," {
			continue
		}
		if first {
			dot := strings.Index(tok, ".")
			if dot < 0 {
				return "ERROR: DROP FIELD requires <block>.<field> for the first field"
			}
			block = tok[:dot]
			fields = append(fields, tok[dot+1:])
			first = false
		} else {
			fields = append(fields, tok)
		}
	}
	if block == "" || len(fields) == 0 {
		return "ERROR: DROP FIELD requires at least one <block>.<field>"
	}

	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	op := UpdateOp{Unset: fields}
	fieldList := strings.Join(fields, ", ")

	if whereIdx == -1 {
		count, err := eng.Update(db, block, []Filter{}, nil, op)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return fmt.Sprintf("Dropped field(s) %s from %d document(s) in %s", fieldList, count, block)
	}

	whereTokens := tokens[whereIdx+1:]
	if len(whereTokens) == 0 {
		return "ERROR: WHERE requires a condition"
	}
	where, _, err := parse.ParseWhere(whereTokens)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	result, err := eng.Find(db, block, &Query{Where: where})
	if err != nil {
		return "ERROR: " + err.Error()
	}
	var count int64
	for _, doc := range result.Docs {
		n, err := eng.Update(db, block, []Filter{{Field: "id", Op: "=", Value: doc.ID}}, nil, op)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		count += n
	}
	return fmt.Sprintf("Dropped field(s) %s from %d document(s) in %s", fieldList, count, block)
}

// handleUpsert implements:
//
//	UPSERT <block> <id> SET <field>=<value>[, <field2>=<value2> ...]
//	UPSERT <block> <id> <json-object>
//
// It updates the document if <id> already exists, otherwise inserts a
// new one with that id -- reusing eng.Update/eng.FindByID/eng.Insert
// as-is rather than adding a new storage-level primitive.
func handleUpsert(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 4 {
		return "ERROR: usage: UPSERT <block> <id> SET <field>=<value>[, ...] | UPSERT <block> <id> <json>"
	}
	block := tokens[1]
	id := tokens[2]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}

	setData := map[string]any{}
	if strings.ToUpper(tokens[3]) == "SET" {
		if len(tokens) < 5 {
			return "ERROR: UPSERT ... SET requires at least one assignment"
		}
		body := strings.Join(tokens[4:], " ")
		for _, part := range strings.Split(body, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				return "ERROR: invalid assignment near '" + part + "'"
			}
			key := strings.TrimSpace(kv[0])
			if key == "" {
				return "ERROR: invalid assignment near '" + part + "'"
			}
			setData[key] = coerceDDLValue(kv[1])
		}
	} else {
		body := strings.Join(tokens[3:], " ")
		if err := json.Unmarshal([]byte(body), &setData); err != nil {
			return "ERROR: invalid JSON body for UPSERT: " + err.Error()
		}
	}
	if len(setData) == 0 {
		return "ERROR: UPSERT requires at least one field to set"
	}

	if _, found := eng.FindByID(db, block, id); found {
		n, err := eng.Update(db, block, []Filter{{Field: "id", Op: "=", Value: id}}, nil, UpdateOp{Set: setData})
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if n == 0 {
			return "ERROR: UPSERT found document " + id + " but the update matched nothing"
		}
		return fmt.Sprintf("Upserted (updated) %s in %s", id, block)
	}

	setData["id"] = id
	doc, err := eng.Insert(db, block, setData)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Upserted (inserted) %s in %s", doc.ID, block)
}
