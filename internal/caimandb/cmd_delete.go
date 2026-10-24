package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"fmt"
	"strconv"
	"strings"
)

func handleDelete(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: DELETE requires block name"
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

	// Verificar si hay tokens suficientes
	if len(tokens) >= 3 {
		nextToken := tokens[2]
		upperNext := strings.ToUpper(nextToken)

		// Caso: DELETE block ALL
		if upperNext == "ALL" {
			if isInteractive() {
				fmt.Print("Delete ALL documents from " + block + "? (y/N): ")
				var response string
				fmt.Scanln(&response)
				if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
					return "Delete cancelled"
				}
			}
			count, err := execDelete(eng, sess, db, block, []Filter{}, nil)
			if err != nil {
				return "ERROR: " + err.Error()
			}
			return fmt.Sprintf("Deleted %d documents from %s", count, block)
		}

		// Caso: DELETE block WHERE ...
		if upperNext == "WHERE" {
			idx := 3 // start after WHERE
			var err error
			where, filters, err = parseWhereClause(tokens, &idx)
			if err != nil {
				// Si falla, intentar extraer manualmente
				where = nil
				if len(tokens) >= 5 && tokens[3] == "=" {
					fieldName := tokens[2] // WHERE ya fue procesado
					fieldValue := tokens[4]
					fieldValue = strings.Trim(fieldValue, "\"'")
					var filter Filter
					filter.Field = fieldName
					filter.Op = "="
					if num, err := strconv.ParseFloat(fieldValue, 64); err == nil {
						filter.Value = num
					} else {
						filter.Value = fieldValue
					}
					filters = []Filter{filter}
				} else {
					return "ERROR: " + err.Error()
				}
			}
		} else if upperNext == "ID" && len(tokens) >= 4 {
			// DELETE block ID valor
			idValue := tokens[3]
			idValue = strings.Trim(idValue, "\"'")
			var filter Filter
			filter.Field = "id"
			filter.Op = "="
			if num, err := strconv.ParseFloat(idValue, 64); err == nil {
				filter.Value = num
			} else {
				filter.Value = idValue
			}
			filters = []Filter{filter}
		} else if upperNext == "_ID" && len(tokens) >= 5 && tokens[3] == "=" {
			// DELETE block _id = "valor"
			idValue := tokens[4]
			idValue = strings.Trim(idValue, "\"'")
			var filter Filter
			filter.Field = "id" // Convertir _id a id
			filter.Op = "="
			if num, err := strconv.ParseFloat(idValue, 64); err == nil {
				filter.Value = num
			} else {
				filter.Value = idValue
			}
			filters = []Filter{filter}
		} else if len(tokens) >= 5 && tokens[3] == "=" {
			// DELETE block campo = "valor"
			fieldName := nextToken
			fieldValue := tokens[4]
			fieldValue = strings.Trim(fieldValue, "\"'")
			var filter Filter
			filter.Field = fieldName
			filter.Op = "="
			if num, err := strconv.ParseFloat(fieldValue, 64); err == nil {
				filter.Value = num
			} else {
				filter.Value = fieldValue
			}
			filters = []Filter{filter}
		} else {
			// Asumir que es un ID directo: DELETE block valor
			idValue := nextToken
			idValue = strings.Trim(idValue, "\"'")
			var filter Filter
			filter.Field = "id"
			filter.Op = "="
			if num, err := strconv.ParseFloat(idValue, 64); err == nil {
				filter.Value = num
			} else {
				filter.Value = idValue
			}
			filters = []Filter{filter}
		}
	}

	// VERIFICAR QUE NO QUEDE _id EN LOS FILTROS
	for idx := range filters {
		if filters[idx].Field == "_id" {
			filters[idx].Field = "id"
		}
	}

	// len(filters) == 0 alone doesn't mean "no condition was given": a
	// WHERE with OR/NOT/parens (anything that isn't a pure top-level
	// AND-chain) makes parseWhereClause return a nil flat list on
	// purpose even though where holds a real, non-empty condition --
	// see parseWhereClause's doc comment in cmd_filters_util.go. Only
	// reject when neither filters nor where were ever populated.
	if len(filters) == 0 && where == nil {
		return "ERROR: DELETE requires WHERE clause, ID, or ALL"
	}

	count, err := execDelete(eng, sess, db, block, filters, where)
	if err != nil {
		return "ERROR: " + err.Error()
	}

	if count == 0 {
		return "Deleted 0 documents (verify your condition)"
	}
	return fmt.Sprintf("Deleted %d documents", count)
}
