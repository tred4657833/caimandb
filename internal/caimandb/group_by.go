//
// This file implements FIND's GROUP BY / HAVING clause:
//
//	FIND movies SELECT title, COUNT(actors) as actors_count, year
//	WHERE year >= 2000
//	GROUP BY title, year
//	HAVING COUNT(actors) >= 5
//
// It runs in postProcessDocs, after WHERE has already selected the
// matching documents and before ORDER BY/LIMIT: groupDocs collapses those
// documents into one synthetic Document per distinct GroupBy value
// combination, evaluates any COUNT(...)/arithmetic SELECT items against
// each group, and drops groups that don't satisfy HAVING.
package caimandb

import (
	"caimandb/internal/caimandb/query"
	"fmt"
	"strings"
)

// groupDocs collapses docs into one row per distinct combination of
// q.GroupBy field values, in first-seen order. Each output row's Data
// holds the GroupBy field values plus whatever q.SelectItems asks for:
// a plain field is copied from the group's representative document (the
// first doc seen for that group), COUNT(field)/arithmetic items are
// computed against it (via aggregateContext, so COUNT can reference a
// RELATE alias like "actors" without needing every related doc fetched).
// When q.SelectItems is empty, each row instead gets a "count" field
// with the number of documents that fell into that group -- a plain
// GROUP BY with no explicit aggregate still returns something useful.
// q.Having, if set, is evaluated per group (via the same expression
// evaluator WHERE uses) and groups that don't satisfy it are dropped.
func (e *Engine) groupDocs(dbName, block string, docs []Document, q *Query) []Document {
	if len(q.GroupBy) == 0 {
		return docs
	}

	// TODO(pendiente-motor): WITH ROLLUP / WITH CUBE no existen. Hoy
	// groupDocs solo produce un renglón por combinación exacta de
	// q.GroupBy -- para ROLLUP hace falta, además de esos renglones,
	// subtotales por cada prefijo de q.GroupBy (p. ej. GROUP BY pais,
	// ciudad WITH ROLLUP agrega un renglón por pais con ciudad=NULL, y
	// un gran total con ambos NULL) y para CUBE, subtotales para cada
	// combinación posible de campos, no solo los prefijos. Necesitaría
	// un flag en Query (q.Rollup/q.Cube, threading desde el parser de
	// GROUP BY) y una segunda pasada aquí que, tras construir "groups"
	// como hoy, genere agregaciones adicionales agrupando por
	// subconjuntos de q.GroupBy en vez de por la clave completa.
	def, _ := e.relMgr.Get(dbName, block)

	type group struct {
		first Document
		count int
	}
	order := make([]string, 0, len(docs))
	groups := make(map[string]*group, len(docs))

	for _, d := range docs {
		keyParts := make([]string, len(q.GroupBy))
		for i, f := range q.GroupBy {
			keyParts[i] = fmt.Sprint(getNested(d.Data, f))
		}
		key := strings.Join(keyParts, "\x1f")

		g, ok := groups[key]
		if !ok {
			g = &group{first: d}
			groups[key] = g
			order = append(order, key)
		}
		g.count++
	}

	out := make([]Document, 0, len(order))
	for _, key := range order {
		g := groups[key]
		aggData := aggregateContext(g.first, def)

		row := make(map[string]any, len(q.GroupBy)+len(q.SelectItems)+1)
		for _, f := range q.GroupBy {
			row[f] = getNested(aggData, f)
		}

		if len(q.SelectItems) == 0 {
			row["count"] = g.count
		}
		for _, it := range q.SelectItems {
			switch it.Kind {
			case query.SelectPlain:
				if _, exists := row[it.Raw]; !exists {
					row[it.Raw] = getNested(aggData, it.Raw)
				}
			case query.SelectCount:
				row[it.Alias] = query.CountValue(it.CountField, aggData)
			case query.SelectExprArith:
				row[it.Alias] = query.EvalArith(it, aggData)
			}
		}

		if q.Having != nil {
			// HAVING can reference a raw aggregate expression that never
			// made it into row (e.g. "HAVING COUNT(actors) >= 5" with no
			// matching COUNT(actors) in SELECT), so evaluate it against
			// aggData -- which has every field/relation-placeholder the
			// representative document has -- merged with the row values
			// SELECT already computed (so HAVING can also reference a
			// SELECT alias directly).
			havingData := make(map[string]any, len(aggData)+len(row))
			for k, v := range aggData {
				havingData[k] = v
			}
			for k, v := range row {
				havingData[k] = v
			}
			if !evalExpr(Document{Data: havingData}, q.Having) {
				continue
			}
		}

		out = append(out, Document{ID: g.first.ID, ShardID: g.first.ShardID, Data: row})
	}

	return out
}
