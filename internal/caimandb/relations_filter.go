//
// This file lets FIND's WHERE clause filter on RELATE-alias fields, e.g.
//
//	RELATE movies USE directors, actors, genres
//	FIND movies SELECT title, directors.name
//	WHERE directors.name == "Christopher Nolan"
//
// SELECT already resolves "directors.name" after filtering (see
// relations_resolve.go), but matching happens earlier, against the raw
// stored document -- which only has the foreign-key field (e.g.
// "director_id"), not a nested "directors" object. The functions below
// figure out which RELATE aliases a query's WHERE clause actually
// references, fetch just those related documents, and attach them to a
// filtering copy of the document under the alias key so matchFilter's
// existing dotted-path lookup (getNestedValues) resolves them exactly
// like it already does for SELECT-driven projection.
package caimandb

import (
	"strings"

	"caimandb/internal/caimandb/parse"
)

// splitFieldRoot splits "alias.field" into ("alias", "field", true), or
// returns (field, "", false) for a field with no dot.
func splitFieldRoot(field string) (root, rest string, hasDot bool) {
	idx := strings.Index(field, ".")
	if idx <= 0 {
		return field, "", false
	}
	return field[:idx], field[idx+1:], true
}

// exprFieldRoots walks e (and, via Left/Right/Child, its whole subtree)
// collecting the root of every leaf condition's field into roots.
func exprFieldRoots(e *parse.Expr, roots map[string]bool) {
	if e == nil {
		return
	}
	if e.Kind == parse.KindCondition {
		if root, _, ok := splitFieldRoot(e.Field); ok {
			roots[root] = true
		}
	}
	exprFieldRoots(e.Left, roots)
	exprFieldRoots(e.Right, roots)
	exprFieldRoots(e.Child, roots)
}

// filterAliasRoots returns the set of RELATE aliases (registered in def)
// that q's WHERE clause actually references, so callers only pay to
// resolve relations a query needs. Returns nil when there's nothing to
// resolve (no relation def, or no alias referenced).
func filterAliasRoots(q *Query, def *RelationDef) map[string]bool {
	if def == nil || len(def.Targets) == 0 {
		return nil
	}

	roots := make(map[string]bool)
	if q.Where != nil {
		exprFieldRoots(q.Where, roots)
	} else {
		for _, f := range q.Filters {
			if root, _, ok := splitFieldRoot(f.Field); ok {
				roots[root] = true
			}
		}
	}
	if len(roots) == 0 {
		return nil
	}

	needed := make(map[string]bool)
	for _, t := range def.Targets {
		if roots[t.Alias] {
			needed[t.Alias] = true
		}
	}
	if len(needed) == 0 {
		return nil
	}
	return needed
}

// attachRelationsForFilter returns a copy of doc whose Data has each alias
// in needed resolved and nested under that alias key -- the related
// document's Data for a single-valued FK, or a slice of them for an
// array FK -- so WHERE conditions on "<alias>.<field>" can be evaluated
// with the same dotted-path lookup SELECT projection already uses. The
// original doc (and its Data map) is left untouched; only the returned
// copy carries the extra fields, so callers can still use the pristine
// doc for anything returned to the user.
func (e *Engine) attachRelationsForFilter(doc Document, def *RelationDef, needed map[string]bool) Document {
	if len(needed) == 0 || def == nil {
		return doc
	}

	out := make(map[string]any, len(doc.Data)+len(needed))
	for k, v := range doc.Data {
		out[k] = v
	}

	for _, t := range def.Targets {
		if !needed[t.Alias] {
			continue
		}
		ids := fkValues(doc.Data, t.FK)
		if isArrayFK(doc.Data, t.FK) {
			var vals []any
			for _, id := range ids {
				if rdoc, ok := e.FindByID(t.DB, t.Block, id); ok {
					vals = append(vals, rdoc.Data)
				}
			}
			out[t.Alias] = vals
		} else if len(ids) > 0 {
			if rdoc, ok := e.FindByID(t.DB, t.Block, ids[0]); ok {
				out[t.Alias] = rdoc.Data
			}
		}
	}

	doc.Data = out
	return doc
}

// aggregateContext returns the data map used to evaluate COUNT(...) /
// arithmetic SELECT items and HAVING conditions for one representative
// document of a GROUP BY group. It starts from the document's own
// fields, then for every RELATE alias not already present as a plain
// field, adds a same-length placeholder slice built from the foreign-key
// values -- e.g. alias "actors" with FK "actor_id" -- so COUNT(actors)
// reports how many related ids there are without needing to fetch every
// related document just to count them.
func aggregateContext(doc Document, def *RelationDef) map[string]any {
	data := make(map[string]any, len(doc.Data))
	for k, v := range doc.Data {
		data[k] = v
	}
	if def == nil {
		return data
	}
	for _, t := range def.Targets {
		if _, exists := data[t.Alias]; exists {
			continue
		}
		ids := fkValues(doc.Data, t.FK)
		if ids == nil {
			continue
		}
		data[t.Alias] = make([]any, len(ids))
	}
	return data
}
