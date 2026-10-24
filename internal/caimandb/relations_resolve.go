package caimandb

import "strings"

// relSelectPlan splits the fields of a FIND ... SELECT into the ones that
// resolve directly on the source document ("plain") and the ones that
// follow a RELATE definition to another block ("relation"), grouped by
// alias -> requested sub-paths (an empty sub-path means "the whole related
// document").
type relSelectPlan struct {
	plainFields    []string
	relationFields map[string][]string
}

func planSelect(def *RelationDef, selectFields []string) relSelectPlan {
	plan := relSelectPlan{relationFields: make(map[string][]string)}

	aliasSet := make(map[string]bool)
	if def != nil {
		for _, t := range def.Targets {
			aliasSet[t.Alias] = true
		}
	}

	for _, raw := range selectFields {
		// A trailing ";" can end up attached to the last field when a
		// command is terminated with a semicolon, since the tokenizer only
		// splits on whitespace.
		field := strings.TrimSuffix(strings.TrimSpace(raw), ";")
		if field == "" {
			continue
		}

		parts := strings.SplitN(field, ".", 2)
		root := parts[0]
		if aliasSet[root] {
			sub := ""
			if len(parts) == 2 {
				sub = parts[1]
			}
			plan.relationFields[root] = append(plan.relationFields[root], sub)
			continue
		}

		plan.plainFields = append(plan.plainFields, field)
	}

	return plan
}

// resolveRelations rewrites each document's Data according to q.Select /
// q.Exclude, resolving any SELECT field whose root matches a RELATE alias
// registered for dbName.block by fetching the related document(s) --
// possibly from another database or block -- and nesting them under that
// alias. Fields with no matching relation are projected as before.
func (e *Engine) resolveRelations(dbName, block string, docs []Document, q *Query) []Document {
	if len(q.Select) == 0 && len(q.Exclude) == 0 {
		return docs
	}

	def, _ := e.relMgr.Get(dbName, block)
	if def == nil || len(def.Targets) == 0 || len(q.Select) == 0 {
		for i := range docs {
			docs[i].Data = project(docs[i].Data, q.Select, q.Exclude)
		}
		return docs
	}

	plan := planSelect(def, q.Select)
	if len(plan.relationFields) == 0 {
		for i := range docs {
			docs[i].Data = project(docs[i].Data, q.Select, q.Exclude)
		}
		return docs
	}

	targetsByAlias := make(map[string]RelationTarget, len(def.Targets))
	for _, t := range def.Targets {
		targetsByAlias[t.Alias] = t
	}

	// Collect every distinct related id we'll need per alias, so each is
	// fetched only once even when many source documents share it.
	neededIDs := make(map[string]map[string]bool, len(plan.relationFields))
	for alias := range plan.relationFields {
		neededIDs[alias] = make(map[string]bool)
	}
	for _, doc := range docs {
		for alias := range plan.relationFields {
			target, ok := targetsByAlias[alias]
			if !ok {
				continue
			}
			for _, id := range fkValues(doc.Data, target.FK) {
				neededIDs[alias][id] = true
			}
		}
	}

	relatedDocs := make(map[string]map[string]Document, len(plan.relationFields))
	for alias, ids := range neededIDs {
		target := targetsByAlias[alias]
		fetched := make(map[string]Document, len(ids))
		for id := range ids {
			if rdoc, ok := e.FindByID(target.DB, target.Block, id); ok {
				fetched[id] = *rdoc
			}
		}
		relatedDocs[alias] = fetched
	}

	for i := range docs {
		var out map[string]any
		if len(plan.plainFields) > 0 {
			out = project(docs[i].Data, plan.plainFields, q.Exclude)
		} else {
			// Every SELECT field resolved to a relation alias -- project()
			// treats an empty include list as "select everything", which
			// isn't right here since the user did specify a SELECT.
			out = make(map[string]any, len(plan.relationFields))
		}

		for alias, subPaths := range plan.relationFields {
			target, ok := targetsByAlias[alias]
			if !ok {
				continue
			}

			ids := fkValues(docs[i].Data, target.FK)
			multi := isArrayFK(docs[i].Data, target.FK)

			var vals []any
			for _, id := range ids {
				if rdoc, ok := relatedDocs[alias][id]; ok {
					vals = append(vals, projectRelated(rdoc.Data, subPaths))
				}
			}

			switch {
			case multi:
				out[alias] = vals
			case len(vals) > 0:
				out[alias] = vals[0]
			default:
				out[alias] = nil
			}
		}

		docs[i].Data = out
	}

	return docs
}

// fkValues extracts the related id(s) stored under the foreign-key field
// on a source document, whether it's a single scalar id or an array of ids.
func fkValues(data map[string]any, fk string) []string {
	v, ok := data[fk]
	if !ok {
		return nil
	}
	if arr, ok := v.([]any); ok {
		ids := make([]string, 0, len(arr))
		for _, item := range arr {
			if id := idFieldToString(item); id != "" {
				ids = append(ids, id)
			}
		}
		return ids
	}
	if id := idFieldToString(v); id != "" {
		return []string{id}
	}
	return nil
}

func isArrayFK(data map[string]any, fk string) bool {
	_, ok := data[fk].([]any)
	return ok
}

// projectRelated builds the nested value attached under a relation alias:
// the whole related document's data when the alias was selected bare
// ("director"), or just the requested sub-fields when dotted paths were
// given ("director.name").
func projectRelated(data map[string]any, subPaths []string) any {
	wantsWhole := false
	var fields []string
	for _, sp := range subPaths {
		if sp == "" {
			wantsWhole = true
			continue
		}
		fields = append(fields, sp)
	}
	if wantsWhole || len(fields) == 0 {
		return data
	}
	return project(data, fields, nil)
}
