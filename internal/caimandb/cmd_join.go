package caimandb

import (
	"fmt"
	"strings"
)

// joinKeyValue resolves the value used to match a JOIN condition side. The
// document's own id is stored on Document.ID, not inside Data, so "_id"
// and "id" have to be special-cased -- looking them up via getNested
// against Data always returns nil (the key simply isn't there), which is
// why every JOIN using "block._id" used to silently match 0 rows.
func joinKeyValue(doc Document, field string) string {
	if field == "_id" || field == "id" {
		return doc.ID
	}
	return fmt.Sprint(getNested(doc.Data, field))
}

func handleJoin(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 7 {
		return "ERROR: JOIN requires: JOIN block1 WITH block2 ON block1.field = block2.field"
	}

	block1 := tokens[1]
	if strings.ToUpper(tokens[2]) != "WITH" {
		return "ERROR: Expected WITH keyword"
	}
	block2 := tokens[3]
	if strings.ToUpper(tokens[4]) != "ON" {
		return "ERROR: Expected ON keyword"
	}

	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block1) {
		return "ERROR: Block not found: " + block1
	}
	if !eng.dirMgr.BlockExists(db, block2) {
		return "ERROR: Block not found: " + block2
	}

	cond := strings.Join(tokens[5:], " ")
	cond = strings.TrimSpace(cond)

	result1, err := eng.Find(db, block1, &Query{Limit: 10000})
	if err != nil {
		return "ERROR: " + err.Error()
	}

	result2, err := eng.Find(db, block2, &Query{Limit: 10000})
	if err != nil {
		return "ERROR: " + err.Error()
	}

	parts := strings.Split(cond, "=")
	if len(parts) != 2 {
		return "ERROR: Invalid join condition. Use: block1.field = block2.field"
	}

	field1 := strings.TrimSpace(parts[0])
	field2 := strings.TrimSpace(parts[1])

	if strings.Contains(field1, ".") {
		field1 = strings.SplitN(field1, ".", 2)[1]
	}
	if strings.Contains(field2, ".") {
		field2 = strings.SplitN(field2, ".", 2)[1]
	}

	index2 := make(map[string]Document)
	for _, doc := range result2.Docs {
		val := joinKeyValue(doc, field2)
		if _, exists := index2[val]; !exists {
			index2[val] = doc
		}
	}

	var joined []map[string]any
	for _, doc1 := range result1.Docs {
		val := joinKeyValue(doc1, field1)
		if doc2, ok := index2[val]; ok {
			joinedDoc := make(map[string]any)
			for k, v := range doc1.Data {
				joinedDoc[block1+"."+k] = v
			}
			for k, v := range doc2.Data {
				joinedDoc[block2+"."+k] = v
			}
			joined = append(joined, joinedDoc)
		}
	}

	return fmt.Sprintf("Joined %d documents from %s and %s:\n%s",
		len(joined), block1, block2, jsonOut(joined))
}
