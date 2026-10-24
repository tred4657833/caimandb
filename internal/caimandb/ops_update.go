package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"encoding/json"
	"fmt"
)

type UpdateOp struct {
	Set  map[string]any
	Inc  map[string]float64
	Dec  map[string]float64
	Push map[string]any
	Pull map[string]any

	// Unset lists dotted field paths to remove entirely from the
	// document (used by DROP FIELD). Unlike Set-ting a field to nil,
	// this deletes the key so it no longer appears in the document at
	// all (matters for EXISTS/IS NULL semantics and for JSON output).
	Unset []string

	// SetDefault sets a field only on documents where it is not
	// already present (used by ADD FIELD ... DEFAULT <value>).
	// Documents that already have the field, even with a falsy value
	// (0, "", false), are left untouched.
	SetDefault map[string]any
}

// Update, like Delete/Count/aggregations, takes a where *parse.Expr
// alongside filters []Filter: parseWhereClause (cmd_update.go) builds
// the real parenthesized/NOT/AND-OR tree, and matchesWhere
// (query_filter.go, called from updateLocal in ops_local.go) uses the
// tree when present, falling back to the flat filters otherwise. This
// needed none of the WAL-shape changes Delete's own migration did:
// UPDATE's WAL entry is already the fully-computed new document body
// (ops_local.go's e.wal.Write("update", ...)), not a filter
// description to re-match at recovery time the way DELETE's is -- see
// wal_recovery.go's "update" case, which just replays the document
// bytes directly.
func (e *Engine) Update(dbName, block string, filters []Filter, where *parse.Expr, op UpdateOp) (int64, error) {
	shardID := ""
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return 0, err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op:       "update",
			DB:       dbName,
			Block:    block,
			Filters:  filters,
			Where:    where,
			UpdateOp: &op,
			ShardID:  shardID,
		}
		cmdData, err := json.Marshal(cmd)
		if err != nil {
			return 0, err
		}
		future := e.cluster.raft.Apply(cmdData, raftTimeout)
		if err := future.Error(); err != nil {
			return 0, err
		}
		if n, ok := future.Response().(int64); ok {
			return n, nil
		}
		return 0, fmt.Errorf("invalid response from Raft")
	}
	return e.updateLocal(dbName, block, filters, where, op, shardID)
}

func applyUpdateOp(doc *Document, op UpdateOp) {
	for k, v := range op.Set {
		setNested(doc.Data, k, v)
	}
	for k, v := range op.Inc {
		setNested(doc.Data, k, getNestedF64(doc.Data, k)+v)
	}
	for k, v := range op.Dec {
		setNested(doc.Data, k, getNestedF64(doc.Data, k)-v)
	}
	for k, v := range op.Push {
		arr := getNestedArr(doc.Data, k)
		setNested(doc.Data, k, append(arr, v))
	}
	for k, v := range op.Pull {
		arr := getNestedArr(doc.Data, k)
		var out []any
		for _, item := range arr {
			if fmt.Sprint(item) != fmt.Sprint(v) {
				out = append(out, item)
			}
		}
		setNested(doc.Data, k, out)
	}
	for _, path := range op.Unset {
		deleteNested(doc.Data, path)
	}
	for k, v := range op.SetDefault {
		if !hasNested(doc.Data, k) {
			setNested(doc.Data, k, v)
		}
	}
}
