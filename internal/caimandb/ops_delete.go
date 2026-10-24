package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
)

// Delete removes every document in block matching filters/where.
//
// where, when non-nil, is the parenthesized/NOT/AND-OR expression tree
// produced by parseWhereClause (cmd_delete.go) and takes priority over
// filters for deciding matches (see matchesWhere in query_filter.go).
// filters is still always passed and still always populated by the
// caller (parseWhereClause returns both the tree and a best-effort flat
// list): it's what deleteLocal/the WAL fall back to when where is nil,
// and it's the only thing the query optimizer's index-selection
// heuristic (AnalyzeQuery) ever looks at, tree or no tree.
func (e *Engine) Delete(dbName, block string, filters []Filter, where *parse.Expr) (int64, error) {
	shardID := ""
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return 0, err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op:      "delete",
			DB:      dbName,
			Block:   block,
			Filters: filters,
			Where:   where,
			ShardID: shardID,
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
	return e.deleteLocal(dbName, block, filters, where, shardID)
}

// Count returns how many documents in block match filters/where. where
// takes priority over filters for matching, same as Delete -- see
// matchesWhere. The single-equality index-lookup fast path below only
// ever looks at filters (matching AnalyzeQuery's existing behavior),
// since that optimization only fires when filters is a genuine
// single-condition, top-level query -- exactly the shape flat has when
// where is nil or a pure AND-chain of one leaf.
func (e *Engine) Count(dbName, block string, filters []Filter, where *parse.Expr) (int64, error) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return 0, nil
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	if len(filters) == 1 && filters[0].Op == "=" {
		plan := e.queryOpt.AnalyzeQuery(filters)
		if plan.UseIndex {
			result, err := e.lookupByIndex(dbName, block, plan.IndexField, plan.IndexValue, 0, 1)
			if err == nil {
				return result.Total, nil
			}
		}
	}

	prefix := docKeyPrefix(block)
	var count int64

	// noCondition is the real "match everything" case: no flat filters
	// *and* no expression tree. len(filters) == 0 alone isn't enough
	// once where exists -- a WHERE with OR/NOT/parens (anything that
	// isn't a pure top-level AND-chain) makes parseWhereClause return a
	// query is very much still a real condition that needs evaluating
	// via where, not skipped as if there were none.
	noCondition := len(filters) == 0 && where == nil

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		if noCondition {
			opts.PrefetchValues = false
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			if noCondition {
				count++
				continue
			}
			var doc Document
			err := item.Value(func(v []byte) error {
				return json.Unmarshal(v, &doc)
			})
			if err != nil {
				continue
			}
			if matchesWhere(doc, where, filters) {
				count++
			}
		}
		return nil
	})
	return count, err
}
