package caimandb

import (
	"encoding/json"
	"go.uber.org/zap"
)

func (e *Engine) CreateBlock(dbName, block string) error {
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op:    "create_block",
			DB:    dbName,
			Block: block,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		future := e.cluster.raft.Apply(data, raftTimeout)
		return future.Error()
	}
	return e.createBlockLocal(dbName, block)
}

func (e *Engine) DropBlock(dbName, block string) error {
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op:    "drop_block",
			DB:    dbName,
			Block: block,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		future := e.cluster.raft.Apply(data, raftTimeout)
		return future.Error()
	}
	return e.dropBlockLocal(dbName, block)
}

func (e *Engine) RenameBlock(dbName, oldName, newName string) error {
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op:    "rename_block",
			DB:    dbName,
			Block: oldName,
			ID:    newName,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		future := e.cluster.raft.Apply(data, raftTimeout)
		return future.Error()
	}
	return e.renameBlockLocal(dbName, oldName, newName)
}

func (e *Engine) ShowBlocks(dbName string) ([]string, error) {
	return e.dirMgr.ListBlocks(dbName)
}

func (e *Engine) DescribeBlock(dbName, block string) (map[string]any, error) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return nil, ErrBlockNotFound
	}

	// EnsureBlockMeta (rather than LoadBlockMeta) so a missing or
	// stale-named block.json -- e.g. left behind by an older rename, or
	// a block that predates the metadata file -- gets transparently
	// rebuilt/corrected instead of failing SHOW BLOCKS for that block.
	meta, err := e.dirMgr.EnsureBlockMeta(dbName, block)
	if err != nil {
		return nil, err
	}

	count, _ := e.Count(dbName, block, nil, nil)

	cfg, _ := e.intelEngine.GetConfig(dbName)
	indexes := []string{}
	if cfg != nil {
		indexes = cfg.Indexes
	}

	return map[string]any{
		"name":       block,
		"db":         dbName,
		"created_at": meta.CreatedAt,
		"updated_at": meta.UpdatedAt,
		"documents":  count,
		"doc_count":  meta.DocCount,
		"node_id":    e.nodeID,
		"size_bytes": meta.SizeBytes,
		"indexes":    indexes,
	}, nil
}

func (e *Engine) EmptyBlock(dbName, block string) (int64, error) {
	count, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}
	// EmptyBlock (used by CLEAR) always called deleteLocal directly, with
	// no cluster/leader check at all -- unlike every other write in this
	// file, it never went through Raft even on a real multi-node
	// cluster's leader, let alone rejected on a non-leader. Route it
	// through Delete with an empty filter set (its existing semantics:
	// delete everything) so it gets the same clusterWriteGate handling
	// as every other write.
	deleted, err := e.Delete(dbName, block, []Filter{}, nil)
	if err != nil {
		return deleted, err
	}
	log().Info("Block emptied",
		zap.String("db", dbName),
		zap.String("block", block),
		zap.Int64("deleted", deleted))
	return deleted, nil
}
