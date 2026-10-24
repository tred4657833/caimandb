package caimandb

import (
	"bytes"
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/parse"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"io"
	"os"
	"sync"
	"time"
)

type RaftFSM struct {
	engine *Engine
	mu     sync.RWMutex
}

type RaftCommand struct {
	Op      string           `json:"op"`
	DB      string           `json:"db,omitempty"`
	Block   string           `json:"block,omitempty"`
	ID      string           `json:"id,omitempty"`
	ShardID string           `json:"shard_id,omitempty"`
	Data    map[string]any   `json:"data,omitempty"`
	Docs    []map[string]any `json:"docs,omitempty"`
	Filters []Filter         `json:"filters,omitempty"`
	// Where is the parenthesized/NOT/AND-OR expression tree for
	// commands migrated to parseWhereClause (currently DELETE -- see
	// cmd_delete.go). nil for commands still on the flat parseFilters
	// path, and for older log entries replayed from before this field
	// existed: the FSM and deleteLocal fall back to Filters in that
	// case (matchesWhere in query_filter.go), matching how the command
	// behaved before migration.
	Where    *parse.Expr `json:"where,omitempty"`
	UpdateOp *UpdateOp   `json:"update_op,omitempty"`
	TxID     string      `json:"tx_id,omitempty"`

	// Mutations carries a transaction's full set of raw key/value
	// mutations for the "tx_commit" op -- see TxManager.Commit in
	// transaction.go. Replicating the literal mutations (rather than the
	// original INSERT/UPDATE/DELETE calls that produced them) is what
	// makes every node in the cluster apply exactly the same bytes,
	// matching how WAL-based tx_commit recovery already works.
	Mutations []TxMutation `json:"mutations,omitempty"`
}

func NewRaftFSM(engine *Engine) *RaftFSM {
	return &RaftFSM{engine: engine}
}

func (f *RaftFSM) Apply(raftLog *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(raftLog.Data, &cmd); err != nil {
		log().Error("Failed to unmarshal Raft command", zap.Error(err))
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	metrics.MetricRaftCommits.Inc()

	start := time.Now()
	var result interface{}
	var err error

	switch cmd.Op {
	case "insert":
		result, err = f.engine.insertWithIDLocal(cmd.DB, cmd.Block, cmd.ID, cmd.Data, cmd.ShardID)
	case "insert_batch":
		ins, errs := f.engine.insertBatchLocal(cmd.DB, cmd.Block, cmd.Docs)
		result = &BatchInsertResult{Inserted: ins, Errors: errs}
	case "update":
		if cmd.UpdateOp != nil {
			result, err = f.engine.updateLocal(cmd.DB, cmd.Block, cmd.Filters, cmd.Where, *cmd.UpdateOp, cmd.ShardID)
		}
	case "delete":
		result, err = f.engine.deleteLocal(cmd.DB, cmd.Block, cmd.Filters, cmd.Where, cmd.ShardID)
	case "create_db":
		err = f.engine.createDBLocal(cmd.DB)
	case "drop_db":
		err = f.engine.dropDBLocal(cmd.DB)
	case "create_block":
		err = f.engine.createBlockLocal(cmd.DB, cmd.Block)
	case "drop_block":
		err = f.engine.dropBlockLocal(cmd.DB, cmd.Block)
	case "rename_block":
		err = f.engine.renameBlockLocal(cmd.DB, cmd.Block, cmd.ID)
	case "tx_commit":
		err = f.engine.applyTxCommitLocal(cmd.TxID, cmd.DB, cmd.Block, cmd.Mutations)
	default:
		log().Warn("Unknown Raft command", zap.String("op", cmd.Op))
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}

	if err != nil {
		log().Error("Raft operation failed",
			zap.String("op", cmd.Op),
			zap.String("db", cmd.DB),
			zap.Error(err))
		return err
	}

	metrics.MetricOpsDuration.WithLabelValues(cmd.Op, cmd.DB, cmd.Block, cmd.ShardID, f.engine.nodeID).
		Observe(time.Since(start).Seconds())

	return result
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snapshot := &RaftSnapshot{
		engine: f.engine,
		data:   make(map[string][]byte),
	}

	dbs, err := f.engine.ShowDBs()
	if err != nil {
		return nil, err
	}

	for _, db := range dbs {
		path := f.engine.dirMgr.DBPath(db)
		dbHandle, err := f.engine.pool.OpenDataPath(path)
		if err != nil {
			continue
		}

		var buf bytes.Buffer
		_, err = dbHandle.Backup(&buf, 0)
		if err != nil {
			continue
		}
		snapshot.data[db] = buf.Bytes()
	}

	return snapshot, nil
}

func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Persist (below) encodes s.data directly -- a bare map[string][]byte,
	// not the RaftSnapshot struct itself (whose fields are unexported and
	// so would silently decode as empty via encoding/json, since json
	// ignores unexported fields on both Marshal and Unmarshal). Decode
	// into the same map type Persist actually wrote, or every Restore
	// silently applies zero data.
	var data map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}

	for db, raw := range data {
		path := f.engine.dirMgr.DBPath(db)
		os.RemoveAll(path)
		dbHandle, err := f.engine.pool.OpenDataPath(path)
		if err != nil {
			return err
		}
		if err := dbHandle.Load(bytes.NewReader(raw), 512); err != nil {
			return err
		}
	}

	return nil
}

type RaftSnapshot struct {
	engine *Engine
	data   map[string][]byte
}

func (s *RaftSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *RaftSnapshot) Release() {}
