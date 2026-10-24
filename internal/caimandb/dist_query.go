package caimandb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DistributedQueryExecutor struct {
	engine   *Engine
	cluster  *ClusterManager
	shardMgr *ShardManager
	nodeID   string
	client   *http.Client
}

func NewDistributedQueryExecutor(engine *Engine, cluster *ClusterManager, shardMgr *ShardManager) *DistributedQueryExecutor {
	return &DistributedQueryExecutor{
		engine:   engine,
		cluster:  cluster,
		shardMgr: shardMgr,
		nodeID:   cluster.nodeID,
		client: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10000,
				MaxIdleConnsPerHost: 10000,
				IdleConnTimeout:     120 * time.Second,
				DisableCompression:  false,
			},
		},
	}
}

type DistributedQueryResult struct {
	Results       []QueryResult
	TotalDocs     int64
	TotalScanned  int64
	Errors        []error
	NodesQueried  int
	ShardsQueried int
}

func (dqe *DistributedQueryExecutor) ExecuteDistributedQuery(dbName, block string, q *Query) (*DistributedQueryResult, error) {
	start := time.Now()
	defer func() {
		log().Debug("Distributed query completed",
			zap.String("db", dbName),
			zap.String("block", block),
			zap.Duration("duration", time.Since(start)))
	}()

	if dqe.cluster == nil || !dqe.cluster.started {
		result, err := dqe.engine.Find(dbName, block, q)
		if err != nil {
			return nil, err
		}
		return &DistributedQueryResult{
			Results:       []QueryResult{*result},
			TotalDocs:     result.Total,
			TotalScanned:  result.Scanned,
			NodesQueried:  1,
			ShardsQueried: 0,
		}, nil
	}

	if !dqe.cluster.IsLeader() {
		leader := dqe.cluster.GetLeader()
		if leader == "" {
			return nil, fmt.Errorf("no leader available")
		}
		return dqe.forwardToLeader(leader, dbName, block, q)
	}

	shardIDs := dqe.shardMgr.GetShardsForFilter(dbName, block, q.Filters)
	if len(shardIDs) == 0 {
		result, err := dqe.engine.Find(dbName, block, q)
		if err != nil {
			return nil, err
		}
		return &DistributedQueryResult{
			Results:       []QueryResult{*result},
			TotalDocs:     result.Total,
			TotalScanned:  result.Scanned,
			NodesQueried:  1,
			ShardsQueried: 0,
		}, nil
	}

	nodeShards := make(map[string][]string)
	for _, shardID := range shardIDs {
		if dqe.shardMgr.IsShardPrimary(shardID) || dqe.shardMgr.IsShardLocal(shardID) {
			nodeShards[dqe.nodeID] = append(nodeShards[dqe.nodeID], shardID)
		} else {
			primary := dqe.shardMgr.GetShardPrimary(shardID)
			if primary != "" {
				nodeShards[primary] = append(nodeShards[primary], shardID)
			}
		}
	}

	var mu sync.Mutex
	results := make([]QueryResult, 0)
	errors := make([]error, 0)
	totalDocs := int64(0)
	totalScanned := int64(0)
	var nodesQueried int32 = 0

	// errgroup replaces the previous hand-rolled sync.WaitGroup + buffered
	// "sem" channel semaphore: SetLimit gives the same bounded fan-out
	// (at most 5000 nodes/shards in flight at once) with the concurrency
	// cap enforced by the group itself instead of a channel callers had
	// to remember to drain in every return path. Per-node/shard errors
	// are still collected into `errors` (a partial result, not a fatal
	// one) rather than returned through the group, since one node being
	// unreachable shouldn't fail shards that did answer.
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(5000)

	for node, shards := range nodeShards {
		if node == "" {
			continue
		}
		nodeCopy, shardsCopy := node, shards
		g.Go(func() error {
			defer func() {
				if r := recover(); r != nil {
					log().Error("dist_query_fanout panic recovered", zap.Any("panic", r))
				}
			}()
			atomic.AddInt32(&nodesQueried, 1)

			if nodeCopy == dqe.nodeID {
				for _, shardID := range shardsCopy {
					// Find only scopes results to a single shard via
					// q.ShardID (see ops_find.go) -- passing the shared q
					// unmodified here previously ran the exact same
					// unfiltered query once per local shard, duplicating
					// every document (and inflating TotalDocs/TotalScanned)
					// by however many shards this node happens to hold for
					// this block. Each shard needs its own copy of q with
					// ShardID set; q itself must not be mutated in place,
					// since the same pointer is also used concurrently by
					// this fan-out's other node/shard goroutines.
					shardQuery := *q
					shardQuery.ShardID = shardID
					result, err := dqe.engine.Find(dbName, block, &shardQuery)
					if err != nil {
						mu.Lock()
						errors = append(errors, fmt.Errorf("local shard %s: %w", shardID, err))
						mu.Unlock()
						continue
					}
					mu.Lock()
					results = append(results, *result)
					totalDocs += result.Total
					totalScanned += result.Scanned
					mu.Unlock()
				}
			} else {
				shardIDs := strings.Join(shardsCopy, ",")
				resp, err := dqe.forwardQueryToNode(nodeCopy, dbName, block, q, shardIDs)
				if err != nil {
					mu.Lock()
					errors = append(errors, fmt.Errorf("node %s shards %s: %w", nodeCopy, shardIDs, err))
					mu.Unlock()
					return nil
				}
				mu.Lock()
				results = append(results, *resp)
				totalDocs += resp.Total
				totalScanned += resp.Scanned
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()

	return &DistributedQueryResult{
		Results:       results,
		TotalDocs:     totalDocs,
		TotalScanned:  totalScanned,
		Errors:        errors,
		NodesQueried:  int(nodesQueried),
		ShardsQueried: len(shardIDs),
	}, nil
}

// Inter-node RPC bodies (this file only -- client-facing HTTP APIs
// elsewhere are untouched) are encoded with msgpack rather than
// encoding/json: it's a same-binary-version wire format (both ends are
// always CaimanDB nodes), so there's no interop reason to pay JSON's
// text-encoding overhead on every cross-node query fan-out. Smaller
// payloads and cheaper (de)serialization matter here specifically
// because this path runs once per shard, per query, under fan-out.
const distRPCContentType = "application/msgpack"

func (dqe *DistributedQueryExecutor) forwardToLeader(leader, dbName, block string, q *Query) (*DistributedQueryResult, error) {
	req := struct {
		DB    string `msgpack:"db"`
		Block string `msgpack:"block"`
		Query *Query `msgpack:"query"`
	}{
		DB:    dbName,
		Block: block,
		Query: q,
	}

	data, err := msgpack.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := dqe.client.Post(
		fmt.Sprintf("http://%s/raft/query", leader),
		distRPCContentType,
		bytes.NewReader(data),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result DistributedQueryResult
	if err := msgpack.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (dqe *DistributedQueryExecutor) forwardQueryToNode(node, dbName, block string, q *Query, shardIDs string) (*QueryResult, error) {
	req := struct {
		DB       string `msgpack:"db"`
		Block    string `msgpack:"block"`
		Query    *Query `msgpack:"query"`
		ShardIDs string `msgpack:"shard_ids"`
	}{
		DB:       dbName,
		Block:    block,
		Query:    q,
		ShardIDs: shardIDs,
	}

	data, err := msgpack.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := dqe.client.Post(
		fmt.Sprintf("http://%s/raft/query_local", node),
		distRPCContentType,
		bytes.NewReader(data),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result QueryResult
	if err := msgpack.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (dqe *DistributedQueryExecutor) HandleQueryLocal(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB       string `msgpack:"db"`
		Block    string `msgpack:"block"`
		Query    *Query `msgpack:"query"`
		ShardIDs string `msgpack:"shard_ids"`
	}
	if err := msgpack.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.ShardIDs != "" {
		shards := strings.Split(req.ShardIDs, ",")
		hasPrimary := false
		for _, shardID := range shards {
			if dqe.shardMgr.IsShardPrimary(shardID) || dqe.shardMgr.IsShardLocal(shardID) {
				hasPrimary = true
				break
			}
		}
		if !hasPrimary {
			http.Error(w, "Not primary for any of the requested shards", http.StatusServiceUnavailable)
			return
		}
	}

	result, err := dqe.engine.Find(req.DB, req.Block, req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", distRPCContentType)
	msgpack.NewEncoder(w).Encode(result)
}

func (dqe *DistributedQueryExecutor) HandleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `msgpack:"db"`
		Block string `msgpack:"block"`
		Query *Query `msgpack:"query"`
	}
	if err := msgpack.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := dqe.ExecuteDistributedQuery(req.DB, req.Block, req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", distRPCContentType)
	msgpack.NewEncoder(w).Encode(result)
}
