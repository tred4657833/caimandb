package caimandb

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func (e *Engine) CreateDB(name string) error {
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op: "create_db",
			DB: name,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		future := e.cluster.raft.Apply(data, raftTimeout)
		return future.Error()
	}
	return e.createDBLocal(name)
}

func (e *Engine) DropDB(name string) error {
	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op: "drop_db",
			DB: name,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		future := e.cluster.raft.Apply(data, raftTimeout)
		return future.Error()
	}
	return e.dropDBLocal(name)
}

func (e *Engine) UseDB(name string) error {
	if _, err := os.Stat(e.dirMgr.DBPath(name)); err != nil {
		return fmt.Errorf("DB '%s' does not exist", name)
	}
	return nil
}

func (e *Engine) ShowDBs() ([]string, error) {
	return e.dirMgr.ListDatabases()
}

func (e *Engine) DescribeDB(name string) (map[string]any, error) {
	if !e.dirMgr.DBExists(name) {
		return nil, ErrDBNotFound
	}

	meta, err := e.dirMgr.LoadDBMeta(name)
	if err != nil {
		meta = map[string]any{
			"name":       name,
			"created_at": time.Now().Unix(),
		}
	}

	blocks, _ := e.ShowBlocks(name)
	meta["blocks"] = len(blocks)
	meta["block_list"] = blocks
	meta["node_id"] = e.nodeID
	meta["shards"] = e.shardMgr.shardCount
	meta["max_shards"] = e.config.MaxShardsPerNode

	if cfg, ok := e.intelEngine.GetConfig(name); ok {
		meta["config"] = cfg
	}

	if e.cluster != nil {
		meta["cluster_role"] = e.cluster.raft.State().String()
		meta["leader"] = string(e.cluster.raft.Leader())
		meta["cluster_mode"] = "auto"
		meta["cluster_nodes"] = len(e.cluster.GetPeers()) + 1
	}
	return meta, nil
}

func (e *Engine) StatsDB(name string) (map[string]any, error) {
	blocks, _ := e.ShowBlocks(name)
	var total int64
	for _, b := range blocks {
		c, _ := e.Count(name, b, nil, nil)
		total += c
	}
	sz, _ := dirSize(e.dirMgr.DBPath(name))

	intelStats := e.intelEngine.stats[name]
	pred, _ := e.intelEngine.predictor.GetPrediction(name)

	stats := map[string]any{
		"db":                   name,
		"blocks":               len(blocks),
		"documents":            total,
		"size":                 fmtBytes(sz),
		"ops":                  e.opCount.Load(),
		"l1_cache":             e.l1Cache.Stats(),
		"l2_cache":             e.l2Cache.Stats(),
		"metrics":              e.metrics.Stats(),
		"node_id":              e.nodeID,
		"shards":               e.shardMgr.shardCount,
		"replicas":             e.shardMgr.replicaCount,
		"auto_mode":            true,
		"max_shards":           e.config.MaxShardsPerNode,
		"startup_duration":     time.Since(e.startupTime).Seconds(),
		"fast_startup":         e.config.FastStartup,
		"lazy_index_loading":   e.config.LazyIndexLoading,
		"compress_large_docs":  e.config.CompressLargeDocs,
		"max_document_size":    e.config.MaxDocumentSize,
		"compress_threshold":   e.config.CompressThreshold,
		"index_skip_threshold": e.config.IndexSkipThreshold,
		"large_doc_threshold":  e.config.LargeDocThreshold,
		"wal":                  e.wal.Stats(),
		"rate_limit":           e.config.RateLimit,
		"buffer_pool":          e.bufferPool.Stats(),
	}

	if intelStats != nil {
		stats["intelligent"] = map[string]any{
			"doc_count":      intelStats.DocCount,
			"avg_doc_size":   intelStats.AvgDocSize,
			"query_count":    intelStats.QueryCount,
			"avg_query_time": intelStats.AvgQueryTime,
			"index_hit_rate": intelStats.IndexHitRate,
			"cache_hit_rate": intelStats.CacheHitRate,
			"shard_skew":     intelStats.ShardSkew,
			"growth_rate":    intelStats.GrowthRate,
			"last_optimized": intelStats.LastOptimized,
			"total_shards":   intelStats.TotalShards,
			"total_nodes":    intelStats.TotalNodes,
		}
	}

	if pred.ExpectedGrowth != 0 {
		stats["prediction"] = map[string]any{
			"expected_growth":    pred.ExpectedGrowth,
			"peak_time":          pred.PeakTime,
			"recommended_shards": pred.RecommendedShards,
			"recommended_nodes":  pred.RecommendedNodes,
			"confidence":         pred.Confidence,
			"trend":              pred.Trend,
		}
	}

	if e.cluster != nil {
		stats["cluster_role"] = e.cluster.raft.State().String()
		stats["leader"] = string(e.cluster.raft.Leader())
		stats["peers"] = e.cluster.GetPeers()
		stats["total_nodes"] = len(e.cluster.GetPeers()) + 1
		stats["max_nodes"] = e.config.MaxClusterNodes
	}

	// Add FLEX-COLUMN stats
	if e.flexEngine != nil {
		stats["flex_column"] = e.flexEngine.Stats()
	}

	return stats, nil
}

func (e *Engine) RenameDB(oldName, newName string) error {
	if err := validateName(newName); err != nil {
		return err
	}
	if oldName == newName {
		return nil
	}

	if !e.dirMgr.DBExists(oldName) {
		return ErrDBNotFound
	}
	if e.dirMgr.DBExists(newName) {
		return fmt.Errorf("database %s already exists", newName)
	}

	oldPath := e.dirMgr.DBPath(oldName)
	newPath := e.dirMgr.DBPath(newName)

	// Also covers a database that is "en arranque": renameDirRobust
	// force-closes every open Badger handle under oldPath (the DB's own
	// handles plus every block's __data/__index handles underneath it)
	// before attempting the OS rename, so a database that is mid-startup
	// or mid-write never blocks or corrupts the rename. It never removes
	// oldPath until data is confirmed present at newPath.
	lockKey := oldName
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	e.markRenaming(oldName)
	defer e.unmarkRenaming(oldName)

	if err := e.renameDirRobust(oldPath, newPath); err != nil {
		return fmt.Errorf("rename database %s -> %s: %w", oldName, newName, err)
	}

	e.intelEngine.mu.Lock()
	defer e.intelEngine.mu.Unlock()

	if cfg, ok := e.intelEngine.configs[oldName]; ok {
		cfg.Name = newName
		e.intelEngine.configs[newName] = cfg
		delete(e.intelEngine.configs, oldName)
	}
	if stats, ok := e.intelEngine.stats[oldName]; ok {
		e.intelEngine.stats[newName] = stats
		delete(e.intelEngine.stats, oldName)
	}

	if meta, err := e.dirMgr.LoadDBMeta(oldName); err == nil {
		meta["name"] = newName
		meta["renamed_at"] = time.Now().Unix()
		if err := e.dirMgr.SaveDBMeta(newName, meta); err != nil {
			log().Warn("Failed to save DB meta for new name", zap.Error(err))
		}
	}

	// Every block underneath also has its own block.json carrying this
	// DB's old name in its DB field, plus a Name field that's already
	// correct (blocks aren't renamed by a DB rename) but may have been
	// missing/unreadable to begin with. EnsureBlockMeta fixes both in
	// one pass per block, so SHOW BLOCKS and DESCRIBE BLOCK stay
	// accurate under the new name instead of surfacing a stale-metadata
	// or missing-file error the next time they're read.
	if blocks, err := e.dirMgr.ListBlocks(newName); err == nil {
		for _, block := range blocks {
			if _, err := e.dirMgr.EnsureBlockMeta(newName, block); err != nil {
				log().Warn("Could not verify/repair block metadata after DB rename",
					zap.String("db", newName), zap.String("block", block), zap.Error(err))
			}
		}
	}

	safeGo("db_rename_reopen", func() {
		time.Sleep(100 * time.Millisecond)
		if _, err := e.pool.OpenDataPath(newPath); err != nil {
			log().Warn("Failed to reopen DB after rename",
				zap.String("db", newName),
				zap.Error(err))
		}
	})

	log().Info("Database renamed successfully",
		zap.String("old", oldName),
		zap.String("new", newName))

	e.l1Cache.DeleteByPrefix(oldName + "/")

	return nil
}

func (e *Engine) waitForFileRelease(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for files to release")
		}

		lockFile := filepath.Join(path, "LOCK")
		if _, err := os.Stat(lockFile); err != nil {
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		if f, err := os.OpenFile(lockFile, os.O_RDWR, 0); err == nil {
			f.Close()
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		time.Sleep(100 * time.Millisecond)
		runtime.GC()
	}
}

func (e *Engine) copyDatabaseFilesComplete(src, dst string) error {
	log().Info("Copying complete database files",
		zap.String("src", src),
		zap.String("dst", dst))

	if err := os.MkdirAll(dst, 0750); err != nil {
		return err
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		destPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(destPath, info.Mode())
		}

		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				time.Sleep(200 * time.Millisecond)
			}

			if err := copyFileWithRetry(path, destPath); err == nil {
				return nil
			} else if attempt == 2 {
				return fmt.Errorf("failed to copy %s: %w", filepath.Base(path), err)
			}
		}
		return nil
	})
}

func copyFileWithRetry(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0750); err != nil {
		return err
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	if err := dstFile.Sync(); err != nil {
		return err
	}

	return nil
}
