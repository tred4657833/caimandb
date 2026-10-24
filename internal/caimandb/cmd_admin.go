package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func handleAnalyze(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: ANALYZE requires object (DB or BLOCK)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		var db string
		if len(tokens) >= 3 {
			db = tokens[2]
		} else {
			db = sess.currentDB
		}
		if db == "" {
			return "ERROR: No database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.DBExists(db) {
			return "ERROR: Database not found: " + db
		}
		stats, err := eng.StatsDB(db)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if intelStats, ok := eng.intelEngine.stats[db]; ok {
			intelStats.ShardSkew = eng.shardMgr.calculateSkew()
			eng.intelEngine.UpdateStats(db, intelStats)
		}
		return "Database analyzed: " + db + "\n" + jsonOut(stats)

	case "BLOCK":
		var db, block string
		if len(tokens) >= 4 {
			db = tokens[2]
			block = tokens[3]
		} else if len(tokens) >= 3 {
			block = tokens[2]
			db = sess.currentDB
		}
		if db == "" {
			return "ERROR: No database specified and no database in use"
		}
		if block == "" {
			return "ERROR: Block name required"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.BlockExists(db, block) {
			return "ERROR: Block not found: " + block
		}
		info, err := eng.DescribeBlock(db, block)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		count, _ := eng.Count(db, block, nil, nil)
		info["analyzed_docs"] = count
		info["analyzed_at"] = time.Now().Unix()
		return "Block analyzed: " + block + "\n" + jsonOut(info)

	default:
		return "ERROR: Unknown ANALYZE object: " + subCmd
	}
}

func handleOptimize(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: OPTIMIZE requires object (DB or BLOCK)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		var db string
		if len(tokens) >= 3 {
			db = tokens[2]
		} else {
			db = sess.currentDB
		}
		if db == "" {
			return "ERROR: No database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.DBExists(db) {
			return "ERROR: Database not found: " + db
		}
		rounds, err := eng.Compact(db)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if cfg, ok := eng.intelEngine.GetConfig(db); ok {
			cfg.UpdatedAt = time.Now().Unix()
			eng.intelEngine.SetConfig(cfg)
		}
		return "Database optimized: " + db + " (GC rounds: " + strconv.Itoa(rounds) + ")"

	case "BLOCK":
		var db, block string
		if len(tokens) >= 4 {
			db = tokens[2]
			block = tokens[3]
		} else if len(tokens) >= 3 {
			block = tokens[2]
			db = sess.currentDB
		}
		if db == "" {
			return "ERROR: No database specified and no database in use"
		}
		if block == "" {
			return "ERROR: Block name required"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if !eng.dirMgr.BlockExists(db, block) {
			return "ERROR: Block not found: " + block
		}
		if err := eng.RebuildBlock(db, block); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Block optimized: " + block

	default:
		return "ERROR: Unknown OPTIMIZE object: " + subCmd
	}
}

func handleExport(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: EXPORT requires block and file"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	if !eng.dirMgr.BlockExists(db, block) {
		return "ERROR: Block not found: " + block
	}
	if err := eng.authorizeBlockAccess(sess, db, block, "read"); err != nil {
		return "ERROR: " + err.Error()
	}

	var filters []Filter
	var where *parse.Expr
	var dest string
	i := 2

	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
		case "WHERE":
			// Migrated to parseWhereClause/parse.Expr: real
			// parentheses/NOT/AND-OR precedence, same pattern already
			// used for DELETE/CLEAR/COUNT/aggregations/UPDATE/VIEW
			// CREATE (see query_filter.go's matchesWhere and
			// docs/known-limitations.md). This was the last command
			// still on the flat parseFilters. Read-only (EXPORT),
			// so -- same as VIEW CREATE and aggregations before it --
			// no WAL/Raft shape changes were needed.
			i++
			var err error
			where, filters, err = parseWhereClause(tokens, &i)
			if err != nil {
				return "ERROR: " + err.Error()
			}
		case "SELECT":
			if i+1 < len(tokens) {
				i += 2
			} else {
				i++
			}
		case "TO":
			i++
			if i >= len(tokens) {
				return "ERROR: EXPORT TO requires a file name"
			}
			dest = tokens[i]
			i++
		default:
			// Tolerate the legacy "EXPORT <block> \"file\"" form (no TO keyword).
			if dest == "" {
				dest = tokens[i]
			}
			i++
		}
	}

	dest = strings.Trim(dest, "\"'")
	if dest == "" {
		return "ERROR: EXPORT requires a destination file name (TO \"file\")"
	}

	if len(filters) > 0 || where != nil {
		q := &Query{Filters: filters, Where: where, Limit: 1000000}
		result, err := eng.Find(db, block, q)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		csvPath, jsonPath, count, err := eng.ExportDocsBoth(result.Docs, dest)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return fmt.Sprintf("Exported %d documents to %s and %s", count, csvPath, jsonPath)
	}

	csvPath, jsonPath, count, err := eng.ExportBoth(db, block, dest)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Exported %d documents to %s and %s", count, csvPath, jsonPath)
}

func handleImport(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: IMPORT requires block and file"
	}
	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}

	// Documented syntax (help.go) is "IMPORT <block> FROM \"file\"", but this
	// legacy path used to treat tokens[2] as the filename unconditionally --
	// so a literal "FROM" was swallowed as the source path and os.Open(src)
	// failed with a confusing "open FROM: no such file or directory". Accept
	// an optional FROM keyword here, same as the streaming IMPORT ... FROM
	// FILE syntax already does, while still allowing the old bare-filename
	// form for backward compatibility.
	fileIdx := 2
	if strings.ToUpper(tokens[2]) == "FROM" {
		if len(tokens) < 4 {
			return "ERROR: FROM requires a file path"
		}
		fileIdx = 3
	}
	src := tokens[fileIdx]
	src = strings.Trim(src, "\"'")

	inserted, errors, err := eng.Import(db, block, src)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Imported: %d documents, Errors: %d", inserted, errors)
}

func handleBackupCmd(eng *Engine, sess *Session, tokens []string) string {
	// Documented syntax (help.go, docs/NQL_SYNTAX.en.md) is
	// "BACKUP <db> TO <file>", but this used to read tokens[2] as the
	// destination unconditionally -- so the literal keyword "TO" became
	// the file name and the real path was ignored. The bare
	// "BACKUP <db> <file>" form still works.
	if len(tokens) < 3 {
		return "ERROR: BACKUP requires DB name and file (BACKUP <db> TO \"<file>\")"
	}
	db := tokens[1]
	destIdx := 2
	if strings.EqualFold(tokens[2], "TO") {
		if len(tokens) < 4 {
			return "ERROR: BACKUP ... TO requires a file name"
		}
		destIdx = 3
	}
	dest := strings.Trim(tokens[destIdx], "\"'")

	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	if !eng.dirMgr.DBExists(db) {
		return "ERROR: Database not found: " + db
	}

	if err := eng.Backup(db, dest); err != nil {
		return "ERROR: " + err.Error()
	}
	return "Backup completed: " + dest
}

func handleRestoreCmd(eng *Engine, sess *Session, tokens []string) string {
	// Same fix as handleBackupCmd above: "RESTORE <db> FROM <file>" is
	// the documented form, and "FROM" used to be swallowed as the
	// source path.
	if len(tokens) < 3 {
		return "ERROR: RESTORE requires DB name and file (RESTORE <db> FROM \"<file>\")"
	}
	db := tokens[1]
	srcIdx := 2
	if strings.EqualFold(tokens[2], "FROM") {
		if len(tokens) < 4 {
			return "ERROR: RESTORE ... FROM requires a file name"
		}
		srcIdx = 3
	}
	src := strings.Trim(tokens[srcIdx], "\"'")

	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	if err := eng.Restore(db, src); err != nil {
		return "ERROR: " + err.Error()
	}
	return "Restore completed: " + src
}

func handleCompactCmd(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: COMPACT requires database name"
	}
	db := tokens[1]

	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	if !eng.dirMgr.DBExists(db) {
		return "ERROR: Database not found: " + db
	}

	rounds, err := eng.Compact(db)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Compact completed: %d GC rounds", rounds)
}

// handleCheckpointCmd implementa el comando manual CHECKPOINT: fuerza un
// punto de recuperación seguro ahora mismo, sin esperar al checkpoint
// periódico (ver checkpoint.go).
func handleCheckpointCmd(eng *Engine, sess *Session, tokens []string) string {
	stats, err := eng.Checkpoint()
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return fmt.Sprintf("Checkpoint completed: %d database(s) synced, WAL rotated: %v, took %s",
		stats.SyncedDBs, stats.Rotated, stats.Duration)
}

func handleShardCmd(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: SHARD requires command (STATUS, REBALANCE, SCALE)"
	}
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "STATUS":
		sm := eng.shardMgr
		sm.mu.RLock()
		defer sm.mu.RUnlock()
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Shard Manager Status:\n  Node: %s\n  Shards: %d\n  Replicas: %d\n  Max Shards: %d\n",
			sm.nodeID, sm.shardCount, sm.replicaCount, eng.config.MaxShardsPerNode))
		sb.WriteString(fmt.Sprintf("  Auto-Scale: %v, Predictive: %v\n",
			eng.config.AutoSplitEnabled, eng.config.PredictiveScaling))
		for id, shard := range sm.shards {
			sb.WriteString(fmt.Sprintf("  %s: docs=%d, size=%s, primary=%s, nodes=%v, status=%s\n",
				id, shard.DocCount, fmtBytes(shard.SizeBytes), shard.Primary, shard.Nodes, shard.Status))
		}
		sb.WriteString(fmt.Sprintf("  Total docs: %d, Skew: %.2f\n", sm.totalDocs(), sm.calculateSkew()))
		return sb.String()

	case "REBALANCE":
		eng.shardMgr.BalanceShards()
		return "Shard rebalance triggered"

	case "SCALE":
		if len(tokens) < 4 {
			return "ERROR: SHARD SCALE requires database name and target shard count"
		}
		db := tokens[2]
		target, err := strconv.Atoi(tokens[3])
		if err != nil {
			return "ERROR: Invalid shard count: " + tokens[3]
		}
		if target < 4 {
			return "ERROR: Shard count must be at least 4"
		}
		if target > eng.config.MaxShardsPerNode {
			return fmt.Sprintf("ERROR: Shard count exceeds max (%d)", eng.config.MaxShardsPerNode)
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if cfg, ok := eng.intelEngine.GetConfig(db); ok {
			cfg.ShardCount = target
			cfg.UpdatedAt = time.Now().Unix()
			eng.intelEngine.SetConfig(cfg)
			return fmt.Sprintf("Database %s scaled to %d shards", db, target)
		}
		return "ERROR: Database not found: " + db

	default:
		return "ERROR: unknown SHARD command: " + subCmd
	}
}

func handleClusterCmd(eng *Engine, sess *Session, tokens []string) string {
	if eng.cluster == nil {
		return "Cluster mode is not enabled"
	}

	status := map[string]any{
		"node_id":   eng.nodeID,
		"state":     eng.cluster.raft.State().String(),
		"is_leader": eng.cluster.IsLeader(),
		"leader":    eng.cluster.GetLeader(),
		"peers":     eng.cluster.GetPeers(),
		"started":   eng.cluster.started,
		"max_nodes": eng.config.MaxClusterNodes,
		"auto_mode": true,
	}
	return jsonOut(status)
}
