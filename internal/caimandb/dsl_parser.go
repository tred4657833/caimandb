package caimandb

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"caimandb/internal/caimandb/parse"
)

type Session struct {
	currentDB string
	authUser  string
	authRole  string
	// authDatabase es la base a la que está ligada la identidad
	// autenticada (User.Database, ver hierarchy.go). Vacío para
	// ADMIN_GENERAL y para la consola local sin autenticar. No cambia
	// durante la sesión aunque currentDB sí lo haga vía USE/CD.
	authDatabase string
	token        string
	currentTx    *Transaction
}

func parseAndExec(eng *Engine, sess *Session, input string) string {
	// Un comentario "--" al final de una sentencia se recorta aquí, no
	// solo en el lector de scripts (script_reader.go): por la consola
	// web y el CLI las sentencias llegan de una en una por HTTP, sin
	// pasar por ese lector, y hasta ahora solo se descartaban las que
	// empezaban por "--". Una línea con código y un comentario al final
	// se tokenizaba entera, así que el comentario se colaba como tokens
	// extra y el comando fallaba con su mensaje de sintaxis. Los flags
	// "--type:" / "--format:" de FIND se conservan (ver nqlInlineFlags).
	input = strings.TrimSpace(stripNQLComment(input))
	// Un ';' final es opcional (como en SQL): si está, se ignora en vez
	// de tratarse como parte del último valor/token.
	input = strings.TrimSuffix(input, ";")
	input = strings.TrimSpace(input)
	if input == "" || strings.HasPrefix(input, "--") {
		return ""
	}

	if !eng.rateLimiter.Allow(sess.authUser) {
		return "ERROR: rate limit exceeded"
	}

	if err := enforceSessionIsolation(eng, sess); err != nil {
		return "ERROR: " + err.Error()
	}

	blocked, reason, err := eng.riskEngine.Evaluate(input, sess.authUser)
	if err != nil && blocked {
		return "ERROR: " + reason
	}

	tokens := parse.Tokenize(input)
	if len(tokens) == 0 {
		return ""
	}

	cmd := strings.ToUpper(tokens[0])

	switch cmd {
	case "VERSION":
		return "CaimanDB v" + Version + " (DataEngine - Enterprise Edition - OPTIMIZED FOR LARGE DOCUMENTS)"

	case "PING":
		return "PONG " + time.Now().UTC().Format(time.RFC3339)

	case "STATUS":
		return handleStatusCmd(eng, sess)

	case "HEALTH":
		return handleHealthCmd(eng)

	case "HELP":
		return helpText()

	case "EXIT", "QUIT", "\\Q":
		return "__EXIT__"

	case "PWD":
		if sess.currentDB != "" {
			return filepath.Join(eng.dataRoot, sess.currentDB)
		}
		return eng.dataRoot

	case "CD":
		if len(tokens) < 2 {
			return "ERROR: CD requires database name"
		}
		db := tokens[1]
		if sess.authRole != "" && !IsGeneralAdmin(sess.authRole) &&
			!UserCanAccessDB(User{Role: sess.authRole, Database: sess.authDatabase}, db) &&
			!eng.hasAnyGrantInDB(sess.authUser, db) {
			// A granular GRANT (acl.go) to a specific block in db is enough
			// to let the user USE/CD into db, even though their role alone
			// wouldn't -- otherwise a block-level grant in another database
			// would be unreachable. authorizeBlockAccess still enforces the
			// actual per-block/per-action check once they touch a block.
			return "ERROR: acceso denegado: " + sess.authRole + " no administra la base '" + db + "'"
		}
		if err := eng.UseDB(db); err != nil {
			return "ERROR: " + err.Error()
		}
		sess.currentDB = db
		return "Now using database: " + db

	case "LS":
		if len(tokens) >= 2 && !strings.HasPrefix(tokens[1], "-") {
			db := tokens[1]
			blocks, err := eng.ShowBlocks(db)
			if err != nil {
				return "ERROR: " + err.Error()
			}
			if len(blocks) == 0 {
				return "No blocks found in " + db
			}
			var sb strings.Builder
			sb.WriteString("Blocks in " + db + ":\n")
			for _, b := range blocks {
				sb.WriteString("  " + b + "\n")
			}
			return sb.String()
		}
		if sess.currentDB != "" {
			blocks, err := eng.ShowBlocks(sess.currentDB)
			if err != nil {
				return "ERROR: " + err.Error()
			}
			if len(blocks) == 0 {
				return "No blocks found in " + sess.currentDB
			}
			var sb strings.Builder
			sb.WriteString("Blocks in " + sess.currentDB + ":\n")
			for _, b := range blocks {
				sb.WriteString("  " + b + "\n")
			}
			return sb.String()
		}
		dbs, err := eng.ShowDBs()
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if len(dbs) == 0 {
			return "No databases found"
		}
		var sb strings.Builder
		sb.WriteString("Databases:\n")
		for _, db := range dbs {
			sb.WriteString("  " + db + "\n")
		}
		return sb.String()

	case "TREE":
		return handleTree(eng, sess)

	case "CREATE":
		if len(tokens) >= 2 && (strings.ToUpper(tokens[1]) == "INDEX" || strings.ToUpper(tokens[1]) == "UNIQUE") {
			return handleCreateIndex(eng, sess, tokens)
		}
		if len(tokens) < 3 {
			return "ERROR: CREATE requires at least 3 arguments"
		}
		return handleCreate(eng, sess, tokens)

	case "DROP":
		if len(tokens) >= 2 && strings.ToUpper(tokens[1]) == "FIELD" {
			return handleDropField(eng, sess, tokens)
		}
		if len(tokens) >= 2 && strings.ToUpper(tokens[1]) == "INDEX" {
			return handleDropIndex(eng, sess, tokens)
		}
		if len(tokens) < 3 {
			return "ERROR: DROP requires at least 3 arguments"
		}
		return handleDrop(eng, sess, tokens)

	case "RENAME":
		if len(tokens) < 5 {
			return "ERROR: RENAME requires object, old name, TO, and new name"
		}
		return handleRename(eng, sess, tokens)

	case "GRANT":
		return handleGrant(eng, sess, tokens)

	case "REVOKE":
		return handleRevoke(eng, sess, tokens)

	case "SHOW":
		if len(tokens) < 2 {
			return "ERROR: SHOW requires an argument"
		}
		return handleShow(eng, sess, tokens)

	case "INFO":
		if len(tokens) < 3 {
			return "ERROR: INFO requires DB or BLOCK and name"
		}
		return handleInfo(eng, sess, tokens)

	case "DESCRIBE":
		if len(tokens) < 3 {
			return "ERROR: DESCRIBE requires DB or BLOCK and name"
		}
		return handleDescribe(eng, sess, tokens)

	case "STATS":
		return handleStats(eng, sess, tokens)

	case "SIZE":
		return handleSize(eng, sess, tokens)

	case "USE":
		if len(tokens) < 2 {
			return "ERROR: USE requires database name"
		}
		db := tokens[1]
		if sess.authRole != "" && !IsGeneralAdmin(sess.authRole) &&
			!UserCanAccessDB(User{Role: sess.authRole, Database: sess.authDatabase}, db) &&
			!eng.hasAnyGrantInDB(sess.authUser, db) {
			// A granular GRANT (acl.go) to a specific block in db is enough
			// to let the user USE/CD into db, even though their role alone
			// wouldn't -- otherwise a block-level grant in another database
			// would be unreachable. authorizeBlockAccess still enforces the
			// actual per-block/per-action check once they touch a block.
			return "ERROR: acceso denegado: " + sess.authRole + " no administra la base '" + db + "'"
		}
		if err := eng.UseDB(db); err != nil {
			return "ERROR: " + err.Error()
		}
		sess.currentDB = db
		return "Using database: " + db

	case "INSERT":
		return handleInsert(eng, sess, tokens)

	case "FIND":
		return handleFind(eng, sess, tokens)

	case "EXPLAIN":
		return handleExplain(eng, sess, tokens)

	case "GET":
		return handleGet(eng, sess, tokens)

	case "SEARCH":
		return handleSearch(eng, sess, tokens)

	case "UPDATE":
		return handleUpdate(eng, sess, tokens)

	case "DELETE":
		return handleDelete(eng, sess, tokens)

	case "CLEAR", "EMPTY":
		return handleClear(eng, sess, tokens)

	case "COUNT":
		return handleCount(eng, sess, tokens)

	case "SUM", "AVG", "MIN", "MAX", "MEDIAN", "MODE", "STDDEV":
		return handleAggregate(eng, sess, cmd, tokens)

	case "GROUP":
		return handleGroup(eng, sess, tokens)

	case "JOIN":
		return handleJoin(eng, sess, tokens)

	case "RELATE":
		return handleRelate(eng, sess, tokens)

	case "EXPORT":
		return handleExport(eng, sess, tokens)

	case "IMPORT":
		// Two IMPORT syntaxes coexist:
		//   IMPORT <block> FROM FILE '<path>' [FORMAT ...] [BATCH ...]  -> streaming (cmd_import.go)
		//   IMPORT <block> <file>                                       -> legacy, whole-file (cmd_admin.go)
		if len(tokens) >= 4 && strings.ToUpper(tokens[2]) == "FROM" && strings.ToUpper(tokens[3]) == "FILE" {
			return handleImportCmd(eng, sess, tokens)
		}
		return handleImport(eng, sess, tokens)

	case "BACKUP":
		// New encrypted/incremental JSONL backup (cmd_backup_jsonl.go):
		// "BACKUP <db>" or "BACKUP <db> FULL" -- no destination file, the
		// path is chosen automatically under backup/arw/. Anything else
		// (a destination file given, as in "BACKUP <db> <file>") is the
		// older Badger-native snapshot command below, kept as-is.
		if len(tokens) == 2 || (len(tokens) == 3 && strings.ToUpper(tokens[2]) == "FULL") {
			return handleBackupJSONL(eng, sess, tokens)
		}
		return handleBackupCmd(eng, sess, tokens)

	case "RESTORE":
		// "RESTORE <db>" and "RESTORE <db> AS OF <ts>" both go to the
		// encrypted JSONL restore (handleRestoreJSONL); anything else
		// (specifically "RESTORE <db> FROM <file>") is the older
		// Badger-native snapshot command below, kept as-is.
		if len(tokens) == 2 || (len(tokens) == 5 && strings.EqualFold(tokens[2], "AS") && strings.EqualFold(tokens[3], "OF")) {
			return handleRestoreJSONL(eng, sess, tokens)
		}
		return handleRestoreCmd(eng, sess, tokens)

	case "COMPACT":
		return handleCompactCmd(eng, sess, tokens)

	case "VERIFY":
		if len(tokens) >= 2 && strings.ToUpper(tokens[1]) == "BACKUP" {
			return handleVerifyBackup(eng, sess, tokens)
		}
		return "ERROR: unknown VERIFY command (expected: VERIFY BACKUP <db>)"

	case "CHECKPOINT":
		return handleCheckpointCmd(eng, sess, tokens)

	case "ANALYZE":
		return handleAnalyze(eng, sess, tokens)

	case "OPTIMIZE":
		return handleOptimize(eng, sess, tokens)

	case "REBUILD":
		return handleRebuild(eng, sess, tokens)

	case "CHECK":
		return handleCheck(eng, sess, tokens)

	case "REPAIR":
		return handleRepair(eng, sess, tokens)

	case "SHARD":
		return handleShardCmd(eng, sess, tokens)

	case "CLUSTER":
		return handleClusterCmd(eng, sess, tokens)

	case "VIEW":
		return handleViewCmd(eng, sess, tokens)

	// FLEX-COLUMN commands
	case "FLEX":
		return handleFlexCommand(eng, sess, tokens)

	// ACID TRANSACTION COMMANDS
	case "BEGIN":
		return handleTransaction(eng, sess, tokens)

	case "COMMIT":
		return handleTransaction(eng, sess, tokens)

	case "ROLLBACK", "ABORT":
		return handleTransaction(eng, sess, tokens)

	case "TX", "TRANSACTION":
		return handleTransaction(eng, sess, tokens)

	case "BULK":
		return handleBulkCmd(eng, sess, tokens)

	case "TRUNCATE":
		return handleTruncate(eng, sess, tokens)

	case "UPSERT":
		return handleUpsert(eng, sess, tokens)

	case "ADD":
		if len(tokens) >= 2 && strings.ToUpper(tokens[1]) == "FIELD" {
			return handleAddField(eng, sess, tokens)
		}
		return "ERROR: unknown ADD command (expected: ADD FIELD <block>.<field> DEFAULT <value>)"

	default:
		if len(tokens) == 1 {
			viewsMu.RLock()
			_, ok := views[tokens[0]]
			viewsMu.RUnlock()
			if ok {
				return handleViewCmd(eng, sess, []string{"VIEW", tokens[0]})
			}
		}
		return fmt.Sprintf("ERROR: unknown command '%s' -- type HELP for available commands", tokens[0])
	}
}

func handleStatusCmd(eng *Engine, sess *Session) string {
	status := map[string]any{
		"version":          Version,
		"build":            Build,
		"node_id":          eng.nodeID,
		"data_root":        eng.dataRoot,
		"current_db":       sess.currentDB,
		"shards":           eng.shardMgr.shardCount,
		"replicas":         eng.shardMgr.replicaCount,
		"ops":              eng.opCount.Load(),
		"memory":           getMemStats(),
		"l1_cache":         eng.l1Cache.Stats(),
		"metrics":          eng.metrics.Stats(),
		"uptime":           time.Since(eng.startupTime).Seconds(),
		"fast_startup":     eng.config.FastStartup,
		"max_nodes":        eng.config.MaxClusterNodes,
		"max_shards":       eng.config.MaxShardsPerNode,
		"max_doc_size":     eng.config.MaxDocumentSize,
		"compress_large":   eng.config.CompressLargeDocs,
		"lazy_index":       eng.config.LazyIndexLoading,
		"predictive_scale": eng.config.PredictiveScaling,
		"auto_cluster":     eng.config.AutoCluster,
		"buffer_pool":      eng.bufferPool.Stats(),
		"risk_engine": map[string]any{
			"enabled": eng.riskEngine.config.Enabled,
			"rules":   len(eng.riskEngine.rules),
		},
		"flex_column": eng.flexEngine.Stats(),
		"turbo":       eng.TurboStats(),
		"linedat":     eng.LineDatStats(),
	}
	if eng.cluster != nil {
		status["cluster"] = map[string]any{
			"role":   eng.cluster.raft.State().String(),
			"leader": string(eng.cluster.raft.Leader()),
			"peers":  eng.cluster.GetPeers(),
		}
	}
	if eng.wal != nil {
		status["wal"] = eng.wal.Stats()
	}
	return jsonOut(status)
}

func handleHealthCmd(eng *Engine) string {
	health := map[string]any{
		"status":           "ok",
		"version":          Version,
		"uptime":           time.Since(eng.startupTime).Seconds(),
		"node_id":          eng.nodeID,
		"shards":           eng.shardMgr.shardCount,
		"ops":              eng.opCount.Load(),
		"max_nodes":        eng.config.MaxClusterNodes,
		"max_shards":       eng.config.MaxShardsPerNode,
		"fast_startup":     eng.config.FastStartup,
		"lazy_index":       eng.config.LazyIndexLoading,
		"compress_large":   eng.config.CompressLargeDocs,
		"max_doc_size":     eng.config.MaxDocumentSize,
		"auto_cluster":     eng.config.AutoCluster,
		"predictive_scale": eng.config.PredictiveScaling,
		"flex_column":      eng.flexEngine.Stats(),
	}
	return jsonOut(health)
}

func handleTree(eng *Engine, sess *Session) string {
	var sb strings.Builder
	sb.WriteString("Data Tree:\n")
	sb.WriteString(eng.dataRoot + "/\n")

	dbs, err := eng.ShowDBs()
	if err != nil {
		return "ERROR: " + err.Error()
	}

	for _, db := range dbs {
		prefix := "├── "
		sb.WriteString(prefix + db + "/\n")
		blocks, _ := eng.ShowBlocks(db)
		for i, block := range blocks {
			if i == len(blocks)-1 {
				sb.WriteString("    └── " + block + "/\n")
			} else {
				sb.WriteString("    ├── " + block + "/\n")
			}
		}
	}
	return sb.String()
}
