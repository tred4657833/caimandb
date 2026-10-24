package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strings"
	"time"
)

type QueryServer struct {
	engine    *Engine
	config    *Config
	mux       *http.ServeMux
	server    *http.Server
	tokenMgr  *TokenManager
	tlsStopCh chan struct{}
}

func NewQueryServer(engine *Engine, config *Config) *QueryServer {
	qs := &QueryServer{
		engine:   engine,
		config:   config,
		mux:      http.NewServeMux(),
		tokenMgr: engine.tokenManager,
	}

	qs.mux.HandleFunc("/query", qs.handleQuery)
	qs.mux.HandleFunc("/status", qs.handleStatus)
	qs.mux.HandleFunc("/health", qs.handleHealth)
	qs.mux.HandleFunc("/watch", qs.handleWatch)
	qs.mux.HandleFunc("/entities", qs.handleEntities)

	// Serve the pure HTML/CSS/JS admin console (ui/) directly at "/",
	// same-origin with the JSON API above. This is what lets the
	// console be opened as http://host:QueryPort/ with zero extra
	// moving parts: no separate static server, no file:// CORS
	// restrictions, and no cross-origin request at all for the common
	// case. If the directory is missing, "/" just 404s; the API
	// endpoints registered above are unaffected. More specific patterns
	// (e.g. "/query") always take precedence over "/" in ServeMux, so
	// this can't shadow the API routes.
	if uiDir := config.UIDir; uiDir != "" {
		if info, err := os.Stat(uiDir); err == nil && info.IsDir() {
			qs.mux.Handle("/", http.FileServer(http.Dir(uiDir)))
		} else {
			log().Warn("UI directory not found, console will not be served",
				zap.String("ui_dir", uiDir))
		}
	}

	return qs
}

// withCORS lets a browser-based client (e.g. the bundled ui/ admin
// console, typically served from a different origin/port than the
// query server itself) call these endpoints directly with the user's
// own Basic Auth credentials. Only headers/methods this API actually
// uses are allowed; no cookies or credentials-mode wildcarding.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Max-Age", "600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// resolveEngine autentica la petición y devuelve el Engine al que debe
// enrutarse: con Basic Auth, las credenciales pueden pertenecer al
// servidor raíz o a cualquier servidor aprovisionado con CREATE SERVER
// (todos comparten este mismo puerto -- ver server_manager.go), así que
// se resuelven contra el ServerManager cuando está disponible. Con
// Bearer/JWT se mantiene el comportamiento histórico (servidor raíz),
// ya que el token no lleva información de servidor en esta versión.
func (qs *QueryServer) resolveEngine(r *http.Request) (*Engine, User, bool) {
	user, pass, ok := r.BasicAuth()
	if ok {
		if qs.engine.serverMgr != nil {
			if eng, u, authOK := qs.engine.serverMgr.Authenticate(user, pass); authOK {
				return eng, u, true
			}
			return nil, User{}, false
		}
		if authOK, u := qs.engine.AuthUserDetailed(user, pass); authOK {
			return qs.engine, u, true
		}
		return nil, User{}, false
	}

	token := r.Header.Get("Authorization")
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")
		if claims, err := qs.tokenMgr.ValidateToken(token); err == nil {
			ctx := context.WithValue(r.Context(), "user", claims.Username)
			ctx = context.WithValue(ctx, "role", claims.Role)
			*r = *r.WithContext(ctx)
			return qs.engine, User{Username: claims.Username, Role: claims.Role}, true
		}
	}

	return nil, User{}, false
}

// auth conserva la firma histórica (solo bool) para el resto de
// endpoints (status/entities/health/watch), que hoy siguen atendiendo
// exclusivamente contra el servidor raíz.
func (qs *QueryServer) auth(r *http.Request) bool {
	_, _, ok := qs.resolveEngine(r)
	return ok
}

func (qs *QueryServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	eng, u, ok := qs.resolveEngine(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "Missing 'query' parameter", http.StatusBadRequest)
		return
	}

	db := r.URL.Query().Get("db")
	if db == "" {
		db = u.DefaultDatabase
	}
	if db == "" {
		db = "default"
	}

	sess := &Session{currentDB: db, authUser: u.Username, authRole: u.Role, authDatabase: u.Database}

	result := parseAndExec(eng, sess, query)

	if result == "__EXIT__" {
		result = "Connection closed"
	}

	// Echo sess.currentDB (the database this statement actually ended up
	// running against, after parseAndExec) rather than the pre-execution
	// "db" variable. For most statements these are identical, but for
	// USE/CD -- which mutate sess.currentDB as a side effect -- echoing
	// the stale pre-execution "db" told callers the wrong thing: a client
	// that trusts this field to track "what database is active now"
	// (e.g. the Studio editor's "Run all", see runActiveTab in studio.js)
	// would only learn about the switch by separately pattern-matching the
	// query text for "USE <db>", since the field itself never reflected
	// it. Every session-mutating command (USE/CD) now reports its actual
	// resulting database here.
	resp := map[string]any{
		"success": true,
		"result":  result,
		"db":      sess.currentDB,
	}

	// Alongside the human-readable "result" text (which every command
	// produces, for terminal-style clients), also expose a structured
	// grid for read-only FIND/GET commands so browser UIs (ui/) can
	// render a real data table instead of parsing formatted text. This
	// re-runs the same read against the engine — cheap and read-only —
	// rather than reusing the text formatter, so it reflects the exact
	// same data the "result" field describes.
	if rows, cols, ok := qs.structuredRows(eng, sess, query); ok {
		resp["rows"] = rows
		resp["columns"] = cols
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// structuredRows returns a JSON-friendly document grid for FIND and GET
// commands, so the bundled ui/ console can render real results as a
// table. Any other command (INSERT/UPDATE/DELETE/SHOW/...) or a query
// the engine rejects simply yields ok=false — callers fall back to the
// plain-text "result" field, which every command already produces.
func (qs *QueryServer) structuredRows(eng *Engine, sess *Session, query string) (rows []*Document, cols []string, ok bool) {
	tokens := parse.Tokenize(query)
	if len(tokens) < 2 {
		return nil, nil, false
	}

	cmd := strings.ToUpper(tokens[0])
	db := sess.currentDB
	if db == "" || (cmd != "FIND" && cmd != "GET") {
		return nil, nil, false
	}
	block := tokens[1]

	switch cmd {
	case "GET":
		if len(tokens) < 3 {
			return nil, nil, false
		}
		doc, found := eng.FindByID(db, block, tokens[2])
		if !found {
			return nil, nil, false
		}
		return []*Document{doc}, docColumns([]*Document{doc}), true

	case "FIND":
		if !eng.dirMgr.BlockExists(db, block) {
			return nil, nil, false
		}
		// "FIND <block> <id>" is a single-document shortcut (no WHERE/
		// LIMIT/... clause follows) — handle it the same way as GET.
		if len(tokens) >= 3 {
			next := strings.ToUpper(tokens[2])
			isClause := next == "WHERE" || next == "LIMIT" || next == "ORDER" ||
				next == "SELECT" || next == "EXCLUDE" || next == "OFFSET" ||
				next == "ONLY" || next == "GROUP" || next == "HAVING"
			if !isClause {
				doc, found := eng.FindByID(db, block, tokens[2])
				if !found {
					return nil, nil, false
				}
				return []*Document{doc}, docColumns([]*Document{doc}), true
			}
		}

		q, _, err := buildFindQuery(tokens, 2)
		if err != nil {
			return nil, nil, false
		}
		result, err := eng.Find(db, block, q)
		if err != nil {
			return nil, nil, false
		}
		docs := make([]*Document, len(result.Docs))
		for i := range result.Docs {
			docs[i] = &result.Docs[i]
		}
		return docs, docColumns(docs), true
	}

	return nil, nil, false
}

// docColumns builds a stable column order for a document grid: _id and
// _shard_id first (every document has them), then every data field seen
// across the batch, in first-seen order, so rows with slightly different
// shapes still line up under a consistent header.
func docColumns(docs []*Document) []string {
	seen := map[string]bool{"_id": true, "_shard_id": true}
	cols := []string{"_id", "_shard_id"}
	for _, d := range docs {
		if d == nil {
			continue
		}
		for k := range d.Data {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	return cols
}

func (qs *QueryServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !qs.auth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	dbs, _ := qs.engine.ShowDBs()
	shards := qs.engine.shardMgr.GetAllShards()

	status := map[string]any{
		"version":              Version,
		"node_id":              qs.engine.nodeID,
		"databases":            len(dbs),
		"shards":               len(shards),
		"shard_count":          qs.engine.shardMgr.shardCount,
		"ops":                  qs.engine.opCount.Load(),
		"memory":               getMemStats(),
		"l1_cache":             qs.engine.l1Cache.Stats(),
		"metrics":              qs.engine.metrics.Stats(),
		"max_shards_per_node":  qs.engine.config.MaxShardsPerNode,
		"max_cluster_nodes":    qs.engine.config.MaxClusterNodes,
		"auto_scaling_enabled": qs.engine.config.AutoCluster,
		"predictive_scaling":   qs.engine.config.PredictiveScaling,
		"fast_startup":         qs.engine.config.FastStartup,
		"lazy_index_loading":   qs.engine.config.LazyIndexLoading,
		"compress_large_docs":  qs.engine.config.CompressLargeDocs,
		"max_document_size":    qs.engine.config.MaxDocumentSize,
		"startup_duration":     time.Since(qs.engine.startupTime).Seconds(),
		"flex_stats":           qs.engine.flexEngine.Stats(),
		"change_stream":        qs.engine.changeBus.Stats(),
	}

	if qs.engine.cluster != nil {
		status["cluster"] = map[string]any{
			"role":        qs.engine.cluster.raft.State().String(),
			"leader":      string(qs.engine.cluster.raft.Leader()),
			"peers":       qs.engine.cluster.GetPeers(),
			"total_nodes": len(qs.engine.cluster.GetPeers()) + 1,
			"max_nodes":   qs.engine.config.MaxClusterNodes,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleEntities lists real databases and, for the active one, its blocks
// (collections) — the data the ui/ sidebar renders as the entity tree.
// Kept as its own JSON endpoint (rather than making the browser parse the
// text output of "SHOW DBS"/"SHOW BLOCKS") because eng.ShowDBs/ShowBlocks
// already return plain []string, so no text formatting to undo.
func (qs *QueryServer) handleEntities(w http.ResponseWriter, r *http.Request) {
	eng, u, ok := qs.resolveEngine(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	db := r.URL.Query().Get("db")
	if db == "" {
		db = "default"
	}

	// Aislamiento: db llega como argumento explícito de query string, no
	// vía Session.currentDB, así que no pasa por enforceSessionIsolation
	// -- mismo caso que handleDB/handleBackup/... en http_admin.go. Se
	// deniega antes de tocar el motor si la sesión no es dueña de db.
	sess := &Session{authRole: u.Role, authDatabase: u.Database}
	if err := requireDBAccess(sess, db); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	dbs, err := eng.ShowDBs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// La lista de bases también debe quedar acotada a la propia (misma
	// regla que SHOW DBS / handleShowDBs), no solo el detalle de blocks
	// de la base activa.
	if u.Role != "" && !IsGeneralAdmin(u.Role) {
		owned := make([]string, 0, 1)
		for _, name := range dbs {
			if name == u.Database {
				owned = append(owned, name)
			}
		}
		dbs = owned
	}

	names, err := eng.ShowBlocks(db)
	if err != nil {
		// A database that doesn't exist yet (e.g. the default one, before
		// anything was created) isn't a server error — just no blocks.
		names = []string{}
	}

	// DescribeBlock is a cheap metadata read (no document scan beyond the
	// block's own counter), so it's fine to call it once per block here —
	// real document counts and sizes for the sidebar/dashboard, not
	// estimates.
	blocks := make([]map[string]any, 0, len(names))
	for _, name := range names {
		info, err := eng.DescribeBlock(db, name)
		if err != nil {
			blocks = append(blocks, map[string]any{"name": name})
			continue
		}
		blocks = append(blocks, map[string]any{
			"name":       name,
			"documents":  info["documents"],
			"size_bytes": info["size_bytes"],
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"db":     db,
		"dbs":    dbs,
		"blocks": blocks,
	})
}

func (qs *QueryServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":              "ok",
		"version":             Version,
		"uptime":              time.Since(startTimeGlobal).String(),
		"unlimited_mode":      true,
		"max_nodes":           qs.engine.config.MaxClusterNodes,
		"max_shards":          qs.engine.config.MaxShardsPerNode,
		"fast_startup":        qs.engine.config.FastStartup,
		"lazy_index_loading":  qs.engine.config.LazyIndexLoading,
		"compress_large_docs": qs.engine.config.CompressLargeDocs,
		"max_document_size":   qs.engine.config.MaxDocumentSize,
		"startup_duration":    time.Since(qs.engine.startupTime).Seconds(),
	})
}

// handleWatch streams real-time change events (insert/update/delete) for a
// database/block as Server-Sent Events. Example:
//
//	curl -N "http://host:port/watch?db=mydb&block=users"
//
// db and block are both optional filters; omitting both streams every
// change on the node. The connection stays open until the client
// disconnects or the server shuts down — this is a genuinely long-lived
// stream, not a poll, so it's deliberately exempted from the server's
// normal WriteTimeout (see the ResponseController call below).
func (qs *QueryServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	if !qs.auth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// The QueryServer's http.Server sets a blanket 30s WriteTimeout, which
	// is right for request/response calls but would silently sever a
	// long-lived watch connection after 30 seconds. Lift the deadline for
	// this connection specifically rather than weakening it globally.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	db := r.URL.Query().Get("db")
	block := r.URL.Query().Get("block")

	id, events := qs.engine.changeBus.Subscribe(db, block)
	defer qs.engine.changeBus.Unsubscribe(id)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // don't let reverse proxies buffer the stream
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Heartbeat so idle connections (and any proxy/LB in between) know the
	// stream is still alive, independent of write activity in the engine.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	enc := json.NewEncoder(&sseWriter{w: w})

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			fmt.Fprint(w, "event: change\ndata: ")
			if err := enc.Encode(ev); err != nil {
				return
			}
			fmt.Fprint(w, "\n")
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// sseWriter adapts an io.Writer so json.Encoder's output lands inline in
// an SSE "data:" field without an extra trailing newline breaking the
// event framing (json.Encoder always appends its own newline already).
type sseWriter struct{ w http.ResponseWriter }

func (s *sseWriter) Write(p []byte) (int, error) { return s.w.Write(p) }

func (qs *QueryServer) Start() {
	addr := fmt.Sprintf(":%d", qs.config.QueryPort)
	qs.server = &http.Server{
		Addr:         addr,
		Handler:      withCORS(qs.mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	safeGo("query_server", func() {
		log().Info("HTTP Query Server started",
			zap.Int("port", qs.config.QueryPort),
			zap.String("user", qs.config.QueryUser),
			zap.Int64("max_doc_size", qs.config.MaxDocumentSize))

		if qs.config.TLSEnabled {
			// mTLS + hot cert rotation -- see AdminServer.Start in
			// http_admin.go for the identical reasoning.
			qs.tlsStopCh = make(chan struct{})
			tlsConfig, _, err := buildServerTLSConfig(qs.config, qs.tlsStopCh)
			if err != nil {
				log().Error("Query server TLS setup failed", zap.Error(err))
				return
			}
			qs.server.TLSConfig = tlsConfig
			if err := qs.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				log().Error("Query server failed", zap.Error(err))
			}
		} else {
			if err := qs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log().Error("Query server failed", zap.Error(err))
			}
		}
	})
}

func (qs *QueryServer) Stop() {
	if qs.tlsStopCh != nil {
		close(qs.tlsStopCh)
		qs.tlsStopCh = nil
	}
	if qs.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		qs.server.Shutdown(ctx)
	}
}
