package caimandb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"time"
)

type AdminServer struct {
	engine    *Engine
	config    *Config
	mux       *http.ServeMux
	server    *http.Server
	tokenMgr  *TokenManager
	tlsStopCh chan struct{}
}

func NewAdminServer(engine *Engine, config *Config) *AdminServer {
	as := &AdminServer{
		engine:   engine,
		config:   config,
		mux:      http.NewServeMux(),
		tokenMgr: engine.tokenManager,
	}

	as.mux.HandleFunc("/api/v1/health", as.handleHealth)
	as.mux.HandleFunc("/api/v1/status", as.handleStatus)
	as.mux.HandleFunc("/api/v1/dbs", as.handleDatabases)
	as.mux.HandleFunc("/api/v1/db/", as.handleDB)
	as.mux.HandleFunc("/api/v1/query", as.handleQuery)
	as.mux.HandleFunc("/api/v1/backup", as.handleBackup)
	as.mux.HandleFunc("/api/v1/restore", as.handleRestore)
	as.mux.HandleFunc("/api/v1/compact", as.handleCompact)
	as.mux.HandleFunc("/api/v1/users", as.handleUsers)
	as.mux.HandleFunc("/api/v1/auth/login", as.handleLogin)
	as.mux.HandleFunc("/api/v1/auth/logout", as.handleLogout)
	as.mux.HandleFunc("/api/v1/token", as.handleToken)
	as.mux.HandleFunc("/api/v1/metrics", promhttp.Handler().ServeHTTP)

	// Also expose the same console routes the query server has
	// (/query, /status, /health, /watch, /entities), reusing its exact
	// handlers (they only touch engine/config/tokenMgr, all of which
	// this admin server already has). This lets ui/ log in and work
	// against either port — the admin port (AdminPort, 1556 by
	// default) or the query port (QueryPort, 1555) — instead of only
	// working when it happens to be served from the query port. Since
	// qsShim.engine is the same root Engine (with serverMgr set), these
	// routes already resolve multi-server credentials the same way
	// as.handleQuery below does.
	qsShim := &QueryServer{engine: engine, config: config, tokenMgr: engine.tokenManager}
	as.mux.HandleFunc("/query", qsShim.handleQuery)
	as.mux.HandleFunc("/status", qsShim.handleStatus)
	as.mux.HandleFunc("/health", qsShim.handleHealth)
	as.mux.HandleFunc("/watch", qsShim.handleWatch)
	as.mux.HandleFunc("/entities", qsShim.handleEntities)

	return as
}

// resolveAdmin autentica la petición y devuelve el Engine al que debe
// enrutarse (servidor raíz o el que corresponda según las credenciales,
// cuando hay servidores adicionales aprovisionados con CREATE SERVER --
// ver server_manager.go), junto con los claims del usuario autenticado.
// Con Bearer/JWT se mantiene el comportamiento histórico (servidor
// raíz), ya que el token no lleva información de servidor en esta
// versión.
func (as *AdminServer) resolveAdmin(r *http.Request) (*Engine, *TokenClaims, bool) {
	user, pass, ok := r.BasicAuth()
	if ok {
		if as.engine.serverMgr != nil {
			if eng, u, authOK := as.engine.serverMgr.Authenticate(user, pass); authOK {
				return eng, &TokenClaims{Username: u.Username, Role: u.Role, Database: u.Database}, true
			}
			return nil, nil, false
		}
		if authOK, u := as.engine.AuthUserDetailed(user, pass); authOK {
			return as.engine, &TokenClaims{Username: u.Username, Role: u.Role, Database: u.Database}, true
		}
		return nil, nil, false
	}

	token := r.Header.Get("Authorization")
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")
		if claims, err := as.tokenMgr.ValidateToken(token); err == nil {
			return as.engine, claims, true
		}
	}

	return nil, nil, false
}

// auth conserva la firma histórica para el único caso que no necesita el
// Engine resuelto (ya cubierto por resolveAdmin en todos los demás).
func (as *AdminServer) auth(r *http.Request) (*TokenClaims, bool) {
	_, claims, ok := as.resolveAdmin(r)
	return claims, ok
}

func (as *AdminServer) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	user, pass, ok := r.BasicAuth()
	if !ok {
		http.Error(w, "Unauthorized - Basic Auth required", http.StatusUnauthorized)
		return
	}

	var eng *Engine
	var authRole, authDatabase string
	if as.engine.serverMgr != nil {
		e, u, authOK := as.engine.serverMgr.Authenticate(user, pass)
		if !authOK {
			http.Error(w, "Invalid credentials", http.StatusUnauthorized)
			return
		}
		eng, authRole, authDatabase = e, u.Role, u.Database
	} else {
		authOK, u := as.engine.AuthUserDetailed(user, pass)
		if !authOK {
			http.Error(w, "Invalid credentials", http.StatusUnauthorized)
			return
		}
		eng, authRole, authDatabase = as.engine, u.Role, u.Database
	}

	role := authRole
	database := authDatabase
	if role == "" {
		users, _ := eng.ShowUsers()
		for _, u := range users {
			if u["username"] == user {
				if r, ok := u["role"].(string); ok {
					role = r
				}
				if d, ok := u["database"].(string); ok {
					database = d
				}
				break
			}
		}
	}
	if role == "" {
		role = RoleUser
	}

	token, err := as.tokenMgr.GenerateToken(user, user, role, database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"token":    token,
		"user":     user,
		"role":     role,
		"database": database,
		"expires":  time.Now().Add(as.config.TokenExpiry).Unix(),
	})
}

func (as *AdminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "ok",
		"version":    Version,
		"uptime":     time.Since(startTimeGlobal).String(),
		"max_nodes":  as.config.MaxClusterNodes,
		"max_shards": as.config.MaxShardsPerNode,
	})
}

func (as *AdminServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	dbs, _ := eng.ShowDBs()
	status := map[string]any{
		"version":    Version,
		"node_id":    eng.nodeID,
		"databases":  len(dbs),
		"shards":     eng.shardMgr.shardCount,
		"replicas":   eng.shardMgr.replicaCount,
		"ops":        eng.opCount.Load(),
		"memory":     getMemStats(),
		"l1_cache":   eng.l1Cache.Stats(),
		"metrics":    eng.metrics.Stats(),
		"uptime":     time.Since(eng.startupTime).Seconds(),
		"user":       claims.Username,
		"role":       claims.Role,
		"max_shards": as.config.MaxShardsPerNode,
		"max_nodes":  as.config.MaxClusterNodes,
		"flex_stats": eng.flexEngine.Stats(),
	}

	if eng.cluster != nil {
		status["cluster"] = map[string]any{
			"role":   eng.cluster.raft.State().String(),
			"leader": string(eng.cluster.raft.Leader()),
			"peers":  eng.cluster.GetPeers(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (as *AdminServer) handleDatabases(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case "GET":
		dbs, err := eng.ShowDBs()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Aislamiento: fuera del Admin General, cada quien solo ve su
		// propia base (misma regla que SHOW DBS / handleShowDBs en
		// cmd_show_size.go).
		if claims != nil && claims.Role != "" && !IsGeneralAdmin(claims.Role) {
			owned := make([]string, 0, 1)
			for _, db := range dbs {
				if db == claims.Database {
					owned = append(owned, db)
				}
			}
			dbs = owned
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dbs)

	case "POST":
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Aislamiento: misma regla que CREATE DB por consola/DSL (ver
		// handleCreate en cmd_create_show.go) -- un ADMIN solo puede
		// crear la base que ya tiene asignada como propia; el resto de
		// roles autenticados (fuera de ADMIN_GENERAL) no pueden crear
		// bases en absoluto por este endpoint.
		if claims != nil && claims.Role != "" && !IsGeneralAdmin(claims.Role) {
			if strings.ToUpper(claims.Role) != RoleAdmin || req.Name != claims.Database {
				http.Error(w, "Forbidden: acceso denegado", http.StatusForbidden)
				return
			}
		}
		if err := eng.CreateDB(req.Name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if claims != nil && claims.Role != "" && !IsGeneralAdmin(claims.Role) {
			eng.SetDBOwner(req.Name, claims.Username)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "created", "name": req.Name})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (as *AdminServer) handleDB(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/db/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "Database name required", http.StatusBadRequest)
		return
	}
	dbName := parts[0]

	// Aislamiento: GET/DELETE reciben el nombre de la base como
	// argumento explícito en la URL, así que no pasan por
	// enforceSessionIsolation (que solo cubre Session.currentDB) -- ver
	// docs/hierarchy.md, "Alcance actual y siguiente fase". Se aplica
	// aquí la misma regla que el resto de la consola/DSL.
	if claims != nil {
		sess := &Session{authRole: claims.Role, authDatabase: claims.Database}
		if err := requireDBAccess(sess, dbName); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	switch r.Method {
	case "GET":
		info, err := eng.DescribeDB(dbName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)

	case "DELETE":
		if err := eng.DropDB(dbName); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (as *AdminServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Query string `json:"query"`
		DB    string `json:"db"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sess := &Session{
		currentDB:    req.DB,
		authUser:     claims.Username,
		authRole:     claims.Role,
		authDatabase: claims.Database,
		token:        r.Header.Get("Authorization"),
	}

	result := parseAndExec(eng, sess, req.Query)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": result != "ERROR",
		"result":  result,
		"db":      req.DB,
	})
}

func (as *AdminServer) handleBackup(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DB   string `json:"db"`
		File string `json:"file"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if claims != nil {
		sess := &Session{authRole: claims.Role, authDatabase: claims.Database}
		if err := requireDBAccess(sess, req.DB); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	if err := eng.Backup(req.DB, req.File); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "backup complete"})
}

func (as *AdminServer) handleRestore(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DB   string `json:"db"`
		File string `json:"file"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if claims != nil {
		sess := &Session{authRole: claims.Role, authDatabase: claims.Database}
		if err := requireDBAccess(sess, req.DB); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	if err := eng.Restore(req.DB, req.File); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "restore complete"})
}

func (as *AdminServer) handleCompact(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DB string `json:"db"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if claims != nil {
		sess := &Session{authRole: claims.Role, authDatabase: claims.Database}
		if err := requireDBAccess(sess, req.DB); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	rounds, err := eng.Compact(req.DB)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "compact complete",
		"rounds": rounds,
	})
}

func (as *AdminServer) handleUsers(w http.ResponseWriter, r *http.Request) {
	eng, claims, ok := as.resolveAdmin(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case "GET":
		// Aislamiento: fuera del Admin General, cada quien solo ve los
		// usuarios de su propia base (ver ShowUsersScoped / hierarchy.go).
		users, err := eng.ShowUsersScoped(User{Role: claims.Role, Database: claims.Database})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(users)

	case "POST":
		// Solo ADMIN_GENERAL/ADMIN/SUBADMIN pueden dar de alta usuarios
		// -- esto evita que un USER/DEVELOPER autenticado use este
		// endpoint para crearse permisos más altos. authorizeCreateUser
		// aplica además las reglas de jerarquía (base propia, roles que
		// puede otorgar cada uno).
		if !IsAdminRole(claims.Role) && strings.ToUpper(claims.Role) != RoleSubadmin {
			http.Error(w, "Forbidden: only ADMIN_GENERAL, ADMIN or SUBADMIN can create users", http.StatusForbidden)
			return
		}
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Role     string `json:"role"`
			Database string `json:"database"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		opts := CreateUserOpts{Role: req.Role, Database: req.Database}
		sess := &Session{authUser: claims.Username, authRole: claims.Role, authDatabase: claims.Database}
		if err := authorizeCreateUser(sess, &opts); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if err := eng.CreateUserWithOpts(req.Username, req.Password, opts); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "created"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (as *AdminServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var role, database string
	var ok bool
	if as.engine.serverMgr != nil {
		var u User
		_, u, ok = as.engine.serverMgr.Authenticate(req.Username, req.Password)
		role, database = u.Role, u.Database
	} else {
		var u User
		ok, u = as.engine.AuthUserDetailed(req.Username, req.Password)
		role, database = u.Role, u.Database
	}
	if !ok {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	token, err := as.tokenMgr.GenerateToken(req.Username, req.Username, role, database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"token":    token,
		"user":     req.Username,
		"database": database,
		"role":     role,
		"expires":  time.Now().Add(as.config.TokenExpiry).Unix(),
	})
}

func (as *AdminServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := r.Header.Get("Authorization")
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")
		as.tokenMgr.RevokeToken(token)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "logged out"})
}

func (as *AdminServer) Start() {
	addr := fmt.Sprintf(":%d", as.config.AdminPort)
	as.server = &http.Server{
		Addr:         addr,
		Handler:      withCORS(as.mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	safeGo("admin_server", func() {
		log().Info("Admin server started", zap.Int("port", as.config.AdminPort))
		if as.config.TLSEnabled {
			// mTLS + hot cert rotation (tls_mtls.go) instead of the
			// plain ListenAndServeTLS(certFile, keyFile) this used to
			// call: same server-side-only behavior when
			// TLSClientCAFile is unset, additionally enforcing client
			// certs when it is set.
			as.tlsStopCh = make(chan struct{})
			tlsConfig, _, err := buildServerTLSConfig(as.config, as.tlsStopCh)
			if err != nil {
				log().Error("Admin server TLS setup failed", zap.Error(err))
				return
			}
			as.server.TLSConfig = tlsConfig
			if err := as.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				log().Error("Admin server failed", zap.Error(err))
			}
		} else {
			if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log().Error("Admin server failed", zap.Error(err))
			}
		}
	})
}

func (as *AdminServer) Stop() {
	if as.tlsStopCh != nil {
		close(as.tlsStopCh)
		as.tlsStopCh = nil
	}
	if as.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		as.server.Shutdown(ctx)
	}
}
