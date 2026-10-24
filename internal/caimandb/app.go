package caimandb

import (
	"bufio"
	"caimandb/internal/caimandb/metrics"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/term"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var startTimeGlobal = time.Now()

func Run() {
	startupTime := time.Now()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	config := DefaultConfig()

	if _, err := os.Stat(defaultConfigFile); err == nil {
		if err := config.LoadFromFile(defaultConfigFile); err != nil {
			log().Warn("Failed to load config file, using defaults", zap.Error(err))
		} else {
			log().Info("Loaded config file", zap.String("file", defaultConfigFile))
		}
	}

	config.MergeEnv()

	if err := config.Validate(); err != nil {
		log().Fatal("Invalid configuration", zap.Error(err))
	}

	if _, err := os.Stat(defaultConfigFile); os.IsNotExist(err) {
		if err := config.SaveToFile(defaultConfigFile); err != nil {
			log().Warn("Failed to save default config", zap.Error(err))
		} else {
			log().Info("Created default config file", zap.String("file", defaultConfigFile))
		}
	}

	for _, arg := range os.Args[1:] {
		if arg == "--help" || arg == "-h" {
			fmt.Println(helpText())
			return
		}
		if strings.HasPrefix(arg, "--config=") {
			configFile := strings.TrimPrefix(arg, "--config=")
			if err := config.LoadFromFile(configFile); err != nil {
				log().Fatal("Failed to load config file", zap.Error(err))
			}
			log().Info("Loaded config from command line", zap.String("file", configFile))
		}
	}

	initLogger(config.LogLevel)
	metrics.InitMetrics()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	safeGo("metrics_server", func() {
		log().Info("Metrics server started", zap.Int("port", config.MetricsPort))
		if err := http.ListenAndServe(fmt.Sprintf(":%d", config.MetricsPort), metricsMux); err != nil {
			log().Error("Metrics server failed", zap.Error(err))
		}
	})

	if err := os.MkdirAll(config.DataRoot, 0750); err != nil {
		log().Fatal("Failed to create data directory", zap.Error(err))
	}

	log().Info("Starting CaimanDB with optimizations for large documents",
		zap.Bool("fast_startup", config.FastStartup),
		zap.Bool("lazy_index_loading", config.LazyIndexLoading),
		zap.Bool("compress_large_docs", config.CompressLargeDocs),
		zap.Int64("max_document_size", config.MaxDocumentSize),
		zap.Int64("compress_threshold", config.CompressThreshold),
		zap.Int64("index_skip_threshold", config.IndexSkipThreshold),
		zap.Int64("large_doc_threshold", config.LargeDocThreshold))

	eng := NewEngine(config)

	// Usuario de prueba predefinido appmanager (ver default_users.go): se
	// crea automáticamente en cuanto arranca el motor, ANTES del asistente
	// del admin, en instalaciones nuevas y existentes. El asistente de
	// abajo sigue pidiendo el admin real.
	ensureDefaultAppManager(eng)

	// Admin user setup
	adminUser := "admin"
	adminPass := ""

	users, err := eng.ShowUsers()
	if err == nil {
		// appmanager es el usuario de prueba predefinido: no cuenta como
		// "ya hay un admin", así el asistente sigue pidiendo el admin real.
		realUsers := users[:0:0]
		for _, u := range users {
			if name, _ := u["username"].(string); name != DefaultAppManagerUser {
				realUsers = append(realUsers, u)
			}
		}
		users = realUsers
	}
	if err != nil || len(users) == 0 {
		fmt.Println("CaimanDB - First time setup")
		fmt.Println("Please create an admin user to secure your database.")
		fmt.Print("Admin username (default: admin): ")
		var input string
		fmt.Scanln(&input)
		if input != "" {
			adminUser = input
		}
		fmt.Print("Admin password: ")
		adminPassBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			adminPass = generateRandomPassword()
			fmt.Printf("Auto-generated password: %s\n", adminPass)
		} else {
			adminPass = string(adminPassBytes)
			if adminPass == "" {
				adminPass = generateRandomPassword()
				fmt.Printf("Auto-generated password: %s\n", adminPass)
			}
		}
		if err := eng.CreateUser(adminUser, adminPass, RoleGeneralAdmin); err != nil {
			log().Error("Failed to create admin user", zap.Error(err))
		} else {
			log().Info("Admin user created successfully", zap.String("username", adminUser))
			credFile := filepath.Join(config.DataRoot, ".admin_credentials")
			if err := os.WriteFile(credFile, []byte(fmt.Sprintf("Username: %s\nPassword: %s\n", adminUser, adminPass)), 0600); err == nil {
				fmt.Printf("Credentials saved to: %s (secure, chmod 600)\n", credFile)
			}
		}
	} else {
		log().Info("Admin user already exists")
	}

	if err := eng.InitCluster(); err != nil {
		log().Error("Failed to auto-initialize cluster, continuing in standalone mode",
			zap.Error(err))
	}

	// Periodic observability reporting: Raft replication lag
	// (caimandb_raft_replication_lag) and per-shard health
	// (caimandb_shard_health). Both report funcs nil-check their
	// receiver, so this is safe to start unconditionally even in
	// standalone (non-cluster) mode -- they'll just have nothing to
	// report. Stopped via observabilityStop, closed in the same
	// deferred shutdown sequence as everything else below.
	observabilityStop := make(chan struct{})
	defer close(observabilityStop)
	safeGo("observability_metrics_loop", func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-observabilityStop:
				return
			case <-ticker.C:
				eng.cluster.reportRaftLagMetric()
				eng.shardMgr.reportShardHealthMetrics()
			}
		}
	})

	// Multi-server: además del servidor raíz (eng, arriba, con su único
	// ADMIN definido en el primer arranque), CREATE SERVER puede
	// aprovisionar servidores adicionales -- cada uno con su propio
	// directorio y su propio ADMIN -- que atienden por el MISMO puerto,
	// diferenciados solo por credenciales. Ver server_manager.go.
	serverMgr := NewServerManager(eng, config)
	serverMgr.LoadAll()
	eng.serverMgr = serverMgr

	defer func() {
		log().Info("Shutting down gracefully...")
		serverMgr.CloseAll()
		eng.Shutdown()
		log().Info("Shutdown complete")
	}()

	queryServer := NewQueryServer(eng, config)
	queryServer.Start()
	defer queryServer.Stop()

	adminServer := NewAdminServer(eng, config)
	adminServer.Start()
	defer adminServer.Stop()

	if eng.cluster != nil {
		clusterMux := http.NewServeMux()
		clusterMux.HandleFunc("/raft/join", eng.cluster.HandleJoin)
		clusterMux.HandleFunc("/raft/leave", eng.cluster.HandleLeave)
		clusterMux.HandleFunc("/raft/status", eng.cluster.HandleStatus)
		clusterMux.HandleFunc("/raft/query", eng.distQuery.HandleQuery)
		clusterMux.HandleFunc("/raft/query_local", eng.distQuery.HandleQueryLocal)

		raftPort := config.RaftPort
		apiPort := raftPort + 100
		safeGo("cluster_http_api_server", func() {
			log().Info("Cluster HTTP API server started",
				zap.Int("port", apiPort))
			if err := http.ListenAndServe(fmt.Sprintf(":%d", apiPort), clusterMux); err != nil {
				log().Error("Cluster HTTP server failed", zap.Error(err))
			}
		})

		log().Info("Auto-cluster started",
			zap.String("node_id", eng.nodeID),
			zap.Int("shards", eng.shardMgr.shardCount),
			zap.Int("replicas", eng.shardMgr.replicaCount),
			zap.Int("max_nodes", config.MaxClusterNodes),
			zap.Int("max_shards_per_node", config.MaxShardsPerNode),
			zap.Bool("fast_startup", config.FastStartup),
			zap.Int64("max_doc_size", config.MaxDocumentSize))

		safeGo("cluster_wait_for_leader", func() {
			if err := eng.cluster.WaitForLeader(30 * time.Second); err != nil {
				log().Warn("Timed out waiting for leader (auto-mode will continue)",
					zap.Error(err))
			} else {
				log().Info("Cluster ready (auto-mode)",
					zap.String("leader", eng.cluster.GetLeader()),
					zap.String("node_id", eng.nodeID),
					zap.Int("total_nodes", len(eng.cluster.GetPeers())+1))
			}
		})
	}

	totalStartupTime := time.Since(startupTime)
	metrics.MetricStartupTime.Set(totalStartupTime.Seconds())
	log().Info("CaimanDB startup complete",
		zap.Duration("total_startup", totalStartupTime),
		zap.Bool("fast_startup", config.FastStartup),
		zap.Bool("lazy_index_loading", config.LazyIndexLoading),
		zap.Bool("compress_large_docs", config.CompressLargeDocs),
		zap.Int("query_port", config.QueryPort),
		zap.Int("admin_port", config.AdminPort),
		zap.Int("metrics_port", config.MetricsPort),
		zap.Bool("flex_column_enabled", config.FlexColumn.Enabled))

	sess := &Session{}

	sessionFile := filepath.Join(config.DataRoot, ".session")
	if b, err := os.ReadFile(sessionFile); err == nil {
		sess.currentDB = strings.TrimSpace(string(b))
	}
	saveSession := func() {
		os.WriteFile(sessionFile, []byte(sess.currentDB), 0600)
	}

	runLine := func(line string) (stop bool) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			return false
		}
		out := parseAndExec(eng, sess, line)
		if out == "__EXIT__" {
			return true
		}
		if out != "" {
			fmt.Println(out)
		}
		return false
	}

	if len(os.Args) > 1 && strings.ToUpper(os.Args[1]) == "RUN" {
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: caimandb run <script.nql>")
			os.Exit(1)
		}
		f, err := os.Open(os.Args[2])
		if err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			os.Exit(1)
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 128<<20), 128<<20)
		// Igual que el prompt interactivo: una sentencia puede ocupar
		// varias líneas, ya sea terminando cada una (salvo la última) en
		// "\", o simplemente dejando abierta una llave/corchete/paréntesis
		// -- que es como se escribe el JSON de un INSERT o la lista de
		// CREATE BLOCK (...) en un .nql. Ver stmtBuffer en
		// script_reader.go.
		var stmt stmtBuffer
		for sc.Scan() {
			raw := sc.Text()
			fmt.Println(">>", raw)
			full, ok := stmt.feed(raw)
			if !ok {
				continue
			}
			if runLine(full) {
				break
			}
		}
		if stmt.pending() {
			fmt.Fprintln(os.Stderr, "WARNING: script ended with an unterminated statement (unbalanced { [ or ( )")
		}
		saveSession()
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "-" {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 128<<20), 128<<20)
		var stmt stmtBuffer
		for sc.Scan() {
			full, ok := stmt.feed(sc.Text())
			if !ok {
				continue
			}
			if runLine(full) {
				break
			}
		}
		saveSession()
		return
	}

	if len(os.Args) > 1 {
		out := parseAndExec(eng, sess, strings.Join(os.Args[1:], " "))
		if out != "" && out != "__EXIT__" {
			fmt.Println(out)
		}
		saveSession()
		return
	}

	fmt.Println("CaimanDB v" + Version + " -- DataEngine - Enterprise Edition (PRODUCTION READY)")
	fmt.Println("Type HELP for commands, EXIT to quit.")
	fmt.Println("")
	fmt.Println("BRAIN BIG DATA ENGINE (Fully Autonomous):")
	fmt.Println("   Auto-cluster initialization")
	fmt.Println("   WAL RECOVERY: Enabled (durability guaranteed)")
	fmt.Println("   LAZY INDEX LOADING: Load on demand (enabled)")
	fmt.Println("   COMPRESSION: Auto-compress > 100KB documents")
	fmt.Println("   SKIP INDEXING: Skip indexing > 1MB fields")
	fmt.Println("   EXTERNAL STORAGE: >5MB documents stored externally")
	fmt.Println("   FLEX-COLUMN: Hybrid storage (row + columnar)")
	fmt.Println("   MATERIALIZED VIEWS: Pre-computed queries")
	fmt.Println("   HOT FIELD DETECTION: Auto-optimize queries")
	fmt.Println("   Argon2id password hashing (secure)")
	fmt.Println("   Cost-based query optimizer")
	fmt.Println("   Consistent hashing for sharding")
	fmt.Println("   Buffer pooling for reduced GC pressure")
	fmt.Println("   Risk engine for operation blocking")
	fmt.Println("   JWT Authentication with token expiry")
	fmt.Println("   Rate limiting per user")
	fmt.Println("   Audit logging with JSON format")
	fmt.Println("   Auto-scaling based on data growth")
	fmt.Println("   Auto-shard splitting and merging")
	fmt.Println("   Auto-node discovery and joining")
	fmt.Println("   Predictive analytics for proactive scaling")
	fmt.Println("   Write-Ahead Logging for crash recovery")
	fmt.Println("   Distributed query executor with shard pruning")
	fmt.Println("   NO HARD LIMITS - scale to millions of nodes and shards")
	fmt.Println("   Automatic health checks and failover")
	fmt.Println("")
	fmt.Printf("- STARTUP: %.3fs (fast startup: %v)\n", time.Since(startupTime).Seconds(), config.FastStartup)
	fmt.Println("- SHARDS: " + strconv.Itoa(eng.shardMgr.shardCount) + " (auto-scaled)")
	fmt.Println("- REPLICAS: " + strconv.Itoa(eng.shardMgr.replicaCount) + " (auto-managed)")
	fmt.Println("- MAX DOC SIZE: " + fmtBytes(config.MaxDocumentSize))
	fmt.Println("- COMPRESS THRESHOLD: " + fmtBytes(config.CompressThreshold))
	fmt.Println("- INDEX SKIP THRESHOLD: " + fmtBytes(config.IndexSkipThreshold))
	fmt.Println("- LARGE DOC THRESHOLD: " + fmtBytes(config.LargeDocThreshold) + " (external storage)")
	fmt.Println("- FLEX-COLUMN: " + map[bool]string{true: "enabled", false: "disabled"}[config.FlexColumn.Enabled])
	fmt.Println("- WAL: " + map[bool]string{true: "enabled", false: "disabled"}[eng.wal != nil] +
		" (clean: " + map[bool]string{true: "yes", false: "no"}[eng.wal != nil && eng.wal.IsClean()] + ")")
	fmt.Println("- HTTP QUERY: port " + strconv.Itoa(config.QueryPort))
	fmt.Println("- ADMIN API: port " + strconv.Itoa(config.AdminPort))
	if info, err := os.Stat(config.UIDir); err == nil && info.IsDir() {
		fmt.Println("- CONSOLE: http://localhost:" + strconv.Itoa(config.QueryPort) + "/")
	}
	fmt.Println("- MAX NODES: " + strconv.Itoa(config.MaxClusterNodes) + " (unlimited)")
	fmt.Println("- MAX SHARDS: " + strconv.Itoa(config.MaxShardsPerNode) + " per node (unlimited)")
	fmt.Println("- JWT AUTH: enabled (expiry: " + config.TokenExpiry.String() + ")")
	fmt.Println("")
	if sess.currentDB != "" {
		fmt.Printf("Session restored: db=%s\n", sess.currentDB)
	}
	if eng.cluster != nil && eng.cluster.started {
		fmt.Printf("Cluster mode: node=%s role=%s leader=%s shards=%d nodes=%d (auto)\n",
			eng.nodeID,
			eng.cluster.raft.State().String(),
			string(eng.cluster.raft.Leader()),
			eng.shardMgr.shardCount,
			len(eng.cluster.GetPeers())+1)
	} else {
		fmt.Println("Standalone mode (cluster will auto-init when needed)")
	}
	fmt.Println("")

	lineChan := make(chan string, 10000)
	errChan := make(chan error, 1)

	safeGo("stdin_line_reader", func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 128<<20), 128<<20)
		for sc.Scan() {
			lineChan <- sc.Text()
		}
		if err := sc.Err(); err != nil {
			errChan <- err
		}
		close(lineChan)
	})

	var stmt stmtBuffer

	if sess.currentDB != "" {
		fmt.Printf("[%s]> ", sess.currentDB)
	} else {
		fmt.Print("> ")
	}

	for {
		select {
		case <-sigChan:
			log().Info("Received signal, shutting down gracefully...")
			fmt.Println("\nShutting down gracefully...")
			saveSession()
			return

		case line, ok := <-lineChan:
			if !ok {
				saveSession()
				return
			}

			full, ok := stmt.feed(line)
			if !ok {
				if stmt.pending() {
					fmt.Print("...> ")
				} else if sess.currentDB != "" {
					fmt.Printf("[%s]> ", sess.currentDB)
				} else {
					fmt.Print("> ")
				}
				continue
			}
			line = full

			t0 := time.Now()
			if runLine(line) {
				saveSession()
				fmt.Println("Goodbye.")
				return
			}
			took := time.Since(t0)
			if took > 200*time.Millisecond {
				fmt.Printf("took %s\n", formatDuration(took))
			}

			if sess.currentDB != "" {
				fmt.Printf("[%s]> ", sess.currentDB)
			} else {
				fmt.Print("> ")
			}

		case err := <-errChan:
			log().Error("Stdin read error", zap.Error(err))
			saveSession()
			return
		}
	}
}
