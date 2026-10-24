// server_manager.go implementa el soporte multi-servidor: además del
// servidor raíz (el Engine que arranca normalmente, con su único ADMIN
// definido la primera vez que se ejecuta CaimanDB), un operador puede
// aprovisionar servidores adicionales con CREATE SERVER, cada uno con:
//
//   - su propio directorio en disco, bajo <dataRoot>/servers/<nombre>/
//   - su propio usuario ADMIN (único, igual que el del servidor raíz)
//   - su propio espacio de usuarios/bases/bloques, completamente
//     aislado del resto de servidores (motor de almacenamiento
//     independiente: no comparten Badger ni directorios)
//
// Todos los servidores -- el raíz y los adicionales -- atienden por el
// MISMO puerto (QueryPort/AdminPort configurados una sola vez). Lo único
// que distingue a qué servidor pertenece una conexión son las
// credenciales: el nombre de usuario es único en todo el proceso, así
// que autenticar (usuario, contraseña) alcanza para saber a qué
// servidor -- y por lo tanto a qué directorio de datos -- enrutar la
// petición. Ver el uso de Authenticate() en http_query.go.
package caimandb

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sync"
)

type ServerManager struct {
	mu         sync.RWMutex
	root       *Engine
	baseConfig *Config
	dataRoot   string
	servers    map[string]*Engine // servidores ya arrancados (carga perezosa)
}

// NewServerManager crea el gestor multi-servidor a partir del Engine
// raíz ya construido. baseConfig se usa como plantilla (mismos puertos,
// mismos parámetros por defecto) para cada servidor adicional; solo
// cambia su DataRoot.
func NewServerManager(root *Engine, baseConfig *Config) *ServerManager {
	return &ServerManager{
		root:       root,
		baseConfig: baseConfig,
		dataRoot:   baseConfig.DataRoot,
		servers:    make(map[string]*Engine),
	}
}

func (sm *ServerManager) serversRoot() string {
	return filepath.Join(sm.dataRoot, "servers")
}

func (sm *ServerManager) registryPath() string {
	return filepath.Join(sm.serversRoot(), "__registry.json")
}

func (sm *ServerManager) listRegistry() ([]string, error) {
	raw, err := os.ReadFile(sm.registryPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return nil, err
	}
	return names, nil
}

func (sm *ServerManager) saveRegistry(names []string) error {
	if err := os.MkdirAll(sm.serversRoot(), 0750); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(names, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(sm.registryPath(), raw, 0600)
}

// LoadAll arranca el Engine de cada servidor ya aprovisionado. Se llama
// una vez al iniciar el proceso (app.go); a partir de ahí todos quedan
// disponibles para autenticarse en el mismo puerto que el servidor raíz.
func (sm *ServerManager) LoadAll() {
	names, err := sm.listRegistry()
	if err != nil {
		log().Warn("server manager: failed to read server registry", zap.Error(err))
		return
	}
	for _, name := range names {
		if _, err := sm.getOrStart(name); err != nil {
			log().Warn("server manager: failed to start server", zap.String("server", name), zap.Error(err))
		}
	}
}

// getOrStart devuelve el Engine del servidor 'name', arrancándolo (con
// su propio dataRoot aislado) si todavía no está en memoria.
func (sm *ServerManager) getOrStart(name string) (*Engine, error) {
	sm.mu.RLock()
	eng, ok := sm.servers[name]
	sm.mu.RUnlock()
	if ok {
		return eng, nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if eng, ok := sm.servers[name]; ok {
		return eng, nil
	}

	cfg := *sm.baseConfig // copia superficial: mismos puertos/ajustes, distinto DataRoot
	cfg.DataRoot = filepath.Join(sm.serversRoot(), name)
	cfg.AutoCluster = false // cada servidor es independiente; sin clúster propio
	if err := os.MkdirAll(cfg.DataRoot, 0750); err != nil {
		return nil, err
	}

	eng = NewEngine(&cfg)
	if err := eng.InitCluster(); err != nil {
		log().Warn("server manager: continuing without cluster", zap.String("server", name), zap.Error(err))
	}
	sm.servers[name] = eng
	return eng, nil
}

// CreateServer aprovisiona un nuevo servidor: crea su directorio, arranca
// su Engine y crea su usuario ADMIN_GENERAL (username == "" usa "admin"),
// el Admin General de ese servidor aislado. Falla si el nombre ya existe.
func (sm *ServerManager) CreateServer(name, username, password string) error {
	if err := validateName(name); err != nil {
		return fmt.Errorf("invalid server name: %w", err)
	}
	if username == "" {
		username = "admin"
	}

	names, err := sm.listRegistry()
	if err != nil {
		return err
	}
	for _, n := range names {
		if n == name {
			return fmt.Errorf("server already exists: %s", name)
		}
	}

	eng, err := sm.getOrStart(name)
	if err != nil {
		return err
	}

	if err := eng.CreateUserWithOpts(username, password, CreateUserOpts{
		Role:   RoleGeneralAdmin,
		Server: name,
	}); err != nil {
		return err
	}

	names = append(names, name)
	if err := sm.saveRegistry(names); err != nil {
		return err
	}
	log().Info("Server created", zap.String("server", name), zap.String("admin", username), zap.String("data_root", eng.dataRoot))
	return nil
}

// Authenticate busca (username, password) primero en el servidor raíz y
// luego en cada servidor aprovisionado (arrancándolo bajo demanda si
// hace falta), y devuelve el Engine al que pertenece el usuario
// autenticado. Como todos los servidores comparten el mismo puerto, este
// es el único mecanismo para saber a qué directorio de datos enrutar la
// conexión: la credencial ES la selección de servidor.
func (sm *ServerManager) Authenticate(username, password string) (*Engine, User, bool) {
	if ok, u := sm.root.AuthUserDetailed(username, password); ok {
		return sm.root, u, true
	}

	names, err := sm.listRegistry()
	if err != nil {
		return nil, User{}, false
	}
	for _, name := range names {
		eng, err := sm.getOrStart(name)
		if err != nil {
			continue
		}
		if ok, u := eng.AuthUserDetailed(username, password); ok {
			return eng, u, true
		}
	}
	return nil, User{}, false
}

// ListServers devuelve los nombres de los servidores aprovisionados
// (para SHOW SERVERS).
func (sm *ServerManager) ListServers() []string {
	names, _ := sm.listRegistry()
	return names
}

// CloseAll apaga ordenadamente todos los servidores adicionales (no el
// raíz, que ya gestiona su propio ciclo de vida en app.go).
func (sm *ServerManager) CloseAll() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for name, eng := range sm.servers {
		eng.Shutdown()
		delete(sm.servers, name)
	}
}

// Shutdown libera ordenadamente todos los recursos de un Engine
// (segundo plano, cachés, pool de conexiones Badger, WAL, etc.). Es el
// mismo procedimiento que app.go ejecuta para el servidor raíz al
// apagar el proceso, extraído aquí para poder reutilizarlo con cada
// servidor adicional.
func (e *Engine) Shutdown() {
	if e.cluster != nil {
		e.cluster.Stop()
	}
	// LineDat's background drain workers write to the WAL/Badger pool
	// closed a few lines below, via the same insertBatchLocalDetailed
	// path every other insert uses -- stop them (and wait, bounded, for
	// any in-flight commit to actually finish) before those stores start
	// tearing down, not after. See LineDat.Close's doc comment.
	if e.lineDat != nil {
		e.lineDat.Close()
	}
	if e.auditLogger != nil {
		e.auditLogger.Close()
	}
	if e.rateLimiter != nil {
		e.rateLimiter.Close()
	}
	if e.wal != nil {
		e.wal.Close()
	}
	if e.l2Cache != nil {
		e.l2Cache.Close()
	}
	if e.l1Cache != nil {
		e.l1Cache.Close()
	}
	if e.shardMgr != nil {
		e.shardMgr.Close()
	}
	if e.intelEngine != nil {
		e.intelEngine.Stop()
	}
	if e.flexEngine != nil {
		e.flexEngine.Stop()
	}
	if e.turbo != nil {
		e.turbo.Close()
	}
	if e.wp != nil {
		e.wp.Close()
	}
	if e.pool != nil {
		e.pool.CloseAll()
	}
	e.cancel()
}
