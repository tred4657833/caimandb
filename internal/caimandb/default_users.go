package caimandb

import (
	"os"

	"go.uber.org/zap"
)

// Usuario predefinido de la aplicación. Se aprovisiona automáticamente en
// cada arranque (ver ensureDefaultAppManager) si todavía no existe, con el
// rol más completo del catálogo (RoleGeneralAdmin: control global de todas
// las bases y usuarios del servidor, sin base propia).
//
// SEGURIDAD: la contraseña por defecto está en el código fuente, así que es
// pública para cualquiera que tenga el repositorio. Cámbiela en producción
// (ALTER/DROP+CREATE USER) o defina CAIMANDB_APPMANAGER_PASSWORD antes del
// primer arranque.
const (
	DefaultAppManagerUser     = "appmanager"
	DefaultAppManagerPassword = "passapp"
)

// ensureDefaultAppManager (llamada desde app.go justo tras NewEngine) crea el
// usuario predefinido appmanager si no existe. Es idempotente: si ya existe (por ejemplo, porque el operador le
// cambió la contraseña o el rol) no se toca nada.
func ensureDefaultAppManager(eng *Engine) {
	if _, exists := eng.GetUserByUsername(DefaultAppManagerUser); exists {
		return
	}
	pass := DefaultAppManagerPassword
	if v := os.Getenv("CAIMANDB_APPMANAGER_PASSWORD"); v != "" {
		pass = v
	}
	err := eng.CreateUserWithOpts(DefaultAppManagerUser, pass, CreateUserOpts{
		Role:    RoleGeneralAdmin,
		Status:  "ACTIVE",
		Comment: "Usuario de prueba predefinido (acceso completo)",
	})
	if err != nil {
		log().Error("Failed to create default appmanager user", zap.Error(err))
		return
	}
	log().Info("Default user created", zap.String("username", DefaultAppManagerUser),
		zap.String("role", RoleGeneralAdmin))
}
