// roles.go define el catálogo de roles de usuario de CaimanDB y las
// reglas asociadas (normalización de alias antiguos, validación, y la
// regla de "un solo ADMIN por servidor").
package caimandb

import "strings"

// Roles soportados por CREATE USER ... ROLE <...>
//
// La jerarquía es de dos niveles:
//
//   - RoleGeneralAdmin: control global. Ve y administra todas las bases
//     y todos los usuarios del servidor. Es el único rol que no queda
//     ligado a una base concreta (User.Database == "").
//   - RoleAdmin: dueño de UNA base (identificada en User.Database y en
//     los metadatos de esa base, ver hierarchy.go). Puede crear y
//     administrar SUBADMIN/DEVELOPER/USER dentro de su propia base,
//     pero no ve ni toca las bases de otros admins.
//   - RoleSubadmin/RoleDeveloper/RoleUser: igual que antes, pero ahora
//     también ligados a la base de su administrador (User.Database).
//
// Ver docs/hierarchy.md para el diseño completo.
const (
	RoleGeneralAdmin = "ADMIN_GENERAL" // control global: todas las bases, todos los usuarios
	RoleAdmin        = "ADMIN"         // dueño de una base propia (User.Database)
	RoleSubadmin     = "SUBADMIN"      // administración delegada dentro de la base de su ADMIN
	RoleDeveloper    = "DEVELOPER"     // lectura/escritura de datos y bloques dentro de su base
	RoleUser         = "USER"          // acceso limitado dentro de su base
)

// validRoles enumera los roles válidos para CREATE USER, en el orden en
// que deben mostrarse (SHOW ROLES).
var validRoles = []string{RoleGeneralAdmin, RoleAdmin, RoleSubadmin, RoleDeveloper, RoleUser}

// roleDescriptions documenta cada rol para SHOW ROLES / HELP.
var roleDescriptions = map[string]string{
	RoleGeneralAdmin: "Control global del servidor: todas las bases y todos los usuarios. No pertenece a ninguna base en particular.",
	RoleAdmin:        "Dueño de su propia base (DATABASE obligatorio). Administra subadmins, devs y usuarios de esa base. Aislado del resto.",
	RoleSubadmin:     "Administración delegada dentro de la base de su ADMIN: usuarios y bloques, sin poder crear otro ADMIN.",
	RoleDeveloper:    "Lectura y escritura de datos y bloques dentro de la base asignada.",
	RoleUser:         "Acceso limitado a la base/bloques asignados.",
}

// normalizeRole convierte un token de rol (en cualquier mayúscula/minúscula)
// a su forma canónica, incluyendo los alias heredados de versiones
// anteriores de CaimanDB ("admin", "readwrite", "readonly") para no romper
// instalaciones existentes.
func normalizeRole(role string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case RoleGeneralAdmin, "GENERAL_ADMIN", "SUPERADMIN", "SUPER_ADMIN", "ROOT":
		return RoleGeneralAdmin, true
	case RoleAdmin, "ADMINISTRATOR":
		return RoleAdmin, true
	case RoleSubadmin, "SUB-ADMIN", "SUB_ADMIN":
		return RoleSubadmin, true
	case RoleDeveloper, "DEV", "READWRITE":
		return RoleDeveloper, true
	case RoleUser, "READONLY":
		return RoleUser, true
	default:
		return "", false
	}
}

// IsAdminRole reporta si role (ya normalizado o no) tiene privilegios de
// administrador -- de su propia base (ADMIN) o globales (ADMIN_GENERAL).
// Úsese para gates de "esto requiere ser al menos admin"; para lógica que
// distingue el alcance (una base vs. todas), usar IsGeneralAdmin.
func IsAdminRole(role string) bool {
	r := strings.ToUpper(role)
	return r == RoleAdmin || r == RoleGeneralAdmin
}

// IsGeneralAdmin reporta si role es específicamente el Admin General
// (control global, sin base propia).
func IsGeneralAdmin(role string) bool {
	return strings.ToUpper(role) == RoleGeneralAdmin
}

// ShowRoles devuelve el catálogo de roles para el comando "SHOW ROLES".
func (e *Engine) ShowRoles() []map[string]any {
	out := make([]map[string]any, 0, len(validRoles))
	for _, r := range validRoles {
		out = append(out, map[string]any{
			"role":        r,
			"description": roleDescriptions[r],
		})
	}
	return out
}

// TODO(pendiente-motor): GRANT / REVOKE / SHOW GRANTS -- permisos
// granulares por verbo/bloque (p. ej. "GRANT SELECT, UPDATE ON
// products TO usuario") -- no existen todavía. Hoy el control de acceso
// es solo por rol (los 5 de arriba) + aislamiento de base
// (requireDBAccess/UserCanAccessDB en hierarchy.go): un usuario puede o
// no tocar TODA su base, no hay forma de darle permiso a un DEVELOPER o
// USER sobre un bloque específico ni de restringirlo a ciertos verbos
// (solo lectura de un bloque, por ejemplo).
//
// Esto requiere diseñar el modelo antes de tocar código (ya identificado
// como bloqueante en su momento):
//   - forma de almacenamiento: ¿tabla de grants por (usuario, base,
//     bloque, verbo) en DBConfig, o un documento de permisos aparte?
//   - cómo interactúa con la jerarquía de roles ya existente (¿un GRANT
//     puede ampliar a un USER más allá de lo que su rol ya permite, o
//     solo puede restringir dentro de lo que el rol ya cubre?)
//   - verbos a cubrir: al menos SELECT/INSERT/UPDATE/DELETE/INDEX;
//     decidir si CREATE BLOCK/DROP BLOCK entran en el mismo modelo o
//     quedan reservados a ADMIN/SUBADMIN como hoy
//   - puntos de enforcement: cada handleXxx en cmd_*.go llama hoy a
//     requireDBAccess; un chequeo de grants tendría que vivir en el
//     mismo lugar (o uno nuevo, requireGrant(sess, block, verb)) para no
//     dejar un comando sin cubrir, como pasó con el árbol WHERE (ver
//     docs/known-limitations.md)
//   - SHOW GRANTS necesita poder listar por usuario y por bloque
//
// Ver también el TODO de UPDATE/aggregate/VIEW/admin sobre el árbol
// WHERE en cmd_update.go: mismo patrón de "falta cubrir todos los
// comandos", pero aquí ni siquiera existe el mecanismo base todavía.
