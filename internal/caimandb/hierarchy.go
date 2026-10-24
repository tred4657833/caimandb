// hierarchy.go implementa la administración jerárquica y aislada de
// CaimanDB descrita en docs/hierarchy.md:
//
//	ADMIN_GENERAL
//	│
//	├── BASE A → ADMIN A ── SUBADMIN / DEV / USUARIOS
//	└── BASE B → ADMIN B ── SUBADMIN / DEV / USUARIOS
//
// Regla: Identidad → Rol → Base/Scope → Permisos → Acceso.
//
// Este archivo reúne los puntos que antes no existían en absoluto en el
// motor: quién puede crear qué rol y en qué base (authorizeCreateUser),
// quién es el dueño (owner) de cada base (Set/GetDBOwner, guardado en los
// metadatos de la base -- ver storage.DirectoryManager.Save/LoadDBMeta),
// y si una sesión autenticada puede operar sobre una base dada
// (UserCanAccessDB / enforceSessionIsolation).
package caimandb

import (
	"fmt"
	"strings"
)

// dbMetaOwnerKey es la clave dentro de los metadatos de la base
// (db.json, ver storage.DirectoryManager.SaveDBMeta) donde se guarda el
// username del ADMIN dueño de esa base.
const dbMetaOwnerKey = "admin"

// UserCanAccessDB reporta si u puede operar sobre la base db. El Admin
// General accede a cualquier base; el resto solo a la suya propia
// (u.Database). Esta es la regla de aislamiento central: ningún admin
// puede acceder a otra base salvo el Admin General.
func UserCanAccessDB(u User, db string) bool {
	if IsGeneralAdmin(u.Role) {
		return true
	}
	return db != "" && u.Database != "" && u.Database == db
}

// SetDBOwner registra en los metadatos de db qué ADMIN la administra,
// preservando cualquier otro metadato ya guardado (SaveDBMeta reemplaza
// el archivo completo, así que primero se relee lo existente).
func (e *Engine) SetDBOwner(db, adminUsername string) error {
	meta, err := e.dirMgr.LoadDBMeta(db)
	if err != nil || meta == nil {
		meta = map[string]any{}
	}
	meta[dbMetaOwnerKey] = adminUsername
	return e.dirMgr.SaveDBMeta(db, meta)
}

// GetDBOwner devuelve el username del ADMIN dueño de db, o "" si la base
// no tiene metadatos o aún no tiene dueño asignado (por ejemplo, una
// base creada por el Admin General sin cláusula OWNER).
func (e *Engine) GetDBOwner(db string) string {
	meta, err := e.dirMgr.LoadDBMeta(db)
	if err != nil || meta == nil {
		return ""
	}
	owner, _ := meta[dbMetaOwnerKey].(string)
	return owner
}

// authorizeCreateUser aplica la regla "Identidad → Rol → Base/Scope →
// Permisos → Acceso" al comando CREATE USER. sess es la sesión que
// ejecuta el comando; si sess.authRole está vacío se trata de la
// consola local (operador ya confiado con la máquina/proceso: arranque,
// mantenimiento), que conserva el comportamiento histórico sin
// restricciones adicionales -- las mismas reglas de CreateUserWithOpts
// (rol válido, DATABASE obligatorio salvo ADMIN_GENERAL) siguen
// aplicando de todas formas.
//
// Completa opts.Database con la base de la sesión cuando el llamador no
// la indicó explícitamente, así "CREATE USER x ... ROLE DEVELOPER" (sin
// DATABASE) hereda la base del ADMIN/SUBADMIN que lo ejecuta.
func authorizeCreateUser(sess *Session, opts *CreateUserOpts) error {
	if sess == nil || sess.authRole == "" {
		return nil
	}

	targetRole, ok := normalizeRole(opts.Role)
	if !ok {
		return fmt.Errorf("invalid role: %s", opts.Role)
	}

	// Se normaliza una sola vez aquí: sess.authRole llega ya canónico
	// (mayúsculas) desde el login real (AuthUserDetailed / JWT), pero
	// comparar en crudo sigue siendo frágil -- ver requireDBAccess más
	// abajo para la misma razón. IsGeneralAdmin ya normaliza por su
	// cuenta; el resto de este switch debe hacerlo también para no
	// denegar (o, peor, conceder) por una diferencia de mayúsculas.
	callerRole := strings.ToUpper(sess.authRole)

	switch {
	case IsGeneralAdmin(sess.authRole):
		// El Admin General crea cualquier rol en cualquier base.
		return nil

	case callerRole == RoleAdmin || callerRole == RoleSubadmin:
		// Un ADMIN/SUBADMIN administra únicamente su propia base y solo
		// puede dar de alta SUBADMIN, DEVELOPER o USER -- nunca otro
		// ADMIN ni un ADMIN_GENERAL (eso rompería el aislamiento).
		if targetRole == RoleGeneralAdmin || targetRole == RoleAdmin {
			return fmt.Errorf("solo %s puede crear usuarios con rol %s", RoleGeneralAdmin, targetRole)
		}
		if sess.authDatabase == "" {
			return fmt.Errorf("la sesión no tiene una base asignada")
		}
		if opts.Database == "" {
			opts.Database = sess.authDatabase
		}
		if opts.Database != sess.authDatabase {
			return fmt.Errorf("no puede crear usuarios fuera de su propia base (%s)", sess.authDatabase)
		}
		return nil

	default:
		return fmt.Errorf("el rol %s no tiene permiso para crear usuarios", sess.authRole)
	}
}

// enforceSessionIsolation es el punto central de aislamiento: se llama
// al inicio de parseAndExec para toda sesión autenticada (sess.authRole
// != "", típicamente peticiones HTTP) que no sea Admin General. Impide
// que la sesión opere con sess.currentDB apuntando a una base ajena, sin
// importar cómo haya llegado a valer eso (USE/CD dentro de la sesión, o
// -- el camino más común -- el parámetro ?db= / "db" del endpoint HTTP,
// que fija currentDB directamente sin pasar por USE).
//
// Nota de alcance: esto cierra el acceso vía sess.currentDB. Comandos
// que reciben el nombre de la base como argumento explícito en vez de
// usar currentDB (p.ej. ANALYZE DB <otra>, BACKUP <otra> ...) no quedan
// cubiertos todavía por este gate central -- ver docs/hierarchy.md,
// sección "Alcance actual", para el plan de cerrarlo caso por caso.
func enforceSessionIsolation(eng *Engine, sess *Session) error {
	if sess == nil || sess.authRole == "" || IsGeneralAdmin(sess.authRole) {
		return nil
	}
	if sess.currentDB == "" {
		return nil
	}
	if sess.authDatabase == "" {
		return fmt.Errorf("la sesión no tiene una base asignada")
	}
	if sess.currentDB != sess.authDatabase {
		// A granular GRANT (acl.go) to a specific block in currentDB is
		// enough to keep operating there, even though the role alone
		// only covers authDatabase -- this is the same exception the
		// USE/CD gate in dsl_parser.go already makes, applied here too
		// since this is the central per-command re-check (see the
		// scope note above: currentDB can be set other ways besides
		// USE, e.g. the HTTP endpoint's ?db= param).
		if eng != nil && eng.hasAnyGrantInDB(sess.authUser, sess.currentDB) {
			return nil
		}
		return fmt.Errorf("acceso denegado: %s no administra la base '%s'", sess.authRole, sess.currentDB)
	}
	return nil
}

// requireDBAccess es el gate de aislamiento central para todo comando
// que reciba el nombre de una base como argumento explícito -- en vez
// de depender de sess.currentDB, ya cubierto por enforceSessionIsolation
// (ver la nota de alcance de esa función y docs/hierarchy.md, sección
// "Alcance actual y siguiente fase"). Aplica exactamente la misma regla
// que UserCanAccessDB / enforceSessionIsolation: el Admin General (o la
// consola local sin autenticar, sess.authRole == "") pasa siempre;
// cualquier otra sesión autenticada solo pasa si db es, literalmente,
// su propia base.
//
// Cierra en falso (fail closed) por construcción, sin caso especial
// para la duda: UserCanAccessDB ya devuelve false si db es vacío o si
// sess.authDatabase es vacío (sesión sin base asignada), así que un
// nombre de base ambiguo o una sesión mal formada deniegan en vez de
// colar la operación. No existe una rama que "continúe" ante la duda.
//
// Se debe llamar con el nombre de base ya resuelto (después de aplicar
// el valor por defecto de sess.currentDB cuando el comando lo admite),
// justo antes de tocar el motor -- ver los ~15-20 puntos de inserción
// listados en docs/hierarchy.md (ANALYZE/OPTIMIZE/STATS/SIZE DB,
// BACKUP/RESTORE/COMPACT, SHOW BLOCKS/INDEXES, CREATE/DROP/RENAME
// BLOCK, DROP DB, resolveDBBlock, y los endpoints HTTP equivalentes en
// http_admin.go).
func requireDBAccess(sess *Session, db string) error {
	if sess == nil || sess.authRole == "" || IsGeneralAdmin(sess.authRole) {
		return nil
	}
	if !UserCanAccessDB(User{Role: sess.authRole, Database: sess.authDatabase}, db) {
		return fmt.Errorf("acceso denegado: %s no administra la base '%s'", sess.authRole, db)
	}
	return nil
}

// authorizeDropUser aplica la misma regla de aislamiento que
// authorizeCreateUser, pero para DROP USER: hoy cualquier rol con
// privilegio de administrador podía borrar cualquier usuario por
// username, sin importar su base (ver "Otros pendientes razonables" en
// docs/hierarchy.md). target es el User a borrar (ya cargado por el
// llamador); sess es quien ejecuta el comando.
//
// La consola local sin autenticar (sess.authRole == "") conserva el
// comportamiento histórico. El Admin General puede borrar a cualquiera.
// Un ADMIN/SUBADMIN solo puede borrar SUBADMIN/DEVELOPER/USER dentro de
// su propia base -- nunca a otro ADMIN, nunca al ADMIN_GENERAL, y nunca
// a nadie fuera de su base, ni siquiera por error/duda: si target no
// tiene rol reconocible o no tiene base asignada, se deniega.
func authorizeDropUser(sess *Session, target User) error {
	if sess == nil || sess.authRole == "" {
		return nil
	}
	if IsGeneralAdmin(sess.authRole) {
		return nil
	}
	callerRole := strings.ToUpper(sess.authRole)
	if callerRole != RoleAdmin && callerRole != RoleSubadmin {
		return fmt.Errorf("el rol %s no tiene permiso para borrar usuarios", sess.authRole)
	}
	targetRole, ok := normalizeRole(target.Role)
	if !ok || targetRole == RoleGeneralAdmin || targetRole == RoleAdmin {
		return fmt.Errorf("solo %s puede borrar usuarios con rol %s", RoleGeneralAdmin, target.Role)
	}
	if sess.authDatabase == "" || target.Database != sess.authDatabase {
		return fmt.Errorf("no puede borrar usuarios fuera de su propia base (%s)", sess.authDatabase)
	}
	return nil
}
