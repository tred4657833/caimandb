// hierarchy_isolation_test.go cubre el aislamiento entre bases descrito
// en docs/hierarchy.md: que UserCanAccessDB/requireDBAccess cierren en
// falso (DENY) ante cualquier duda, y que cada punto de entrada que
// recibe el nombre de una base como argumento explícito -- no solo los
// que dependen de Session.currentDB, ya cubiertos por
// enforceSessionIsolation -- rechace el acceso cuando la sesión
// autenticada no es dueña de esa base.
package caimandb

import "testing"

// --- Pruebas unitarias del gate central ------------------------------

// TestUserCanAccessDB_FailsClosed cubre los casos ambiguos: base vacía,
// sesión sin base asignada, o bases que simplemente no coinciden. Ninguno
// de ellos debe conceder acceso -- "en caso de duda, DENY", no "seguir
// adelante".
func TestUserCanAccessDB_FailsClosed(t *testing.T) {
	cases := []struct {
		name string
		u    User
		db   string
		want bool
	}{
		{"general admin accede a cualquier base", User{Role: RoleGeneralAdmin}, "otra_base", true},
		{"general admin accede incluso con db vacío", User{Role: RoleGeneralAdmin}, "", true},
		{"admin accede a su propia base", User{Role: RoleAdmin, Database: "ventas"}, "ventas", true},
		{"admin NO accede a otra base", User{Role: RoleAdmin, Database: "ventas"}, "marketing", false},
		{"db vacío nunca concede acceso, ni con base propia vacía", User{Role: RoleAdmin, Database: ""}, "", false},
		{"sesión sin base asignada nunca concede acceso", User{Role: RoleAdmin, Database: ""}, "ventas", false},
		{"db vacío no concede acceso aunque la sesión tenga base", User{Role: RoleAdmin, Database: "ventas"}, "", false},
		{"subadmin NO accede a otra base", User{Role: RoleSubadmin, Database: "ventas"}, "marketing", false},
		{"developer NO accede a otra base", User{Role: RoleDeveloper, Database: "ventas"}, "marketing", false},
		{"user NO accede a otra base", User{Role: RoleUser, Database: "ventas"}, "marketing", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := UserCanAccessDB(c.u, c.db); got != c.want {
				t.Errorf("UserCanAccessDB(%+v, %q) = %v, want %v", c.u, c.db, got, c.want)
			}
		})
	}
}

// TestRequireDBAccess_FailsClosed ejercita requireDBAccess directamente
// (el gate que ahora usan ANALYZE/OPTIMIZE/BACKUP/RESTORE/COMPACT/SHOW
// BLOCKS/SHOW INDEXES/CREATE BLOCK/DROP DB/RENAME/STATS/SIZE/INFO/
// DESCRIBE/FLEX REINDEX/BEGIN, y el equivalente HTTP en
// http_admin.go/http_query.go), incluyendo el caso de una sesión
// malformada (authRole no vacío pero sin authDatabase).
func TestRequireDBAccess_FailsClosed(t *testing.T) {
	// Consola local sin autenticar: comportamiento histórico, siempre
	// permite (no es una sesión de red).
	if err := requireDBAccess(nil, "cualquier_base"); err != nil {
		t.Errorf("sess nil (consola local) debería permitir, got error: %v", err)
	}
	if err := requireDBAccess(&Session{}, "cualquier_base"); err != nil {
		t.Errorf("sess sin authRole (consola local) debería permitir, got error: %v", err)
	}

	// Admin General: siempre permite.
	if err := requireDBAccess(&Session{authRole: RoleGeneralAdmin}, "cualquier_base"); err != nil {
		t.Errorf("ADMIN_GENERAL debería permitir, got error: %v", err)
	}

	// ADMIN sobre su propia base: permite.
	adminSelf := &Session{authRole: RoleAdmin, authDatabase: "ventas"}
	if err := requireDBAccess(adminSelf, "ventas"); err != nil {
		t.Errorf("ADMIN sobre su propia base debería permitir, got error: %v", err)
	}

	// ADMIN sobre otra base: deniega.
	if err := requireDBAccess(adminSelf, "marketing"); err == nil {
		t.Errorf("ADMIN sobre otra base debería denegar, got nil error")
	}

	// Sesión autenticada sin base asignada (dato inconsistente / login
	// legado): debe denegar, no "continuar" con la operación.
	broken := &Session{authRole: RoleAdmin, authDatabase: ""}
	if err := requireDBAccess(broken, "ventas"); err == nil {
		t.Errorf("sesión sin authDatabase debería denegar, got nil error")
	}

	// db vacío con sesión no-general-admin: debe denegar (no hay “base
	// ambigua que se permite igual”).
	if err := requireDBAccess(adminSelf, ""); err == nil {
		t.Errorf("db vacío debería denegar para una sesión no-ADMIN_GENERAL, got nil error")
	}

	// Case-insensitividad del rol (ver el bug de comparación en crudo
	// corregido en hierarchy.go/cmd_create_show.go/http_admin.go): un rol
	// en minúsculas debe seguir evaluándose como ADMIN, no colar por
	// accidente ni denegar por accidente.
	lower := &Session{authRole: "admin", authDatabase: "ventas"}
	if err := requireDBAccess(lower, "ventas"); err != nil {
		t.Errorf("rol en minúsculas sobre su propia base debería permitir, got error: %v", err)
	}
	if err := requireDBAccess(lower, "marketing"); err == nil {
		t.Errorf("rol en minúsculas sobre otra base debería denegar, got nil error")
	}
}

// --- Aislamiento cruzado a través de la superficie DSL ----------------

// setupTwoTenants crea dos bases, cada una con su propio ADMIN dueño
// (ver Engine.SetDBOwner), y devuelve las sesiones autenticadas
// correspondientes -- tal como las produciría un login real
// (AuthUserDetailed), no una sesión de prueba a medio construir.
func setupTwoTenants(t *testing.T) (eng *Engine, dbA string, sessA *Session, dbB string, sessB *Session) {
	t.Helper()
	eng = newConcurrencyTestEngine(t)

	dbA, dbB = "tenant_a_db", "tenant_b_db"

	general := &Session{authRole: RoleGeneralAdmin}
	for _, db := range []string{dbA, dbB} {
		if out := parseAndExec(eng, general, "CREATE DB "+db); wantsError(out) {
			t.Fatalf("CREATE DB %s failed: %s", db, out)
		}
	}

	sessA = &Session{authUser: "admin_a", authRole: RoleAdmin, authDatabase: dbA}
	sessB = &Session{authUser: "admin_b", authRole: RoleAdmin, authDatabase: dbB}

	if out := parseAndExec(eng, sessA, "CREATE BLOCK "+dbA+" productos"); wantsError(out) {
		t.Fatalf("CREATE BLOCK in %s failed: %s", dbA, out)
	}
	return eng, dbA, sessA, dbB, sessB
}

// TestIsolation_CrossTenantCommandsDenied recorre, uno por uno, los
// puntos de entrada que docs/hierarchy.md marcaba como pendientes de
// cerrar (ANALYZE/OPTIMIZE/BACKUP/RESTORE/COMPACT/SHOW BLOCKS/SHOW
// INDEXES/CREATE BLOCK/DROP DB/RENAME DB/STATS/SIZE/INFO/DESCRIBE/FLEX
// REINDEX/BEGIN/SHARD SCALE) y confirma que el ADMIN de un tenant no
// puede ejecutarlos contra la base del otro tenant.
func TestIsolation_CrossTenantCommandsDenied(t *testing.T) {
	eng, _, sessA, dbB, _ := setupTwoTenants(t)

	commands := []string{
		"ANALYZE DB " + dbB,
		"OPTIMIZE DB " + dbB,
		"BACKUP " + dbB + " /tmp/whatever.bak",
		"RESTORE " + dbB + " /tmp/whatever.bak",
		"COMPACT " + dbB,
		"SHOW BLOCKS " + dbB,
		"SHOW INDEXES " + dbB,
		"CREATE BLOCK " + dbB + " intruso",
		"CREATE BLOCK " + dbB + " (a, b, c)",
		"DROP DB " + dbB,
		"RENAME DB " + dbB + " TO robada",
		"STATS DB " + dbB,
		"SIZE DB " + dbB,
		"INFO DB " + dbB,
		"DESCRIBE DB " + dbB,
		"FLEX REINDEX " + dbB + " nada",
		"BEGIN " + dbB,
		"SHARD SCALE " + dbB + " 4",
	}

	for _, cmd := range commands {
		t.Run(cmd, func(t *testing.T) {
			out := parseAndExec(eng, sessA, cmd)
			if !wantsError(out) {
				t.Fatalf("expected DENY for cross-tenant command %q, got: %s", cmd, out)
			}
		})
	}
}

// TestIsolation_SameTenantCommandsAllowed es el control positivo: los
// mismos comandos, sobre la propia base del admin, no deben quedar
// bloqueados por los nuevos checks (para no convertir el cierre de
// aislamiento en una regresión de falsos DENY).
func TestIsolation_SameTenantCommandsAllowed(t *testing.T) {
	eng, dbA, sessA, _, _ := setupTwoTenants(t)

	commands := []string{
		"ANALYZE DB " + dbA,
		"OPTIMIZE DB " + dbA,
		"COMPACT " + dbA,
		"SHOW BLOCKS " + dbA,
		"CREATE BLOCK " + dbA + " otro_block",
		"STATS DB " + dbA,
		"SIZE DB " + dbA,
		"INFO DB " + dbA,
		"DESCRIBE DB " + dbA,
	}

	for _, cmd := range commands {
		t.Run(cmd, func(t *testing.T) {
			out := parseAndExec(eng, sessA, cmd)
			if wantsError(out) {
				t.Fatalf("expected ALLOW for same-tenant command %q, got: %s", cmd, out)
			}
		})
	}
}

// TestIsolation_GeneralAdminBypassesAllGates confirma que el Admin
// General puede seguir operando sobre cualquier base a través de los
// mismos comandos que ahora quedan cerrados para el resto de roles.
func TestIsolation_GeneralAdminBypassesAllGates(t *testing.T) {
	eng, _, _, dbB, _ := setupTwoTenants(t)
	general := &Session{authRole: RoleGeneralAdmin}

	commands := []string{
		"ANALYZE DB " + dbB,
		"STATS DB " + dbB,
		"SIZE DB " + dbB,
		"SHOW BLOCKS " + dbB,
	}
	for _, cmd := range commands {
		t.Run(cmd, func(t *testing.T) {
			out := parseAndExec(eng, general, cmd)
			if wantsError(out) {
				t.Fatalf("ADMIN_GENERAL should bypass isolation for %q, got: %s", cmd, out)
			}
		})
	}
}

// TestIsolation_DropUserScoped cubre el pendiente de docs/hierarchy.md:
// un ADMIN no debe poder borrar usuarios de otra base, ni a otro ADMIN,
// ni al ADMIN_GENERAL -- solo SUBADMIN/DEVELOPER/USER dentro de su
// propia base.
func TestIsolation_DropUserScoped(t *testing.T) {
	eng, dbA, sessA, dbB, sessB := setupTwoTenants(t)

	// El ADMIN_GENERAL da de alta a admin_b como dueño real de dbB (como
	// lo haría un despliegue real), para que DROP USER admin_b ejercite
	// el caso "otro ADMIN", no solo "usuario inexistente".
	general := &Session{authRole: RoleGeneralAdmin}
	if out := parseAndExec(eng, general, `CREATE USER admin_b IDENTIFIED BY "secretAdminB" ROLE ADMIN DATABASE `+dbB); wantsError(out) {
		t.Fatalf("CREATE USER admin_b failed: %s", out)
	}

	// admin_a crea un usuario dentro de su propia base: debe poder
	// borrarlo.
	if out := parseAndExec(eng, sessA, `CREATE USER dev_a IDENTIFIED BY "secret1" ROLE DEVELOPER`); wantsError(out) {
		t.Fatalf("CREATE USER dev_a failed: %s", out)
	}
	if out := parseAndExec(eng, sessA, "DROP USER dev_a"); wantsError(out) {
		t.Fatalf("expected ALLOW dropping own-tenant user, got: %s", out)
	}

	// admin_b crea un usuario dentro de SU base; admin_a no debe poder
	// borrarlo.
	if out := parseAndExec(eng, sessB, `CREATE USER dev_b IDENTIFIED BY "secret2" ROLE DEVELOPER`); wantsError(out) {
		t.Fatalf("CREATE USER dev_b failed: %s", out)
	}
	if out := parseAndExec(eng, sessA, "DROP USER dev_b"); !wantsError(out) {
		t.Fatalf("expected DENY dropping cross-tenant user, got: %s", out)
	}

	// admin_a no debe poder borrar a admin_b (otro ADMIN).
	if out := parseAndExec(eng, sessA, "DROP USER admin_b"); !wantsError(out) {
		t.Fatalf("expected DENY dropping another ADMIN, got: %s", out)
	}

	_ = dbA
	_ = dbB
}
