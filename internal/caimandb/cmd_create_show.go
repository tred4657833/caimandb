package caimandb

import (
	"fmt"
	"strconv"
	"strings"
)

// stripQuotes quita un par de comillas simples o dobles que envuelvan
// por completo el token (tal como los deja parse.Tokenize, que conserva
// las comillas dentro del token). Si el token no está entre comillas, se
// devuelve tal cual -- así "MiPassword123" y MiPassword123 funcionan
// igual.
func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// parseParenList extracts a comma-separated list of names from a
// parenthesized token group starting at tokens[start] (which must be
// the "(" token -- parse.Tokenize always emits '(' and ')' as their own
// tokens, see its doc comment). Returns the trimmed, non-empty names and
// the index of the token right after the matching ")".
func parseParenList(tokens []string, start int) ([]string, int, error) {
	if start >= len(tokens) || tokens[start] != "(" {
		return nil, start, fmt.Errorf("expected '(' at position %d", start)
	}
	depth := 1
	i := start + 1
	var raw []string
	for i < len(tokens) {
		switch tokens[i] {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				goto done
			}
		}
		raw = append(raw, tokens[i])
		i++
	}
done:
	if depth != 0 {
		return nil, start, fmt.Errorf("unbalanced parentheses")
	}
	parts := strings.Split(strings.Join(raw, " "), ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(stripQuotes(strings.TrimSpace(p)))
		if p != "" {
			names = append(names, p)
		}
	}
	if len(names) == 0 {
		return nil, start, fmt.Errorf("empty list")
	}
	return names, i + 1, nil
}

func handleCreate(eng *Engine, sess *Session, tokens []string) string {
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "DB":
		// CREATE DB <name> [OWNER <admin_username>]
		//
		// Aislamiento (docs/hierarchy.md): un ADMIN solo puede crear la
		// base que ya tiene asignada como propia (sess.authDatabase) --
		// no puede provisionar bases arbitrarias -- y queda registrado
		// automáticamente como su dueño. El Admin General puede crear
		// cualquier base y, opcionalmente, asignarle dueño con OWNER.
		if len(tokens) < 3 {
			return "ERROR: CREATE DB requires database name"
		}
		db := tokens[2]
		owner := ""
		if len(tokens) >= 5 && strings.ToUpper(tokens[3]) == "OWNER" {
			owner = tokens[4]
		}

		if sess.authRole != "" && !IsGeneralAdmin(sess.authRole) {
			if strings.ToUpper(sess.authRole) != RoleAdmin {
				return "ERROR: solo " + RoleGeneralAdmin + " o " + RoleAdmin + " pueden crear bases"
			}
			if db != sess.authDatabase {
				return "ERROR: " + RoleAdmin + " solo puede crear su propia base (" + sess.authDatabase + ")"
			}
			owner = sess.authUser
		}

		if err := eng.CreateDB(db); err != nil {
			return "ERROR: " + err.Error()
		}
		if owner != "" {
			if err := eng.SetDBOwner(db, owner); err != nil {
				return "Database created: " + db + " (WARNING: could not set owner: " + err.Error() + ")"
			}
			return "Database created: " + db + " (owner: " + owner + ")"
		}
		return "Database created: " + db

	case "BLOCK":
		// CREATE BLOCK [<db>] (<block1>, <block2>, ...) creates every
		// listed block in one call; CREATE BLOCK [<db>] <block> (no
		// parens) is the original single-block form, unchanged.
		if len(tokens) >= 3 && tokens[2] == "(" {
			return createBlocks(eng, sess, sess.currentDB, tokens, 2)
		}
		if len(tokens) >= 4 && tokens[3] == "(" {
			return createBlocks(eng, sess, tokens[2], tokens, 3)
		}

		var db, block string
		if len(tokens) == 3 {
			block = tokens[2]
			db = sess.currentDB
			if db == "" {
				return "ERROR: No database in use. Use: CREATE BLOCK <db> <block> or USE <db> first"
			}
		} else if len(tokens) >= 4 {
			db = tokens[2]
			block = tokens[3]
		} else {
			return "ERROR: CREATE BLOCK requires block name"
		}
		if db == "" {
			return "ERROR: No database specified and no database in use"
		}
		if err := requireDBAccess(sess, db); err != nil {
			return "ERROR: " + err.Error()
		}
		if err := eng.CreateBlock(db, block); err != nil {
			return "ERROR: " + err.Error()
		}
		return "Block created: " + block + " in database: " + db

	case "USER":
		if len(tokens) < 3 {
			return "ERROR: CREATE USER requires a username"
		}
		return handleCreateUser(eng, sess, tokens[2], tokens[3:])

	case "SERVER":
		if len(tokens) < 3 {
			return `ERROR: CREATE SERVER requires a server name -- syntax: CREATE SERVER <name> ADMIN [<username>] IDENTIFIED BY "<password>"`
		}
		return handleCreateServer(eng, tokens[2], tokens[3:])

	default:
		return "ERROR: unknown CREATE object: " + subCmd
	}
}

// createBlocks implements the CREATE BLOCK (a, b, c) form: db is the
// target database (already resolved by the caller, from an explicit
// token or the session's current db), tokens[parenStart] is "(". Each
// block is created independently -- there is no multi-block transaction
// underneath CreateBlock -- so a failure partway through still leaves
// every block created before it in place; the result reports exactly
// which succeeded and which didn't rather than pretending it's atomic.
func createBlocks(eng *Engine, sess *Session, db string, tokens []string, parenStart int) string {
	if db == "" {
		return "ERROR: No database specified and no database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}
	names, next, err := parseParenList(tokens, parenStart)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if next != len(tokens) {
		return "ERROR: unexpected tokens after block list"
	}

	var created, failed []string
	for _, block := range names {
		if err := eng.CreateBlock(db, block); err != nil {
			failed = append(failed, block+" ("+err.Error()+")")
		} else {
			created = append(created, block)
		}
	}

	msg := ""
	if len(created) > 0 {
		msg += "Blocks created: " + strings.Join(created, ", ") + " in database: " + db
	}
	if len(failed) > 0 {
		if msg != "" {
			msg += "; "
		}
		msg += "ERROR creating: " + strings.Join(failed, ", ")
	}
	return msg
}

// handleCreateUser implementa la sintaxis completa de CREATE USER, tanto
// "en línea" como "en cascada" (el operador puede escribirla en varias
// líneas: ver el buffer de sentencias en app.go, que las junta en una
// sola antes de llegar aquí). Todas las cláusulas después del nombre de
// usuario son opcionales salvo IDENTIFIED BY, y pueden ir en cualquier
// orden:
//
//	CREATE USER <username> IDENTIFIED BY "<password>"
//	    [ROLE <ADMIN_GENERAL|ADMIN|SUBADMIN|DEVELOPER|USER>]
//	    [DATABASE <database>]
//	    [STATUS <ACTIVE|DISABLED>]
//	    [DEFAULT DATABASE <database>]
//	    [DEFAULT BLOCK <block>]
//	    [PASSWORD EXPIRE <NEVER|DAYS n>]
//	    [ACCOUNT LOCK <ON|OFF>]
//	    [COMMENT "<description>"]
//
// También se acepta la forma corta heredada: CREATE USER <user> <pass>
// [ROLE <role>].
//
// DATABASE <base> es la cláusula que liga el usuario a la jerarquía
// (docs/hierarchy.md): obligatoria para todo rol salvo ADMIN_GENERAL.
// Si se omite y quien ejecuta el comando es un ADMIN/SUBADMIN
// autenticado, se hereda automáticamente su propia base (ver
// authorizeCreateUser).
func handleCreateUser(eng *Engine, sess *Session, username string, args []string) string {
	var password string
	havePassword := false
	opts := CreateUserOpts{}

	// Forma corta heredada: CREATE USER <user> <pass> [ROLE <role>]
	if len(args) > 0 && strings.ToUpper(args[0]) != "IDENTIFIED" {
		password = stripQuotes(args[0])
		havePassword = true
		args = args[1:]
		if len(args) >= 2 && strings.ToUpper(args[0]) == "ROLE" {
			opts.Role = args[1]
			args = args[2:]
		}
	}

	for i := 0; i < len(args); {
		tok := strings.ToUpper(args[i])
		switch tok {
		case "IDENTIFIED":
			if i+2 >= len(args) || strings.ToUpper(args[i+1]) != "BY" {
				return `ERROR: expected IDENTIFIED BY "<password>"`
			}
			password = stripQuotes(args[i+2])
			havePassword = true
			i += 3

		case "ROLE":
			if i+1 >= len(args) {
				return "ERROR: ROLE requires a value (ADMIN, SUBADMIN, DEVELOPER or USER)"
			}
			opts.Role = args[i+1]
			i += 2

		case "STATUS":
			if i+1 >= len(args) {
				return "ERROR: STATUS requires ACTIVE or DISABLED"
			}
			opts.Status = args[i+1]
			i += 2

		case "DEFAULT":
			if i+2 >= len(args) {
				return "ERROR: DEFAULT requires DATABASE <name> or BLOCK <name>"
			}
			switch strings.ToUpper(args[i+1]) {
			case "DATABASE":
				opts.DefaultDatabase = args[i+2]
			case "BLOCK":
				opts.DefaultBlock = args[i+2]
			default:
				return "ERROR: DEFAULT must be followed by DATABASE or BLOCK"
			}
			i += 3

		case "PASSWORD":
			if i+1 >= len(args) || strings.ToUpper(args[i+1]) != "EXPIRE" {
				return "ERROR: expected PASSWORD EXPIRE NEVER, or PASSWORD EXPIRE DAYS <n>"
			}
			if i+2 >= len(args) {
				return "ERROR: PASSWORD EXPIRE requires NEVER or DAYS <n>"
			}
			switch strings.ToUpper(args[i+2]) {
			case "NEVER":
				opts.PasswordExpireDays = 0
				i += 3
			case "DAYS":
				if i+3 >= len(args) {
					return "ERROR: PASSWORD EXPIRE DAYS requires a number of days"
				}
				n, err := strconv.Atoi(args[i+3])
				if err != nil || n < 0 {
					return "ERROR: invalid PASSWORD EXPIRE DAYS value: " + args[i+3]
				}
				opts.PasswordExpireDays = n
				i += 4
			default:
				return "ERROR: PASSWORD EXPIRE must be NEVER or DAYS <n>"
			}

		case "ACCOUNT":
			if i+2 >= len(args) || strings.ToUpper(args[i+1]) != "LOCK" {
				return "ERROR: expected ACCOUNT LOCK ON or ACCOUNT LOCK OFF"
			}
			switch strings.ToUpper(args[i+2]) {
			case "ON":
				opts.AccountLock = true
			case "OFF":
				opts.AccountLock = false
			default:
				return "ERROR: ACCOUNT LOCK must be ON or OFF"
			}
			i += 3

		case "DATABASE":
			if i+1 >= len(args) {
				return "ERROR: DATABASE requires a database name"
			}
			opts.Database = args[i+1]
			i += 2

		case "COMMENT":
			if i+1 >= len(args) {
				return "ERROR: COMMENT requires a value"
			}
			opts.Comment = stripQuotes(args[i+1])
			i += 2

		default:
			return "ERROR: unexpected token in CREATE USER near '" + args[i] + "'"
		}
	}

	if !havePassword {
		return `ERROR: CREATE USER requires IDENTIFIED BY "<password>"`
	}
	if opts.Role == "" {
		opts.Role = RoleUser
	}

	if err := authorizeCreateUser(sess, &opts); err != nil {
		return "ERROR: " + err.Error()
	}

	if err := eng.CreateUserWithOpts(username, password, opts); err != nil {
		return "ERROR: " + err.Error()
	}
	role, _ := normalizeRole(opts.Role)
	if opts.Database != "" {
		return fmt.Sprintf("User created: %s (role: %s, database: %s)", username, role, opts.Database)
	}
	return fmt.Sprintf("User created: %s (role: %s)", username, role)
}

// handleCreateServer implementa:
//
//	CREATE SERVER <name> ADMIN [<username>] IDENTIFIED BY "<password>"
//
// Crea un servidor nuevo (su propio directorio de datos, aislado de los
// demás, y su propio usuario ADMIN) que atiende por el mismo puerto que
// el resto -- lo único que lo distingue son las credenciales de su
// admin. Si <username> se omite, se usa "admin".
func handleCreateServer(eng *Engine, name string, args []string) string {
	if len(args) < 1 || strings.ToUpper(args[0]) != "ADMIN" {
		return `ERROR: syntax: CREATE SERVER <name> ADMIN [<username>] IDENTIFIED BY "<password>"`
	}
	rest := args[1:]

	username := ""
	if len(rest) > 0 && strings.ToUpper(rest[0]) != "IDENTIFIED" {
		username = rest[0]
		rest = rest[1:]
	}

	if len(rest) < 3 || strings.ToUpper(rest[0]) != "IDENTIFIED" || strings.ToUpper(rest[1]) != "BY" {
		return `ERROR: syntax: CREATE SERVER <name> ADMIN [<username>] IDENTIFIED BY "<password>"`
	}
	password := stripQuotes(rest[2])

	if eng.serverMgr == nil {
		return "ERROR: multi-server support is not available on this instance"
	}
	if err := eng.serverMgr.CreateServer(name, username, password); err != nil {
		return "ERROR: " + err.Error()
	}
	if username == "" {
		username = "admin"
	}
	return fmt.Sprintf("Server created: %s (admin: %s) -- same port, reachable with its own credentials", name, username)
}

func handleShow(eng *Engine, sess *Session, tokens []string) string {
	subCmd := strings.ToUpper(tokens[1])
	switch subCmd {
	case "GRANTS":
		return handleShowGrants(eng, sess, tokens)

	case "DBS":
		return handleShowDBs(eng, sess, tokens[2:])

	case "BLOCKS":
		return handleShowBlocks(eng, sess, tokens[2:])

	case "USERS":
		// Aislamiento: fuera de la consola local sin autenticar y del
		// Admin General, cada quien ve solo los usuarios de su propia
		// base (ver ShowUsersScoped / hierarchy.go).
		caller := User{Role: sess.authRole, Database: sess.authDatabase}
		var users []map[string]any
		var err error
		if sess.authRole == "" {
			users, err = eng.ShowUsers()
		} else {
			users, err = eng.ShowUsersScoped(caller)
		}
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if len(users) == 0 {
			return "No users found"
		}
		var sb strings.Builder
		sb.WriteString("Users:\n")
		for _, u := range users {
			sb.WriteString(fmt.Sprintf("  %s (role: %s, database: %v, status: %v, locked: %v)\n",
				u["username"], u["role"], u["database"], u["status"], u["locked"]))
		}
		return sb.String()

	case "ROLES":
		roles := eng.ShowRoles()
		var sb strings.Builder
		sb.WriteString("Roles:\n")
		for _, r := range roles {
			sb.WriteString(fmt.Sprintf("  %-10s %s\n", r["role"], r["description"]))
		}
		return sb.String()

	case "SERVERS":
		if eng.serverMgr == nil {
			return "No additional servers (multi-server not available on this instance)"
		}
		names := eng.serverMgr.ListServers()
		if len(names) == 0 {
			return "No additional servers"
		}
		var sb strings.Builder
		sb.WriteString("Servers:\n")
		for _, n := range names {
			sb.WriteString("  " + n + "\n")
		}
		return sb.String()

	case "CACHE":
		stats := eng.l1Cache.Stats()
		return "L1 Cache:\n" + jsonOut(stats)

	case "INDEXES":
		if len(tokens) >= 4 && strings.ToUpper(tokens[2]) == "ON" {
			return handleShowIndexesOnBlock(eng, sess, tokens[3])
		}
		var db string
		if len(tokens) > 2 {
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
		cfg, ok := eng.intelEngine.GetConfig(db)
		if !ok {
			return "ERROR: Database not found: " + db
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Indexes in %s:\n", db))
		if len(cfg.Indexes) == 0 {
			sb.WriteString("  No indexes defined\n")
		} else {
			for _, idx := range cfg.Indexes {
				sb.WriteString(fmt.Sprintf("  %s\n", idx))
			}
		}
		return sb.String()

	case "SHARDS":
		sm := eng.shardMgr
		sm.mu.RLock()
		defer sm.mu.RUnlock()
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Shard Distribution:\n"))
		sb.WriteString(fmt.Sprintf("%-12s %-10s %-12s %-10s %-20s\n", "SHARD", "DOCS", "SIZE", "PRIMARY", "NODES"))
		sb.WriteString(strings.Repeat("-", 70) + "\n")
		for id, shard := range sm.shards {
			primaryShort := shard.Primary
			if len(primaryShort) > 10 {
				primaryShort = primaryShort[:8] + "..."
			}
			sb.WriteString(fmt.Sprintf("%-12s %-10d %-12s %-10s %-20v\n",
				id, shard.DocCount, fmtBytes(shard.SizeBytes), primaryShort, shard.Nodes))
		}
		sb.WriteString(strings.Repeat("-", 70) + "\n")
		sb.WriteString(fmt.Sprintf("Total shards: %d, Total docs: %d\n", sm.shardCount, sm.totalDocs()))
		return sb.String()

	case "AUTORELATIONS", "AUTO-RELATIONS":
		return handleShowAutoRelations(eng, sess, tokens[2:])

	case "VIEWS":
		viewsMu.RLock()
		defer viewsMu.RUnlock()
		if len(views) == 0 {
			return "No views defined"
		}
		var sb strings.Builder
		sb.WriteString("Views:\n")
		for name, v := range views {
			sb.WriteString(fmt.Sprintf("  %s (db: %s, block: %s)\n", name, v.DB, v.Block))
		}
		return sb.String()

	default:
		return "ERROR: unknown SHOW object: " + subCmd
	}
}
