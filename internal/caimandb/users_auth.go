package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"golang.org/x/crypto/argon2"
	"time"
)

type User struct {
	Username           string `json:"username"`
	PassHash           string `json:"pass_hash"`
	Salt               string `json:"salt"`
	Role               string `json:"role"`
	CreatedAt          int64  `json:"created_at"`
	Locked             bool   `json:"locked"`
	LockedAt           int64  `json:"locked_at"`
	Attempts           int    `json:"attempts"`
	Status             string `json:"status,omitempty"`
	// Database es la base a la que pertenece este usuario dentro de la
	// jerarquía (ver hierarchy.go y docs/hierarchy.md). Obligatoria para
	// todo rol salvo RoleGeneralAdmin, que no queda ligado a una base.
	// No confundir con DefaultDatabase (conveniencia de sesión): esta es
	// la base fuera de la cual el usuario no tiene acceso.
	Database           string `json:"database,omitempty"`
	DefaultDatabase    string `json:"default_database,omitempty"`
	DefaultBlock       string `json:"default_block,omitempty"`
	PasswordExpireDays int    `json:"password_expire_days,omitempty"`
	Comment            string `json:"comment,omitempty"`
	Server             string `json:"server,omitempty"`
}

// CreateUserOpts agrupa las cláusulas opcionales de CREATE USER (ver
// handleCreateUser en cmd_create_show.go) y también se usa internamente
// para aprovisionar el usuario ADMIN de un CREATE SERVER.
type CreateUserOpts struct {
	Role     string
	Database string // base a la que queda ligado el usuario (ver User.Database); vacío solo para ADMIN_GENERAL
	Status   string
	DefaultDatabase    string
	DefaultBlock       string
	PasswordExpireDays int
	AccountLock        bool
	Comment            string
	Server             string
}

func hashPassword(password, saltHex string) string {
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		salt = []byte(saltHex)
	}
	hash := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return hex.EncodeToString(hash)
}

func generateSalt() string {
	salt := make([]byte, argonSaltLen)
	rand.Read(salt)
	return hex.EncodeToString(salt)
}

func verifyPassword(password, saltHex, stored string) bool {
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		salt = []byte(saltHex)
	}
	hash := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return subtle.ConstantTimeCompare([]byte(hex.EncodeToString(hash)), []byte(stored)) == 1
}

func (e *Engine) usersDB() (*badger.DB, error) {
	return e.pool.OpenUserDB()
}

func (e *Engine) CreateUser(username, password, role string) error {
	if role == "" {
		role = "readwrite"
	}
	return e.CreateUserWithOpts(username, password, CreateUserOpts{Role: role})
}

// CreateUserWithOpts crea un usuario con las cláusulas opcionales de
// CREATE USER (rol, estado, base/bloque por defecto, expiración de
// contraseña, bloqueo de cuenta, comentario y servidor). CreateUser
// sigue existiendo como atajo para el caso simple (usuario/contraseña/rol).
func (e *Engine) CreateUserWithOpts(username, password string, opts CreateUserOpts) error {
	if err := validateName(username); err != nil {
		return err
	}
	if len(password) < 6 {
		return fmt.Errorf("password must be at least 6 characters")
	}
	role := opts.Role
	if role == "" {
		role = "readwrite"
	}
	normRole, ok := normalizeRole(role)
	if !ok {
		return fmt.Errorf("invalid role: %s", role)
	}
	role = normRole

	// Aislamiento jerárquico (docs/hierarchy.md): todo usuario, salvo el
	// Admin General, pertenece a exactamente una base. El Admin General
	// no debe quedar ligado a ninguna.
	if IsGeneralAdmin(role) {
		if opts.Database != "" {
			return fmt.Errorf("%s no pertenece a una base concreta; no indique DATABASE", RoleGeneralAdmin)
		}
	} else if opts.Database == "" {
		return fmt.Errorf("el rol %s requiere DATABASE <base> (todo usuario, salvo %s, pertenece a una base)", role, RoleGeneralAdmin)
	}

	saltHex := generateSalt()
	u := User{
		Username:           username,
		PassHash:           hashPassword(password, saltHex),
		Salt:               saltHex,
		Role:               role,
		Database:           opts.Database,
		CreatedAt:          time.Now().Unix(),
		Locked:             opts.AccountLock,
		Status:             opts.Status,
		DefaultDatabase:    opts.DefaultDatabase,
		DefaultBlock:       opts.DefaultBlock,
		PasswordExpireDays: opts.PasswordExpireDays,
		Comment:            opts.Comment,
		Server:             opts.Server,
	}
	if u.Locked {
		u.LockedAt = time.Now().Unix()
	}
	raw, _ := json.Marshal(u)
	db, err := e.usersDB()
	if err != nil {
		return err
	}

	err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		return txn.Set([]byte("u/"+username), raw)
	})

	if err == nil {
		log().Info("User created", zap.String("username", username), zap.String("role", role))
		if e.auditLogger != nil {
			e.auditLogger.Log(AuditLog{
				UserID:   "system",
				Username: "system",
				Action:   "create_user",
				Status:   "success",
				Message:  fmt.Sprintf("user=%s role=%s", username, role),
			})
		}
	}
	return err
}

// GetUserByUsername busca un usuario por nombre sin verificar contraseña
// (a diferencia de AuthUserDetailed). Se usa donde hace falta conocer el
// rol/base de un usuario objetivo antes de autorizar una operación sobre
// él -- por ejemplo authorizeDropUser (hierarchy.go) antes de DROP USER.
// El bool indica si el usuario existe.
func (e *Engine) GetUserByUsername(username string) (User, bool) {
	db, err := e.usersDB()
	if err != nil {
		return User{}, false
	}
	var u User
	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("u/" + username))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error { return json.Unmarshal(v, &u) })
	})
	if err != nil {
		return User{}, false
	}
	return u, true
}

func (e *Engine) DropUser(username string) error {
	db, err := e.usersDB()
	if err != nil {
		return err
	}

	err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		return txn.Delete([]byte("u/" + username))
	})

	if err == nil {
		log().Info("User dropped", zap.String("username", username))
		if e.auditLogger != nil {
			e.auditLogger.Log(AuditLog{
				UserID:   "system",
				Username: "system",
				Action:   "drop_user",
				Status:   "success",
				Message:  fmt.Sprintf("user=%s", username),
			})
		}
	}
	return err
}

func (e *Engine) ShowUsers() ([]map[string]any, error) {
	db, err := e.usersDB()
	if err != nil {
		return nil, err
	}
	var users []map[string]any

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte("u/")); it.ValidForPrefix([]byte("u/")); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var u User
			item.Value(func(v []byte) error { return json.Unmarshal(v, &u) })
			users = append(users, map[string]any{
				"username":   u.Username,
				"role":       u.Role,
				"database":   u.Database,
				"created_at": u.CreatedAt,
				"locked":     u.Locked,
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return users, nil
}

// ShowUsersScoped es ShowUsers filtrado según la jerarquía: el Admin
// General ve todos los usuarios; cualquier otro rol solo ve los usuarios
// de su propia base (caller.Database). Pensado para SHOW USERS y para el
// endpoint HTTP equivalente, de modo que un admin no pueda enumerar
// cuentas de bases ajenas.
func (e *Engine) ShowUsersScoped(caller User) ([]map[string]any, error) {
	all, err := e.ShowUsers()
	if err != nil {
		return nil, err
	}
	if IsGeneralAdmin(caller.Role) {
		return all, nil
	}
	out := make([]map[string]any, 0, len(all))
	for _, u := range all {
		if db, _ := u["database"].(string); db == caller.Database {
			out = append(out, u)
		}
	}
	return out, nil
}

// AuthUser conserva la firma histórica (solo bool + rol) para el
// código existente que no necesita el resto de campos del usuario.
func (e *Engine) AuthUser(username, password string) (bool, string) {
	ok, u := e.AuthUserDetailed(username, password)
	if !ok {
		return false, ""
	}
	return true, u.Role
}

// AuthUserDetailed autentica y, si tiene éxito, devuelve el User
// completo (rol, base/bloque por defecto, etc.), necesario para
// enrutar peticiones HTTP a la base de datos/bloque correctos -- ver
// resolveEngine en http_query.go y resolveAdmin en http_admin.go.
func (e *Engine) AuthUserDetailed(username, password string) (bool, User) {
	db, err := e.usersDB()
	if err != nil {
		metrics.MetricAuthFailures.Inc()
		return false, User{}
	}
	var u User

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("u/" + username))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error { return json.Unmarshal(v, &u) })
	})

	if err != nil {
		metrics.MetricAuthFailures.Inc()
		return false, User{}
	}

	if u.Locked {
		if time.Now().Unix()-u.LockedAt > int64(e.config.LockoutDuration.Seconds()) {
			u.Locked = false
			u.Attempts = 0
			txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
				raw, _ := json.Marshal(u)
				return txn.Set([]byte("u/"+username), raw)
			})
		} else {
			return false, User{}
		}
	}

	if !verifyPassword(password, u.Salt, u.PassHash) {
		u.Attempts++
		if u.Attempts >= e.config.MaxLoginAttempts {
			u.Locked = true
			u.LockedAt = time.Now().Unix()
		}
		txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
			raw, _ := json.Marshal(u)
			return txn.Set([]byte("u/"+username), raw)
		})
		metrics.MetricAuthFailures.Inc()
		log().Warn("Authentication failed", zap.String("username", username))
		return false, User{}
	}

	if u.Attempts > 0 {
		u.Attempts = 0
		txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
			raw, _ := json.Marshal(u)
			return txn.Set([]byte("u/"+username), raw)
		})
	}

	log().Debug("Authentication successful", zap.String("username", username))
	return true, u
}
