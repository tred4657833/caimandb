// acl.go implements granular, per-resource access grants on top of
// (not instead of) the fixed role hierarchy in roles.go/hierarchy.go.
// The role hierarchy still governs coarse DB-level access
// (UserCanAccessDB, checked at USE/CD time in dsl_parser.go); this
// file adds a second, independent layer that can grant a specific
// user read and/or write access to one specific block -- including a
// block in a database that user's role wouldn't otherwise let them
// touch at all. It only ever ADDS access on top of what the role
// hierarchy already grants; it never takes access away from what a
// role already permits (an ADMIN_GENERAL or a DB's own ADMIN is
// unaffected by the absence of a grant).
//
// Grants are stored in the same Badger-backed users DB the User
// records themselves live in (e.usersDB(), see users_auth.go), under
// keys "acl/<username>/<db>/<block>", one JSON-encoded Grant per key.
// A block of "*" means "every block in that database".
package caimandb

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

// Grant is one GRANT ... TO <user> record: username is allowed the
// listed Actions ("read"/"write") on Block (or every block, if Block
// is "*") within DB.
type Grant struct {
	Username  string   `json:"username"`
	DB        string   `json:"db"`
	Block     string   `json:"block"` // "*" = every block in DB
	Actions   []string `json:"actions"`
	CreatedAt int64    `json:"created_at"`
	GrantedBy string   `json:"granted_by"`
}

func aclKey(username, db, block string) []byte {
	return []byte("acl/" + username + "/" + db + "/" + block)
}

// validGrantActions are the only actions GRANT/REVOKE accept. Kept
// deliberately small (read/write) rather than mirroring every command
// name in the DSL -- see authorizeBlockAccess for how FIND/SEARCH map
// to "read" and INSERT/UPDATE/DELETE map to "write".
var validGrantActions = map[string]bool{"read": true, "write": true}

func normalizeGrantActions(raw []string) ([]string, error) {
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, a := range raw {
		a = strings.ToLower(strings.TrimSpace(a))
		if !validGrantActions[a] {
			return nil, fmt.Errorf("invalid GRANT action %q (expected read or write)", a)
		}
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("GRANT requires at least one action (read, write)")
	}
	return out, nil
}

// GrantAccess records that username may perform actions on db.block
// (block "*" for every block in db). Merges with any existing grant
// for the same (username, db, block) rather than replacing it, so
// "GRANT write ..." after an earlier "GRANT read ..." on the same
// resource ends up with both, not just the most recent one.
func (e *Engine) GrantAccess(username, db, block string, actions []string, grantedBy string) error {
	if _, ok := e.GetUserByUsername(username); !ok {
		return fmt.Errorf("user %s does not exist", username)
	}
	actions, err := normalizeGrantActions(actions)
	if err != nil {
		return err
	}
	if db == "" {
		return fmt.Errorf("GRANT requires a database")
	}
	if block == "" {
		block = "*"
	}

	dbHandle, err := e.usersDB()
	if err != nil {
		return err
	}
	key := aclKey(username, db, block)

	err = txHelper.SafeUpdate(dbHandle, func(txn *badger.Txn) error {
		g := Grant{Username: username, DB: db, Block: block, CreatedAt: time.Now().Unix(), GrantedBy: grantedBy}
		if item, ierr := txn.Get(key); ierr == nil {
			_ = item.Value(func(val []byte) error {
				var existing Grant
				if json.Unmarshal(val, &existing) == nil {
					g.Actions = existing.Actions
					g.CreatedAt = existing.CreatedAt
				}
				return nil
			})
		}
		merged := map[string]bool{}
		for _, a := range g.Actions {
			merged[a] = true
		}
		for _, a := range actions {
			merged[a] = true
		}
		g.Actions = g.Actions[:0]
		for _, a := range []string{"read", "write"} { // stable order
			if merged[a] {
				g.Actions = append(g.Actions, a)
			}
		}
		raw, mErr := json.Marshal(g)
		if mErr != nil {
			return mErr
		}
		return txn.Set(key, raw)
	})
	if err == nil {
		log().Info("Access granted", zap.String("user", username), zap.String("db", db),
			zap.String("block", block), zap.Strings("actions", actions), zap.String("granted_by", grantedBy))
	}
	return err
}

// RevokeAccess removes the given actions from an existing grant (or
// the whole grant, if no actions remain after removal). Revoking a
// grant that doesn't exist is not an error -- REVOKE is idempotent,
// matching DROP-style commands elsewhere in the DSL.
func (e *Engine) RevokeAccess(username, db, block string, actions []string) error {
	actions, err := normalizeGrantActions(actions)
	if err != nil {
		return err
	}
	if block == "" {
		block = "*"
	}
	dbHandle, err := e.usersDB()
	if err != nil {
		return err
	}
	key := aclKey(username, db, block)
	err = txHelper.SafeUpdate(dbHandle, func(txn *badger.Txn) error {
		item, gErr := txn.Get(key)
		if gErr == badger.ErrKeyNotFound {
			return nil
		}
		if gErr != nil {
			return gErr
		}
		var g Grant
		if uErr := item.Value(func(val []byte) error { return json.Unmarshal(val, &g) }); uErr != nil {
			return uErr
		}
		remove := map[string]bool{}
		for _, a := range actions {
			remove[a] = true
		}
		remaining := g.Actions[:0]
		for _, a := range g.Actions {
			if !remove[a] {
				remaining = append(remaining, a)
			}
		}
		if len(remaining) == 0 {
			return txn.Delete(key)
		}
		g.Actions = remaining
		raw, mErr := json.Marshal(g)
		if mErr != nil {
			return mErr
		}
		return txn.Set(key, raw)
	})
	if err == nil {
		log().Info("Access revoked", zap.String("user", username), zap.String("db", db),
			zap.String("block", block), zap.Strings("actions", actions))
	}
	return err
}

// ListGrants returns every grant recorded for username, in no
// particular order.
func (e *Engine) ListGrants(username string) ([]Grant, error) {
	dbHandle, err := e.usersDB()
	if err != nil {
		return nil, err
	}
	var grants []Grant
	err = dbHandle.View(func(txn *badger.Txn) error {
		prefix := []byte("acl/" + username + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var g Grant
			if vErr := it.Item().Value(func(val []byte) error { return json.Unmarshal(val, &g) }); vErr == nil {
				grants = append(grants, g)
			}
		}
		return nil
	})
	return grants, err
}

// hasGrantedAccess reports whether username has a recorded grant
// covering action on db.block: an exact-block grant or a "*"
// (whole-database) grant, either one being sufficient.
func (e *Engine) hasGrantedAccess(username, db, block, action string) bool {
	dbHandle, err := e.usersDB()
	if err != nil {
		return false
	}
	found := false
	_ = dbHandle.View(func(txn *badger.Txn) error {
		for _, key := range [][]byte{aclKey(username, db, block), aclKey(username, db, "*")} {
			item, gErr := txn.Get(key)
			if gErr != nil {
				continue
			}
			var g Grant
			if vErr := item.Value(func(val []byte) error { return json.Unmarshal(val, &g) }); vErr != nil {
				continue
			}
			for _, a := range g.Actions {
				if a == action {
					found = true
					return nil
				}
			}
		}
		return nil
	})
	return found
}

// hasAnyGrantInDB reports whether username has at least one grant
// (any block, any action) recorded in db. Used to let USE/CD
// (dsl_parser.go) admit a user into a database their role alone
// wouldn't allow, when they've been granted access to a specific
// block there -- otherwise a block-level grant in another database
// would be unreachable, since a user could never USE into it in the
// first place.
func (e *Engine) hasAnyGrantInDB(username, db string) bool {
	grants, err := e.ListGrants(username)
	if err != nil {
		return false
	}
	for _, g := range grants {
		if g.DB == db {
			return true
		}
	}
	return false
}

// the core CRUD command handlers (FIND/SEARCH -> "read";
// INSERT/UPDATE/DELETE -> "write") right after confirming the block
// exists. It layers on top of, and never below, the existing
// DB-level role-hierarchy gate already enforced at USE/CD time
// (dsl_parser.go, UserCanAccessDB): if the caller's role already
// grants them access to db (general admin, or their own DB per the
// role hierarchy), this always allows the action -- a grant is never
// required to access what a role already provides. A grant only
// matters for the opposite case: extending access to a *specific*
// block in a database the caller's role wouldn't otherwise let them
// touch. The unauthenticated local console (sess.authRole == "")
// keeps its historical unrestricted behavior, same as
// requireDBAccess/UserCanAccessDB elsewhere.
//
// This is deliberately scoped to the primary CRUD surface
// (FIND/SEARCH/INSERT/UPDATE/DELETE) rather than every command in the
// DSL -- see docs/known-limitations.md for what that does and doesn't
// cover.
func (e *Engine) authorizeBlockAccess(sess *Session, db, block, action string) error {
	if sess == nil || sess.authRole == "" {
		return nil
	}
	if UserCanAccessDB(User{Role: sess.authRole, Database: sess.authDatabase}, db) {
		return nil
	}
	if e.hasGrantedAccess(sess.authUser, db, block, action) {
		return nil
	}
	return fmt.Errorf("acceso denegado: %s no tiene permiso de %s sobre %s.%s", sess.authUser, action, db, block)
}
