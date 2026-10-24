//
// Every block already lives in its own physically isolated Badger
// instance (see storage.DirectoryManager.BlockDataPath): OpenBlock opens
// ".../<db>/<block>/__data" and nothing else lives in that instance. But
// the byte-level keys stored *inside* that instance (see keys.go) also
// embed the block's name -- "d/<block>/<id>" for documents,
// "m/<block>" for block meta, "s/<block>/<field>/<value>/<id>" for
// secondary index entries -- even though the directory alone already
// provides full isolation. That embedded name is what RENAME BLOCK must
// account for: after renameDirRobust moves the directory, every read
// scans by the *current* block name, so any key still carrying the old
// name is invisible even though the bytes are sitting right there on
// disk. migrateBlockKeyNames rewrites every such key in place so the
// data stays reachable under the new block name.
package caimandb

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

// migrateBlockKeyNames must be called after the block's directory has
// already been renamed (i.e. after renameDirRobust succeeded), so that
// e.pool.OpenBlock(dbName, newBlock) resolves to the right, already-moved
// directory. It rewrites every document key, secondary index key, and
// the block meta key from the old block name to the new one. It is
// idempotent and safe to retry: keys are only deleted once their
// rewritten counterpart has been durably written, so a crash or error
// partway through never loses data -- at worst some keys remain under
// the old name and get picked up on a retry.
func (e *Engine) migrateBlockKeyNames(dbName, oldBlock, newBlock string) error {
	if oldBlock == newBlock {
		return nil
	}

	db, err := e.pool.OpenBlock(dbName, newBlock)
	if err != nil {
		return fmt.Errorf("open block for key migration: %w", err)
	}

	docsMigrated, err := rewriteKeyPrefix(db, docKeyPrefix(oldBlock), docKeyPrefix(newBlock))
	if err != nil {
		return fmt.Errorf("migrating document keys: %w", err)
	}

	oldIdxPrefix := []byte(fmt.Sprintf("%s%s/", prefixSecondary, oldBlock))
	newIdxPrefix := []byte(fmt.Sprintf("%s%s/", prefixSecondary, newBlock))
	idxMigrated, err := rewriteKeyPrefix(db, oldIdxPrefix, newIdxPrefix)
	if err != nil {
		return fmt.Errorf("migrating secondary index keys: %w", err)
	}

	if err := renameSingleKey(db, metaKey(oldBlock), metaKey(newBlock)); err != nil {
		return fmt.Errorf("migrating block meta key: %w", err)
	}

	if docsMigrated > 0 || idxMigrated > 0 {
		log().Info("Migrated block key names after rename",
			zap.String("db", dbName),
			zap.String("old_block", oldBlock),
			zap.String("new_block", newBlock),
			zap.Int("documents", docsMigrated),
			zap.Int("index_entries", idxMigrated))
	}

	return nil
}

// rewriteKeyPrefix copies every key under oldPrefix to the same key with
// newPrefix substituted in (value unchanged), then deletes the old keys
// only after every new key has been confirmed written. Keys are batched
// in Badger WriteBatches (same pattern as bulk insert in ops_insert.go)
// so this scales to large blocks without hitting a single transaction's
// size limits.
func rewriteKeyPrefix(db *badger.DB, oldPrefix, newPrefix []byte) (int, error) {
	type kv struct {
		key   []byte
		value []byte
	}
	var pairs []kv

	err := txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(oldPrefix); it.ValidForPrefix(oldPrefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			k := make([]byte, len(item.Key()))
			copy(k, item.Key())
			v, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("reading value for key %q: %w", k, err)
			}
			pairs = append(pairs, kv{key: k, value: v})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(pairs) == 0 {
		return 0, nil
	}

	writeNew := func(batch []kv) error {
		wb := db.NewWriteBatch()
		defer wb.Cancel()
		for _, p := range batch {
			suffix := p.key[len(oldPrefix):]
			newKey := append(append([]byte{}, newPrefix...), suffix...)
			if err := wb.Set(newKey, p.value); err != nil {
				return err
			}
		}
		return wb.Flush()
	}

	deleteOld := func(batch []kv) error {
		wb := db.NewWriteBatch()
		defer wb.Cancel()
		for _, p := range batch {
			if err := wb.Delete(p.key); err != nil {
				return err
			}
		}
		return wb.Flush()
	}

	const chunkSize = 1000
	for start := 0; start < len(pairs); start += chunkSize {
		end := start + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[start:end]

		if err := writeNew(chunk); err != nil {
			return 0, fmt.Errorf("writing migrated keys: %w", err)
		}
		// Old keys for this chunk are only removed once their new
		// counterparts are durably written, so a failure here just
		// leaves both copies present -- re-running the migration is a
		// no-op for the already-migrated new keys and safely retries
		// the leftover old ones.
		if err := deleteOld(chunk); err != nil {
			return 0, fmt.Errorf("deleting old keys after migration: %w", err)
		}
	}

	return len(pairs), nil
}

// renameSingleKey moves a single non-prefixed key (the block meta key)
// from oldKey to newKey, preserving its value. A missing oldKey is not
// an error -- the meta key is optional/rarely populated, so "nothing to
// migrate" is the common case.
func renameSingleKey(db *badger.DB, oldKey, newKey []byte) error {
	var value []byte
	var found bool

	err := txHelper.SafeView(db, func(txn *badger.Txn) error {
		item, err := txn.Get(oldKey)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = v
		found = true
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	return txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		if err := txn.Set(newKey, value); err != nil {
			return err
		}
		return txn.Delete(oldKey)
	})
}
