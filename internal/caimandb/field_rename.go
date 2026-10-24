//
// RENAME FIELD is a bulk rewrite of every document in a block, not a
// single-key rename like RENAME BLOCK/RENAME DB (see
// block_rename_migration.go's doc comment for that one): there is no
// directory-level shortcut here, every document that has the old field
// name genuinely has to be read, changed, and written back. Same
// consistency story as RebuildBlock/ReindexFlexColumn (see
// block_repair.go): this runs outside the ACID transaction manager's
// WAL, as a direct, idempotent, restartable Badger operation -- safe to
// re-run (a document already renamed is simply skipped, since it no
// longer has the old field), but a crash mid-run leaves some documents
// renamed and others not rather than atomically all-or-nothing. Re-
// running RENAME FIELD with the same arguments finishes the job.
//
// Scanning and writing are interleaved in fixed-size batches (see
// batchSize below): each pass reads forward from a cursor until it has
// collected one batch's worth of matching documents (or hits the end
// of the block), writes that batch, applies its index/cache side
// effects, then discards it and resumes scanning from the cursor. This
// keeps peak memory at O(batchSize) regardless of how many documents in
// the block actually match, and keeps each read transaction short
// instead of holding one long-lived view open over the whole block.
package caimandb

import (
	"bytes"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"time"
)

// RenameField renames oldField to newField in every document of
// dbName/block that currently has oldField, and rebuilds that
// document's secondary index and FLEX-COLUMN entries to match. Returns
// the number of documents changed. A document that has neither oldField
// nor already has newField (nothing to rename) is left untouched. A
// document that already has newField (e.g. from a previous partial run,
// or a genuine name collision) is skipped rather than overwritten, and
// counted as a conflict in the returned error if any are found.
func (e *Engine) RenameField(dbName, block, oldField, newField string) (int64, error) {
	if oldField == "" || newField == "" {
		return 0, fmt.Errorf("field names must not be empty")
	}
	if oldField == newField {
		return 0, nil
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	lockKey := dbName + "\x00" + block + "\x00rename_field"
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	prefix := docKeyPrefix(block)

	type renameEntry struct {
		key       []byte
		newDoc    Document
		newRaw    []byte
		oldIdxKey [][]byte
	}

	// Streamed in fixed-size batches: each pass scans forward from
	// cursor, stops as soon as it has collected batchSize matching
	// documents (or reaches the end of the block), writes that batch,
	// applies its side effects, and only then discards it and resumes
	// scanning. This bounds memory to O(batchSize) regardless of how
	// many documents in the block actually match, and keeps each read
	// transaction short instead of holding one long-lived view open
	// over the whole block.
	const batchSize = 5000
	renamed := int64(0)
	conflicts := 0
	var cursor []byte // last key examined; nil means "start of prefix"
	doneScanning := false

	for !doneScanning {
		var batch []renameEntry
		var lastSeenKey []byte

		err = txHelper.SafeView(db, func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			defer it.Close()

			if cursor == nil {
				it.Seek(prefix)
			} else {
				it.Seek(cursor)
				if it.ValidForPrefix(prefix) && bytes.Equal(it.Item().Key(), cursor) {
					it.Next() // skip the key we already resumed from
				}
			}

			for ; it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				if item == nil {
					continue
				}
				lastSeenKey = append(lastSeenKey[:0], item.Key()...)

				var raw []byte
				if err := item.Value(func(v []byte) error {
					raw = make([]byte, len(v))
					copy(raw, v)
					return nil
				}); err != nil {
					continue
				}
				var doc Document
				if json.Unmarshal(raw, &doc) != nil {
					continue
				}
				if doc.Data == nil {
					continue
				}
				val, has := doc.Data[oldField]
				if !has {
					continue // already renamed, or never had this field
				}
				if _, taken := doc.Data[newField]; taken {
					conflicts++
					continue
				}

				oldIdxKeys := e.secondaryIndexKeysFor(block, &doc)

				newDoc := doc
				newDoc.Data = make(map[string]any, len(doc.Data))
				for k, v := range doc.Data {
					if k == oldField {
						continue
					}
					newDoc.Data[k] = v
				}
				newDoc.Data[newField] = val
				newDoc.Meta.UpdatedAt = time.Now().Unix()
				newDoc.Meta.Version++

				newRaw, err := json.Marshal(newDoc)
				if err != nil {
					continue
				}

				k := make([]byte, len(item.Key()))
				copy(k, item.Key())

				batch = append(batch, renameEntry{
					key:       k,
					newDoc:    newDoc,
					newRaw:    newRaw,
					oldIdxKey: oldIdxKeys,
				})

				if len(batch) >= batchSize {
					return nil // pause scanning; resume from cursor next pass
				}
			}
			doneScanning = true // reached the end of the prefix this pass
			return nil
		})
		if err != nil {
			return renamed, err
		}

		if lastSeenKey != nil {
			cursor = append([]byte(nil), lastSeenKey...)
		} else if !doneScanning {
			// Defensive: no keys at all were seen but we didn't reach the
			// end either (shouldn't happen) -- stop to avoid looping forever.
			doneScanning = true
		}

		if len(batch) == 0 {
			continue
		}

		err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
			for _, re := range batch {
				for _, ik := range re.oldIdxKey {
					if err2 := txn.Delete(ik); err2 != nil && err2 != badger.ErrKeyNotFound {
						return err2
					}
				}
				if err2 := txn.Set(re.key, re.newRaw); err2 != nil {
					return err2
				}
			}
			return nil
		})
		if err != nil {
			return renamed, fmt.Errorf("renamed %d documents before failing: %w", renamed, err)
		}
		for _, re := range batch {
			if err := e.buildSecondaryIndex(dbName, block, &re.newDoc); err != nil {
				log().Warn("RENAME FIELD: failed to rebuild secondary index",
					zap.String("db", dbName), zap.String("block", block), zap.String("doc_id", re.newDoc.ID), zap.Error(err))
			}
			if e.flexEngine != nil {
				e.flexEngine.DeleteWithColumnar(dbName, block, re.newDoc.ID)
				e.flexEngine.InsertWithColumnar(dbName, block, &re.newDoc)
			}
			cacheKey := e.cacheKey(dbName, block, re.newDoc.ID)
			e.l1Cache.Set(cacheKey, re.newRaw)
			renamed++
		}
		// batch goes out of scope here and is freed before the next pass
	}

	log().Info("RENAME FIELD completed",
		zap.String("db", dbName), zap.String("block", block),
		zap.String("old_field", oldField), zap.String("new_field", newField),
		zap.Int64("renamed", renamed), zap.Int("conflicts", conflicts))

	if conflicts > 0 {
		return renamed, fmt.Errorf("renamed %d document(s); %d document(s) already had field %q and were left unchanged", renamed, conflicts, newField)
	}
	return renamed, nil
}
