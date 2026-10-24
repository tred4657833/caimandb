package caimandb

import (
	"caimandb/internal/caimandb/storage"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"strings"
)

func (e *Engine) RebuildBlock(dbName, block string) error {
	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return err
	}

	prefix := []byte(fmt.Sprintf("%s%s/", prefixSecondary, block))
	err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		var keys [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			k := make([]byte, len(item.Key()))
			copy(k, item.Key())
			keys = append(keys, k)
		}
		for _, k := range keys {
			if err2 := txn.Delete(k); err2 != nil {
				return err2
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	docPrefix := docKeyPrefix(block)
	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(docPrefix); it.ValidForPrefix(docPrefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				continue
			}
			if err := e.buildSecondaryIndex(dbName, block, &doc); err != nil {
				log().Warn("Failed to rebuild index", zap.Error(err))
			}
		}
		return nil
	})

	log().Info("Block indexes rebuilt", zap.String("db", dbName), zap.String("block", block))
	return err
}

// ReindexFlexColumn rebuilds the FLEX-COLUMN in-memory columnar index for
// one block by streaming every document currently on disk through it,
// document by document (never materializing the whole block in memory at
// once, the same discipline RebuildBlock above uses for the secondary
// index). This is the explicit backfill step for a block that was loaded
// while BULK MODE was on: InsertWithColumnar/UpdateWithColumnar skip
// indexing during bulk mode specifically to avoid this same index growing
// unbounded mid-IMPORT (see FlexColumnEngine.bulkModeActive), so a block
// bulk-loaded that way has an empty or stale FLEX-COLUMN index until this
// runs. Safe to run any time, bulk mode or not, and safe to re-run --
// it clears the block's existing entries first.
func (e *Engine) ReindexFlexColumn(dbName, block string) (int64, error) {
	if e.flexEngine == nil || e.flexEngine.config == nil || !e.flexEngine.config.Enabled {
		return 0, nil
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	e.flexEngine.resetBlock(dbName, block)

	var count int64
	docPrefix := docKeyPrefix(block)
	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(docPrefix); it.ValidForPrefix(docPrefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				continue
			}
			e.flexEngine.indexDoc(dbName, block, &doc)
			count++
		}
		return nil
	})

	log().Info("FLEX-COLUMN index rebuilt",
		zap.String("db", dbName), zap.String("block", block), zap.Int64("documents", count))
	return count, err
}

func (e *Engine) CheckBlock(dbName, block string) (map[string]any, error) {
	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, err
	}

	var totalDocs int64
	var corrupted int64
	var meta storage.BlockMeta

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey(block))
		if err == nil {
			item.Value(func(v []byte) error { return json.Unmarshal(v, &meta) })
		}

		prefix := docKeyPrefix(block)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			totalDocs++
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				corrupted++
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return map[string]any{
		"block":      block,
		"db":         dbName,
		"total_docs": totalDocs,
		"corrupted":  corrupted,
		"meta":       meta,
		"status":     "ok",
	}, nil
}

// quarantineCorruptedDocs deletes every document key under block whose
// stored value fails to unmarshal as a Document. Bytes that fail to parse
// as JSON cannot be un-corrupted, so the only safe repair is to remove
// them rather than leave them permanently poisoning reads/scans/index
// rebuilds; every removed key is logged (db/block/key) so the operator
// has a record of exactly what was lost. Returns the number of keys
// removed and the document IDs they belonged to -- RepairBlock uses the
// ID list to automatically try pulling those documents back from backup
// (see restoreDocsFromBackup in ops_backup_jsonl.go) right after this
// runs, rather than requiring an operator to notice and run RESTORE.
func (e *Engine) quarantineCorruptedDocs(dbName, block string) (int64, []string, error) {
	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, nil, err
	}

	docPrefix := docKeyPrefix(block)
	var badKeys [][]byte
	var badIDs []string

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(docPrefix); it.ValidForPrefix(docPrefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				k := make([]byte, len(item.Key()))
				copy(k, item.Key())
				badKeys = append(badKeys, k)
				badIDs = append(badIDs, strings.TrimPrefix(string(k), string(docPrefix)))
			}
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	if len(badKeys) == 0 {
		return 0, nil, nil
	}

	err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		for _, k := range badKeys {
			if err2 := txn.Delete(k); err2 != nil {
				return err2
			}
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	for _, k := range badKeys {
		log().Warn("Removed corrupted document during repair",
			zap.String("db", dbName), zap.String("block", block), zap.ByteString("key", k))
	}

	return int64(len(badKeys)), badIDs, nil
}

func (e *Engine) RepairBlock(dbName, block string) error {
	check, err := e.CheckBlock(dbName, block)
	if err != nil {
		return err
	}
	corrupted := check["corrupted"].(int64)
	if corrupted == 0 {
		log().Info("Block has no corruption", zap.String("block", block))
		return nil
	}

	removed, badIDs, err := e.quarantineCorruptedDocs(dbName, block)
	if err != nil {
		return fmt.Errorf("failed to quarantine corrupted documents: %w", err)
	}
	log().Warn("Quarantined corrupted documents during repair",
		zap.String("db", dbName), zap.String("block", block),
		zap.Int64("removed", removed), zap.Int64("detected", corrupted))

	// The database automatically asks its own backup for the documents
	// it just lost, instead of leaving that to an operator running
	// RESTORE by hand. Best-effort: no backup, or none of the lost IDs
	// happening to be in it, just means fewer documents come back --
	// logged either way, never fatal to the repair itself.
	if len(badIDs) > 0 {
		recovered, recErr := e.restoreDocsFromBackup(dbName, block, badIDs)
		if recErr != nil {
			log().Warn("Automatic restore-from-backup after repair failed",
				zap.String("db", dbName), zap.String("block", block), zap.Error(recErr))
		} else if recovered > 0 {
			log().Info("Automatically restored documents from backup after repair",
				zap.String("db", dbName), zap.String("block", block),
				zap.Int64("recovered", recovered), zap.Int("lost", len(badIDs)))
		}
	}

	// Surviving documents are intact, but the secondary index may still
	// reference the now-deleted keys, so it must be rebuilt from what's
	// left.
	return e.RebuildBlock(dbName, block)
}
