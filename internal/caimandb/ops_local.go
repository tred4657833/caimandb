package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/parse"
	"caimandb/internal/caimandb/storage"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"os"
	"time"
)

func (e *Engine) insertWithIDLocal(dbName, block, id string, data map[string]any, shardID string) (*Document, error) {
	if shardID == "" {
		shardID = e.shardMgr.GetShardForDoc(dbName, block, id)
	}

	if err := e.dirMgr.CreateBlockDirectories(dbName, block); err != nil {
		return nil, err
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, err
	}

	lockKey := dbName + "\x00" + block + "\x00" + shardID
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	now := time.Now().Unix()

	if id != "" {
		// Note: do NOT blindly delete "_id" here. By the time we reach this
		// layer, Insert/InsertBatch/handleInsert have already resolved which
		// of "id"/"_id" (if any) should become the document's id, and a
		// remaining "_id" field (when both "id" and "_id" were supplied) is
		// intentionally kept as plain extra data.
		data["id"] = id
	}

	doc := Document{
		ID:      id,
		Data:    data,
		Meta:    DocMeta{CreatedAt: now, UpdatedAt: now, Version: 1},
		ShardID: shardID,
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	if int64(len(raw)) > e.config.MaxDocumentSize {
		return nil, fmt.Errorf("%w: %d bytes (max: %d)", ErrDocTooLarge, len(raw), e.config.MaxDocumentSize)
	}

	metrics.MetricDocSize.Observe(float64(len(raw)))

	if e.externalStore != nil && int64(len(raw)) > e.config.LargeDocThreshold {
		largeID, err := e.externalStore.Save(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to store large document: %w", err)
		}
		doc.LargeID = largeID
		raw, _ = json.Marshal(doc)
	}

	compressedData := raw
	compressed := false
	if e.config.CompressLargeDocs && int64(len(raw)) > e.config.CompressThreshold {
		if comp, ok, err := storage.CompressData(raw, storage.CompressionZstd); err == nil && ok {
			compressedData = comp
			compressed = true
		}
	}

	key := docKey(block, id)

	// WAL first, Badger second. If the WAL is the basis for
	// replication/recovery, a document that made it into Badger but never
	// into the WAL is a document the client believes is durable but that a
	// crash would silently lose -- so a WAL failure must fail the insert,
	// not just log a warning and continue as if nothing happened.
	if e.wal != nil {
		walData := raw
		if compressed {
			walData = compressedData
		}
		if err := e.wal.Write("insert", dbName, block, walData); err != nil {
			return nil, fmt.Errorf("failed to write to WAL, insert aborted: %w", err)
		}
	}

	err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		return txn.Set(key, compressedData)
	})
	if err != nil {
		return nil, err
	}

	if int64(len(raw)) < e.config.IndexSkipThreshold {
		if err := e.buildSecondaryIndex(dbName, block, &doc); err != nil {
			log().Warn("Failed to build secondary index", zap.Error(err))
		}
	}

	e.queryOpt.UpdateIndexStats("_id", 0, 0)

	e.l1Cache.Set(e.cacheKey(dbName, block, id), raw)
	e.shardMgr.UpdateShardStats(shardID, 1, int64(len(raw)))

	// EnsureBlockMeta rather than LoadBlockMeta: if block.json was ever
	// missing (a legacy block, an interrupted rename, ...) the old
	// LoadBlockMeta-guarded form silently skipped every counter update
	// forever, since there was never a later point where the meta got
	// created. EnsureBlockMeta rebuilds it once so counting resumes.
	if meta, err := e.dirMgr.EnsureBlockMeta(dbName, block); err == nil {
		meta.DocCount++
		meta.SizeBytes += int64(len(raw))
		meta.UpdatedAt = time.Now().Unix()
		e.dirMgr.SaveBlockMeta(dbName, block, meta)
	} else {
		log().Warn("Failed to update block metadata after insert; doc count/size may drift until the next rebuild",
			zap.String("db", dbName), zap.String("block", block), zap.Error(err))
	}

	e.opCount.Add(1)
	e.changeBus.PublishInsert(dbName, block, id, shardID, data)

	// Update FLEX-COLUMN
	if e.flexEngine != nil {
		e.flexEngine.InsertWithColumnar(dbName, block, &doc)
	}

	metrics.MetricOpsTotal.WithLabelValues("insert", dbName, block, shardID, e.nodeID, "success").Inc()
	metrics.MetricDocumentsTotal.WithLabelValues(dbName, block, shardID, e.nodeID).Inc()

	if stats, ok := e.intelEngine.stats[dbName]; ok {
		stats.DocCount++
		stats.AvgDocSize = (stats.AvgDocSize*(stats.DocCount-1) + int64(len(raw))) / stats.DocCount
	}

	if e.auditLogger != nil {
		e.auditLogger.Log(AuditLog{
			UserID:   "system",
			Username: "system",
			Action:   "insert",
			DB:       dbName,
			Block:    block,
			DocID:    id,
			ShardID:  shardID,
			Status:   "success",
			Message:  fmt.Sprintf("size=%d compressed=%v", len(raw), compressed),
		})
	}

	return &doc, nil
}

// updateOneItem applies op to a single candidate document -- identified
// by its already-fetched Badger key+raw value -- if it matches
// where/filters and the shard filter, inside the given transaction.
// Returns matched=false (no error) for a document that doesn't qualify,
// exactly like the old inline loop's "continue" did for every one of
// these checks. This is the single place update semantics live: both
// the full-block scan and the singleIDEquality fast path in updateLocal
// below call it, so a document updated via either path is mutated,
// indexed, cached and WAL'd identically -- see singleIDEquality's doc
// comment for why that equivalence is what makes the fast path safe.
func (e *Engine) updateOneItem(txn *badger.Txn, dbName, block string, key, raw []byte, where *parse.Expr, filters []Filter, shardID string, op UpdateOp, idMatched bool) (matched bool, sizeDelta int64, err error) {
	var doc Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false, 0, nil
	}
	if doc.ShardID != shardID && shardID != "" {
		return false, 0, nil
	}
	// idMatched is true only when the caller already resolved this exact
	// document via the singleIDEquality fast path below -- i.e. it did a
	// direct txn.Get(docKey(block, id)) that succeeded, which already
	// proves identity conclusively. In that case we must NOT re-run
	// matchesWhere with the original "id = <literal>" condition: that
	// condition is evaluated against doc.Data["id"] (a plain, mutable data
	// field mirrored onto every document at insert time -- see
	// ops_insert.go), not against doc.ID/_id, the actual key that was just
	// matched. If that data field was ever dropped (DROP FIELD), renamed
	// (RENAME FIELD), or overwritten by a prior update, matchesWhere would
	// wrongly report no match for a document that unquestionably exists at
	// that key -- exactly the "UPSERT found document X but the update
	// matched nothing" symptom this fixes. The scan path (idMatched=false)
	// still needs the real check, since there it hasn't independently
	// proven anything about this specific document yet.
	if !idMatched && !matchesWhere(doc, where, filters) {
		return false, 0, nil
	}

	// Snapshot the pre-update document: dropSecondaryIndex (below) must
	// remove index entries keyed by the OLD field values, and
	// applyUpdateOp mutates doc in place.
	var oldDoc Document
	if err := json.Unmarshal(raw, &oldDoc); err != nil {
		return false, 0, nil
	}

	applyUpdateOp(&doc, op)
	doc.Meta.UpdatedAt = time.Now().Unix()
	doc.Meta.Version++

	newRaw, err2 := json.Marshal(doc)
	if err2 != nil {
		return false, 0, nil
	}

	if e.externalStore != nil && int64(len(newRaw)) > e.config.LargeDocThreshold {
		largeID, err3 := e.externalStore.Save(newRaw)
		if err3 != nil {
			return false, 0, nil
		}
		doc.LargeID = largeID
		newRaw, _ = json.Marshal(doc)
	}

	if e.config.CompressLargeDocs && int64(len(newRaw)) > e.config.CompressThreshold {
		if comp, ok, cerr := storage.CompressData(newRaw, storage.CompressionZstd); cerr == nil && ok {
			newRaw = comp
		}
	}

	// WAL first: if it fails, skip this document entirely rather than
	// dropping its old index entries and committing a change to Badger
	// that never made it into the WAL (see the same fix in
	// insertWithIDLocal for the rationale -- a WAL-less write is
	// invisible to recovery/replication even though the client would
	// see it as successful).
	if e.wal != nil {
		if werr := e.wal.Write("update", dbName, block, newRaw); werr != nil {
			log().Error("WAL update write failed, skipping document", zap.Error(werr))
			return false, 0, nil
		}
	}

	e.dropSecondaryIndexInto(txn, block, &oldDoc)

	k := make([]byte, len(key))
	copy(k, key)
	if serr := txn.Set(k, newRaw); serr != nil {
		return false, 0, serr
	}

	if int64(len(newRaw)) < e.config.IndexSkipThreshold {
		e.buildSecondaryIndexInto(txn, block, &doc)
	}

	// Update FLEX-COLUMN
	if e.flexEngine != nil {
		e.flexEngine.UpdateWithColumnar(dbName, block, &oldDoc, &doc)
	}

	e.l1Cache.Set(e.cacheKey(dbName, block, doc.ID), newRaw)
	e.changeBus.PublishUpdate(dbName, block, doc.ID, doc.ShardID, doc.Data)

	return true, int64(len(newRaw)) - int64(len(raw)), nil
}

func (e *Engine) updateLocal(dbName, block string, filters []Filter, where *parse.Expr, op UpdateOp, shardID string) (int64, error) {
	start := time.Now()

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	lockKey := dbName + "\x00" + block + "\x00" + shardID
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	var updated int64
	var sizeDelta int64

	if id, ok := singleIDEquality(filters, where); ok {
		// Fast path: WHERE reduces to "id = <literal>" alone, the same
		// single-key lookup FindByID already does in O(1) -- skip the
		// block-wide scan below entirely and go straight to the one key
		// that could possibly match. See singleIDEquality's doc comment
		// and docs/load-testing-findings.md. Note the block-wide lock
		// above is still held for the duration either way -- this closes
		// the scan cost under that lock, not the lock's granularity,
		// which load-testing-findings.md flags separately as a larger,
		// riskier change needing its own design review.
		err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
			item, gerr := txn.Get(docKey(block, id))
			if gerr != nil {
				return nil // no document with this id: 0 updated, not an error
			}
			var raw []byte
			if verr := item.Value(func(v []byte) error {
				raw = make([]byte, len(v))
				copy(raw, v)
				return nil
			}); verr != nil {
				return nil
			}
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			matched, delta, uerr := e.updateOneItem(txn, dbName, block, key, raw, where, filters, shardID, op, true)
			if uerr != nil {
				return uerr
			}
			if matched {
				updated = 1
				sizeDelta = delta
			}
			return nil
		})
	} else {
		prefix := docKeyPrefix(block)
		err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 1000
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				if item == nil {
					continue
				}
				var raw []byte
				if verr := item.Value(func(v []byte) error {
					raw = make([]byte, len(v))
					copy(raw, v)
					return nil
				}); verr != nil {
					continue
				}
				key := make([]byte, len(item.Key()))
				copy(key, item.Key())

				matched, delta, uerr := e.updateOneItem(txn, dbName, block, key, raw, where, filters, shardID, op, false)
				if uerr != nil {
					return uerr
				}
				if matched {
					updated++
					sizeDelta += delta
				}
			}
			return nil
		})
	}

	if updated > 0 {
		if meta, err := e.dirMgr.EnsureBlockMeta(dbName, block); err == nil {
			meta.DocCount += updated
			meta.SizeBytes += sizeDelta
			meta.UpdatedAt = time.Now().Unix()
			e.dirMgr.SaveBlockMeta(dbName, block, meta)
		} else {
			log().Warn("Failed to update block metadata after update; doc count/size may drift until the next rebuild",
				zap.String("db", dbName), zap.String("block", block), zap.Error(err))
		}
	}

	e.opCount.Add(updated)
	metrics.MetricOpsTotal.WithLabelValues("update", dbName, block, shardID, e.nodeID, "success").Add(float64(updated))
	metrics.MetricOpsDuration.WithLabelValues("update", dbName, block, shardID, e.nodeID).Observe(time.Since(start).Seconds())

	if e.auditLogger != nil && updated > 0 {
		e.auditLogger.Log(AuditLog{
			UserID:   "system",
			Username: "system",
			Action:   "update",
			DB:       dbName,
			Block:    block,
			ShardID:  shardID,
			Status:   "success",
			Message:  fmt.Sprintf("updated=%d", updated),
		})
	}

	return updated, err
}

// walDeleteEntry is what gets marshaled into a "delete" WAL entry so
// RecoverWAL (wal_recovery.go) can replay the exact same match logic
// deleteLocal used originally, including the parenthesized/NOT/AND-OR
// expression tree when the DELETE that produced it had one -- not just
// the flat []Filter fallback. WAL entries written before this type
// existed are a bare JSON array (the old `json.Marshal(filters)`); see
// wal_recovery.go for the fallback that still reads those.
type walDeleteEntry struct {
	Filters []Filter    `json:"filters"`
	Where   *parse.Expr `json:"where,omitempty"`
}

// deleteDocEntry pairs a matched document's Badger key with its own ID
// and the index keys computed for it (via secondaryIndexKeysFor, which
// just computes key bytes -- no Badger access). deleteLocal's batched
// commit loop deletes the document key, all of its index keys, and does
// its cache/change-bus cleanup together in the same transaction, instead
// of the old approach of a separate dropSecondaryIndex call per document
// that opened and committed its own transaction per indexed field (see
// the doc comment on buildSecondaryIndexInto in index_secondary.go for
// why that was the dominant cost at scale).
type deleteDocEntry struct {
	key       []byte
	id        string
	indexKeys [][]byte
}

// deleteCandidateFromItem inspects one candidate document -- identified
// by its already-fetched Badger key+raw value -- and, if it matches
// where/filters and the shard filter, does everything deleteLocal needs
// *before* the key is actually removed: writes the WAL entry, cleans up
// any external-storage blob, removes it from FLEX-COLUMN, and computes
// its secondary-index keys. ok is false when the document doesn't
// qualify (wrong shard, doesn't match where/filters) or when the WAL
// write failed, in which case the document is skipped entirely -- the
// same "continue" behavior the old inline loop had for both cases. This
// is the single place delete-candidate selection lives: both the
// full-block scan and the singleIDEquality fast path in deleteLocal
// below call it, so a document reached via either path is vetted and
// staged for deletion identically -- see singleIDEquality's doc comment
// for why that equivalence is what makes the fast path safe.
func (e *Engine) deleteCandidateFromItem(dbName, block string, key, raw []byte, where *parse.Expr, filters []Filter, shardID string, idMatched bool) (entry deleteDocEntry, sizeAdd int64, ok bool) {
	var doc Document
	if json.Unmarshal(raw, &doc) != nil {
		return deleteDocEntry{}, 0, false
	}
	if doc.ShardID != shardID && shardID != "" {
		return deleteDocEntry{}, 0, false
	}
	// See updateOneItem's idMatched doc comment: when the caller already
	// resolved this document via the singleIDEquality direct-key fast
	// path, re-checking "id = <literal>" against doc.Data["id"] is both
	// redundant and unsafe (that data field can be renamed/dropped/stale
	// independently of the real key), so it's skipped here too.
	if !idMatched && !matchesWhere(doc, where, filters) {
		return deleteDocEntry{}, 0, false
	}

	// WAL first: if it fails, skip this document rather than dropping
	// its index/external storage/FLEX-COLUMN entries and only then
	// discovering the delete was never recorded for recovery/replication.
	if e.wal != nil {
		delData, _ := json.Marshal(walDeleteEntry{Filters: filters, Where: where})
		if werr := e.wal.Write("delete", dbName, block, delData); werr != nil {
			log().Error("WAL delete write failed, skipping document", zap.Error(werr))
			return deleteDocEntry{}, 0, false
		}
	}

	if doc.LargeID != "" && e.externalStore != nil {
		if derr := e.externalStore.Delete(doc.LargeID); derr != nil {
			log().Warn("Failed to delete external document", zap.Error(derr))
		}
	}

	// Delete from FLEX-COLUMN
	if e.flexEngine != nil {
		e.flexEngine.DeleteWithColumnar(dbName, block, doc.ID)
	}

	k := make([]byte, len(key))
	copy(k, key)

	return deleteDocEntry{
		key:       k,
		id:        doc.ID,
		indexKeys: e.secondaryIndexKeysFor(block, &doc),
	}, int64(len(raw)), true
}

func (e *Engine) deleteLocal(dbName, block string, filters []Filter, where *parse.Expr, shardID string) (int64, error) {
	start := time.Now()

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	lockKey := dbName + "\x00" + block + "\x00" + shardID
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	var deleted int64
	var toDelete []deleteDocEntry
	var totalSize int64

	if id, ok := singleIDEquality(filters, where); ok {
		// Fast path: WHERE reduces to "id = <literal>" alone, the same
		// single-key lookup FindByID already does in O(1) -- skip the
		// block-wide scan below entirely and go straight to the one key
		// that could possibly match. See singleIDEquality's doc comment
		// and docs/load-testing-findings.md. Note the block-wide lock
		// above is still held for the duration either way -- this closes
		// the scan cost under that lock, not the lock's granularity,
		// which load-testing-findings.md flags separately as a larger,
		// riskier change needing its own design review.
		err = txHelper.SafeView(db, func(txn *badger.Txn) error {
			item, gerr := txn.Get(docKey(block, id))
			if gerr != nil {
				return nil // no document with this id: 0 deleted, not an error
			}
			var raw []byte
			if verr := item.Value(func(v []byte) error {
				raw = make([]byte, len(v))
				copy(raw, v)
				return nil
			}); verr != nil {
				return nil
			}
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			if de, sizeAdd, ok := e.deleteCandidateFromItem(dbName, block, key, raw, where, filters, shardID, true); ok {
				toDelete = append(toDelete, de)
				totalSize += sizeAdd
			}
			return nil
		})
	} else {
		prefix := docKeyPrefix(block)
		err = txHelper.SafeView(db, func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				if item == nil {
					continue
				}
				var raw []byte
				if verr := item.Value(func(v []byte) error {
					raw = make([]byte, len(v))
					copy(raw, v)
					return nil
				}); verr != nil {
					continue
				}
				key := make([]byte, len(item.Key()))
				copy(key, item.Key())

				if de, sizeAdd, ok := e.deleteCandidateFromItem(dbName, block, key, raw, where, filters, shardID, false); ok {
					toDelete = append(toDelete, de)
					totalSize += sizeAdd
				}
			}
			return nil
		})
	}
	if err != nil {
		return 0, err
	}

	batchSize := 10000
	for i := 0; i < len(toDelete); i += batchSize {
		end := i + batchSize
		if end > len(toDelete) {
			end = len(toDelete)
		}
		batch := toDelete[i:end]
		err = txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
			for _, de := range batch {
				if err2 := txn.Delete(de.key); err2 != nil {
					return err2
				}
				for _, ik := range de.indexKeys {
					if err2 := txn.Delete(ik); err2 != nil {
						return err2
					}
				}
				e.l1Cache.Del(e.cacheKey(dbName, block, de.id))
				e.changeBus.PublishDelete(dbName, block, de.id, shardID)
				deleted++
			}
			return nil
		})
		if err != nil {
			return deleted, err
		}
	}

	if deleted > 0 {
		if meta, err := e.dirMgr.EnsureBlockMeta(dbName, block); err == nil {
			meta.DocCount -= deleted
			meta.SizeBytes -= totalSize
			meta.UpdatedAt = time.Now().Unix()
			e.dirMgr.SaveBlockMeta(dbName, block, meta)
		} else {
			log().Warn("Failed to update block metadata after delete; doc count/size may drift until the next rebuild",
				zap.String("db", dbName), zap.String("block", block), zap.Error(err))
		}
	}

	metrics.MetricOpsTotal.WithLabelValues("delete", dbName, block, shardID, e.nodeID, "success").Add(float64(deleted))
	metrics.MetricOpsDuration.WithLabelValues("delete", dbName, block, shardID, e.nodeID).Observe(time.Since(start).Seconds())
	metrics.MetricDocumentsTotal.WithLabelValues(dbName, block, shardID, e.nodeID).Sub(float64(deleted))

	if stats, ok := e.intelEngine.stats[dbName]; ok {
		stats.DocCount -= deleted
	}

	if e.auditLogger != nil && deleted > 0 {
		e.auditLogger.Log(AuditLog{
			UserID:   "system",
			Username: "system",
			Action:   "delete",
			DB:       dbName,
			Block:    block,
			ShardID:  shardID,
			Status:   "success",
			Message:  fmt.Sprintf("deleted=%d", deleted),
		})
	}

	return deleted, nil
}

func (e *Engine) createDBLocal(name string) error {
	if err := validateName(name); err != nil {
		return err
	}

	dbPath := e.dirMgr.DBPath(name)
	if err := os.MkdirAll(dbPath, 0750); err != nil {
		return err
	}

	meta := map[string]any{
		"name":       name,
		"created_at": time.Now().Unix(),
		"version":    Version,
	}
	if err := e.dirMgr.SaveDBMeta(name, meta); err != nil {
		return err
	}

	log().Info("Database created", zap.String("db", name))
	cfg := &DBConfig{
		Name:        name,
		ShardCount:  e.shardMgr.shardCount,
		Replicas:    e.shardMgr.replicaCount,
		CacheSizeMB: e.l1Cache.MaxBytes() / 1024 / 1024,
		Compression: "zstd",
		AutoTune:    true,
		AutoScale:   true,
		Indexes:     []string{},
		Custom:      make(map[string]any),
		CreatedAt:   time.Now().Unix(),
		UpdatedAt:   time.Now().Unix(),
		Stats: &DBStats{
			LastOptimized: time.Now(),
		},
	}
	e.intelEngine.SetConfig(cfg)

	if e.auditLogger != nil {
		e.auditLogger.Log(AuditLog{
			UserID:   "system",
			Username: "system",
			Action:   "create_db",
			DB:       name,
			Status:   "success",
		})
	}
	return nil
}

func (e *Engine) dropDBLocal(name string) error {
	path := e.dirMgr.DBPath(name)
	// CloseAndRemove only matches an exact key in the pool, but every
	// block's Badger handle is keyed by its own nested path (e.g.
	// "<db>/<block>/__data"), one or more levels below the DB root. A
	// DB with any blocks in it would leave those handles open, and
	// os.RemoveAll below would then try to delete files still held
	// open -- silently tolerated on Linux, but a hard error on
	// Windows ("The process cannot access the file because it is
	// being used by another process."). CloseAndRemoveForceByPrefix
	// closes every handle at or under path, so this actually covers
	// all of the DB's blocks.
	if closed := e.pool.CloseAndRemoveForceByPrefix(path); len(closed) > 0 {
		log().Info("Closed block DBs before drop", zap.String("db", name), zap.Strings("paths", closed))
	}

	if err := os.RemoveAll(path); err != nil {
		return err
	}

	log().Info("Database dropped", zap.String("db", name))
	e.intelEngine.mu.Lock()
	delete(e.intelEngine.configs, name)
	delete(e.intelEngine.stats, name)
	e.intelEngine.mu.Unlock()

	if e.auditLogger != nil {
		e.auditLogger.Log(AuditLog{
			UserID:   "system",
			Username: "system",
			Action:   "drop_db",
			DB:       name,
			Status:   "success",
		})
	}
	return nil
}

func (e *Engine) createBlockLocal(dbName, block string) error {
	if err := validateName(block); err != nil {
		return err
	}

	if !e.dirMgr.DBExists(dbName) {
		return ErrDBNotFound
	}

	if err := e.dirMgr.CreateBlockDirectories(dbName, block); err != nil {
		return err
	}

	meta := storage.BlockMeta{
		Name:      block,
		DB:        dbName,
		CreatedAt: time.Now().Unix(),
		DocCount:  0,
		SizeBytes: 0,
	}
	if err := e.dirMgr.SaveBlockMeta(dbName, block, meta); err != nil {
		return err
	}

	log().Info("Block created", zap.String("db", dbName), zap.String("block", block))
	return nil
}

func (e *Engine) dropBlockLocal(dbName, block string) error {
	lockKey := dbName + "\x00" + block
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	// A block has two separate Badger instances (__data and __index),
	// each under its own subpath below the block root. Closing only
	// BlockDataPath leaves __index's handle open, which then makes
	// RemoveBlockDirectories fail on Windows the same way DropDB used
	// to (see dropDBLocal). Close everything under the block root.
	blockPath := e.dirMgr.BlockPath(dbName, block)
	if closed := e.pool.CloseAndRemoveForceByPrefix(blockPath); len(closed) > 0 {
		log().Info("Closed block DBs before drop", zap.String("block", block), zap.Strings("paths", closed))
	}

	if err := e.dirMgr.RemoveBlockDirectories(dbName, block); err != nil {
		return err
	}

	log().Info("Block dropped", zap.String("db", dbName), zap.String("block", block))
	return nil
}

func (e *Engine) renameBlockLocal(dbName, oldName, newName string) error {
	if err := validateName(newName); err != nil {
		return err
	}
	if oldName == newName {
		return nil
	}

	lockKey := dbName + "\x00" + oldName
	e.lockManager.Lock(lockKey)
	defer e.lockManager.Unlock(lockKey)

	e.markRenaming(lockKey)
	defer e.unmarkRenaming(lockKey)

	if !e.dirMgr.BlockExists(dbName, oldName) {
		return ErrBlockNotFound
	}

	if e.dirMgr.BlockExists(dbName, newName) {
		return fmt.Errorf("block %s already exists", newName)
	}

	oldPath := e.dirMgr.BlockPath(dbName, oldName)
	newPath := e.dirMgr.BlockPath(dbName, newName)

	// renameDirRobust force-closes any open Badger handle under oldPath
	// (the block's __data and __index instances) before renaming, so a
	// block that is "en arranque" -- still open from a query, insert, or
	// startup load -- never blocks the OS rename with "Access is
	// denied"/"used by another process", and never corrupts the block:
	// the original directory is only removed once the data is confirmed
	// present at newPath.
	if err := e.renameDirRobust(oldPath, newPath); err != nil {
		return fmt.Errorf("rename block %s -> %s: %w", oldName, newName, err)
	}

	// The directory move alone isn't enough: document and secondary-index
	// keys stored inside the block's Badger instance also carry the old
	// block name as part of the key bytes, so they'd otherwise become
	// unreadable under the new name even though the data is physically
	// intact. This brings the stored keys in line with the new name.
	if err := e.migrateBlockKeyNames(dbName, oldName, newName); err != nil {
		return fmt.Errorf("rename block %s -> %s: directory renamed but key migration failed: %w", oldName, newName, err)
	}

	// EnsureBlockMeta both corrects the Name field (block.json's own
	// bytes still say oldName until rewritten -- moving the directory
	// doesn't touch its content) and rebuilds the file from scratch if
	// it was missing or unreadable to begin with, so RENAME BLOCK can no
	// longer leave a block that SHOW BLOCKS subsequently fails to
	// describe.
	if _, err := e.dirMgr.EnsureBlockMeta(dbName, newName); err != nil {
		log().Warn("Could not verify/repair block metadata after rename",
			zap.String("db", dbName), zap.String("old", oldName), zap.String("new", newName), zap.Error(err))
	}

	e.l1Cache.DeleteByPrefix(dbName + "/" + oldName + "/")

	log().Info("Block renamed",
		zap.String("db", dbName),
		zap.String("old", oldName),
		zap.String("new", newName))
	return nil
}
