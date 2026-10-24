package caimandb

import (
	"caimandb/internal/caimandb/parse"
	"caimandb/internal/caimandb/storage"
	"caimandb/internal/caimandb/wal"
	"encoding/json"
	"hash/crc32"

	"go.uber.org/zap"
)

// walTxCommitEntry mirrors the payload TxManager.Commit marshals into a
// "tx_commit" WAL entry (see transaction.go): the full set of raw
// key/value mutations a committed transaction made, exactly as they were
// about to be (or already were) applied to Badger. Replaying this list
// via applyMutations -- rather than re-running the UPDATE/DELETE filter
// that produced the transaction -- is what makes recovery exact: the
// mutations are the literal keys/values the transaction committed, not a
// query that has to be re-matched against whatever state storage happens
// to be in after a partial crash.
type walTxCommitEntry struct {
	TxID      string       `json:"tx_id"`
	Mutations []TxMutation `json:"mutations"`
	DB        string       `json:"db"`
	Block     string       `json:"block"`
	Timestamp int64        `json:"timestamp"`
}

// RecoverWAL replays WAL entries written since the last clean shutdown,
// applying them directly to engine's local storage paths.
//
// This used to be a method on *wal.WAL (WAL.RecoverApply). It now lives
// here, in the root package, because applying entries requires calling
// Engine's unexported local insert/update/delete paths — a dependency
// the low-level wal package must not have on the engine. wal only
// exposes the generic log primitives (ReadAll, IsClean, MarkRecovered,
// PruneToLastSegment) that this function composes.
func RecoverWAL(w *wal.WAL, engine *Engine) (int64, error) {
	if w.IsClean() {
		log().Info("WAL clean shutdown, skipping recovery")
		return 0, nil
	}

	entries, err := w.ReadAll()
	if err != nil {
		return 0, err
	}

	if len(entries) == 0 {
		return 0, nil
	}

	log().Info("Starting WAL recovery",
		zap.Int("entries", len(entries)))

	// Detach the WAL from the engine for the duration of this replay.
	// insertWithIDLocal/updateLocal/deleteLocal (and insertBatchLocal,
	// though tx_commit's applyMutations does not go through them) all
	// write to e.wal unconditionally as part of their normal path -- so
	// without this, every single replayed entry would sit in the
	// engine's normal WAL.Write() path, blocking until the write loop's
	// batcher actually flushes it.
	//
	// That block is real, per-call latency: this replay loop is
	// strictly serial (each entry must land before the next is
	// applied), so it never fills the writer's 100-entry size trigger.
	// Every entry ends up paying the batcher's quiet/cap window
	// (walQuietWindow/walLingerWindow, ~2-20ms -- see wal.go) on its
	// own, one blocking round-trip per entry. That is the entire cost
	// of a slow recovery: ~28s for 10k entries, ~150s for 50k, scaling
	// linearly with WAL size instead of being dominated by the actual
	// Badger/index work.
	//
	// It is also pure waste: the entries being replayed are, by
	// definition, already durably on disk -- that's the WAL segment
	// this function is reading them from. Re-appending each one to the
	// (now active) WAL buys no additional durability, and whatever
	// does get re-appended during this pass is discarded moments later
	// anyway: RotateAndPruneFresh (below, once replay finishes) starts
	// the post-recovery segment empty specifically because these
	// entries are "durably reflected in storage" the instant Badger's
	// write lands -- see its call site comment.
	//
	// Nil-ing engine.wal makes every local op's `if e.wal != nil` check
	// skip that write entirely, for free, without touching four
	// separate call sites or their unit tests. This is safe because
	// RecoverWAL runs single-threaded during startup, strictly before
	// the engine serves any request or accepts any write (see the two
	// call sites in engine_main.go) -- nothing else can observe or rely
	// on engine.wal while it's nil. w itself (the parameter used for
	// RotateAndPruneFresh/MarkRecovered below) is unaffected, since
	// it's a local variable, not read through engine.wal.
	realWAL := engine.wal
	engine.wal = nil
	defer func() { engine.wal = realWAL }()

	applied := int64(0)
	batchSize := 5000

	// touchedKeys tracks which cache keys this recovery pass has already
	// written, across the whole WAL (not just the current batch) --
	// collisions between an "insert" entry and a much later "tx_commit"
	// entry are exactly what this bug is about, so the window has to
	// stay open for the full replay, not just one batch.
	//
	// See the comment above the Wait() call below for why this map
	// exists at all and why it is only consulted for repeat writes to
	// the same key, not for every entry.
	touchedKeys := make(map[string]struct{}, 1024)

	// waitIfRewriting calls l1Cache.Wait() only when about to write one
	// of keys again after this recovery pass already wrote it earlier,
	// then records all of keys as touched (whether or not a Wait() was
	// needed). Called with the keys BEFORE they are actually applied.
	waitIfRewriting := func(keys ...string) {
		needsWait := false
		for _, k := range keys {
			if _, seen := touchedKeys[k]; seen {
				needsWait = true
				break
			}
		}
		if needsWait && engine.l1Cache != nil {
			engine.l1Cache.Wait()
		}
		for _, k := range keys {
			touchedKeys[k] = struct{}{}
		}
	}

	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}
		batch := entries[i:end]

		for _, entry := range batch {
			if crc32.ChecksumIEEE(entry.Data) != entry.Checksum {
				log().Warn("WAL entry checksum mismatch, skipping",
					zap.Uint64("id", entry.ID))
				continue
			}

			entryData := entry.Data
			if entry.Compressed {
				if decompressed, err := storage.DecompressData(entry.Data); err == nil {
					entryData = decompressed
				}
			}

			switch entry.Op {
			case "insert":
				var doc Document
				if err := json.Unmarshal(entryData, &doc); err != nil {
					continue
				}
				waitIfRewriting(engine.cacheKey(entry.DB, entry.Block, doc.ID))
				if _, err := engine.insertWithIDLocal(entry.DB, entry.Block, doc.ID, doc.Data, doc.ShardID); err != nil {
					log().Warn("WAL insert recovery failed", zap.Error(err))
					continue
				}
				applied++
			case "update":
				var op UpdateOp
				if err := json.Unmarshal(entryData, &op); err != nil {
					continue
				}
				// Unlike "insert" and "tx_commit", a plain (non-
				// transactional) update's target keys aren't known until
				// its filter is evaluated against current data -- there's
				// no cheap way to list them up front the way there is for
				// the other two cases. Rather than skip the safety check
				// for this op (or plumb the matched-ID list back out of
				// updateLocal), treat it as touching everything seen so
				// far: if this recovery pass has written anything yet,
				// Wait() once before this op, then start the touched set
				// over, since everything before this point is now
				// guaranteed flushed and doesn't need tracking anymore.
				// This op type doesn't appear in the crash-recovery path
				// this fix targets (TxManager routes UPDATE/DELETE through
				// "tx_commit" -- see transaction.go), so it costs at most
				// one Wait() per plain-update entry actually replayed.
				if len(touchedKeys) > 0 && engine.l1Cache != nil {
					engine.l1Cache.Wait()
					touchedKeys = make(map[string]struct{}, 1024)
				}
				if _, err := engine.updateLocal(entry.DB, entry.Block, nil, nil, op, ""); err != nil {
					log().Warn("WAL update recovery failed", zap.Error(err))
					continue
				}
				applied++
			case "delete":
				var filters []Filter
				var where *parse.Expr
				var wrapped walDeleteEntry
				if err := json.Unmarshal(entryData, &wrapped); err == nil {
					// Current format: {"filters": [...], "where": {...}}.
					filters = wrapped.Filters
					where = wrapped.Where
				} else if err := json.Unmarshal(entryData, &filters); err != nil {
					// Pre-migration format: a bare []Filter JSON array,
					// from a WAL segment written before walDeleteEntry
					// existed. filters is reused as-is; where stays nil,
					// so deleteLocal falls back to the flat evaluator --
					// exactly how this entry would have been replayed
					// before this migration.
					continue
				}
				// delete's target keys aren't known ahead of its filter
				// evaluation, so treat it as touching everything written
				// so far in this pass.
				if len(touchedKeys) > 0 && engine.l1Cache != nil {
					engine.l1Cache.Wait()
					touchedKeys = make(map[string]struct{}, 1024)
				}
				if _, err := engine.deleteLocal(entry.DB, entry.Block, filters, where, ""); err != nil {
					log().Warn("WAL delete recovery failed", zap.Error(err))
					continue
				}
				applied++
			case "tx_commit":
				// Transaction commits are logged as one entry holding every
				// raw key/value mutation the transaction made (see
				// TxManager.Commit), not as individual insert/update/delete
				// entries. Replay them the same way a live commit applies
				// them -- via applyMutations -- so recovery reproduces
				// exactly what the transaction did, including on top of
				// older insert/update/delete entries earlier in this same
				// WAL (e.g. the seed inserts a transaction's UPDATE/DELETE
				// ran against): entries are replayed strictly in log order,
				// so this entry correctly supersedes those.
				var txEntry walTxCommitEntry
				if err := json.Unmarshal(entryData, &txEntry); err != nil {
					log().Warn("WAL tx_commit recovery failed to decode", zap.Error(err))
					continue
				}
				if len(txEntry.Mutations) == 0 {
					applied++
					continue
				}
				mutationKeys := make([]string, 0, len(txEntry.Mutations))
				for _, m := range txEntry.Mutations {
					if m.Document != nil {
						mutationKeys = append(mutationKeys, engine.cacheKey(entry.DB, entry.Block, m.Document.ID))
					}
				}
				waitIfRewriting(mutationKeys...)
				tempTx := &Transaction{
					ID:        txEntry.TxID,
					DB:        entry.DB,
					Block:     entry.Block,
					Mutations: txEntry.Mutations,
				}
				if err := engine.txManager.applyMutations(tempTx); err != nil {
					log().Warn("WAL tx_commit recovery failed to apply", zap.Error(err))
					continue
				}
				applied++
			}
		}

		if i > 0 && i%10000 == 0 {
			log().Info("WAL recovery progress",
				zap.Int("processed", i),
				zap.Int("total", len(entries)),
				zap.Int64("applied", applied))
		}
	}

	// One final Wait() after the loop: whatever the last entries wrote
	// to the L1 cache must be visible to the engine's very first reads
	// once RecoverWAL returns and startup continues (e.g. the caller
	// immediately serving requests), not just to a later entry that
	// happens to rewrite the same key.
	if engine.l1Cache != nil {
		engine.l1Cache.Wait()
	}

	if applied > 0 {
		// w.MarkRecovered() is kept in memory only, so a second in-process
		// call to RecoverApply (engine startup calls it once directly and
		// once more from InitCluster when auto-cluster is on) doesn't
		// re-apply the same entries. It is intentionally NOT written to
		// the .clean marker file: that file may only be written by a
		// graceful Close() (see checkCleanliness for why persisting it
		// here would cause silent data loss on a later crash).
		w.MarkRecovered()

		// The entries we just applied are now durably reflected in
		// storage. The segment we read them from is still the *active*
		// segment (new writes are about to be appended to it), so a plain
		// PruneToLastSegment would leave those already-applied entries on
		// disk ahead of the new ones — replaying them again on a second,
		// pre-Close crash. RotateAndPruneFresh closes that window by
		// starting the post-recovery segment empty: insert is idempotent
		// by ID, but update ops (e.g. increments) are not, so this
		// matters. See its doc comment in wal.go for the full story.
		if err := w.RotateAndPruneFresh(); err != nil {
			log().Warn("Failed to rotate WAL to a fresh segment after recovery; "+
				"already-applied entries remain on disk and could be replayed "+
				"again on a subsequent crash before the next clean shutdown",
				zap.Error(err))
		}
	}

	log().Info("WAL recovery completed",
		zap.Int64("applied", applied),
		zap.Int("entries", len(entries)))
	return applied, nil
}
