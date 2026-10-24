package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/parse"
	"caimandb/internal/caimandb/storage"
	"caimandb/internal/caimandb/wal"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"os"
	"sort"
	"sync"
	"time"
)

// TxStatusCommitUnknown is a distinct Transaction.Status value, separate
// from "pending"/"committed"/"aborted": the durability of this
// transaction's tx_commit WAL entry could not be confirmed (Write()
// succeeded but the following Sync()/fsync failed). See
// commitDurabilityUnknownError's doc comment for the full reasoning.
//
// A transaction in this state must never be treated as abortable
// (Abort()'s "transaction not pending" guard already blocks this, since
// this status is never "pending") and none of its resources -- staged
// external blobs above all -- may be released until it resolves, either
// via the live reconciler (txReconciler) or, ultimately, via a full WAL
// replay (RecoverWAL) on the next restart.
const TxStatusCommitUnknown = "commit_unknown"

type Transaction struct {
	ID         string
	DB         string
	Block      string
	Status     string // "pending", "committed", "aborted"
	StartTime  time.Time
	Operations []TxOperation
	Mutations  []TxMutation
	mu         sync.RWMutex
	done       chan struct{}
	Version    int64
}

type TxOperation struct {
	Type    string // "insert", "update", "delete"
	ID      string
	Data    map[string]any
	Filters []Filter
	Update  *UpdateOp

	// SnapshotIDs is the set of document IDs Filters matched when this
	// operation was staged (Update/Delete only). validatePhantomReads
	// re-runs Filters at commit time and compares against this set --
	// see that function's doc comment for why this is what actually
	// distinguishes "repeatable_read" from "read_committed" in an engine
	// whose transactions have no explicit in-transaction Get.
	SnapshotIDs []string
}

type TxMutation struct {
	Key      []byte
	Value    []byte
	Type     string // "set", "delete"
	Document *Document
	OldValue []byte // For rollback

	// OldLargeID is the external-store blob ID that this mutation makes
	// unreferenced (the previous large-doc blob on an Update, or the
	// deleted document's blob on a Delete). It must not be removed from
	// external storage until the mutation is durably committed -- deleting
	// it during staging would break Abort()'s ability to leave the old
	// document/blob pair intact. Cleaned up from applyMutations() only
	// after the Badger WriteBatch has flushed successfully.
	OldLargeID string

	// NewLargeID is the external-store blob ID this mutation created during
	// staging (Insert or Update saving a large document). If the
	// transaction is aborted, this blob was never referenced by any
	// committed document and must be deleted to avoid leaking it forever.
	NewLargeID string
}

type TxManager struct {
	mu           sync.RWMutex
	transactions map[string]*Transaction
	wal          *wal.WAL
	engine       *Engine
	timeout      time.Duration
	maxRetries   int
	stopChan     chan struct{}
	cleanupTick  *time.Ticker
	reconciler   *txReconciler

	// isolation selects which of validateReadCommitted/
	// validateRepeatableRead/validateSerializable runs at commit time
	// (see validateMutation). Their actual guarantees, honestly stated:
	//   - "read_committed": a staged mutation conflicts if the target
	//     document's stored version has advanced past the version this
	//     transaction staged against. This is a real read-committed
	//     write-write conflict check.
	//   - "repeatable_read": the read_committed check above, PLUS a
	//     phantom-read guard for filter-based Update/Delete (see
	//     validatePhantomReads): the filter is re-evaluated at commit
	//     time and must match the same document set it matched at
	//     staging time. This does not provide full snapshot isolation --
	//     this engine's transactions have no explicit in-transaction Get,
	//     only staged mutations, so there is no broader read set to make
	//     repeatable beyond what a transaction's own Insert/Update/Delete
	//     calls already observe.
	//   - "serializable": the above, plus a check that no other
	//     currently-pending transaction has staged a mutation against the
	//     same document ID. This is single-key conflict detection, not a
	//     full serializability guarantee (no predicate locking, no
	//     dependency-graph/SSI cycle detection) -- it will not catch
	//     every write-skew anomaly between concurrent transactions that
	//     never touch the same document ID.
	isolation string
}

func NewTxManager(engine *Engine, wal *wal.WAL, timeout time.Duration) *TxManager {
	if timeout == 0 {
		timeout = defaultTransactionTimeout * time.Second
	}
	tm := &TxManager{
		transactions: make(map[string]*Transaction),
		engine:       engine,
		wal:          wal,
		timeout:      timeout,
		maxRetries:   3,
		stopChan:     make(chan struct{}),
		cleanupTick:  time.NewTicker(30 * time.Second),
		isolation:    "repeatable_read",
	}
	tm.reconciler = newTxReconciler(tm)
	safeGoLoop("tx_cleanup_loop", tm.cleanupLoop)
	return tm
}

// TODO(pendiente-motor): SAVEPOINT / ROLLBACK TO SAVEPOINT y
// LOCK BLOCK ... IN EXCLUSIVE MODE no existen todavía.
//
//   - SAVEPOINT: Transaction (arriba) no tiene noción de puntos
//     intermedios -- Mutations es una sola lista lineal. Necesitaría
//     un mapa savepoint-name -> índice en Mutations (y en Operations)
//     tomado en el momento de SAVEPOINT <nombre>, y que
//     ROLLBACK TO SAVEPOINT <nombre> trunque ambas listas a ese índice
//     en vez de abortar tx completo como hace Abort() hoy -- OJO: los
//     blobs externos liberados por releaseOrphanedBlobs() están
//     pensados para "toda la tx se aborta", habría que revisar qué
//     mutaciones parciales quedan huérfanas si el rollback es parcial.
//   - LOCK BLOCK ... IN EXCLUSIVE MODE: hoy no hay bloqueo explícito a
//     nivel de bloque que un cliente pueda pedir -- el control de
//     concurrencia es el que ya dan los niveles de aislamiento
//     (validateReadCommitted/validateRepeatableRead/
//     validateSerializable/validatePhantomReads más abajo en este
//     archivo). Un LOCK explícito necesitaría su propio mapa de locks
//     por bloque (bloque -> tx.ID actual dueño), tomado en Begin/al
//     ejecutar LOCK BLOCK y liberado en Commit/Abort, coordinado con
//     lockManager (ver el TODO de internal/caimandb como paquete único
//     en docs/known-limitations.md, que ya lista lockManager entre los
//     campos de Engine con más acoplamiento).
func (tm *TxManager) Begin(dbName, block string) (*Transaction, error) {
	if dbName == "" {
		return nil, fmt.Errorf("database name is required")
	}

	tx := &Transaction{
		ID:         newID(),
		DB:         dbName,
		Block:      block,
		Status:     "pending",
		StartTime:  time.Now(),
		Operations: make([]TxOperation, 0),
		Mutations:  make([]TxMutation, 0),
		done:       make(chan struct{}),
		Version:    1,
	}

	tm.mu.Lock()
	tm.transactions[tx.ID] = tx
	tm.mu.Unlock()

	log().Info("Transaction started",
		zap.String("tx_id", tx.ID),
		zap.String("db", dbName),
		zap.String("block", block),
		zap.String("isolation", tm.isolation))

	return tx, nil
}

// The three ways applyTxCommitLocal's WAL-then-apply pipeline can fail
// are NOT interchangeable, even though earlier code treated them as the
// same "committed, self-heals on restart" outcome. Each carries a
// different durability guarantee, and conflating them is exactly what
// caused this pipeline's crash-integrity bug: a transaction could be
// reported as "committed" (blocking Abort()'s external-blob cleanup
// forever) even when nothing about it -- not the WAL entry, not the
// document mutation -- was ever durable or ever will be, because
// WAL.Write() itself failed before anything was appended.
//
// commitLogWriteFailedError: WAL.Write() failed. Nothing was durably
// appended (a partial/corrupt line, if anything, which WAL recovery's
// per-entry checksum check discards). WAL replay will NEVER see or
// replay this transaction, on this restart or any future one -- so it
// is not just safe but necessary to treat this exactly like an Abort:
// release any external blobs the transaction staged, and never report
// it as committed.
type commitLogWriteFailedError struct{ cause error }

func (e *commitLogWriteFailedError) Error() string {
	return fmt.Sprintf("WAL commit write failed, transaction not durable: %v", e.cause)
}
func (e *commitLogWriteFailedError) Unwrap() error { return e.cause }

// commitDurabilityUnknownError: WAL.Write() succeeded (the entry was
// appended and flushed) but the subsequent WAL.Sync() (fsync) failed.
// Whether the entry actually reached disk is now genuinely unknown --
// some filesystems/OSes surface an fsync error after the data has
// already landed, some don't, and this process has no reliable way to
// tell which happened. This is the COMMIT_UNKNOWN / RECOVERY_REQUIRED
// state: the transaction must not be reported as committed (storage may
// not reflect it) NOR as aborted (the WAL entry might still be found and
// replayed on the next restart). All of its resources must be retained
// until it resolves -- see TxStatusCommitUnknown.
type commitDurabilityUnknownError struct{ cause error }

func (e *commitDurabilityUnknownError) Error() string {
	return fmt.Sprintf("WAL sync failed after write succeeded, commit durability unknown: %v", e.cause)
}
func (e *commitDurabilityUnknownError) Unwrap() error { return e.cause }

// commitApplyPendingError: the tx_commit WAL entry is confirmed durable
// (both Write() and Sync() succeeded) but applying the mutations to
// local storage (applyMutations, i.e. the Badger write) failed even
// after this function's own inline retries. Unlike the two errors
// above, this transaction genuinely IS committed -- WAL recovery is
// guaranteed to find and replay this entry on the next restart -- but
// forcing every recovery of this kind to wait for a full process
// restart is worse for availability than it needs to be, hence the live
// reconciler (see tx_reconciler.go) keeps retrying applyMutations in
// the background instead.
type commitApplyPendingError struct{ cause error }

func (e *commitApplyPendingError) Error() string {
	return fmt.Sprintf("transaction committed to WAL but local apply failed (will self-heal): %v", e.cause)
}
func (e *commitApplyPendingError) Unwrap() error { return e.cause }

func (tm *TxManager) Commit(tx *Transaction) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != "pending" {
		return fmt.Errorf("transaction already %s", tx.Status)
	}

	if len(tx.Mutations) == 0 {
		tx.Status = "committed"
		close(tx.done)
		return nil
	}

	log().Info("Committing transaction",
		zap.String("tx_id", tx.ID),
		zap.Int("mutations", len(tx.Mutations)))

	// Checked here, before anything touches the WAL, rather than from
	// inside applyMutations alongside validateReadCommitted's
	// version-conflict check: applyMutations can run again later against
	// a bare {ID, DB, Block, Mutations} reconstruction (Commit()'s own
	// retries, and the live tx reconciler in tx_reconciler.go) that
	// never carries tx.Operations, since a transaction that is
	// durably logged must never subsequently be rejected -- the WAL
	// entry already exists and will be replayed on recovery regardless.
	// A phantom-read conflict, like any other isolation-level rejection,
	// has to be caught before that point of no return, not after.
	if tm.isolation == "repeatable_read" || tm.isolation == "serializable" {
		if err := tm.validatePhantomReads(tx); err != nil {
			tx.Status = "aborted"
			tm.releaseOrphanedBlobs(tx)
			close(tx.done)
			metrics.MetricTransactionRollbacks.Inc()
			return err
		}
	}

	// Every other write path (Insert/Update/Delete/CreateDB/DropDB/
	// CreateBlock/DropBlock/RenameBlock, and their Turbo equivalents)
	// decides between the Raft-replicated route and a direct local write
	// via clusterWriteGate() before touching storage -- see
	// ErrNotRaftLeader's doc comment in cluster.go for why that shared
	// gate exists: without it, a non-leader follower silently persists a
	// write to its own local storage only, invisible to the rest of the
	// cluster and never present in the Raft log. Transactions must go
	// through the exact same gate at commit time (staging via
	// Begin/Insert/Update/Delete only computes intended mutations in
	// memory -- nothing is written to storage until here), or a
	// BEGIN...COMMIT run against a follower would hit precisely that bug.
	viaRaft, gateErr := tm.engine.clusterWriteGate()
	if gateErr != nil {
		return gateErr
	}

	if viaRaft {
		cmd := RaftCommand{
			Op:        "tx_commit",
			DB:        tx.DB,
			Block:     tx.Block,
			TxID:      tx.ID,
			Mutations: tx.Mutations,
		}
		cmdData, err := json.Marshal(cmd)
		if err != nil {
			tx.Status = "aborted"
			return fmt.Errorf("failed to encode transaction for Raft: %w", err)
		}

		future := tm.engine.cluster.raft.Apply(cmdData, raftTimeout)
		if err := future.Error(); err != nil {
			tx.Status = "aborted"
			return err
		}
		if respErr, ok := future.Response().(error); ok && respErr != nil {
			// future.Error() == nil means the command itself reached
			// consensus and is durable in the Raft log on a majority of
			// nodes -- only this node's own FSM.Apply (applyTxCommitLocal)
			// failed to reflect it in its local storage (e.g. this node's
			// disk full). The transaction's effect is real from the
			// cluster's point of view regardless of which of
			// applyTxCommitLocal's three stages failed locally, so --
			// committed, not aborted. Register it with the live
			// reconciler so this node catches up by retrying the local
			// apply in the background instead of only on its next
			// restart or a later Raft snapshot restore.
			tx.Status = "committed"
			close(tx.done)
			tm.reconciler.trackApplyPending(tx)
			log().Error("Transaction committed via Raft but local apply failed on this node; live reconciliation will retry",
				zap.String("tx_id", tx.ID), zap.Error(respErr))
			return fmt.Errorf("transaction committed but local apply failed on this node (retrying in background): %w", respErr)
		}
	} else if err := tm.engine.applyTxCommitLocal(tx.ID, tx.DB, tx.Block, tx.Mutations); err != nil {
		var logFailed *commitLogWriteFailedError
		var durUnknown *commitDurabilityUnknownError
		var applyPending *commitApplyPendingError

		switch {
		case errors.As(err, &logFailed):
			// Nothing about this transaction is or ever will be durable
			// (see commitLogWriteFailedError) -- this is not a commit
			// that "self-heals later", it simply did not happen. Treat
			// it exactly like an Abort: release any staged external
			// blobs now, rather than leaking them forever behind a
			// permanently-blocked Abort() (tx.Status would otherwise
			// never again be "pending").
			tx.Status = "aborted"
			tm.releaseOrphanedBlobs(tx)
			close(tx.done)
			metrics.MetricTransactionRollbacks.Inc()
			log().Error("Transaction commit failed before its WAL entry was written; transaction did not happen",
				zap.String("tx_id", tx.ID), zap.Error(err))
			return fmt.Errorf("transaction not committed: %w", err)

		case errors.As(err, &durUnknown):
			// Durability of the tx_commit WAL entry is genuinely
			// unknown. Do not report committed (storage may not
			// reflect it yet) or aborted (the entry might still be
			// replayed on restart) -- see TxStatusCommitUnknown. Every
			// resource this transaction holds (in particular any
			// staged external blob) must stay untouched until the live
			// reconciler or a future WAL replay resolves this.
			tx.Status = TxStatusCommitUnknown
			close(tx.done)
			tm.reconciler.trackUnknown(tx)
			log().Error("Transaction commit durability unknown (WAL sync failed after write succeeded); awaiting recovery",
				zap.String("tx_id", tx.ID), zap.Error(err))
			return fmt.Errorf("transaction commit durability unknown, awaiting recovery: %w", err)

		case errors.As(err, &applyPending):
			// The transaction is durably logged (WAL Write+Sync both
			// succeeded) -- WAL recovery is guaranteed to apply it on
			// the next restart regardless of what happens here. Mark it
			// committed so in-process readers agree with what recovery
			// will eventually converge to, and hand it to the live
			// reconciler so this process catches up without waiting
			// for a restart.
			tx.Status = "committed"
			close(tx.done)
			tm.reconciler.trackApplyPending(tx)
			log().Error("Transaction durably logged but local apply failed; live reconciliation will retry",
				zap.String("tx_id", tx.ID), zap.Error(err))
			return fmt.Errorf("transaction committed to WAL but local apply failed (retrying in background): %w", err)

		default:
			// Any other failure (e.g. the WAL payload itself could not
			// be encoded) happened before anything was attempted
			// against the WAL at all. Treat conservatively as not
			// committed, same as commitLogWriteFailedError.
			tx.Status = "aborted"
			tm.releaseOrphanedBlobs(tx)
			close(tx.done)
			metrics.MetricTransactionRollbacks.Inc()
			log().Error("Transaction commit failed before touching the WAL; transaction did not happen",
				zap.String("tx_id", tx.ID), zap.Error(err))
			return fmt.Errorf("transaction not committed: %w", err)
		}
	}

	tx.Status = "committed"
	close(tx.done)

	metrics.MetricTransactionCommits.Inc()

	if stats, ok := tm.engine.intelEngine.stats[tx.DB]; ok {
		stats.LastOptimized = time.Now()
	}

	log().Info("Transaction committed successfully",
		zap.String("tx_id", tx.ID),
		zap.Duration("duration", time.Since(tx.StartTime)))

	return nil
}

// applyTxCommitLocal durably logs (via this node's local WAL) then applies
// a transaction's mutation set to this node's local storage. It is the
// tx_commit equivalent of insertWithIDLocal/updateLocal/deleteLocal: called
// directly by TxManager.Commit on a single-node/no-cluster deployment, and,
// in a multi-node Raft cluster, once per node from RaftFSM.Apply's
// "tx_commit" case -- so every node ends up with its own local WAL entry
// and its own applied Badger state, exactly mirroring how every other write
// path's *Local function is shared between the direct and the
// Raft-replicated route.
//
// The WAL-ahead-of-data ordering here (log before apply) mirrors every
// other write path in this codebase, precisely so recovery has a durable
// record of anything that might have reached storage: writing the WAL
// entry after applying, as this used to do (before the WAL-ahead-of-data
// fix), leaves a window where a crash between the Badger flush and the WAL
// write is invisible to recovery.
func (e *Engine) applyTxCommitLocal(txID, db, block string, mutations []TxMutation) error {
	tm := e.txManager
	if len(mutations) == 0 {
		return nil
	}

	if tm.wal != nil {
		walData, err := json.Marshal(map[string]interface{}{
			"tx_id":     txID,
			"mutations": mutations,
			"db":        db,
			"block":     block,
			"timestamp": time.Now().Unix(),
		})
		if err != nil {
			// Nothing was attempted against the WAL yet -- same
			// guarantee as a Write() failure (see
			// commitLogWriteFailedError): this transaction cannot have
			// become durable.
			return &commitLogWriteFailedError{cause: fmt.Errorf("failed to encode transaction for WAL: %w", err)}
		}
		if err := tm.wal.Write("tx_commit", db, block, walData); err != nil {
			return &commitLogWriteFailedError{cause: err}
		}
		if err := tm.wal.Sync(); err != nil {
			return &commitDurabilityUnknownError{cause: err}
		}
	}

	tempTx := &Transaction{ID: txID, DB: db, Block: block, Mutations: mutations}

	var err error
	for retry := 0; retry < tm.maxRetries; retry++ {
		err = tm.applyMutations(tempTx)
		if err == nil {
			return nil
		}
		if retry < tm.maxRetries-1 {
			log().Warn("Transaction commit retry",
				zap.String("tx_id", txID),
				zap.Int("retry", retry+1),
				zap.Error(err))
			time.Sleep(time.Duration(retry+1) * 100 * time.Millisecond)
		}
	}
	// The WAL entry (if any) is already durable at this point -- only
	// the local apply is failing. See commitApplyPendingError.
	return &commitApplyPendingError{cause: err}
}

func (tm *TxManager) Abort(tx *Transaction) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != "pending" {
		return fmt.Errorf("transaction already %s", tx.Status)
	}

	tx.Status = "aborted"
	close(tx.done)

	// No lock release here: locks are only ever acquired inside
	// applyMutations(), which runs during Commit(), never during staging
	// (Insert/Update/Delete). An aborted transaction never reached
	// applyMutations, so it never held any locks -- calling
	// lockManager.Unlock() on keys that were never locked previously
	// caused an unrecoverable "sync: Unlock of unlocked RWMutex" fatal
	// error (not even a recoverable panic) on any Abort() that followed a
	// staged mutation. See releaseLocks' doc comment.

	// Any large-document blob saved to external storage while this
	// transaction was staging (Insert, or Update replacing a document's
	// blob) was never referenced by anything that got committed. Clean it
	// up now so aborting doesn't leak it permanently.
	tm.releaseOrphanedBlobs(tx)

	metrics.MetricTransactionRollbacks.Inc()

	log().Info("Transaction aborted",
		zap.String("tx_id", tx.ID),
		zap.Duration("duration", time.Since(tx.StartTime)))

	return nil
}

// releaseOrphanedBlobs deletes every external-store blob this transaction
// created during staging (Insert, or Update replacing a document's blob)
// that never became durably referenced by anything. Shared by Abort()
// and by Commit()'s commitLogWriteFailedError path -- the only two cases
// where this transaction is GUARANTEED to never be replayed by WAL
// recovery, which is what makes releasing these resources safe. Must
// never be called for a commitDurabilityUnknownError
// (TxStatusCommitUnknown): see that type's doc comment for why.
func (tm *TxManager) releaseOrphanedBlobs(tx *Transaction) {
	if tm.engine.externalStore == nil {
		return
	}
	for _, m := range tx.Mutations {
		if m.NewLargeID != "" {
			if err := tm.engine.externalStore.Delete(m.NewLargeID); err != nil {
				log().Warn("Failed to clean up orphaned external blob",
					zap.String("tx_id", tx.ID), zap.String("large_id", m.NewLargeID), zap.Error(err))
			}
		}
	}
}

// liveBlobRefs returns every external-store blob ID referenced by a
// NewLargeID across all transactions currently tracked in memory
// (pending, commit_unknown, or otherwise not yet reaped) -- see
// runBlobGC in blob_gc.go, which excludes these from garbage collection
// as a defense-in-depth backstop for blobGCMinAge's timing assumption.
func (tm *TxManager) liveBlobRefs() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var ids []string
	for _, tx := range tm.transactions {
		for _, m := range tx.Mutations {
			if m.NewLargeID != "" {
				ids = append(ids, m.NewLargeID)
			}
		}
	}
	return ids
}

func (tm *TxManager) applyMutations(tx *Transaction) error {
	db, err := tm.engine.pool.OpenBlock(tx.DB, tx.Block)
	if err != nil {
		return err
	}

	lockKeys := make([]string, len(tx.Mutations))
	for i, m := range tx.Mutations {
		lockKeys[i] = string(m.Key)
	}
	sort.Strings(lockKeys)

	for _, key := range lockKeys {
		tm.engine.lockManager.Lock(key)
	}

	defer func() {
		for _, key := range lockKeys {
			tm.engine.lockManager.Unlock(key)
		}
	}()

	for _, m := range tx.Mutations {
		if err := tm.validateMutation(tx, m); err != nil {
			return err
		}
	}

	wb := db.NewWriteBatch()
	defer wb.Cancel()

	for _, m := range tx.Mutations {
		if m.Type == "set" {
			if err := wb.Set(m.Key, m.Value); err != nil {
				return err
			}
		} else if m.Type == "delete" {
			if err := wb.Delete(m.Key); err != nil {
				return err
			}
		}
	}

	// Test-only crash hooks. They are inert unless the environment variable
	// is explicitly set by the external crash-recovery test worker. Keeping
	// the hook immediately around the durable Badger WriteBatch boundary lets
	// us test both sides of a transaction commit without weakening normal
	// production behavior.
	transactionChaosPoint("tx_before_flush")

	if err := wb.Flush(); err != nil {
		return err
	}

	transactionChaosPoint("tx_after_flush")

	// L1 cache is only updated now that wb.Flush() has confirmed the
	// mutations are durably committed to Badger -- not while merely
	// staging them into the WriteBatch above. Every other write path
	// (insertWithIDLocal, updateLocal, deleteLocal) already follows this
	// order for the same reason: updating the cache first meant that if
	// Flush() failed with a non-fatal error (disk full, a transient
	// Badger error that doesn't crash the process), the cache would keep
	// serving documents that were never actually persisted -- or, on a
	// delete, keep reporting a document gone that Badger still has --
	// until the next write to that key or a process restart. A crash
	// right here loses the in-memory cache along with everything else,
	// so it never exposed this; only a live, non-fatal Flush() failure
	// did.
	for _, m := range tx.Mutations {
		if m.Document == nil {
			continue
		}
		cacheKey := tm.engine.cacheKey(tx.DB, tx.Block, m.Document.ID)
		switch m.Type {
		case "set":
			tm.engine.l1Cache.Set(cacheKey, m.Value)
		case "delete":
			tm.engine.l1Cache.Del(cacheKey)
		}
	}

	// Now that the mutations are durably committed to Badger, any external
	// blob a mutation superseded (an Update's previous large-doc blob, or
	// a Delete's blob) is safe to remove -- doing this earlier, during
	// staging, was the bug: a later Abort() would have no way to restore a
	// blob that was already gone, breaking rollback.
	if tm.engine.externalStore != nil {
		for _, m := range tx.Mutations {
			if m.OldLargeID != "" && m.OldLargeID != m.NewLargeID {
				if err := tm.engine.externalStore.Delete(m.OldLargeID); err != nil {
					log().Warn("Failed to delete superseded external blob after commit",
						zap.String("tx_id", tx.ID), zap.String("large_id", m.OldLargeID), zap.Error(err))
				}
			}
		}
	}

	for _, m := range tx.Mutations {
		if m.Document != nil && m.Type == "set" {
			if err := tm.engine.buildSecondaryIndex(tx.DB, tx.Block, m.Document); err != nil {
				log().Warn("Failed to build index in transaction", zap.Error(err))
			}
			// Update FLEX-COLUMN
			if tm.engine.flexEngine != nil {
				tm.engine.flexEngine.InsertWithColumnar(tx.DB, tx.Block, m.Document)
			}
		}
	}

	for _, m := range tx.Mutations {
		if m.Document != nil {
			tm.engine.shardMgr.UpdateShardStats(m.Document.ShardID, 1, int64(len(m.Value)))
		}
	}

	return nil
}

func (tm *TxManager) validateMutation(tx *Transaction, m TxMutation) error {
	switch tm.isolation {
	case "read_committed":
		return tm.validateReadCommitted(tx, m)
	case "repeatable_read":
		return tm.validateRepeatableRead(tx, m)
	case "serializable":
		return tm.validateSerializable(tx, m)
	default:
		return tm.validateRepeatableRead(tx, m)
	}
}

func (tm *TxManager) validateReadCommitted(tx *Transaction, m TxMutation) error {
	if m.Document == nil {
		return nil
	}

	existing, ok := tm.engine.FindByID(tx.DB, tx.Block, m.Document.ID)
	if ok && m.Type == "set" {
		if existing.Meta.Version > m.Document.Meta.Version {
			return fmt.Errorf("version conflict: document %s was modified", m.Document.ID)
		}
	}
	return nil
}

func (tm *TxManager) validateRepeatableRead(tx *Transaction, m TxMutation) error {
	return tm.validateReadCommitted(tx, m)
}

// validatePhantomReads is what actually makes "repeatable_read"
// different from "read_committed" in this engine. Transactions here have
// no explicit in-transaction Get -- Insert/Update/Delete only stage
// mutations -- so validateReadCommitted's version check (does the
// document I already staged against still have the version I staged it
// against) is the only conflict check that applies to both isolation
// levels equally, which is exactly what the reviewer flagged: it made
// "repeatable_read" a no-op alias for "read_committed".
//
// The one place this engine DOES read at staging time without pinning
// what it read is a filter-based Update/Delete: Filters gets re-run
// against live storage every time it's evaluated, so a concurrent
// transaction inserting or deleting a document that also matches
// Filters -- a classic phantom read -- would otherwise go completely
// undetected between staging and commit. This re-runs every staged
// Update/Delete's Filters right before commit and requires the matched
// ID set to be unchanged from what was staged (TxOperation.SnapshotIDs).
func (tm *TxManager) validatePhantomReads(tx *Transaction) error {
	for _, op := range tx.Operations {
		if op.Filters == nil {
			continue
		}
		result, err := tm.engine.Find(tx.DB, tx.Block, &Query{
			Filters: op.Filters,
			Limit:   10000,
		})
		if err != nil {
			return err
		}
		nowIDs := make(map[string]struct{}, len(result.Docs))
		for _, doc := range result.Docs {
			nowIDs[doc.ID] = struct{}{}
		}
		if len(nowIDs) != len(op.SnapshotIDs) {
			return fmt.Errorf("repeatable read violated: a concurrent change altered which documents match this transaction's %s filter", op.Type)
		}
		for _, id := range op.SnapshotIDs {
			if _, ok := nowIDs[id]; !ok {
				return fmt.Errorf("repeatable read violated: a concurrent change altered which documents match this transaction's %s filter", op.Type)
			}
		}
	}
	return nil
}

func (tm *TxManager) validateSerializable(tx *Transaction, m TxMutation) error {
	if m.Document == nil {
		return nil
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	for id, otherTx := range tm.transactions {
		if id == tx.ID || otherTx.Status != "pending" {
			continue
		}
		for _, om := range otherTx.Mutations {
			if om.Document != nil && om.Document.ID == m.Document.ID {
				return fmt.Errorf("serializable conflict: document %s is being modified by transaction %s",
					m.Document.ID, id)
			}
		}
	}
	return nil
}

func (tm *TxManager) cleanupLoop() {
	for {
		select {
		case <-tm.cleanupTick.C:
			tm.cleanupExpired()
		case <-tm.stopChan:
			return
		}
	}
}

func (tm *TxManager) cleanupExpired() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()
	for id, tx := range tm.transactions {
		if tx.Status == "pending" && now.Sub(tx.StartTime) > tm.timeout {
			log().Warn("Transaction expired, aborting",
				zap.String("tx_id", id),
				zap.Duration("age", now.Sub(tx.StartTime)))
			go tm.Abort(tx)
		}
		// commit_unknown transactions are deliberately excluded here: an
		// unresolved one must stay in this map (and thus visible to the
		// live reconciler and to GetTx/Stats) indefinitely, not vanish
		// after an hour just because it hasn't resolved yet. It only
		// leaves this map once the reconciler (or a future recovery
		// pass) resolves it to committed or the process restarts.
		if tx.Status != "pending" && tx.Status != TxStatusCommitUnknown && now.Sub(tx.StartTime) > time.Hour {
			delete(tm.transactions, id)
		}
	}
}

func (tm *TxManager) Insert(tx *Transaction, data map[string]any) (*Document, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != "pending" {
		return nil, fmt.Errorf("transaction not active")
	}

	id := resolveDocID(data)
	if id == "" {
		id = newID()
	}
	data["id"] = id
	doc := &Document{
		ID:   id,
		Data: data,
		Meta: DocMeta{
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
			Version:   1,
		},
		ShardID: tm.engine.shardMgr.GetShardForDoc(tx.DB, tx.Block, id),
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	if int64(len(raw)) > tm.engine.config.MaxDocumentSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrDocTooLarge, len(raw))
	}

	if tm.engine.externalStore != nil && int64(len(raw)) > tm.engine.config.LargeDocThreshold {
		largeID, err := tm.engine.externalStore.Save(raw)
		if err != nil {
			return nil, err
		}
		doc.LargeID = largeID
		raw, _ = json.Marshal(doc)
	}

	if tm.engine.config.CompressLargeDocs && int64(len(raw)) > tm.engine.config.CompressThreshold {
		if comp, ok, err := storage.CompressData(raw, storage.CompressionZstd); err == nil && ok {
			raw = comp
		}
	}

	tx.Mutations = append(tx.Mutations, TxMutation{
		Key:        docKey(tx.Block, id),
		Value:      raw,
		Type:       "set",
		Document:   doc,
		NewLargeID: doc.LargeID,
	})

	tx.Operations = append(tx.Operations, TxOperation{
		Type: "insert",
		ID:   id,
		Data: data,
	})

	metrics.MetricDocSize.Observe(float64(len(raw)))

	return doc, nil
}

func (tm *TxManager) Update(tx *Transaction, filters []Filter, where *parse.Expr, op UpdateOp) (int64, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != "pending" {
		return 0, fmt.Errorf("transaction not active")
	}

	result, err := tm.engine.Find(tx.DB, tx.Block, &Query{
		Filters: filters,
		Where:   where,
		Limit:   10000,
	})
	if err != nil {
		return 0, err
	}

	if len(result.Docs) == 0 {
		return 0, nil
	}

	snapshotIDs := make([]string, 0, len(result.Docs))
	for _, doc := range result.Docs {
		snapshotIDs = append(snapshotIDs, doc.ID)
	}

	updated := 0
	for _, doc := range result.Docs {
		if tm.isModified(tx, doc.ID) {
			continue
		}

		oldRaw, _ := json.Marshal(doc)
		// Captured before applyUpdateOp: it only mutates doc.Data, never
		// doc.LargeID, but capturing explicitly here (rather than relying
		// on that) keeps this correct even if that changes later. This is
		// the blob the update is about to replace, if any.
		oldLargeID := doc.LargeID

		applyUpdateOp(&doc, op)
		doc.Meta.UpdatedAt = time.Now().Unix()
		doc.Meta.Version++

		newRaw, err := json.Marshal(doc)
		if err != nil {
			continue
		}

		var newLargeID string
		if tm.engine.externalStore != nil && int64(len(newRaw)) > tm.engine.config.LargeDocThreshold {
			largeID, err := tm.engine.externalStore.Save(newRaw)
			if err != nil {
				continue
			}
			doc.LargeID = largeID
			newLargeID = largeID
			newRaw, _ = json.Marshal(doc)
		}

		if tm.engine.config.CompressLargeDocs && int64(len(newRaw)) > tm.engine.config.CompressThreshold {
			if comp, ok, err := storage.CompressData(newRaw, storage.CompressionZstd); err == nil && ok {
				newRaw = comp
			}
		}

		tx.Mutations = append(tx.Mutations, TxMutation{
			Key:        docKey(tx.Block, doc.ID),
			Value:      newRaw,
			Type:       "set",
			Document:   &doc,
			OldValue:   oldRaw,
			OldLargeID: oldLargeID,
			NewLargeID: newLargeID,
		})

		updated++
	}

	// Record what Filters matched right now, at staging time -- see
	// validatePhantomReads (called at commit time under
	// "repeatable_read"/"serializable" isolation), which re-runs Filters
	// and requires the match set to still be exactly this one.
	tx.Operations = append(tx.Operations, TxOperation{
		Type:        "update",
		Filters:     filters,
		SnapshotIDs: snapshotIDs,
	})

	return int64(updated), nil
}

func (tm *TxManager) Delete(tx *Transaction, filters []Filter, where *parse.Expr) (int64, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != "pending" {
		return 0, fmt.Errorf("transaction not active")
	}

	result, err := tm.engine.Find(tx.DB, tx.Block, &Query{
		Filters: filters,
		Where:   where,
		Limit:   10000,
	})
	if err != nil {
		return 0, err
	}

	if len(result.Docs) == 0 {
		return 0, nil
	}

	snapshotIDs := make([]string, 0, len(result.Docs))
	for _, doc := range result.Docs {
		snapshotIDs = append(snapshotIDs, doc.ID)
	}

	deleted := 0
	for _, doc := range result.Docs {
		if tm.isModified(tx, doc.ID) {
			continue
		}

		oldRaw, _ := json.Marshal(doc)

		// The external blob is NOT deleted here during staging -- only
		// recorded via OldLargeID for applyMutations to remove after the
		// delete is durably committed. Deleting it eagerly, before the
		// transaction is known to commit, meant a later Abort() had no way
		// to bring the blob back, permanently breaking rollback for any
		// deleted large document.
		tx.Mutations = append(tx.Mutations, TxMutation{
			Key:        docKey(tx.Block, doc.ID),
			Type:       "delete",
			Document:   &doc,
			OldValue:   oldRaw,
			OldLargeID: doc.LargeID,
		})

		deleted++
	}

	tx.Operations = append(tx.Operations, TxOperation{
		Type:        "delete",
		Filters:     filters,
		SnapshotIDs: snapshotIDs,
	})

	return int64(deleted), nil
}

func (tm *TxManager) isModified(tx *Transaction, docID string) bool {
	for _, m := range tx.Mutations {
		if m.Document != nil && m.Document.ID == docID {
			return true
		}
	}
	return false
}

func (tm *TxManager) GetTx(id string) (*Transaction, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	tx, ok := tm.transactions[id]
	return tx, ok
}

func (tm *TxManager) GetActiveTransactions() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	active := make([]string, 0)
	for id, tx := range tm.transactions {
		if tx.Status == "pending" {
			active = append(active, id)
		}
	}
	return active
}

func (tm *TxManager) Stats() map[string]any {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	active := 0
	committed := 0
	aborted := 0

	for _, tx := range tm.transactions {
		switch tx.Status {
		case "pending":
			active++
		case "committed":
			committed++
		case "aborted":
			aborted++
		}
	}

	return map[string]any{
		"active":    active,
		"committed": committed,
		"aborted":   aborted,
		"total":     len(tm.transactions),
		"timeout":   tm.timeout.String(),
		"isolation": tm.isolation,
	}
}

func (tm *TxManager) Close() {
	close(tm.stopChan)
	if tm.cleanupTick != nil {
		tm.cleanupTick.Stop()
	}
	if tm.reconciler != nil {
		tm.reconciler.close()
	}
}

// transactionChaosPoint is deliberately a no-op in normal operation. The
// integration crash tests set CAIMANDB_TX_CRASH_POINT and wait for the
// corresponding marker before killing the worker process. This gives the
// tests a deterministic point immediately before/after the transaction's
// Badger WriteBatch flush.
func transactionChaosPoint(point string) {
	if os.Getenv("CAIMANDB_TX_CRASH_POINT") != point {
		return
	}
	fmt.Printf("CHAOS_POINT %s\n", point)
	_ = os.Stdout.Sync()
	for {
		time.Sleep(time.Second)
	}
}

// The following bridges are intentionally small and exist for the external
// crash-test worker. They keep the real transaction manager unexported while
// allowing a separate OS process to stage/commit a transaction.
func (e *Engine) TxManagerForTestBegin(db, block string) (*Transaction, error) {
	return e.txManager.Begin(db, block)
}
func (e *Engine) TxManagerForTestUpdate(tx *Transaction, filters []Filter, op UpdateOp) (int64, error) {
	return e.txManager.Update(tx, filters, nil, op)
}
func (e *Engine) TxManagerForTestDelete(tx *Transaction, filters []Filter) (int64, error) {
	return e.txManager.Delete(tx, filters, nil)
}
func (e *Engine) TxManagerForTestCommit(tx *Transaction) error {
	return e.txManager.Commit(tx)
}
