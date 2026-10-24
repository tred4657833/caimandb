package caimandb

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// txReconciler drives live recovery of transactions Commit() could not
// immediately resolve, without waiting for a process restart. A full WAL
// replay (RecoverWAL, run at startup) remains the authoritative backstop
// for everything this reconciler tracks -- if the process crashes before
// this reconciler resolves something, recovery is what actually
// guarantees correctness. This reconciler exists purely so a live,
// still-running process doesn't leave an operation dangling until its
// next restart just because one attempt hit a transient failure.
//
// It tracks exactly the two outcomes Commit() can leave unresolved (see
// commitDurabilityUnknownError and commitApplyPendingError in
// transaction.go):
//
//   - unknown (TxStatusCommitUnknown): the tx_commit WAL entry was handed
//     to Write() but the following Sync() (fsync) failed, so durability
//     of the *log entry* is unconfirmed. Resolved by retrying Sync():
//     once it succeeds, the entry is confirmed durable and the
//     transaction's mutations are applied, exactly like a normal commit.
//   - pending (still "committed", but not yet applied locally): the
//     tx_commit WAL entry is confirmed durable, but applyMutations()
//     (the Badger write) failed even after Commit()'s own inline
//     retries. Resolved by retrying applyMutations() until it succeeds.
//
// Both queues use capped exponential backoff per transaction, so a
// persistent failure (e.g. a genuinely full disk) logs periodically
// instead of spinning the ticker or spamming the log on every tick.
type txReconciler struct {
	tm       *TxManager
	tick     *time.Ticker
	stopChan chan struct{}

	mu      sync.Mutex
	unknown map[string]*reconcileState
	pending map[string]*reconcileState
}

type reconcileState struct {
	tx       *Transaction
	attempts int
	nextTry  time.Time
}

func newTxReconciler(tm *TxManager) *txReconciler {
	r := &txReconciler{
		tm:       tm,
		tick:     time.NewTicker(2 * time.Second),
		stopChan: make(chan struct{}),
		unknown:  make(map[string]*reconcileState),
		pending:  make(map[string]*reconcileState),
	}
	safeGoLoop("tx_reconcile_loop", r.loop)
	return r
}

// trackUnknown registers a transaction whose commit durability is
// unknown (commitDurabilityUnknownError) for live resolution.
func (r *txReconciler) trackUnknown(tx *Transaction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unknown[tx.ID] = &reconcileState{tx: tx}
}

// trackApplyPending registers a durably-logged transaction whose local
// apply failed (commitApplyPendingError, or a Raft-path local apply
// failure) for live retry.
func (r *txReconciler) trackApplyPending(tx *Transaction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[tx.ID] = &reconcileState{tx: tx}
}

func (r *txReconciler) loop() {
	for {
		select {
		case <-r.tick.C:
			r.tryResolveAll()
		case <-r.stopChan:
			return
		}
	}
}

func (r *txReconciler) close() {
	close(r.stopChan)
	r.tick.Stop()
}

// reconcileBackoff returns a per-transaction retry delay that doubles
// with each failed attempt (1s, 2s, 4s, ... capped at 2 minutes), so a
// transaction stuck behind a long-lived failure (a genuinely full disk,
// say) is retried with decreasing frequency instead of hammering the
// same failing operation every 2-second tick forever.
func reconcileBackoff(attempts int) time.Duration {
	shift := attempts
	if shift > 6 {
		shift = 6
	}
	d := time.Duration(1<<uint(shift)) * time.Second
	if d > 2*time.Minute {
		d = 2 * time.Minute
	}
	return d
}

func (r *txReconciler) tryResolveAll() {
	now := time.Now()

	r.mu.Lock()
	var unknownDue, pendingDue []*reconcileState
	for _, st := range r.unknown {
		if st.nextTry.IsZero() || now.After(st.nextTry) {
			unknownDue = append(unknownDue, st)
		}
	}
	for _, st := range r.pending {
		if st.nextTry.IsZero() || now.After(st.nextTry) {
			pendingDue = append(pendingDue, st)
		}
	}
	r.mu.Unlock()

	for _, st := range unknownDue {
		r.resolveUnknown(st)
	}
	for _, st := range pendingDue {
		r.resolveApplyPending(st)
	}
}

// resolveUnknown attempts to confirm durability of a commit_unknown
// transaction's WAL entry by retrying Sync(). If that succeeds, the
// mutations are applied exactly as a normal commit would. Applying here
// is safe even if the original Sync() failure turns out to have been a
// false negative (data already on disk): applyMutations is idempotent
// (Badger Set/Delete, secondary-index writes, and blob cleanup all
// tolerate being repeated), so a future WAL replay re-applying the same
// entry after a crash is harmless.
func (r *txReconciler) resolveUnknown(st *reconcileState) {
	tx := st.tx
	tm := r.tm

	if tm.wal == nil {
		// Should not happen: commit_unknown only ever arises from a
		// Sync() failure, which requires a WAL to have been configured
		// in the first place. Nothing to resolve either way.
		r.mu.Lock()
		delete(r.unknown, tx.ID)
		r.mu.Unlock()
		return
	}

	if err := tm.wal.Sync(); err != nil {
		r.recordFailure(tx.ID, st, err,
			"commit_unknown: WAL sync retry still failing, transaction remains unresolved")
		return
	}

	tempTx := &Transaction{ID: tx.ID, DB: tx.DB, Block: tx.Block, Mutations: tx.Mutations}
	if err := tm.applyMutations(tempTx); err != nil {
		// The log entry's durability is now confirmed -- this is no
		// longer ambiguous, just a plain apply failure. Hand it to the
		// apply-pending queue instead of continuing to retry it here as
		// unknown.
		r.mu.Lock()
		delete(r.unknown, tx.ID)
		r.pending[tx.ID] = &reconcileState{tx: tx}
		r.mu.Unlock()
		log().Warn("commit_unknown resolved to durable, but local apply failed; moved to apply-pending retry",
			zap.String("tx_id", tx.ID), zap.Error(err))
		return
	}

	tx.mu.Lock()
	tx.Status = "committed"
	tx.mu.Unlock()

	r.mu.Lock()
	delete(r.unknown, tx.ID)
	r.mu.Unlock()

	log().Info("Transaction commit_unknown resolved to committed via live reconciliation",
		zap.String("tx_id", tx.ID))
}

// resolveApplyPending retries applying a durably-logged transaction's
// mutations to local storage.
func (r *txReconciler) resolveApplyPending(st *reconcileState) {
	tx := st.tx
	tempTx := &Transaction{ID: tx.ID, DB: tx.DB, Block: tx.Block, Mutations: tx.Mutations}

	if err := r.tm.applyMutations(tempTx); err != nil {
		r.recordFailure(tx.ID, st, err,
			"apply-pending: local apply retry still failing, transaction remains unresolved")
		return
	}

	r.mu.Lock()
	delete(r.pending, tx.ID)
	r.mu.Unlock()

	log().Info("Transaction apply-pending resolved via live reconciliation",
		zap.String("tx_id", tx.ID))
}

// recordFailure bumps the retry counter/backoff for a still-unresolved
// transaction and logs at a decreasing frequency (every attempt at
// first, then every 5th) so a long-lived failure doesn't spam the log
// once every 2-second tick forever.
func (r *txReconciler) recordFailure(id string, st *reconcileState, err error, msg string) {
	r.mu.Lock()
	st.attempts++
	st.nextTry = time.Now().Add(reconcileBackoff(st.attempts))
	attempts := st.attempts
	r.mu.Unlock()

	if attempts <= 1 || attempts%5 == 0 {
		log().Warn(msg, zap.String("tx_id", id), zap.Int("attempts", attempts), zap.Error(err))
	}
}
