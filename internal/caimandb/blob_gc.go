package caimandb

import (
	"encoding/json"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

// blobGCMinAge is how old an external-store blob must be before a GC
// sweep will even consider deleting it. This is deliberately much larger
// than any realistic transaction lifetime (TxManager.timeout defaults to
// well under an hour) so the sweep can never race a transaction that
// saved a blob during Insert/Update staging and simply hasn't committed
// yet -- by the time a blob is old enough to be a GC candidate, any
// transaction that could still legitimately reference it has long since
// either committed (referencing it from a document) or been aborted/
// timed out (which already deletes it via releaseOrphanedBlobs).
const blobGCMinAge = 2 * time.Hour

// blobGCInterval is how often the sweep runs. Orphaned blobs only ever
// come from a crash between a transaction staging a blob and that
// transaction reaching Commit()/Abort() -- not a routine occurrence --
// so there is no benefit to running this any more often, and every run
// costs a full document scan of every block.
const blobGCInterval = 1 * time.Hour

func (e *Engine) startBlobGC() {
	if e.externalStore == nil {
		return
	}
	safeGoLoop("blob_gc_loop", e.blobGCLoop)
}

func (e *Engine) blobGCLoop() {
	ticker := time.NewTicker(blobGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.runBlobGC()
		case <-e.stopChan:
			return
		}
	}
}

// runBlobGC deletes every external-store blob that is (a) older than
// blobGCMinAge and (b) not referenced by any document's LargeID in any
// database/block. It never touches a document, and it only ever deletes
// a blob file -- worst case on any mistake here is a slower-than-
// necessary disk-space reclaim, never data loss for anything actually
// referenced (see blobGCMinAge for why staged-but-not-yet-committed
// blobs are never candidates in the first place).
func (e *Engine) runBlobGC() {
	blobs, err := e.externalStore.List()
	if err != nil {
		log().Warn("Blob GC: failed to list external store", zap.Error(err))
		return
	}
	if len(blobs) == 0 {
		return
	}

	cutoff := time.Now().Add(-blobGCMinAge)
	candidates := make(map[string]struct{})
	for _, b := range blobs {
		if b.ModTime.Before(cutoff) {
			candidates[b.ID] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		return
	}

	dbs, err := e.ShowDBs()
	if err != nil {
		log().Warn("Blob GC: failed to list databases", zap.Error(err))
		return
	}
	for _, dbName := range dbs {
		blocks, err := e.ShowBlocks(dbName)
		if err != nil {
			log().Warn("Blob GC: failed to list blocks", zap.String("db", dbName), zap.Error(err))
			continue
		}
		for _, block := range blocks {
			e.unmarkReferencedBlobs(dbName, block, candidates)
			if len(candidates) == 0 {
				break
			}
		}
		if len(candidates) == 0 {
			break
		}
	}

	if len(candidates) == 0 {
		return
	}

	// Defense in depth against blobGCMinAge's assumption: a transaction
	// that is still open in memory (pending, or stuck unresolved in
	// commit_unknown/apply-pending -- see tx_reconciler.go) can hold a
	// NewLargeID that is not yet visible in any document via the block
	// scan above, however unlikely it is for one to still be open past
	// blobGCMinAge. Exclude those explicitly rather than relying on the
	// age margin alone.
	if e.txManager != nil {
		for _, id := range e.txManager.liveBlobRefs() {
			delete(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return
	}

	deleted := 0
	for id := range candidates {
		if err := e.externalStore.Delete(id); err != nil {
			log().Warn("Blob GC: failed to delete orphaned blob", zap.String("large_id", id), zap.Error(err))
			continue
		}
		deleted++
	}
	if deleted > 0 {
		log().Info("Blob GC: removed orphaned external blobs", zap.Int("count", deleted))
	}
}

// unmarkReferencedBlobs streams every document in one block and removes
// any blob ID it references (doc.LargeID) from candidates, the same
// document-scan discipline RebuildBlock/ReindexFlexColumn use (never
// materializing a whole block in memory at once).
func (e *Engine) unmarkReferencedBlobs(dbName, block string, candidates map[string]struct{}) {
	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		log().Warn("Blob GC: failed to open block", zap.String("db", dbName), zap.String("block", block), zap.Error(err))
		return
	}

	docPrefix := docKeyPrefix(block)
	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(docPrefix); it.ValidForPrefix(docPrefix); it.Next() {
			if len(candidates) == 0 {
				return nil
			}
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				continue
			}
			if doc.LargeID != "" {
				delete(candidates, doc.LargeID)
			}
		}
		return nil
	})
	if err != nil {
		log().Warn("Blob GC: failed to scan block", zap.String("db", dbName), zap.String("block", block), zap.Error(err))
	}
}
