package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/storage"
	"caimandb/internal/caimandb/wal"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// defaultGenerateBatchFlushSize is the fallback used when
// Config.GenerateBatchFlushSize is 0 (unset). 2000 (up from the previous
// hardcoded 1000) buys fewer, larger Badger WriteBatch commits for bulk
// loads -- Badger amortizes its per-commit overhead (value-log sync,
// LSM compaction bookkeeping) better over bigger batches -- while still
// bounding a single in-flight WriteBatch's memory to a few thousand
// documents instead of the whole insert. Tune via GenerateBatchFlushSize
// in the config if your workload's document size makes 2000 too large or
// too small for comfortable memory use.
const defaultGenerateBatchFlushSize = 2000

func (e *Engine) generateBatchFlushSize() int {
	if e.config.GenerateBatchFlushSize > 0 {
		return e.config.GenerateBatchFlushSize
	}
	return defaultGenerateBatchFlushSize
}

// idFieldToString normalizes a raw "id"/"_id" field value (as decoded from
// JSON) into a usable string ID. Returns "" if the value is empty/unusable.
func idFieldToString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return ""
	}
}

// resolveDocID figures out which ID a document should use based on any
// user-supplied "id" and/or "_id" fields, mutating data as needed:
//   - {"id": X}                -> use X, "_id" (if any) is left untouched as a normal field
//   - {"_id": X}                -> "_id" is converted into "id": X (the "_id" key is removed)
//   - {"id": X, "_id": Y}       -> "id" (X) wins as the document ID; "_id" stays as plain extra data
//   - neither present/empty     -> caller should generate a new engine ID
func resolveDocID(data map[string]any) string {
	var idStr, uidStr string
	if v, ok := data["id"]; ok {
		idStr = idFieldToString(v)
	}
	if v, ok := data["_id"]; ok {
		uidStr = idFieldToString(v)
	}

	if idStr != "" {
		// "id" always takes priority; any "_id" present becomes plain extra data.
		return idStr
	}
	if uidStr != "" {
		// Lone "_id" is promoted to be the document's "id".
		delete(data, "_id")
		return uidStr
	}
	return ""
}

func (e *Engine) Insert(dbName, block string, data map[string]any) (*Document, error) {
	id := resolveDocID(data)
	if id == "" {
		id = newID()
	}
	data["id"] = id
	if err := e.checkUniqueConstraints(dbName, block, data, ""); err != nil {
		return nil, err
	}
	return e.insertWithID(dbName, block, id, data)
}

func (e *Engine) insertWithID(dbName, block, id string, data map[string]any) (*Document, error) {
	shardID := e.shardMgr.GetShardForDoc(dbName, block, id)

	viaRaft, err := e.clusterWriteGate()
	if err != nil {
		return nil, err
	}
	if viaRaft {
		cmd := RaftCommand{
			Op:      "insert",
			DB:      dbName,
			Block:   block,
			ID:      id,
			ShardID: shardID,
			Data:    data,
		}
		cmdData, err := json.Marshal(cmd)
		if err != nil {
			return nil, err
		}
		future := e.cluster.raft.Apply(cmdData, raftTimeout)
		if err := future.Error(); err != nil {
			return nil, err
		}
		if doc, ok := future.Response().(*Document); ok {
			return doc, nil
		}
		return nil, fmt.Errorf("invalid response from Raft")
	}
	return e.insertWithIDLocal(dbName, block, id, data, shardID)
}

func (e *Engine) InsertBatch(dbName, block string, docs []map[string]any) (int64, int64) {
	if len(docs) == 0 {
		return 0, 0
	}

	// Un lote más grande que MaxBatchSize se procesa en varios trozos de
	// MaxBatchSize en vez de truncarse: truncar significaba descartar en
	// silencio todo lo que pasara del límite (p. ej. insertar 1,000,000
	// de documentos de una vez con MaxBatchSize=10000 solo insertaba los
	// primeros 10,000 y perdía el resto). Cada trozo sigue el mismo
	// camino (write-batch de Badger, flush cada 1000) que antes, así que
	// el rendimiento por documento no cambia -- solo se deja de perder
	// datos en lotes grandes.
	maxChunk := e.config.MaxBatchSize
	if maxChunk <= 0 {
		maxChunk = 10000
	}
	if len(docs) > maxChunk {
		var inserted, errors int64
		for i := 0; i < len(docs); i += maxChunk {
			end := i + maxChunk
			if end > len(docs) {
				end = len(docs)
			}
			ins, errs := e.InsertBatch(dbName, block, docs[i:end])
			inserted += ins
			errors += errs
		}
		return inserted, errors
	}

	viaRaft, gateErr := e.clusterWriteGate()
	if gateErr != nil {
		log().Error("Rejecting insert_batch: not the raft leader", zap.Int("doc_count", len(docs)), zap.Error(gateErr))
		return 0, int64(len(docs))
	}
	if viaRaft {
		// Real batching under cluster: earlier this fell all the way
		// back to one e.Insert() (and therefore one raft.Apply(), i.e.
		// one consensus round-trip) per document, which threw away the
		// write-batch path entirely and made loading large volumes into
		// a cluster far slower than standalone mode. Instead, chunk the
		// batch and send each chunk through a single Raft command; the
		// FSM applies the whole chunk via insertBatchLocal (write-batch,
		// one WAL append per doc, one block-meta update) on every node,
		// leader included, so there's exactly one consensus round-trip
		// per chunk instead of per document.
		const raftBatchChunk = 1000
		var inserted, errors int64
		for i := 0; i < len(docs); i += raftBatchChunk {
			end := i + raftBatchChunk
			if end > len(docs) {
				end = len(docs)
			}
			chunk := docs[i:end]

			cmd := RaftCommand{
				Op:    "insert_batch",
				DB:    dbName,
				Block: block,
				Docs:  chunk,
			}
			cmdData, err := json.Marshal(cmd)
			if err != nil {
				errors += int64(len(chunk))
				continue
			}
			future := e.cluster.raft.Apply(cmdData, raftTimeout)
			if err := future.Error(); err != nil {
				log().Error("Raft apply failed for insert_batch chunk", zap.Error(err))
				errors += int64(len(chunk))
				continue
			}
			if res, ok := future.Response().(*BatchInsertResult); ok {
				inserted += res.Inserted
				errors += res.Errors
			} else {
				// Shouldn't happen, but don't silently under-report.
				errors += int64(len(chunk))
			}
		}
		return inserted, errors
	}

	return e.insertBatchLocal(dbName, block, docs)
}

// BatchInsertResult is the Raft FSM response for an "insert_batch" command,
// letting the leader (and the Apply() caller on it) learn the real
// inserted/error counts from the single Apply() covering the whole chunk,
// the same way a single insert's *Document is returned today.
type BatchInsertResult struct {
	Inserted int64
	Errors   int64
}

// insertBatchLocal performs the actual batched write to this node's local
// storage: write-batch inserts (flushed every 1000 documents), WAL-before-
// write per document, external storage for large documents, and inline
// (not detached) secondary-index/FLEX-COLUMN/change-bus updates so callers
// never observe a successful insert before it's queryable. Called directly
// in standalone mode, and from the Raft FSM (on every node) for clustered
// "insert_batch" commands.
func (e *Engine) insertBatchLocal(dbName, block string, docs []map[string]any) (int64, int64) {
	inserted, errs := e.insertBatchLocalDetailed(dbName, block, docs)
	var errCount int64
	for _, err := range errs {
		if err != nil {
			errCount++
		}
	}
	return inserted, errCount
}

// preparedDoc holds a document that has passed validation/marshal/
// compression and is waiting to be committed (WAL + Badger write-batch +
// index) as part of a bulk insert.
type preparedDoc struct {
	origIdx        int
	doc            Document
	rawLen         int64
	compressedData []byte
	walData        []byte
}

// prepareInsertDoc does the per-document work of a bulk insert that has no
// shared mutable state and therefore is safe to run concurrently across
// many documents at once: assigning/validating the ID, marshaling to JSON,
// spilling oversized documents to external storage, and optionally
// compressing. It touches neither the WAL nor Badger, so callers can fan
// this out across a worker pool and only serialize the actual commit step.
func (e *Engine) prepareInsertDoc(dbName, block string, origIdx int, data map[string]any, now int64) (*preparedDoc, error) {
	id := resolveDocID(data)
	if id == "" {
		id = newID()
	}
	data["id"] = id
	shardID := e.shardMgr.GetShardForDoc(dbName, block, id)

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
		return nil, fmt.Errorf("document exceeds maximum size of %d bytes", e.config.MaxDocumentSize)
	}

	if e.externalStore != nil && int64(len(raw)) > e.config.LargeDocThreshold {
		largeID, err3 := e.externalStore.Save(raw)
		if err3 != nil {
			return nil, err3
		}
		doc.LargeID = largeID
		raw, _ = json.Marshal(doc)
	}

	compressedData := raw
	if e.config.CompressLargeDocs && int64(len(raw)) > e.config.CompressThreshold {
		if comp, ok, err := storage.CompressData(raw, storage.CompressionZstd); err == nil && ok {
			compressedData = comp
		}
	}

	return &preparedDoc{
		origIdx:        origIdx,
		doc:            doc,
		rawLen:         int64(len(raw)),
		compressedData: compressedData,
		walData:        compressedData,
	}, nil
}

// insertBatchLocalDetailed is insertBatchLocal's implementation, plus a
// per-document []error (same order/length as docs, nil entry = that
// document was inserted successfully). insertBatchLocal itself just sums
// this into the aggregate (inserted, errors) pair it has always returned,
// so its behavior is unchanged; the detailed slice exists for callers that
// need to know *which* item failed, such as the turbo auto-batcher, which
// coalesces what look like independent single-document INSERTs into one
// call here and needs to route each one's own result back to its caller.
func (e *Engine) insertBatchLocalDetailed(dbName, block string, docs []map[string]any) (int64, []error) {
	errs := make([]error, len(docs))

	if err := e.dirMgr.CreateBlockDirectories(dbName, block); err != nil {
		for i := range errs {
			errs[i] = err
		}
		return 0, errs
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return 0, errs
	}

	var inserted atomic.Int64
	now := time.Now().Unix()
	var totalSize int64

	var docsToIndex []Document
	processed := 0

	// prepared[i] holds docs[i] once it's passed prep (nil if it failed
	// prep and errs[i] already explains why).
	prepared := make([]*preparedDoc, len(docs))

	// Prep -- JSON marshal, the optional external-store spill for
	// oversized docs, and optional zstd compression -- is pure per-document
	// work with no shared mutable state between documents, so it
	// parallelizes across cores with zero locking: each worker only ever
	// writes to its own docs[i]/prepared[i]/errs[i] slot. This used to run
	// inline in the same single-threaded loop as the WAL/Badger commit
	// work below; splitting it out is what actually puts more than one
	// core to work on a bulk insert instead of serializing everything
	// behind the block's single write lock.
	//
	// Deliberately done BEFORE the block lock is acquired below (it used
	// to be after): prep touches no Badger/WAL state, only this call's own
	// docs/prepared/errs slices, so it's just as safe to run unlocked --
	// and running it unlocked is what actually lets several concurrent
	// callers (e.g. GENERATE's worker goroutines, ops_turbo's batcher)
	// share the CPU-bound prep work for their *different* batches at the
	// same time, instead of queuing one whole call behind another's lock
	// before even starting to prep. The only thing that genuinely has to
	// be serialized per block is the WAL append + Badger WriteBatch commit
	// right below, since those touch shared on-disk state.
	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers > len(docs) {
		numWorkers = len(docs)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}
	if numWorkers == 1 {
		for i, data := range docs {
			pd, err2 := e.prepareInsertDoc(dbName, block, i, data, now)
			if err2 != nil {
				errs[i] = err2
				continue
			}
			prepared[i] = pd
		}
	} else {
		idxCh := make(chan int, numWorkers*2)
		var wg sync.WaitGroup
		wg.Add(numWorkers)
		for wnum := 0; wnum < numWorkers; wnum++ {
			go func() {
				defer wg.Done()
				for i := range idxCh {
					pd, err2 := e.prepareInsertDoc(dbName, block, i, docs[i], now)
					if err2 != nil {
						errs[i] = err2
						continue
					}
					prepared[i] = pd
				}
			}()
		}
		for i := range docs {
			idxCh <- i
		}
		close(idxCh)
		wg.Wait()
	}

	// The block lock (and the WriteBatch it protects) is acquired here,
	// right before the actual commit -- not up at the top of the function
	// anymore. This is the one piece of this call that genuinely must be
	// exclusive per block: the WAL append and the Badger WriteBatch both
	// touch shared on-disk state, so two concurrent commits into the same
	// block still have to happen one at a time. But now that's the ONLY
	// part that waits in line; every caller's (CPU-bound) prep phase above
	// already ran unlocked, so by the time a worker gets here its batch is
	// already fully prepared and ready to commit as fast as the previous
	// holder releases the lock, instead of only starting to prep once it's
	// already at the front of the queue.
	lockKey := dbName + "\x00" + block
	e.lockManager.Lock(lockKey)
	// NOT deferred anymore -- see the explicit Unlock calls below. This
	// used to hold the block's exclusive write lock for the entire rest
	// of the function, including the docsToIndex bookkeeping loop further
	// down (shard stats, change-bus publish, FLEX-COLUMN) which touches
	// none of the state this lock actually protects (the WAL append and
	// the Badger WriteBatch). For a 20000-doc IMPORT batch that meant
	// every other writer/reader of this block queued behind not just one
	// WAL+WriteBatch commit but also 20000 iterations of unrelated,
	// individually-locked work (shardMgr.UpdateShardStats alone takes its
	// own RLock per call), turning what should be a short exclusive
	// section into one several times longer. EnsureBlockMeta/
	// SaveBlockMeta right below DOES still need this lock: it's a
	// plain read-modify-write (meta.DocCount += ...) over a JSON file
	// with no locking of its own, so two concurrent batches into the
	// same block committing at the same time would lose one side's
	// update if it ran unlocked. The docsToIndex loop's callees
	// (shardMgr, changeBus, flexEngine) all already manage their own
	// internal synchronization and were only ever serialized here as a
	// side effect of the lock not being released early enough -- they
	// don't need it for correctness, only the code above them does.
	wb := db.NewWriteBatch()
	defer wb.Cancel()

	// commitAll writes every successfully-prepared document's WAL entry as
	// ONE WriteBatch call for the whole insert (one lock acquisition, one
	// encode pass, one fsync total instead of one per flush-chunk), then
	// -- only if that succeeded -- walks the prepared docs in original
	// order committing them to Badger in chunks of flushChunkSize (chunked
	// here purely to bound how large a single Badger WriteBatch grows in
	// memory; unrelated to WAL fsync grouping now that the two are
	// decoupled). See generateBatchFlushSize/Config.GenerateBatchFlushSize
	// to tune the chunk size.
	flushChunkSize := e.generateBatchFlushSize()

	commitAll := func() {
		var walEntries []wal.WALBatchEntry
		if e.wal != nil {
			walEntries = make([]wal.WALBatchEntry, 0, len(docs))
			for _, pd := range prepared {
				if pd == nil {
					continue
				}
				walEntries = append(walEntries, wal.WALBatchEntry{Op: "insert", DB: dbName, Block: block, Data: pd.walData})
			}
			if err := e.wal.WriteBatch(walEntries); err != nil {
				log().Error("WAL batch insert write failed, skipping insert",
					zap.Error(err), zap.Int("count", len(walEntries)))
				for _, pd := range prepared {
					if pd != nil {
						errs[pd.origIdx] = err
					}
				}
				return
			}
		}

		for _, pd := range prepared {
			if pd == nil {
				continue
			}
			if err2 := wb.Set(docKey(block, pd.doc.ID), pd.compressedData); err2 != nil {
				errs[pd.origIdx] = err2
				continue
			}

			// Index entries go into the SAME write batch as the document,
			// instead of a separate call that used to open and commit its
			// own Badger transaction per indexed field (see
			// buildSecondaryIndexInto's doc comment in index_secondary.go).
			if pd.rawLen < e.config.IndexSkipThreshold {
				e.buildSecondaryIndexInto(wb, block, &pd.doc)
			}

			e.l1Cache.Set(e.cacheKey(dbName, block, pd.doc.ID), pd.compressedData)
			docsToIndex = append(docsToIndex, pd.doc)
			inserted.Add(1)
			totalSize += pd.rawLen
			processed++

			if processed%flushChunkSize == 0 {
				if err2 := wb.Flush(); err2 != nil {
					log().Error("Batch flush failed", zap.Error(err2))
					return
				}
				wb = db.NewWriteBatch()
			}
		}
	}

	commitAll()

	if err2 := wb.Flush(); err2 != nil {
		log().Error("Final batch flush failed", zap.Error(err2))
		e.lockManager.Unlock(lockKey)
		return inserted.Load(), errs
	}

	if inserted.Load() > 0 {
		if meta, err := e.dirMgr.EnsureBlockMeta(dbName, block); err == nil {
			meta.DocCount += inserted.Load()
			meta.SizeBytes += totalSize
			meta.UpdatedAt = time.Now().Unix()
			e.dirMgr.SaveBlockMeta(dbName, block, meta)
		} else {
			log().Warn("Failed to update block metadata after batch insert; doc count/size may drift until the next rebuild",
				zap.String("db", dbName), zap.String("block", block), zap.Error(err))
		}
	}

	// Everything from here on (shard stats, change-bus, FLEX-COLUMN) is
	// self-synchronized and doesn't touch the WAL/Badger/block-meta state
	// the lock above protects, so release it now instead of holding it
	// through the whole bookkeeping loop below -- see the comment where
	// the lock was acquired.
	e.lockManager.Unlock(lockKey)

	// Indexing/FLEX-COLUMN/change-bus happen synchronously, before we
	// return, instead of in a detached goroutine. The previous
	// fire-and-forget goroutine let InsertBatch's caller see a successful
	// response before the secondary index existed, so a FIND on an
	// indexed field run immediately after a successful InsertBatch could
	// legitimately miss documents that were, from the client's point of
	// view, already committed. Doing this inline removes that
	// read-after-write race at the cost of InsertBatch's own latency,
	// which is the right trade for correctness. (Secondary index entries
	// themselves are now written above, inline with each document's own
	// write batch entry -- this loop only handles the non-Badger
	// bookkeeping: shard stats, the change bus, and FLEX-COLUMN.)
	for _, doc := range docsToIndex {
		e.shardMgr.UpdateShardStats(doc.ShardID, 1, 0)
		e.changeBus.PublishInsert(dbName, block, doc.ID, doc.ShardID, doc.Data)

		// Update FLEX-COLUMN
		if e.flexEngine != nil {
			e.flexEngine.InsertWithColumnar(dbName, block, &doc)
		}
	}

	e.opCount.Add(inserted.Load())
	metrics.MetricOpsTotal.WithLabelValues("insert_batch", dbName, block, "", e.nodeID, "success").Add(float64(inserted.Load()))
	metrics.MetricDocumentsTotal.WithLabelValues(dbName, block, "", e.nodeID).Add(float64(inserted.Load()))

	return inserted.Load(), errs
}
