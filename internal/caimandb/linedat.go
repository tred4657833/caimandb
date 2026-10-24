package caimandb

// LineDat is CaimanDB's temporal high-speed ingestion buffer. It is not a
// separate service: it's one more step inside the same Engine's insertion
// pipeline, sitting between a caller's INSERT/GENERATE/IMPORT and the
// permanent storage commit (insertBatchLocalDetailed -- the exact same
// WAL+Badger path every insert has always used).
//
//	Aplicación
//	    v
//	  LineDat            <- accepts the write, durably journals it,
//	    v                   acknowledges the caller almost instantly
//	Bloques temporales   <- in-memory + on-disk pending queue, per (db,block)
//	    v
//	 CaimanDB            <- background workers drain the queue into
//	                         Badger+WAL progressively, in batches
//
// Why: loading a large volume (e.g. INSERT ... GENERATE 1000000) no longer
// has to block the caller for the whole commit -- LineDat can keep
// receiving documents while CaimanDB catches up in the background, so the
// user/application can keep working immediately, even though the actual
// permanent write keeps happening continuously underneath.
//
// Reads stay consistent while a transfer is in flight: FindByID and Find
// (see ops_find.go) transparently check LineDat for anything that hasn't
// reached CaimanDB yet, so a FIND run right after an INSERT still sees the
// document even if it's technically still sitting in a temporary block.
//
// LineDat also doubles as a crash-safety net for the transfer itself: every
// accepted document is appended to an on-disk journal before being handed
// back to the caller. If the process dies before a document is confirmed
// committed to CaimanDB, the journal still has it; newLineDat replays that
// journal on the next startup and the background drain workers pick up
// right where they left off. Once every pending document in a shard has
// been confirmed committed, that shard's journal is cleared -- there is
// nothing left to recover.
//
// Concurrency design (sharded, no single global lock):
//
// A single mutex guarding every block's pending queue AND every journal
// write is the one thing that would turn "massive" into "serialized" --
// every insert, regardless of which block or client it came from, would
// queue up behind the same lock and the same fsync. So LineDat is split
// into LineDatShards independent lanes, each with its own mutex, its own
// in-memory queues and its own on-disk journal file. A document's (db,
// block) always hashes to the same lane, so a single block is still
// always drained by exactly one worker at a time (order within a block is
// preserved), but different blocks -- or different high-rate clients
// writing to different blocks -- now make progress in parallel instead of
// funneling through one lock. Within a shard, each block keeps a real FIFO
// (a doubly linked list + an id->element index) instead of a bare map, so
// draining a block pulls its next batch in O(batch size), not by scanning
// every pending document in that block on every drain tick.
//
// Resource management: LineDat is deliberately NOT an unbounded buffer.
// Two global caps -- LineDatMaxPending (document count) and
// LineDatMaxPendingBytes (approximate memory), tracked with lock-free
// atomics across every shard -- bound how much can sit in the temporary
// blocks at once. Under ordinary load neither cap is ever reached and
// every insert keeps acknowledging in the microsecond range ("por arriba,
// en segundos"). Under sustained massive load that outruns CaimanDB's
// commit rate, EnqueueDocs/EnqueueBatch briefly slow the caller down
// (waitForCapacity) instead of growing memory without limit -- the queue
// self-regulates to whatever rate the background transfer can sustain
// ("abajo procesa", permanently, at a pace the node can actually afford).
// The background transfer itself runs on a small fixed-size worker pool
// (LineDatDrainWorkers, not one goroutine per block and not unbounded), so
// a sudden burst of many concurrently written blocks can't spawn
// arbitrarily many goroutines either.
import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// maxLineDatAttempts bounds how many times the background drain workers
// retry a document that keeps failing to commit (e.g. it genuinely
// exceeds MaxDocumentSize) before giving up on it. Without a ceiling, one
// permanently-invalid document would retry forever, occupying a slot in
// its block's batch every drain cycle. The document is logged and counted
// in Stats()["dropped"] when this happens -- it is not silently lost from
// the logs, only from the queue.
const maxLineDatAttempts = 5

// lineDatJournalCompactThreshold is the per-shard on-disk journal size
// (bytes) past which the next drain touching that shard compacts it down
// to just the still-pending "put" records, dropping the accumulated
// "done" history. Keeps a long-running node's journals bounded instead of
// growing forever.
const lineDatJournalCompactThreshold = 16 << 20

// Resource-management defaults, used whenever the matching Config field
// is left at zero. See the package doc comment above for the rationale.
const (
	defaultLineDatMaxPending          = 250_000         // documents held in the temporary blocks at once, across every shard
	defaultLineDatMaxPendingBytes     = 256 << 20       // ~256MB of estimated document data buffered at once
	defaultLineDatMaxBatchBytes       = 8 << 20         // ~8MB per single commit to CaimanDB, on top of the doc-count cap
	defaultLineDatBackpressureTimeout = 3 * time.Second // longest a caller is slowed down before being accepted anyway
	defaultLineDatDrainWorkers        = 8               // fixed-size pool shared by every shard; never one goroutine per block
	defaultLineDatShards              = 16              // independent lanes (own lock, own queues, own journal file)
)

// LineDatEntry is one document staged in LineDat, waiting to be
// transferred to CaimanDB's permanent storage.
type LineDatEntry struct {
	Seq      uint64
	DB       string
	Block    string
	ID       string
	Data     map[string]any
	ShardID  string
	Enqueued time.Time
	Attempts int

	// SizeBytes is a cheap approximate footprint of Data, computed once at
	// enqueue time and reused for both the pendingBytes accounting and
	// per-batch byte budgeting during drain -- avoids re-walking/encoding
	// the document more than once on the hot path.
	SizeBytes int64

	// elem is this entry's node in its block's FIFO list (see
	// lineDatBlockQueue), so both a last-write-wins replacement and a
	// post-commit removal are O(1) instead of a map/list scan.
	elem *list.Element
}

func (e *LineDatEntry) toDocument() *Document {
	return &Document{
		ID:      e.ID,
		Data:    e.Data,
		ShardID: e.ShardID,
		Meta: DocMeta{
			CreatedAt: e.Enqueued.Unix(),
			UpdatedAt: e.Enqueued.Unix(),
			Version:   1,
		},
	}
}

// lineDatRecord is the on-disk journal record shape: either "put" (a
// document entering LineDat) or "done" (that seq has been durably
// transferred to CaimanDB, or permanently given up on, and can be
// forgotten). Replaying a shard's journal and keeping only "put" seqs
// with no matching "done" reconstructs exactly the set of documents that
// shard still owes CaimanDB. Seq numbers are only unique within a single
// shard's journal, never globally.
type lineDatRecord struct {
	Op    string         `json:"op"`
	Seq   uint64         `json:"seq"`
	DB    string         `json:"db,omitempty"`
	Block string         `json:"block,omitempty"`
	ID    string         `json:"id,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

// lineDatBlockQueue is one block's pending documents: a real FIFO (order)
// plus an id index (byID) so EnqueueDocs, drain, and a last-write-wins
// replacement are all O(1)/O(batch) instead of scanning every pending
// document in the block.
type lineDatBlockQueue struct {
	order *list.List // of *LineDatEntry, oldest first
	byID  map[string]*list.Element
}

func newLineDatBlockQueue() *lineDatBlockQueue {
	return &lineDatBlockQueue{order: list.New(), byID: make(map[string]*list.Element)}
}

// lineDatShard is one independent lane of LineDat: its own lock, its own
// per-block queues, its own on-disk journal file. A given (db,block) key
// always hashes to the same shard (see LineDat.shardFor), so a single
// block's document order is still preserved and only ever drained by one
// worker at a time -- but unrelated blocks in different shards never wait
// on each other's lock or journal fsync.
type lineDatShard struct {
	idx int

	mu       sync.Mutex
	seq      uint64
	blocks   map[string]*lineDatBlockQueue // blockKey -> queue
	draining map[string]bool               // blockKey -> currently owned by a drain worker

	journalPath string
	jf          *os.File
	jw          *bufio.Writer
}

// LineDat is the temporal ingestion engine described in the package doc
// comment above. The zero value is not usable; construct with newLineDat.
type LineDat struct {
	engine *Engine

	batchSize     int
	maxBatchBytes int64
	flushEvery    time.Duration
	drainWorkers  int
	numShards     int

	maxPending          int64
	maxPendingBytes     int64
	backpressureTimeout time.Duration

	journalDir string
	shards     []*lineDatShard

	wakeChan chan struct{}
	stopChan chan struct{}
	// doneChan is closed once the drain dispatcher and every worker it
	// owns have fully exited (see run()'s deferred cleanup). Close()
	// waits on it (bounded) so it never returns -- and Shutdown() never
	// proceeds to close the WAL/Badger pool -- while a drain worker
	// could still be mid-commit against them.
	doneChan chan struct{}
	doneOnce sync.Once
	stopped  atomic.Bool

	pendingCount atomic.Int64
	pendingBytes atomic.Int64
	droppedCount atomic.Int64
	committed    atomic.Int64
	throttled    atomic.Int64 // how many EnqueueDocs calls have had to wait for capacity
}

// newLineDat creates LineDat's per-shard on-disk journals (or opens/
// replays existing ones from a previous run) and returns a LineDat ready
// to have Start called on it.
func newLineDat(e *Engine) (*LineDat, error) {
	dir := e.config.LineDatDir
	if dir == "" {
		dir = filepath.Join(e.config.DataRoot, systemDB, "linedat")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("linedat: create dir: %w", err)
	}

	batchSize := e.config.LineDatBatchSize
	if batchSize <= 0 {
		batchSize = 2000
	}
	flushEvery := e.config.LineDatFlushInterval
	if flushEvery <= 0 {
		flushEvery = 25 * time.Millisecond
	}
	maxBatchBytes := e.config.LineDatMaxBatchBytes
	if maxBatchBytes <= 0 {
		maxBatchBytes = defaultLineDatMaxBatchBytes
	}
	maxPending := e.config.LineDatMaxPending
	if maxPending <= 0 {
		maxPending = defaultLineDatMaxPending
	}
	maxPendingBytes := e.config.LineDatMaxPendingBytes
	if maxPendingBytes <= 0 {
		maxPendingBytes = defaultLineDatMaxPendingBytes
	}
	backpressureTimeout := e.config.LineDatBackpressureTimeout
	if backpressureTimeout <= 0 {
		backpressureTimeout = defaultLineDatBackpressureTimeout
	}
	drainWorkers := e.config.LineDatDrainWorkers
	if drainWorkers <= 0 {
		drainWorkers = runtime.GOMAXPROCS(0)
		if drainWorkers > defaultLineDatDrainWorkers {
			drainWorkers = defaultLineDatDrainWorkers
		}
		if drainWorkers < 1 {
			drainWorkers = 1
		}
	}
	numShards := e.config.LineDatShards
	if numShards <= 0 {
		numShards = defaultLineDatShards
	}

	ld := &LineDat{
		engine:              e,
		batchSize:           batchSize,
		maxBatchBytes:       maxBatchBytes,
		flushEvery:          flushEvery,
		drainWorkers:        drainWorkers,
		numShards:           numShards,
		maxPending:          int64(maxPending),
		maxPendingBytes:     maxPendingBytes,
		backpressureTimeout: backpressureTimeout,
		journalDir:          dir,
		shards:              make([]*lineDatShard, numShards),
		wakeChan:            make(chan struct{}, 1),
		stopChan:            make(chan struct{}),
		doneChan:            make(chan struct{}),
	}

	var totalRecovered int64
	for i := 0; i < numShards; i++ {
		sh := &lineDatShard{
			idx:         i,
			blocks:      make(map[string]*lineDatBlockQueue),
			draining:    make(map[string]bool),
			journalPath: filepath.Join(dir, fmt.Sprintf("pending-%02d.log", i)),
		}
		ld.shards[i] = sh

		recovered, err := ld.recoverShard(sh)
		if err != nil {
			log().Warn("LineDat: journal recovery failed for shard, starting it with an empty pending queue",
				zap.Int("shard", i), zap.Error(err))
		}
		totalRecovered += recovered

		if err := ld.openShardJournalForAppend(sh); err != nil {
			return nil, fmt.Errorf("linedat: open journal for shard %d: %w", i, err)
		}

		if recovered > 0 {
			// Fold the recovered "put" set back down to just what's
			// still pending, dropping the prior run's accumulated
			// "done"/"put" history -- otherwise a node that's crashed
			// and restarted repeatedly with the same handful of stuck
			// documents would grow that shard's journal by that same
			// handful every single time.
			if err := ld.compactShard(sh); err != nil {
				log().Warn("LineDat: initial journal compaction failed for shard", zap.Int("shard", i), zap.Error(err))
			}
		} else {
			ld.clearShardJournal(sh)
		}
	}

	if totalRecovered > 0 {
		log().Info("LineDat: resuming transfer of documents pending from a previous run",
			zap.Int64("pending", totalRecovered))
	}

	return ld, nil
}

// Start launches LineDat's background transfer workers: a small,
// fixed-size pool (never one goroutine per block, never unbounded) that
// periodically -- and whenever new documents arrive -- drains every
// block's pending queue into CaimanDB, batchSize documents (or
// maxBatchBytes, whichever comes first) at a time, via the engine's
// normal local commit path.
func (ld *LineDat) Start() {
	safeGoLoop("linedat_drain_loop", ld.run)
}

// lineDatWorkItem identifies one block's queue, in one shard, that a
// drain worker has been asked to process. blockKey is the composite
// "db\x00block" key (see blockKey()), not just a block name.
type lineDatWorkItem struct {
	shard    *lineDatShard
	blockKey string
}

// run is the drain dispatcher. Dispatching a block is non-blocking -- if
// every worker is currently busy, that block is simply retried on the
// next tick/wake instead of stalling the dispatcher (and therefore every
// other block, in every other shard) behind whichever queue happens to be
// slow.
func (ld *LineDat) run() {
	workCh := make(chan lineDatWorkItem, ld.drainWorkers*2)
	var wg sync.WaitGroup
	for i := 0; i < ld.drainWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workCh {
				ld.drainBlock(item.shard, item.blockKey)
			}
		}()
	}
	defer func() {
		close(workCh)
		wg.Wait()
		// Guarded with sync.Once rather than a bare close: if run()
		// were ever restarted after a recovered panic (see
		// logging.SafeGoLoop), this defer runs again on the fresh
		// invocation too, and closing an already-closed channel
		// panics. Once.Do makes the signal idempotent regardless of
		// how many times run() ends up executing.
		ld.doneOnce.Do(func() { close(ld.doneChan) })
	}()

	ticker := time.NewTicker(ld.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ld.stopChan:
			return
		case <-ld.engine.ctx.Done():
			return
		case <-ticker.C:
			ld.dispatchAll(workCh)
		case <-ld.wakeChan:
			ld.dispatchAll(workCh)
		}
	}
}

// dispatchAll offers every block that currently has pending documents (in
// every shard, and not already owned by a worker) to the drain pool. A
// block already being drained is skipped so at most one worker ever
// touches a given block's queue at a time -- otherwise two workers could
// race to pull from the same FIFO and commit the same documents twice,
// wasting exactly the CPU/IO this design exists to conserve.
func (ld *LineDat) dispatchAll(workCh chan<- lineDatWorkItem) {
	for _, sh := range ld.shards {
		sh.mu.Lock()
		var ready []string
		for key, q := range sh.blocks {
			if q.order.Len() > 0 && !sh.draining[key] {
				sh.draining[key] = true
				ready = append(ready, key)
			}
		}
		sh.mu.Unlock()

		for _, key := range ready {
			select {
			case workCh <- lineDatWorkItem{shard: sh, blockKey: key}:
			default:
				// Pool is saturated this cycle; release the reservation
				// so this block is picked up on the next tick/wake
				// instead of being stuck marked "draining" with no
				// worker actually assigned to it.
				sh.mu.Lock()
				delete(sh.draining, key)
				sh.mu.Unlock()
			}
		}
	}
}

// Close stops the background workers and waits (with a bound) for them to
// fully exit before returning. This matters for shutdown ordering: the
// engine closes the WAL and Badger pool right after Close() returns, and
// a drain worker still mid-commit at that moment would be writing to a
// store being torn down underneath it. A worker still running past the
// timeout just logs a warning and is left to finish on its own -- the
// document it was committing stays safely recorded in its shard's
// journal either way, so the worst case is a retried commit on the next
// startup, never lost data.
//
// Documents still pending (not yet reserved by a worker) remain durably
// in their shard's on-disk journal and are picked up again the next time
// newLineDat runs (e.g. after a restart) -- Close does not wait for the
// entire queue to drain, only for in-flight work to stop.
func (ld *LineDat) Close() {
	if ld.stopped.CompareAndSwap(false, true) {
		close(ld.stopChan)
	}

	select {
	case <-ld.doneChan:
	case <-time.After(5 * time.Second):
		log().Warn("LineDat: drain workers did not stop within 5s of Close(); continuing shutdown anyway")
	}

	for _, sh := range ld.shards {
		sh.mu.Lock()
		if sh.jw != nil {
			sh.jw.Flush()
		}
		if sh.jf != nil {
			sh.jf.Close()
		}
		sh.mu.Unlock()
	}
}

func blockKey(dbName, block string) string {
	return turboKey(dbName, block)
}

// shardFor deterministically routes a (db,block) key to one of LineDat's
// independent lanes -- always the same lane for the same block, so a
// block's document order and its "only one worker drains it at a time"
// guarantee both still hold, while different blocks spread across shards
// and stop contending on a single lock/journal.
func (ld *LineDat) shardFor(key string) *lineDatShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return ld.shards[h.Sum32()%uint32(ld.numShards)]
}

// EnqueueDocs durably stages docs for dbName/block and returns immediately
// -- it does not wait for the documents to actually reach CaimanDB's
// permanent storage, only for them to be safely recorded in LineDat's own
// journal. The returned *Document slice mirrors InsertBatch's per-document
// contract: same order as docs, a nil error for accepted documents, a
// non-nil error only for documents rejected outright (e.g. nil data).
func (ld *LineDat) EnqueueDocs(dbName, block string, docs []map[string]any) ([]*Document, []error) {
	if len(docs) == 0 {
		return nil, nil
	}

	// Resource governor: if the temporary blocks are already at (or over)
	// capacity, this briefly slows the caller down instead of letting the
	// in-memory queue grow without bound. Under normal load this returns
	// immediately -- it's only sustained massive inserts that outrun
	// CaimanDB's commit rate that ever feel this.
	ld.waitForCapacity()

	now := time.Now()
	results := make([]*Document, len(docs))
	errs := make([]error, len(docs))
	entries := make([]*LineDatEntry, 0, len(docs))

	key := blockKey(dbName, block)
	sh := ld.shardFor(key)

	sh.mu.Lock()
	q := sh.blocks[key]
	if q == nil {
		q = newLineDatBlockQueue()
		sh.blocks[key] = q
	}
	for i, data := range docs {
		if data == nil {
			errs[i] = fmt.Errorf("document cannot be nil")
			continue
		}
		id := resolveDocID(data)
		if id == "" {
			id = newID()
		}
		data["id"] = id
		shardID := ld.engine.shardMgr.GetShardForDoc(dbName, block, id)
		size := approxDocSize(data)

		sh.seq++
		e := &LineDatEntry{
			Seq:       sh.seq,
			DB:        dbName,
			Block:     block,
			ID:        id,
			Data:      data,
			ShardID:   shardID,
			Enqueued:  now,
			SizeBytes: size,
		}
		// A second write to the same ID before the first drains simply
		// replaces it here (last write wins, same as two ordinary
		// inserts to the same ID would resolve in Badger): the old FIFO
		// node is dropped and the new one goes to the back, in O(1) via
		// the stored list.Element -- the earlier entry's own journal
		// "put" record becomes moot and is dropped during the next
		// compaction.
		if oldEl, existed := q.byID[id]; existed {
			old := oldEl.Value.(*LineDatEntry)
			ld.pendingBytes.Add(size - old.SizeBytes)
			q.order.Remove(oldEl)
		} else {
			ld.pendingCount.Add(1)
			ld.pendingBytes.Add(size)
		}
		e.elem = q.order.PushBack(e)
		q.byID[id] = e.elem

		entries = append(entries, e)
		results[i] = e.toDocument()
	}
	sh.mu.Unlock()

	if len(entries) > 0 {
		if err := ld.appendPut(sh, entries); err != nil {
			// The batch is accepted and visible to FindByID/Find either
			// way (it's already in the in-memory queue above); only its
			// crash-safety is degraded until the next successful
			// journal write. Refusing an already-accepted batch because
			// the safety net couldn't be written would be worse than
			// serving it with a logged, reduced durability guarantee.
			log().Error("LineDat: failed to persist pending journal entries; not crash-safe until the next successful journal write",
				zap.Int("shard", sh.idx), zap.Error(err), zap.Int("count", len(entries)))
		}
		ld.wake()
	}

	return results, errs
}

// EnqueueBatch is EnqueueDocs's InsertBatch-shaped counterpart: same
// (inserted, errors) int64-count return as InsertBatch, where "inserted"
// means "durably accepted by LineDat" rather than "already committed to
// Badger" -- see InsertBatchViaLineDat's doc comment in ops_turbo.go for
// the full trade-off.
func (ld *LineDat) EnqueueBatch(dbName, block string, docs []map[string]any) (int64, int64) {
	_, errs := ld.EnqueueDocs(dbName, block, docs)
	var inserted, errCount int64
	for _, err := range errs {
		if err != nil {
			errCount++
		} else {
			inserted++
		}
	}
	return inserted, errCount
}

func (ld *LineDat) wake() {
	select {
	case ld.wakeChan <- struct{}{}:
	default:
	}
}

// waitForCapacity is LineDat's backpressure valve: it blocks the calling
// goroutine (an INSERT, or one GENERATE/IMPORT chunk) while the temporary
// blocks are at or above the configured document/byte caps (tracked
// globally, across every shard, via lock-free atomics), nudging the drain
// pool to run in the meantime, and gives up after backpressureTimeout so
// a caller is never stuck indefinitely -- a queue that's still full after
// the timeout is accepted anyway (logged) rather than turned into an
// outright failure, since refusing an otherwise-valid write is worse than
// a slightly late acknowledgment.
//
// This is what keeps "arriba" fast under ordinary load (the caps are sized
// generously enough that a normal INSERT never touches this path) while
// keeping "abajo" bounded under sustained massive load: instead of memory
// growing with however fast the client can push data, the effective
// accept rate settles to whatever rate the background drain can actually
// sustain.
func (ld *LineDat) waitForCapacity() {
	if ld.maxPending <= 0 && ld.maxPendingBytes <= 0 {
		return
	}
	if !ld.overCapacity() {
		return
	}

	waited := false
	deadline := time.Now().Add(ld.backpressureTimeout)
	for ld.overCapacity() {
		if !time.Now().Before(deadline) {
			log().Warn("LineDat: temporary blocks still at capacity after the backpressure wait; accepting this write anyway",
				zap.Int64("pending_docs", ld.pendingCount.Load()),
				zap.Int64("pending_bytes", ld.pendingBytes.Load()),
				zap.Duration("waited", ld.backpressureTimeout))
			break
		}
		waited = true
		ld.wake() // don't wait for the next tick to try draining more
		time.Sleep(2 * time.Millisecond)
	}
	if waited {
		ld.throttled.Add(1)
	}
}

func (ld *LineDat) overCapacity() bool {
	if ld.maxPending > 0 && ld.pendingCount.Load() >= ld.maxPending {
		return true
	}
	if ld.maxPendingBytes > 0 && ld.pendingBytes.Load() >= ld.maxPendingBytes {
		return true
	}
	return false
}

// approxDocSize is a cheap, allocation-light estimate of a document's
// in-memory footprint -- good enough to budget the temporary blocks by,
// without paying for a full JSON encode of every document on the insert
// hot path (which is exactly the kind of overhead LineDat exists to keep
// off that path). depth guards against pathological deep nesting.
func approxDocSize(v any) int64 {
	return approxDocSizeDepth(v, 0)
}

func approxDocSizeDepth(v any, depth int) int64 {
	if depth > 12 {
		return 32
	}
	switch t := v.(type) {
	case nil:
		return 8
	case string:
		return int64(len(t)) + 16
	case bool:
		return 1
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 8
	case map[string]any:
		n := int64(16)
		for k, val := range t {
			n += int64(len(k)) + 16 + approxDocSizeDepth(val, depth+1)
		}
		return n
	case []any:
		n := int64(16)
		for _, val := range t {
			n += approxDocSizeDepth(val, depth+1)
		}
		return n
	default:
		return 32
	}
}

// Lookup returns the pending (not-yet-committed-to-CaimanDB) version of a
// document, if LineDat is currently holding one. Used by FindByID as a
// fallback so a read immediately following an accepted write still sees
// it.
func (ld *LineDat) Lookup(dbName, block, id string) (*Document, bool) {
	key := blockKey(dbName, block)
	sh := ld.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	q := sh.blocks[key]
	if q == nil {
		return nil, false
	}
	el, ok := q.byID[id]
	if !ok {
		return nil, false
	}
	return el.Value.(*LineDatEntry).toDocument(), true
}

// PendingForBlock returns a snapshot of every document currently staged in
// LineDat for dbName/block, for merging into FIND scan/index results (see
// mergeLineDatPending in ops_find.go).
func (ld *LineDat) PendingForBlock(dbName, block string) []Document {
	key := blockKey(dbName, block)
	sh := ld.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	q := sh.blocks[key]
	if q == nil || q.order.Len() == 0 {
		return nil
	}
	docs := make([]Document, 0, q.order.Len())
	for el := q.order.Front(); el != nil; el = el.Next() {
		docs = append(docs, *el.Value.(*LineDatEntry).toDocument())
	}
	return docs
}

// drainBlock commits one batch's worth of a single block's pending
// documents to CaimanDB. It is only ever invoked by a drain-pool worker
// for a block it has reserved in shard.draining (see dispatchAll), so it
// always releases that reservation before returning, on every exit path.
func (ld *LineDat) drainBlock(sh *lineDatShard, key string) {
	defer func() {
		sh.mu.Lock()
		delete(sh.draining, key)
		sh.mu.Unlock()
	}()

	dbName, block, ok := splitTurboKey(key)
	if !ok {
		return
	}

	sh.mu.Lock()
	q := sh.blocks[key]
	if q == nil || q.order.Len() == 0 {
		sh.mu.Unlock()
		return
	}
	batch := make([]*LineDatEntry, 0, ld.batchSize)
	var batchBytes int64
	for el := q.order.Front(); el != nil; el = el.Next() {
		e := el.Value.(*LineDatEntry)
		// Cap each commit by both document count (ld.batchSize) and
		// approximate byte size (ld.maxBatchBytes): a handful of very
		// large documents shouldn't be able to force one oversized
		// commit just because they're under the count limit. Always let
		// at least one document through even if it alone exceeds the
		// byte budget, so an unusually large single document doesn't
		// starve forever. Walking the FIFO from the front means this
		// only ever costs O(batch size + a few skipped oversized
		// entries), never a scan of the whole block.
		if len(batch) > 0 && batchBytes+e.SizeBytes > ld.maxBatchBytes {
			continue
		}
		batch = append(batch, e)
		batchBytes += e.SizeBytes
		if len(batch) >= ld.batchSize {
			break
		}
	}
	sh.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	docsData := make([]map[string]any, len(batch))
	for i, e := range batch {
		docsData[i] = e.Data
	}

	// The real commit: the exact same WAL-append + Badger WriteBatch +
	// index/FLEX-COLUMN/change-bus path every other insert path (plain
	// Insert, InsertBatch, the turbo batcher) already uses. LineDat only
	// changes *when* this runs relative to the caller, never *how* a
	// document ends up durably stored.
	_, errs := ld.engine.insertBatchLocalDetailed(dbName, block, docsData)

	var doneSeqs []uint64
	sh.mu.Lock()
	q = sh.blocks[key]
	for i, e := range batch {
		if errs[i] != nil {
			e.Attempts++
			if e.Attempts < maxLineDatAttempts {
				continue // leave it in the FIFO, retried next cycle
			}
			log().Error("LineDat: document exceeded max transfer attempts, giving up",
				zap.String("db", dbName), zap.String("block", block),
				zap.String("doc_id", e.ID), zap.Int("attempts", e.Attempts),
				zap.Error(errs[i]))
			ld.droppedCount.Add(1)
		} else {
			ld.committed.Add(1)
		}
		// Only remove/finalize if nothing re-enqueued this ID since we
		// snapshotted the batch (the FIFO element is only still ours if
		// the id index still points at this exact entry's node).
		if q != nil {
			if cur, ok := q.byID[e.ID]; ok && cur == e.elem {
				q.order.Remove(e.elem)
				delete(q.byID, e.ID)
				ld.pendingCount.Add(-1)
				ld.pendingBytes.Add(-e.SizeBytes)
			}
		}
		doneSeqs = append(doneSeqs, e.Seq)
	}
	if q != nil && q.order.Len() == 0 {
		delete(sh.blocks, key)
	}
	sh.mu.Unlock()

	if len(doneSeqs) > 0 {
		// There's now room in the queue (in count and/or in bytes) and
		// possibly more of this same block still waiting: nudge the
		// dispatcher to run again immediately (waitForCapacity's own
		// short poll will notice the freed-up room on its own within a
		// couple of milliseconds either way) instead of idling until the
		// next flushEvery tick.
		ld.wake()
		if err := ld.appendDone(sh, doneSeqs); err != nil {
			log().Warn("LineDat: failed to append journal 'done' records", zap.Int("shard", sh.idx), zap.Error(err))
		}
	}

	if ld.shardPendingCount(sh) == 0 {
		ld.clearShardJournal(sh)
	} else if ld.shardJournalSizeAtLeast(sh, lineDatJournalCompactThreshold) {
		if err := ld.compactShard(sh); err != nil {
			log().Warn("LineDat: journal compaction failed for shard", zap.Int("shard", sh.idx), zap.Error(err))
		}
	}
}

// shardPendingCount reports how many documents this shard alone is
// currently holding (used only to decide whether that shard's journal can
// be cleared -- the global pendingCount/pendingBytes atomics are what
// waitForCapacity and Stats use).
func (ld *LineDat) shardPendingCount(sh *lineDatShard) int {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	n := 0
	for _, q := range sh.blocks {
		n += q.order.Len()
	}
	return n
}

// Stats returns a snapshot for STATUS/diagnostics.
func (ld *LineDat) Stats() map[string]any {
	return map[string]any{
		"enabled":           true,
		"pending":           ld.pendingCount.Load(),
		"pending_bytes":     ld.pendingBytes.Load(),
		"committed":         ld.committed.Load(),
		"dropped":           ld.droppedCount.Load(),
		"throttled_batches": ld.throttled.Load(), // EnqueueDocs calls that had to wait for capacity
		"max_pending":       ld.maxPending,
		"max_pending_bytes": ld.maxPendingBytes,
		"drain_workers":     ld.drainWorkers,
		"shards":            ld.numShards,
	}
}

// ---- journal persistence (per shard) ----

// openShardJournalForAppend opens (or creates) a shard's journal file for
// writing. Deliberately NOT opened with os.O_APPEND: on Windows, a file
// handle opened with O_APPEND only gets the FILE_APPEND_DATA access right,
// which excludes the right to truncate/SetEndOfFile -- so a later
// Truncate(0) call (see clearShardJournal) fails with "Access is denied"
// on every single shard, every startup. Opening O_RDWR instead and
// seeking to the current end once here gives the same effective
// "subsequent writes append" behavior (every write after this happens
// through this same handle, serialized by sh.mu, so nothing else moves
// the file cursor), while keeping the handle fully truncatable on every
// platform.
func (ld *LineDat) openShardJournalForAppend(sh *lineDatShard) error {
	f, err := os.OpenFile(sh.journalPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		f.Close()
		return err
	}
	sh.mu.Lock()
	sh.jf = f
	sh.jw = bufio.NewWriterSize(f, 64<<10)
	sh.mu.Unlock()
	return nil
}

// appendPut durably records entries as "put" journal lines in their
// shard's journal: one buffered write for the whole batch, then a single
// fsync -- so a whole Enqueue call (which may itself be many documents
// from one GENERATE/IMPORT chunk) pays for one fsync total instead of one
// per document. Different shards' journals are independent files, so two
// concurrent Enqueue calls landing in different shards never wait on the
// same fsync.
func (ld *LineDat) appendPut(sh *lineDatShard, entries []*LineDatEntry) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.jw == nil {
		return fmt.Errorf("journal not open")
	}
	for _, e := range entries {
		rec := lineDatRecord{Op: "put", Seq: e.Seq, DB: e.DB, Block: e.Block, ID: e.ID, Data: e.Data}
		b, err := json.Marshal(rec)
		if err != nil {
			continue
		}
		sh.jw.Write(b)
		sh.jw.WriteByte('\n')
	}
	if err := sh.jw.Flush(); err != nil {
		return err
	}
	return sh.jf.Sync()
}

// appendDone marks seqs (within this shard's journal) as durably
// transferred (or permanently given up on). No fsync is required here for
// correctness: replaying a "put" whose "done" record didn't make it to
// disk before a crash just means the document is re-committed to
// CaimanDB on the next startup, which is a harmless idempotent overwrite
// of the same ID.
func (ld *LineDat) appendDone(sh *lineDatShard, seqs []uint64) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.jw == nil {
		return fmt.Errorf("journal not open")
	}
	for _, seq := range seqs {
		rec := lineDatRecord{Op: "done", Seq: seq}
		b, _ := json.Marshal(rec)
		sh.jw.Write(b)
		sh.jw.WriteByte('\n')
	}
	return sh.jw.Flush()
}

func (ld *LineDat) shardJournalSizeAtLeast(sh *lineDatShard, n int64) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.jf == nil {
		return false
	}
	info, err := sh.jf.Stat()
	if err != nil {
		return false
	}
	return info.Size() >= n
}

// clearShardJournal is called once a shard has nothing left pending: there
// is nothing left worth recovering from that shard's journal, so it is
// reset to empty rather than left to grow indefinitely. This is the "se
// borre" step: a fully-drained shard leaves no trace on disk to replay.
//
// This deliberately does NOT call Truncate on the already-open handle
// directly: on Windows, Truncate (SetEndOfFile) needs GENERIC_WRITE, and
// depending on how the handle was originally obtained that right may not
// be present -- closing and reopening with O_TRUNC sidesteps that
// entirely and behaves identically on every platform.
func (ld *LineDat) clearShardJournal(sh *lineDatShard) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.jf != nil {
		sh.jf.Close()
		sh.jf = nil
		sh.jw = nil
	}

	// Recreate the file empty (O_TRUNC), then reopen it read-write
	// (never O_APPEND -- see openShardJournalForAppend's doc comment).
	if tf, err := os.OpenFile(sh.journalPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644); err != nil {
		log().Warn("LineDat: failed to clear empty journal", zap.Int("shard", sh.idx), zap.Error(err))
		return
	} else {
		tf.Close()
	}

	f, err := os.OpenFile(sh.journalPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		log().Warn("LineDat: failed to reopen journal after clearing", zap.Int("shard", sh.idx), zap.Error(err))
		return
	}
	// File was just recreated empty (O_TRUNC above), so position 0 is
	// already the end -- no seek needed, but Seek(0, SEEK_END) would be a
	// harmless no-op here regardless.
	sh.jf = f
	sh.jw = bufio.NewWriterSize(f, 256<<10)
}

// compactShard rewrites one shard's journal to contain only "put" records
// for what is still actually pending in that shard, dropping every "done"
// record and every already-superseded/committed "put" -- bounds each
// journal's size across a long-running node's lifetime regardless of how
// much has flowed through it.
func (ld *LineDat) compactShard(sh *lineDatShard) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	tmpPath := sh.journalPath + ".compact"
	tf, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(tf, 256<<10)
	for _, q := range sh.blocks {
		for el := q.order.Front(); el != nil; el = el.Next() {
			e := el.Value.(*LineDatEntry)
			rec := lineDatRecord{Op: "put", Seq: e.Seq, DB: e.DB, Block: e.Block, ID: e.ID, Data: e.Data}
			b, err := json.Marshal(rec)
			if err != nil {
				continue
			}
			w.Write(b)
			w.WriteByte('\n')
		}
	}
	if err := w.Flush(); err != nil {
		tf.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		os.Remove(tmpPath)
		return err
	}
	tf.Close()

	if sh.jf != nil {
		sh.jf.Close()
	}
	if err := os.Rename(tmpPath, sh.journalPath); err != nil {
		return err
	}
	f, err := os.OpenFile(sh.journalPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	// Freshly renamed-into-place file containing exactly what we just
	// wrote and synced above -- seek to its end so subsequent
	// appendPut/appendDone calls continue writing after it, same as
	// openShardJournalForAppend does on a normal (non-compaction)
	// startup. Not opened with O_APPEND: see openShardJournalForAppend's
	// doc comment for why that breaks Truncate on Windows.
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		f.Close()
		return err
	}
	sh.jf = f
	sh.jw = bufio.NewWriterSize(f, 256<<10)
	return nil
}

// recoverShard replays one shard's existing journal (if any) left over
// from a previous run, rebuilding that shard's in-memory queues from
// whatever "put" records have no matching "done". Called once per shard,
// before that shard's journal is (re)opened for append. Returns how many
// documents it recovered as still-pending.
func (ld *LineDat) recoverShard(sh *lineDatShard) (int64, error) {
	f, err := os.Open(sh.journalPath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()

	puts := make(map[uint64]*LineDatEntry)
	done := make(map[uint64]bool)

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec lineDatRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			// A partial/corrupt trailing line (e.g. the process died
			// mid-write) is skipped rather than aborting recovery of
			// everything before it -- LineDat is a safety net, not
			// itself a single point of failure for startup.
			continue
		}
		switch rec.Op {
		case "put":
			puts[rec.Seq] = &LineDatEntry{
				Seq: rec.Seq, DB: rec.DB, Block: rec.Block, ID: rec.ID, Data: rec.Data,
			}
		case "done":
			done[rec.Seq] = true
		}
	}

	var maxSeq uint64
	var recovered int64
	for seq, e := range puts {
		if seq > maxSeq {
			maxSeq = seq
		}
		if done[seq] {
			continue
		}
		e.Enqueued = time.Now()
		e.ShardID = ld.engine.shardMgr.GetShardForDoc(e.DB, e.Block, e.ID)
		e.SizeBytes = approxDocSize(e.Data)
		key := blockKey(e.DB, e.Block)
		q := sh.blocks[key]
		if q == nil {
			q = newLineDatBlockQueue()
			sh.blocks[key] = q
		}
		// Two "put"s for the same ID across the journal's history:
		// later Seq wins (last write, same rule Enqueue itself applies).
		if oldEl, exists := q.byID[e.ID]; !exists {
			ld.pendingCount.Add(1)
			ld.pendingBytes.Add(e.SizeBytes)
			e.elem = q.order.PushBack(e)
			q.byID[e.ID] = e.elem
			recovered++
		} else if old := oldEl.Value.(*LineDatEntry); old.Seq < e.Seq {
			ld.pendingBytes.Add(e.SizeBytes - old.SizeBytes)
			q.order.Remove(oldEl)
			e.elem = q.order.PushBack(e)
			q.byID[e.ID] = e.elem
		}
	}
	sh.seq = maxSeq
	return recovered, nil
}
