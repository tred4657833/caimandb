// Package wal implements CaimanDB's write-ahead log: durable,
// segmented, optionally-batched append-only logging with configurable
// fsync policy and crash recovery support (ReadAll + the exported
// clean/prune helpers used by the root package's recovery routine).
package wal

import (
	"bufio"
	"caimandb/internal/caimandb/logging"
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/storage"
	"encoding/json"
	"fmt"
	"github.com/natefinch/atomic"
	"github.com/smallnest/ringbuffer"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// walSegmentSize and maxWALSegments are duplicated (rather than imported)
// from the root package's constants.go: wal is imported by the root
// caimandb package, so importing back would create a cycle. Keep these in
// sync if the root values change.
const (
	walSegmentSize = 128 << 20
	maxWALSegments = 32
)

// fmtBytes is duplicated (rather than imported) from the root package's
// misc_utils.go to keep this package dependency-free.
func fmtBytes(n int64) string {
	const (
		KB int64 = 1024
		MB       = 1024 * KB
		GB       = 1024 * MB
		TB       = 1024 * GB
		PB       = 1024 * TB
	)
	switch {
	case n >= PB:
		return fmt.Sprintf("%.3f PB", float64(n)/float64(PB))
	case n >= TB:
		return fmt.Sprintf("%.3f TB", float64(n)/float64(TB))
	case n >= GB:
		return fmt.Sprintf("%.3f GB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.3f MB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.3f KB", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

type WALEntry struct {
	ID         uint64
	Timestamp  int64
	Op         string
	DB         string
	Block      string
	Data       []byte
	Checksum   uint32
	Compressed bool
}

// walSubmission pairs a WALEntry with a completion channel so a streaming
// Write() call can block until the entry has actually been handed to the
// OS (written + bufio-flushed, and fsynced if the batch was sync-forced),
// instead of returning success the instant the entry lands in writeChan.
// done is buffered (cap 1) so flushBatch never blocks delivering the
// result, even if the original Write() caller already gave up because the
// WAL was closed concurrently.
type walSubmission struct {
	entry WALEntry
	done  chan error
}

// WALSyncPolicy controls when the WAL forces data to stable storage
// (fsync), trading durability against write throughput/latency:
//
//   - WALSyncAlways:   fsync after every flushed batch. Strongest
//     durability (bounded loss window of a single batch), lowest
//     throughput under heavy write load.
//   - WALSyncInterval: fsync only on the periodic flush-ticker tick, not on
//     every size-triggered batch flush. This is "group commit": many
//     batches ride a single fsync, so throughput scales with batch size
//     while the maximum data-loss window stays bounded by the ticker
//     interval (default 5s, configurable). This is the default — it's the
//     standard tradeoff for a real-time, high-throughput engine.
//   - WALSyncOff:      never fsync explicitly; rely on the OS to flush
//     dirty pages eventually. Fastest, but a crash (not just a process
//     exit) can lose recent writes. Only meaningful for caches/derived
//     data, not source-of-truth durability.
type WALSyncPolicy int

const (
	WALSyncInterval WALSyncPolicy = iota
	WALSyncAlways
	WALSyncOff
)

type WAL struct {
	mu          sync.Mutex
	dir         string
	segments    []string
	currentSeg  *os.File
	segWriter   *bufio.Writer // buffers currentSeg so a batch of N entries is ~1 write syscall, not N
	currentID   uint64
	maxSize     int64
	maxSegs     int
	lastClean   time.Time
	clean       bool
	streaming   bool
	stopChan    chan struct{}
	writeChan   chan walSubmission
	wg          sync.WaitGroup
	flushTicker *time.Ticker
	bufferPool  *storage.BufferPool
	syncPolicy  WALSyncPolicy
	dirtyBytes  int64 // bytes written since the last fsync, for stats/diagnostics

	// recent mirrors the last recentBufSize bytes successfully written to
	// the current segment, purely for diagnostics (e.g. a future "tail
	// the WAL" admin command/CLI view) without paying for a disk read.
	// recentMu guards the read-then-write-back "peek" in RecentBytes,
	// since the ring buffer's own thread-safety only covers each
	// individual Read/Write call, not that pair being atomic together.
	recent   *ringbuffer.RingBuffer
	recentMu sync.Mutex
}

// recentBufSize is the size of the in-memory recent-writes mirror.
const recentBufSize = 1 << 20 // 1MB

func NewWAL(dir string, maxSegs int, streaming bool) (*WAL, error) {
	if maxSegs <= 0 {
		maxSegs = maxWALSegments
	}
	wal := &WAL{
		dir:         dir,
		maxSize:     walSegmentSize,
		maxSegs:     maxSegs,
		lastClean:   time.Now(),
		clean:       true,
		streaming:   streaming,
		stopChan:    make(chan struct{}),
		writeChan:   make(chan walSubmission, 10000),
		flushTicker: time.NewTicker(5 * time.Second),
		bufferPool:  storage.NewBufferPool(128 << 20),
		syncPolicy:  WALSyncInterval,
		recent:      ringbuffer.New(recentBufSize).SetOverwrite(true),
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	if err := wal.recover(); err != nil {
		return nil, err
	}

	wal.wg.Add(1)
	logging.SafeGo("wal_write_loop", wal.writeLoop)

	return wal, nil
}

// SetSyncPolicy changes the fsync/group-commit policy. Safe to call any
// time after NewWAL, including while the WAL is actively taking writes.
func (w *WAL) SetSyncPolicy(p WALSyncPolicy) {
	w.mu.Lock()
	w.syncPolicy = p
	w.mu.Unlock()
}

// SyncPolicy returns the current fsync/group-commit policy, e.g. so a
// caller can save it before a temporary change (BULK MODE) and restore it
// afterward.
func (w *WAL) SyncPolicy() WALSyncPolicy {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncPolicy
}

func (w *WAL) alwaysSync() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncPolicy == WALSyncAlways
}

// walLingerWindow bounds how long an entry can sit in the in-memory batch
// before it's actually written to the segment file, measured from the
// *first* entry in the batch. Without this, a batch under 100 entries
// would only get flushed on the next flushTicker tick (default 5s) —
// meaning Write() could report success while the entry existed only as an
// unwritten value in a Go channel for up to 5 seconds. This is independent
// from flushTicker/syncPolicy, which control when a *written* batch gets
// fsynced, not when it gets written at all.
const walLingerWindow = 20 * time.Millisecond

// walQuietWindow is the adaptive counterpart to walLingerWindow: it resets
// on every new arrival, so a batch that goes quiet (no writer submitted
// anything else right behind it -- the common case for sequential,
// non-concurrent writers) gets written promptly instead of always waiting
// out the full walLingerWindow cap. A burst of concurrent writes keeps
// resetting this and grows the batch normally, bounded by walLingerWindow
// (from the first entry) or the 100-entry size trigger, same as before.
const walQuietWindow = 2 * time.Millisecond

func (w *WAL) writeLoop() {
	defer w.wg.Done()
	var batch []walSubmission
	var quietC <-chan time.Time
	var capC <-chan time.Time

	flush := func(sync bool) {
		w.flushBatch(batch, sync)
		batch = nil
		quietC = nil
		capC = nil
	}

	for {
		select {
		case sub, ok := <-w.writeChan:
			if !ok {
				// forceSync=true: this is shutdown, don't let the last
				// batch sit unsynced.
				flush(true)
				return
			}
			batch = append(batch, sub)
			if capC == nil {
				// Hard cap timer, set once from the first entry -- this
				// is the same worst-case bound walLingerWindow always
				// guaranteed, unaffected by how often quietC resets.
				capC = time.After(walLingerWindow)
			}
			// Every arrival pushes the "go quiet" flush point back out.
			quietC = time.After(walQuietWindow)
			if len(batch) >= 100 {
				// Size-triggered flush: under WALSyncInterval this does
				// NOT fsync, so a burst of writes turns into one buffered
				// write + one fsync per tick instead of one fsync per
				// 100 entries — this is the group-commit throughput win.
				// Under WALSyncAlways every batch still gets its own fsync.
				flush(w.alwaysSync())
			}
		case <-quietC:
			// No new arrival for walQuietWindow: flush now instead of
			// waiting out the rest of the (much longer) hard cap. This is
			// what makes isolated/sequential writes fast.
			if len(batch) > 0 {
				flush(w.alwaysSync())
			} else {
				quietC = nil
			}
		case <-capC:
			// Sustained traffic kept resetting quietC -- force a flush
			// anyway so the unwritten window never exceeds walLingerWindow.
			if len(batch) > 0 {
				flush(w.alwaysSync())
			} else {
				capC = nil
			}
		case <-w.flushTicker.C:
			// Ticker-triggered flush always syncs (when a sync policy is
			// active) so the max unsynced window is bounded by the ticker
			// interval regardless of write rate.
			if len(batch) > 0 {
				flush(true)
			} else {
				w.periodicSync()
			}
		case <-w.stopChan:
			flush(true)
			return
		}
	}
}

// flushBatch encodes a batch of entries into a single in-memory buffer
// (reused via bufferPool to avoid per-batch allocation) and issues one
// Write() call against the segment's buffered writer, instead of the
// previous approach of calling json.Encoder.Encode directly against the
// unbuffered *os.File once per entry (N syscalls per batch). It then
// flushes the bufio.Writer and, depending on syncPolicy/forceSync, fsyncs.
func (w *WAL) flushBatch(batch []walSubmission, forceSync bool) {
	if len(batch) == 0 {
		if forceSync {
			w.periodicSync()
		}
		return
	}

	w.mu.Lock()

	buf := w.bufferPool.Get()
	defer w.bufferPool.Put(buf)

	// pending holds every submission whose entry made it into buf, i.e.
	// everything that still needs to be told the outcome of the write
	// below. Entries that failed to even marshal are reported immediately
	// and excluded, since no write attempt will be made for them.
	pending := make([]walSubmission, 0, len(batch))
	for _, sub := range batch {
		encoded, err := json.Marshal(sub.entry)
		if err != nil {
			logging.Log().Error("Failed to encode WAL entry", zap.Error(err))
			sub.done <- err
			continue
		}
		buf = append(buf, encoded...)
		buf = append(buf, '\n')
		pending = append(pending, sub)
	}

	var opErr error
	if len(buf) > 0 {
		if _, err := w.segWriter.Write(buf); err != nil {
			logging.Log().Error("Failed to write WAL batch", zap.Error(err))
			opErr = err
		} else {
			w.dirtyBytes += int64(len(buf))
			// SetOverwrite(true) means this never blocks or errors on a
			// full buffer -- it just quietly drops the oldest bytes,
			// which is exactly what a bounded "recent activity" mirror
			// wants (last-N-bytes, not all-time history). Guarded by
			// recentMu (not w.mu, which this function already holds) so
			// it can never interleave with RecentBytes' read-then-write-
			// back peek -- the ring buffer's own thread-safety covers
			// each individual Read/Write call, not that pair staying
			// atomic against a concurrent Write from here.
			w.recentMu.Lock()
			w.recent.Write(buf)
			w.recentMu.Unlock()
		}
	}

	if opErr == nil {
		if err := w.segWriter.Flush(); err != nil {
			logging.Log().Error("Failed to flush WAL segment buffer", zap.Error(err))
			opErr = err
		} else if forceSync {
			if err := w.syncLocked(); err != nil {
				opErr = err
			}
		}
	}

	if opErr == nil {
		if stat, err := w.currentSeg.Stat(); err != nil {
			logging.Log().Error("Failed to stat WAL segment", zap.Error(err))
		} else if stat.Size() >= w.maxSize {
			if err := w.rotate(); err != nil {
				logging.Log().Error("Failed to rotate WAL segment", zap.Error(err))
			}
		}
	}

	w.mu.Unlock()

	for _, sub := range pending {
		sub.done <- opErr
	}
}

// periodicSync flushes+syncs the current segment even when no new batch
// arrived this tick, so a low-traffic WAL still reaches a synced state
// promptly instead of leaving a stale unsynced tail indefinitely.
func (w *WAL) periodicSync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.segWriter != nil {
		if err := w.segWriter.Flush(); err != nil {
			logging.Log().Error("Failed to flush WAL segment buffer", zap.Error(err))
			return err
		}
	}
	return w.syncLocked()
}

// syncLocked fsyncs the current segment according to syncPolicy. Caller
// must hold w.mu. Returns the fsync error (if any) so forceSync callers
// in flushBatch can surface it to waiting Write() submitters instead of
// silently reporting success on a write that was never made durable.
func (w *WAL) syncLocked() error {
	if w.syncPolicy == WALSyncOff || w.currentSeg == nil || w.dirtyBytes == 0 {
		return nil
	}
	if err := w.currentSeg.Sync(); err != nil {
		logging.Log().Error("Failed to fsync WAL segment", zap.Error(err))
		return err
	}
	w.dirtyBytes = 0
	return nil
}

func (w *WAL) recover() error {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return err
	}
	sort.Strings(files)

	if len(files) == 0 {
		return w.newSegment()
	}

	w.segments = files
	lastSeg := files[len(files)-1]
	f, err := os.OpenFile(lastSeg, os.O_RDWR|os.O_APPEND, 0640)
	if err != nil {
		return err
	}

	base := filepath.Base(lastSeg)
	idStr := strings.TrimPrefix(strings.TrimSuffix(base, ".log"), "wal-")
	w.currentID, _ = strconv.ParseUint(idStr, 10, 64)

	// Cleanliness must be determined (and the marker invalidated) before
	// any early return, or w.clean is left at its constructor default
	// (true) and recovery gets silently skipped for what could actually
	// be an unclean shutdown.
	w.checkCleanliness()

	stat, _ := f.Stat()
	if stat.Size() >= w.maxSize {
		w.currentSeg = f
		return w.rotate()
	}

	w.setSegment(f)

	return nil
}

// setSegment installs f as the active WAL segment and wraps it in a fresh
// buffered writer. Centralized here so every path that opens/creates a
// segment (recover, newSegment) stays consistent — previously only
// newSegment set up the (then json.Encoder-based) writer, so resuming an
// existing under-size segment on restart left writes going straight to the
// unbuffered file.
func (w *WAL) setSegment(f *os.File) {
	w.currentSeg = f
	w.segWriter = bufio.NewWriterSize(f, 256*1024)
	w.dirtyBytes = 0
}

func (w *WAL) checkCleanliness() {
	markerFile := filepath.Join(w.dir, ".clean")
	if _, err := os.Stat(markerFile); err == nil {
		w.clean = true
		logging.Log().Info("WAL clean shutdown detected, skipping recovery")
	} else {
		w.clean = false
		logging.Log().Info("WAL unclean shutdown detected, will recover on next startup")
	}

	// Remove the marker now, regardless of which branch fired above: we are
	// about to resume accepting writes, so the marker must not go on
	// claiming "clean" for whatever happens next. It is only written again
	// by a graceful Close(). This was the root cause of a silent data-loss
	// bug: start cleanly -> marker written -> process later crashes (kill,
	// power loss, panic) instead of shutting down gracefully -> the old
	// marker is still sitting on disk -> next restart sees it, believes
	// the WAL is clean, and skips replaying everything written since the
	// last graceful Close, discarding it permanently.
	if err := os.Remove(markerFile); err != nil && !os.IsNotExist(err) {
		logging.Log().Warn("Failed to remove stale WAL clean marker", zap.Error(err))
	}
}

func (w *WAL) newSegment() error {
	w.currentID++
	fname := filepath.Join(w.dir, fmt.Sprintf("wal-%d.log", w.currentID))
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	w.setSegment(f)
	w.segments = append(w.segments, fname)
	return nil
}

// pruneToLastSegmentLocked deletes every WAL segment except the most
// recent one. Caller must hold w.mu. It never touches the .clean marker
// file — see checkCleanliness/Close for why that marker may only be
// written on a graceful shutdown.
func (w *WAL) pruneToLastSegmentLocked() {
	if len(w.segments) > 1 {
		for i := 0; i < len(w.segments)-1; i++ {
			os.Remove(w.segments[i])
		}
		w.segments = w.segments[len(w.segments)-1:]
	}
}

func (w *WAL) rotate() error {
	if err := w.currentSeg.Close(); err != nil {
		return err
	}
	if err := w.newSegment(); err != nil {
		return err
	}
	for len(w.segments) > w.maxSegs {
		oldest := w.segments[0]
		os.Remove(oldest)
		w.segments = w.segments[1:]
	}
	return nil
}

func (w *WAL) Write(op string, db, block string, data []byte) error {
	compressed := false
	entryData := data

	if len(data) > 102400 {
		if compressedData, ok, err := storage.CompressData(data, storage.CompressionZstd); err == nil && ok {
			entryData = compressedData
			compressed = true
		}
	}

	entry := WALEntry{
		ID:         w.currentID,
		Timestamp:  time.Now().UnixNano(),
		Op:         op,
		DB:         db,
		Block:      block,
		Data:       entryData,
		Checksum:   crc32.ChecksumIEEE(entryData),
		Compressed: compressed,
	}

	if w.streaming {
		done := make(chan error, 1)
		select {
		case w.writeChan <- walSubmission{entry: entry, done: done}:
		case <-w.stopChan:
			return fmt.Errorf("WAL closed")
		}
		select {
		case err := <-done:
			if err != nil {
				return err
			}
		case <-w.stopChan:
			// The WAL is shutting down. The submission may or may not have
			// been flushed by the in-flight shutdown flush; we can no
			// longer wait for a definitive answer, so surface this to the
			// caller instead of silently reporting success.
			return fmt.Errorf("WAL closed while waiting for write confirmation")
		}
	} else {
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		w.mu.Lock()
		if _, err := w.segWriter.Write(encoded); err != nil {
			w.mu.Unlock()
			return err
		}
		if _, err := w.segWriter.Write([]byte{'\n'}); err != nil {
			w.mu.Unlock()
			return err
		}
		w.dirtyBytes += int64(len(encoded)) + 1
		if err := w.segWriter.Flush(); err != nil {
			w.mu.Unlock()
			return err
		}
		if w.syncPolicy == WALSyncAlways {
			if err := w.syncLocked(); err != nil {
				w.mu.Unlock()
				return err
			}
		}
		stat, err := w.currentSeg.Stat()
		if err != nil {
			w.mu.Unlock()
			return err
		}
		if stat.Size() >= w.maxSize {
			if err := w.rotate(); err != nil {
				w.mu.Unlock()
				return err
			}
		}
		w.mu.Unlock()
	}

	metrics.MetricWALWrites.Inc()
	return nil
}

// WALBatchEntry is one entry to write via WriteBatch.
type WALBatchEntry struct {
	Op    string
	DB    string
	Block string
	Data  []byte
}

// WriteBatch writes multiple entries as a single WAL operation: one lock
// acquisition, one encode pass, one write to the segment's buffered
// writer, one Flush, and (subject to syncPolicy) one fsync for the whole
// group.
//
// help when *independent* callers submit concurrently -- they do nothing
// for a single goroutine that calls Write() for document 1, blocks until
// it's confirmed, then calls Write() for document 2, and so on, which is
// exactly what a bulk-insert loop (insertBatchLocalDetailed) does even
// though it's logically "one batch". Each of those Write() calls would
// still pay its own quiet-window wait, one after another, because from
// the WAL's point of view they never overlap. WriteBatch sidesteps the
// streaming channel/timer machinery entirely for this case: the caller
// already knows it has N documents ready together, so hand them all to
// the WAL in one call instead of pretending they're N separate streaming
// submissions.
//
// Same durability contract as Write(): explicit fsync only under
// WALSyncAlways; WALSyncInterval relies on the periodic ticker (as it
// already does for the non-streaming Write() path), and WALSyncOff never
// syncs explicitly. A failure partway through means none of the batch is
// counted written -- callers should treat the whole group as failed
// rather than trying to salvage a prefix, matching how a failed
// wb.Flush() already aborts the rest of insertBatchLocalDetailed's chunk.
func (w *WAL) WriteBatch(entries []WALBatchEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	buf := w.bufferPool.Get()
	defer w.bufferPool.Put(buf)

	for _, e := range entries {
		compressed := false
		entryData := e.Data
		if len(e.Data) > 102400 {
			if compressedData, ok, err := storage.CompressData(e.Data, storage.CompressionZstd); err == nil && ok {
				entryData = compressedData
				compressed = true
			}
		}

		entry := WALEntry{
			ID:         w.currentID,
			Timestamp:  time.Now().UnixNano(),
			Op:         e.Op,
			DB:         e.DB,
			Block:      e.Block,
			Data:       entryData,
			Checksum:   crc32.ChecksumIEEE(entryData),
			Compressed: compressed,
		}

		encoded, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to encode WAL batch entry: %w", err)
		}
		buf = append(buf, encoded...)
		buf = append(buf, '\n')
	}

	if _, err := w.segWriter.Write(buf); err != nil {
		return err
	}
	w.dirtyBytes += int64(len(buf))

	if err := w.segWriter.Flush(); err != nil {
		return err
	}
	if w.syncPolicy == WALSyncAlways {
		if err := w.syncLocked(); err != nil {
			return err
		}
	}

	stat, err := w.currentSeg.Stat()
	if err != nil {
		return err
	}
	if stat.Size() >= w.maxSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	metrics.MetricWALWrites.Add(float64(len(entries)))
	return nil
}

func (w *WAL) ReadAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []WALEntry

	for _, segFile := range w.segments {
		f, err := os.Open(segFile)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(f)
		for {
			var entry WALEntry
			if err := dec.Decode(&entry); err == io.EOF {
				break
			} else if err != nil {
				// A truncated/malformed entry -- most commonly the tail of
				// the segment left mid-write by a crash -- makes the
				// underlying reader permanently unable to complete the
				// current token: the *os.File keeps returning io.EOF once
				// exhausted, and the decoder doesn't discard the partial
				// bytes it already buffered on error. Retrying Decode()
				// here (previously `continue`) reproduced the exact same
				// error forever, hanging this call -- and everything else
				// waiting on w.mu -- indefinitely. NDJSON has no
				// resync marker, so skipping past one bad entry isn't
				// safe either; stop replaying this segment at the first
				// corruption and treat everything read so far as the
				// valid recovery point.
				logging.Log().Warn("WAL entry corrupt or truncated, stopping replay of segment",
					zap.String("segment", segFile), zap.Error(err))
				break
			}
			entries = append(entries, entry)
		}
		f.Close()
	}

	return entries, nil
}

func (w *WAL) Close() error {
	close(w.stopChan)
	w.flushTicker.Stop()
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	markerFile := filepath.Join(w.dir, ".clean")
	if err := atomic.WriteFile(markerFile, strings.NewReader("clean")); err != nil {
		logging.Log().Warn("Failed to write WAL clean marker", zap.Error(err))
	}

	if w.segWriter != nil {
		if err := w.segWriter.Flush(); err != nil {
			logging.Log().Warn("Failed to flush WAL segment on close", zap.Error(err))
		}
	}
	if w.currentSeg != nil {
		w.currentSeg.Sync()
		return w.currentSeg.Close()
	}
	return nil
}

// Truncate discards old WAL segments, keeping only the active one. It is
// currently only used by the FastStartup path, which deliberately skips
// replaying the WAL. It intentionally does NOT write the .clean marker:
// FastStartup means recovery was skipped, not that the data is consistent
// and safe to declare clean — see checkCleanliness for why a wrongly
// persisted marker causes silent data loss on a later crash.
// IsClean reports whether the WAL believes the last shutdown was clean
// (i.e. recovery can be skipped). Used by the root package's recovery
// routine, which needs this but must not reach into the unexported
// `clean` field directly.
func (w *WAL) IsClean() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.clean
}

// MarkRecovered records, in memory only, that recovery has been applied
// so a second in-process recovery attempt (some startup paths call the
// recovery routine more than once) doesn't re-apply the same entries.
// Intentionally NOT persisted to the on-disk marker file — see
// checkCleanliness for why persisting it here would cause silent data
// loss on a later crash.
func (w *WAL) MarkRecovered() {
	w.mu.Lock()
	w.clean = true
	w.mu.Unlock()
}

// PruneToLastSegment discards all WAL segments except the most recent
// one. Exported so the root package's recovery routine can call it after
// successfully applying recovered entries, without reaching into the
// unexported pruneToLastSegmentLocked.
//
// Callers that just replayed the *currently active* segment (e.g. crash
// recovery on startup, before any new writes have been accepted) must
// use RotateAndPruneFresh instead — see its doc comment for why calling
// this alone is unsafe in that case.
func (w *WAL) PruneToLastSegment() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneToLastSegmentLocked()
}

// RotateAndPruneFresh closes the current segment, opens a brand new
// (empty) one, and then deletes every other segment, so the WAL is left
// with exactly one on-disk segment containing zero entries.
//
// This exists specifically for the startup recovery path. Recovery reads
// every existing segment via ReadAll — including whatever segment is
// still "current" (open for append) — and applies every entry found.
// PruneToLastSegment alone deletes all segments *except* that current
// one, so the entries just applied stay sitting on disk in the segment
// new writes are about to be appended to. If the process crashes again
// before the next graceful Close (so no fresh .clean marker), the next
// startup's recovery reads that same segment again and re-applies those
// already-applied entries a second time. That's harmless for an
// ID-keyed insert, but corrupts a non-idempotent update (e.g. an `$inc`
// gets applied twice). Rotating to a fresh empty segment before pruning
// closes that window: nothing "already applied" is left on disk to be
// replayed again, since only the fresh segment survives and everything
// appended to it from here on is genuinely new.
func (w *WAL) RotateAndPruneFresh() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotate(); err != nil {
		return err
	}
	w.pruneToLastSegmentLocked()
	return nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneToLastSegmentLocked()
	return nil
}

// RecentBytes returns (a copy of) up to the last 1MB of raw WAL bytes
// successfully written to the current segment -- for a "tail the WAL"
// diagnostic view without a disk read. The ring buffer only exposes a
// destructive Read, so this reads everything currently buffered and
// immediately writes it straight back (guarded by recentMu so no other
// caller can interleave between the two), leaving the mirror's contents
// unchanged from the caller's point of view.
func (w *WAL) RecentBytes() []byte {
	w.recentMu.Lock()
	defer w.recentMu.Unlock()

	n := w.recent.Length()
	if n <= 0 {
		return nil
	}
	buf := make([]byte, n)
	read, _ := w.recent.Read(buf)
	buf = buf[:read]
	if read > 0 {
		w.recent.Write(buf)
	}
	out := make([]byte, len(buf))
	copy(out, buf)
	return out
}

func (w *WAL) Stats() map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()

	var totalSize int64
	for _, seg := range w.segments {
		if info, err := os.Stat(seg); err == nil {
			totalSize += info.Size()
		}
	}

	syncPolicyName := "interval"
	switch w.syncPolicy {
	case WALSyncAlways:
		syncPolicyName = "always"
	case WALSyncOff:
		syncPolicyName = "off"
	}

	return map[string]any{
		"segments":    len(w.segments),
		"total_size":  fmtBytes(totalSize),
		"max_segs":    w.maxSegs,
		"max_size":    fmtBytes(w.maxSize),
		"clean":       w.clean,
		"streaming":   w.streaming,
		"current_id":  w.currentID,
		"buffer_pool": w.bufferPool.Stats(),
		"sync_policy": syncPolicyName,
		"dirty_bytes": w.dirtyBytes,
	}
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.segWriter != nil {
		if err := w.segWriter.Flush(); err != nil {
			return err
		}
	}
	if w.currentSeg != nil {
		if err := w.currentSeg.Sync(); err != nil {
			return err
		}
		w.dirtyBytes = 0
	}
	return nil
}
