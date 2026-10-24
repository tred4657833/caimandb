package caimandb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// BulkImportOptions controls Engine.BulkImportReader.
type BulkImportOptions struct {
	// BatchSize is how many decoded documents are grouped into a single
	// insertBatchLocalDetailed call (one block-lock acquisition, one
	// Badger write-batch, one WAL append per doc, inline indexing).
	// Defaults to 20000 when <= 0 -- deliberately larger than
	// InsertBatch's own MaxBatchSize-driven chunking default (10000),
	// since a caller reaching for BulkImportReader has already opted
	// into a bulk load and is trading a little extra memory (one
	// BatchSize-sized slice of decoded docs at a time) and per-chunk
	// latency for materially fewer chunks over a very large file.
	BatchSize int

	// Format selects how the reader is decoded:
	//   "ndjson" (default) -- one JSON object per line, the standard
	//     bulk-load format; a blank line is skipped.
	//   "array" -- a single top-level JSON array of objects, decoded
	//     with a streaming token-based decoder so even a huge array file
	//     is never held in memory beyond the current batch.
	Format string

	// ContinueOnError, when true (the default), keeps importing after a
	// document fails to decode or insert, collecting up to
	// MaxErrorSamples example messages and an exact Errors count. When
	// false, the first error stops the import immediately -- everything
	// already flushed in earlier chunks stays committed, since bulk
	// import is only transactional within a single chunk's own
	// write-batch, never across the whole file.
	ContinueOnError bool

	// MaxErrorSamples caps how many individual error messages are kept
	// in the final report; defaults to 20 when <= 0. The returned Errors
	// *count* is always exact regardless of this cap -- only the number
	// of retained example messages is limited, so a file with a million
	// malformed lines doesn't also cost a million-string slice.
	MaxErrorSamples int

	// Progress, if set, is called synchronously after every flushed
	// chunk with the running totals so a caller can report progress on
	// a very large import without waiting for the whole file to finish.
	Progress func(BulkImportProgress)

	// SkipBulkModeToggle, when true, leaves the engine's current BULK
	// MODE / WAL sync policy untouched instead of enabling bulk mode for
	// the duration of this import. Useful when a caller is running
	// several imports concurrently under a BULK MODE ON they manage
	// themselves, since a plain enable/disable here would otherwise
	// switch bulk mode back off out from under a sibling import as soon
	// as this one finishes.
	SkipBulkModeToggle bool
}

// BulkImportProgress is one snapshot handed to BulkImportOptions.Progress.
type BulkImportProgress struct {
	Decoded  int64
	Inserted int64
	Errors   int64
	Elapsed  time.Duration
}

// BulkImportResult is BulkImportReader's final report.
type BulkImportResult struct {
	Decoded      int64
	Inserted     int64
	Errors       int64
	ErrorSamples []string
	Took         time.Duration
}

// BulkImportReader streams documents from r into dbName/block in large
// batches, reusing the exact same write-batch/WAL/index/compression path
// as InsertBatch (insertBatchLocalDetailed) for every chunk. This adds no
// new on-disk format or indexing logic -- it's a large-batch-friendly
// front door around a path that already exists, built to keep both memory
// and per-item overhead flat regardless of file size:
//
//   - Never holds the whole input in memory: only one BatchSize-sized
//     slice of already-decoded documents at a time.
//   - Never holds the whole result in memory: only aggregate counters
//     plus a small, capped sample of error messages.
//   - Pays for one block-lock acquisition and one Badger write-batch
//     flush cadence per BatchSize documents (insertBatchLocalDetailed
//     itself flushes every 1000 within that), instead of one lock
//     acquisition per document the way a loop of single Insert() calls
//     would.
//   - Automatically widens the turbo batcher's thresholds and relaxes
//     the WAL's fsync policy for the duration (see Engine.SetBulkMode),
//     unless the caller opted out via SkipBulkModeToggle.
//
// The target block is created if it doesn't exist yet, matching
// InsertBatch's own behavior.
func (e *Engine) BulkImportReader(dbName, block string, r io.Reader, opts BulkImportOptions) (*BulkImportResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 20000
	}
	if opts.MaxErrorSamples <= 0 {
		opts.MaxErrorSamples = 20
	}
	format := opts.Format
	if format == "" {
		format = "ndjson"
	}
	if format != "ndjson" && format != "array" {
		return nil, fmt.Errorf("unsupported bulk import format %q -- expected \"ndjson\" or \"array\"", format)
	}

	if !opts.SkipBulkModeToggle {
		e.SetBulkMode(true)
		defer e.SetBulkMode(false)
	}

	if err := e.dirMgr.CreateBlockDirectories(dbName, block); err != nil {
		return nil, err
	}

	start := time.Now()
	res := &BulkImportResult{}

	// Decoding (CPU-bound JSON parsing, one line/element at a time) and
	// committing a batch (WAL append + Badger WriteBatch, largely
	// I/O-bound and already serialized behind the block's write lock in
	// insertBatchLocalDetailed) used to run strictly one after the
	// other on this single goroutine: decode 20000 docs, THEN block
	// this whole goroutine waiting on the commit, THEN decode the next
	// 20000 -- so the CPU sat idle during every commit and the disk sat
	// idle during every decode, for the entire file. Splitting decode
	// (this goroutine) and commit (the inserter goroutine below) into
	// two goroutines connected by a small buffered channel lets batch
	// N+1 start decoding while batch N is still being committed, which
	// is the actual fix for "loses concurrency" on IMPORT: it doesn't
	// add parallelism within a single commit (that still has to be
	// serialized per block, see insertBatchLocalDetailed), it removes
	// the artificial serialization between two phases that never
	// needed to wait on each other. Capacity 2 (not larger) is
	// deliberate: it's enough to keep the inserter fed without ever
	// blocking on it, without holding more than ~2 BatchSize-sized
	// slices of decoded docs in memory at once, keeping IMPORT's memory
	// profile close to its original one-batch-at-a-time shape.
	//
	// Running decode and insert concurrently means both goroutines can
	// now touch error/progress bookkeeping at the same time (a decode
	// failure on one batch racing a commit failure being reported for
	// the previous one), which the old strictly-sequential version
	// never had to worry about. counters below are atomic and
	// errMu/errorSamples is the one piece of shared mutable state
	// (a plain []string) both goroutines can append to, guarded
	// explicitly instead of relying on the two phases never overlapping.
	var decodedCnt, insertedCnt, errCnt atomic.Int64
	var errMu sync.Mutex
	errorSamples := make([]string, 0, opts.MaxErrorSamples)
	recordErr := func(msg string) {
		errCnt.Add(1)
		errMu.Lock()
		if len(errorSamples) < opts.MaxErrorSamples {
			errorSamples = append(errorSamples, msg)
		}
		errMu.Unlock()
	}

	type flushedBatch struct {
		docs []map[string]any
	}
	// Measured on a single-vCPU box (see CHANGELOG): the goroutine+channel
	// split below has no spare core to actually overlap decode with
	// commit on, so all it pays for there is goroutine/channel/atomic
	// overhead with zero offsetting benefit -- and measured slower and
	// less consistent (~95-153s) than the plain synchronous loop
	// (~91-94s) on that hardware. GOMAXPROCS(0) <= 1 is the direct,
	// correct signal for "there's no second core for this to help on"
	// (as opposed to guessing from NumCPU, which ignores an explicit
	// GOMAXPROCS override), so fall back to the original synchronous
	// decode-then-commit loop in that case instead of paying for a
	// pipeline that can't pay for itself.
	pipelined := runtime.GOMAXPROCS(0) > 1

	var flush func()
	var addDoc func(map[string]any)

	if !pipelined {
		batch := make([]map[string]any, 0, opts.BatchSize)
		flush = func() {
			if len(batch) == 0 {
				return
			}
			inserted, errs := e.insertBatchLocalDetailed(dbName, block, batch)
			insertedCnt.Add(inserted)
			for _, err := range errs {
				if err == nil {
					continue
				}
				recordErr(err.Error())
			}
			batch = make([]map[string]any, 0, opts.BatchSize)
			if opts.Progress != nil {
				opts.Progress(BulkImportProgress{
					Decoded:  decodedCnt.Load(),
					Inserted: insertedCnt.Load(),
					Errors:   errCnt.Load(),
					Elapsed:  time.Since(start),
				})
			}
		}
		addDoc = func(doc map[string]any) {
			decodedCnt.Add(1)
			batch = append(batch, doc)
			if len(batch) >= opts.BatchSize {
				flush()
			}
		}
	}

	batchCh := make(chan flushedBatch, 2)
	doneCh := make(chan struct{})

	if pipelined {
		go func() {
			defer close(doneCh)
			for fb := range batchCh {
				inserted, errs := e.insertBatchLocalDetailed(dbName, block, fb.docs)
				insertedCnt.Add(inserted)
				for _, err := range errs {
					if err == nil {
						continue
					}
					recordErr(err.Error())
				}
				if opts.Progress != nil {
					opts.Progress(BulkImportProgress{
						Decoded:  decodedCnt.Load(),
						Inserted: insertedCnt.Load(),
						Errors:   errCnt.Load(),
						Elapsed:  time.Since(start),
					})
				}
			}
		}()

		batch := make([]map[string]any, 0, opts.BatchSize)
		flush = func() {
			if len(batch) == 0 {
				return
			}
			batchCh <- flushedBatch{docs: batch}
			batch = make([]map[string]any, 0, opts.BatchSize)
		}

		addDoc = func(doc map[string]any) {
			decodedCnt.Add(1)
			batch = append(batch, doc)
			if len(batch) >= opts.BatchSize {
				flush()
			}
		}
	} else {
		close(batchCh)
		close(doneCh)
	}

	var decodeErr error
	if format == "ndjson" {
		decodeErr = decodeNDJSONDocs(r, opts, recordErr, addDoc)
	} else {
		decodeErr = decodeJSONArrayDocs(r, opts, recordErr, addDoc)
	}

	// Flush whatever's pending regardless of how decoding ended, so a
	// ContinueOnError=false stop (or an I/O error) still commits every
	// document decoded before the failure instead of discarding a
	// partially-filled final batch.
	flush()
	if pipelined {
		close(batchCh)
	}
	<-doneCh

	res.Decoded = decodedCnt.Load()
	res.Inserted = insertedCnt.Load()
	res.Errors = errCnt.Load()
	res.ErrorSamples = errorSamples
	res.Took = time.Since(start)

	if decodeErr != nil {
		return res, decodeErr
	}
	return res, nil
}

// decodeNDJSONDocs reads one JSON object per line from r, calling addDoc
// for each successfully decoded document and recordErr for each line that
// fails to decode. recordErr must be safe to call concurrently with the
// insert goroutine draining previously-flushed batches (see
// BulkImportReader).
func decodeNDJSONDocs(r io.Reader, opts BulkImportOptions, recordErr func(string), addDoc func(map[string]any)) error {
	scanner := bufio.NewScanner(r)
	// A document can legitimately be larger than bufio.Scanner's 64KB
	// default token limit; grow it well past any reasonable single-line
	// document instead of silently erroring out on the first large one.
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var doc map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&doc); err != nil {
			recordErr(fmt.Sprintf("line %d: %v", lineNo, err))
			if !opts.ContinueOnError {
				return fmt.Errorf("line %d: %w", lineNo, err)
			}
			continue
		}
		addDoc(unwrapExportEnvelope(doc))
	}
	return scanner.Err()
}

// decodeJSONArrayDocs reads a single top-level JSON array of objects from
// r using a streaming token decoder, calling addDoc for each element and
// recordErr for each element that fails to decode. recordErr must be safe
// to call concurrently with the insert goroutine draining
// previously-flushed batches (see BulkImportReader).
func decodeJSONArrayDocs(r io.Reader, opts BulkImportOptions, recordErr func(string), addDoc func(map[string]any)) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected a top-level JSON array")
	}

	idx := 0
	for dec.More() {
		idx++
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			recordErr(fmt.Sprintf("element %d: %v", idx, err))
			if !opts.ContinueOnError {
				return fmt.Errorf("element %d: %w", idx, err)
			}
			continue
		}
		addDoc(unwrapExportEnvelope(doc))
	}
	return nil
}
