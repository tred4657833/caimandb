package turbo

import "sync/atomic"

// BulkMode is a simple on/off switch a caller can flip while doing a large
// load (IMPORT, a long run of INSERTs, etc.) so batchers can widen their
// windows, the WAL can relax its fsync policy, and secondary work like
// indexing/compression can be deferred -- then flip back off afterward to
// restore normal low-latency behavior.
//
// BulkMode itself holds no policy; it's just the shared flag other pieces
// (Coordinator.BatchThresholds, the caller's own WAL sync policy switch,
// etc.) read to decide what to do.
type BulkMode struct {
	on atomic.Bool
}

// Enable turns bulk mode on.
func (b *BulkMode) Enable() { b.on.Store(true) }

// Disable turns bulk mode off.
func (b *BulkMode) Disable() { b.on.Store(false) }

// Enabled reports whether bulk mode is currently on.
func (b *BulkMode) Enabled() bool { return b.on.Load() }
