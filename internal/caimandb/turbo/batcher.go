package turbo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Batcher coalesces concurrent Submit calls into a single flush call once
// either maxSize items have accumulated, a short "quiet window" has passed
// since the last item arrived, or a hard maxWait cap (measured from the
// first item in the current batch) is reached -- whichever comes first.
//
// This is adaptive group commit: under low/sequential load (one Submit at
// a time, nothing else arriving) a batch flushes almost immediately after
// quietWait, instead of always paying the full maxWait like a fixed-window
// batcher would. Under concurrent bursts, each new arrival resets the
// quiet timer, so the batch keeps growing -- exactly like before -- until
// either traffic goes quiet or maxWait/maxSize forces a flush, which keeps
// the same worst-case latency bound as the old fixed-window design.
//
// flush is called with the batch's items in submission order and must
// return a slice of exactly len(items) errors (nil entries mean success).
// Each Submit call blocks only until its own item has been flushed, not
// until the whole batch completes any slower way -- flush runs once for
// the whole batch and every waiter is released as soon as it returns.
//
// The zero value is not usable; construct with NewBatcher.
type Batcher[T any] struct {
	maxSize   atomic.Int64
	maxWait   atomic.Int64 // time.Duration stored as int64 nanoseconds
	quietWait atomic.Int64 // time.Duration stored as int64 nanoseconds

	flush func([]T) []error

	mu         sync.Mutex
	pending    []T
	waiters    []chan error
	capTimer   *time.Timer // fires maxWait after the first item in this batch
	quietTimer *time.Timer // reset on every arrival; fires quietWait after the last one
}

// deriveQuietWait picks the adaptive "flush soon if nothing else shows up"
// window for a given maxWait cap: short enough that an isolated, sequential
// Submit doesn't pay for the whole cap, but long enough to still catch
// near-simultaneous arrivals (goroutine/network scheduling jitter). Capped
// below maxWait so it can never itself exceed the hard bound.
func deriveQuietWait(maxWait time.Duration) time.Duration {
	q := maxWait / 8
	if q < 200*time.Microsecond {
		q = 200 * time.Microsecond
	}
	if q > maxWait {
		q = maxWait
	}
	return q
}

// NewBatcher creates a Batcher that flushes at maxSize items, after a short
// adaptive quiet period with no new arrivals, or at maxWait (from the first
// item), whichever comes first.
func NewBatcher[T any](maxSize int, maxWait time.Duration, flush func([]T) []error) *Batcher[T] {
	if maxSize < 1 {
		maxSize = 1
	}
	if maxWait <= 0 {
		maxWait = time.Millisecond
	}
	b := &Batcher[T]{flush: flush}
	b.maxSize.Store(int64(maxSize))
	b.maxWait.Store(int64(maxWait))
	b.quietWait.Store(int64(deriveQuietWait(maxWait)))
	return b
}

// SetThresholds updates the batch size/wait thresholds (and re-derives the
// adaptive quiet window from the new maxWait) live. Safe to call
// concurrently with Submit; takes effect for the next batch that starts
// forming (a batch already in flight keeps its original cap timer, though
// any further arrivals in it will reset their quiet timer using the new
// value).
func (b *Batcher[T]) SetThresholds(maxSize int, maxWait time.Duration) {
	if maxSize < 1 {
		maxSize = 1
	}
	if maxWait <= 0 {
		maxWait = time.Millisecond
	}
	b.maxSize.Store(int64(maxSize))
	b.maxWait.Store(int64(maxWait))
	b.quietWait.Store(int64(deriveQuietWait(maxWait)))
}

// Submit adds v to the current (or a new) batch and blocks until that
// batch has been flushed, returning this item's individual result.
func (b *Batcher[T]) Submit(v T) error {
	ch := make(chan error, 1)

	b.mu.Lock()
	b.pending = append(b.pending, v)
	b.waiters = append(b.waiters, ch)
	full := len(b.pending) >= int(b.maxSize.Load())
	first := len(b.pending) == 1

	var items []T
	var waiters []chan error
	if full {
		items, waiters = b.drainLocked()
	} else {
		if first {
			wait := time.Duration(b.maxWait.Load())
			b.capTimer = time.AfterFunc(wait, b.onTimer)
		}
		// Every arrival pushes the quiet-flush point out again -- an
		// isolated Submit flushes after just quietWait, but a steady
		// stream of arrivals keeps deferring it until capTimer (or
		// maxSize) forces the issue.
		quiet := time.Duration(b.quietWait.Load())
		if b.quietTimer != nil {
			b.quietTimer.Stop()
		}
		b.quietTimer = time.AfterFunc(quiet, b.onTimer)
	}
	b.mu.Unlock()

	if items != nil {
		b.runFlush(items, waiters)
	}

	return <-ch
}

// onTimer is the callback for both capTimer and quietTimer. Whichever
// fires first drains and flushes the batch; the other is a no-op because
// drainLocked finds nothing left to drain (guarded by len(b.pending)==0).
func (b *Batcher[T]) onTimer() {
	b.mu.Lock()
	items, waiters := b.drainLocked()
	b.mu.Unlock()
	if items != nil {
		b.runFlush(items, waiters)
	}
}

// drainLocked must be called with b.mu held. It hands back the current
// batch and resets internal state for the next one.
func (b *Batcher[T]) drainLocked() ([]T, []chan error) {
	if len(b.pending) == 0 {
		return nil, nil
	}
	items := b.pending
	waiters := b.waiters
	b.pending = nil
	b.waiters = nil
	if b.capTimer != nil {
		b.capTimer.Stop()
		b.capTimer = nil
	}
	if b.quietTimer != nil {
		b.quietTimer.Stop()
		b.quietTimer = nil
	}
	return items, waiters
}

func (b *Batcher[T]) runFlush(items []T, waiters []chan error) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("batch flush panic: %v", r)
			for _, w := range waiters {
				w <- err
			}
		}
	}()

	errs := b.flush(items)
	for i, w := range waiters {
		var e error
		if i < len(errs) {
			e = errs[i]
		}
		w <- e
	}
}

// Pending returns the number of items currently buffered for the batch in
// progress, for stats/metrics.
func (b *Batcher[T]) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// BatcherGroup manages one Batcher[T] per key, created lazily on first use.
// This is the shape most callers actually want: e.g. one auto-coalescing
// batch per (database, block) instead of one global batch that would force
// unrelated writes to wait on each other.
type BatcherGroup[T any] struct {
	maxSize int
	maxWait time.Duration
	flush   func(key string, items []T) []error

	mu     sync.Mutex
	groups map[string]*Batcher[T]
}

// NewBatcherGroup creates a BatcherGroup with the given default thresholds.
// flush receives the key each batch belongs to along with its items.
func NewBatcherGroup[T any](maxSize int, maxWait time.Duration, flush func(key string, items []T) []error) *BatcherGroup[T] {
	return &BatcherGroup[T]{
		maxSize: maxSize,
		maxWait: maxWait,
		flush:   flush,
		groups:  make(map[string]*Batcher[T]),
	}
}

// Submit routes v into the batch for key, creating that batch's Batcher on
// first use, and blocks until it has been flushed.
func (g *BatcherGroup[T]) Submit(key string, v T) error {
	g.mu.Lock()
	b, ok := g.groups[key]
	if !ok {
		k := key // capture per-key for the closure below
		b = NewBatcher[T](g.maxSize, g.maxWait, func(items []T) []error {
			return g.flush(k, items)
		})
		g.groups[key] = b
	}
	g.mu.Unlock()
	return b.Submit(v)
}

// SetThresholds updates the default thresholds for keys created from now
// on, and live-updates every existing key's batcher too.
func (g *BatcherGroup[T]) SetThresholds(maxSize int, maxWait time.Duration) {
	g.mu.Lock()
	g.maxSize = maxSize
	g.maxWait = maxWait
	existing := make([]*Batcher[T], 0, len(g.groups))
	for _, b := range g.groups {
		existing = append(existing, b)
	}
	g.mu.Unlock()

	for _, b := range existing {
		b.SetThresholds(maxSize, maxWait)
	}
}

// Stats returns the number of active keys and their combined pending item
// count, for stats/metrics.
func (g *BatcherGroup[T]) Stats() map[string]any {
	g.mu.Lock()
	keys := make([]*Batcher[T], 0, len(g.groups))
	for _, b := range g.groups {
		keys = append(keys, b)
	}
	n := len(g.groups)
	g.mu.Unlock()

	pending := 0
	for _, b := range keys {
		pending += b.Pending()
	}
	return map[string]any{
		"active_keys": n,
		"pending":     pending,
	}
}
