// Package cluster holds CaimanDB's node-local, dependency-free
// concurrency/fan-out infrastructure: the real-time change-event bus
// (ChangeBus) and the generic background worker pool (WorkerPool).
//
// The Raft-based multi-node coordination logic (ClusterManager) is NOT
// here — it holds a direct *Engine reference and is tightly coupled to
// the root package's internals, so it remains there (see cluster.go)
// to avoid an import cycle. This package only has the two pieces that
// are genuinely standalone.
package cluster

import (
	"caimandb/internal/caimandb/metrics"
	"sync"
	"sync/atomic"
	"time"
)

// ChangeEvent describes a single document mutation. It's the payload
// delivered to real-time subscribers (HTTP SSE watchers, the NQL WATCH
// command, etc).
type ChangeEvent struct {
	Op        string         `json:"op"` // "insert" | "update" | "delete"
	DB        string         `json:"db"`
	Block     string         `json:"block"`
	ID        string         `json:"id"`
	ShardID   string         `json:"shard_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Timestamp int64          `json:"timestamp"`
}

// subscriberBufferSize bounds how many events a single slow subscriber can
// lag behind by before being dropped. Real-time delivery to N subscribers
// must never let the Nth slow reader stall the write path, so publish is
// always non-blocking (see ChangeBus.Publish).
const subscriberBufferSize = 256

// replayBufferSize bounds the short-term history ChangeBus keeps so a
// subscriber that just connected can immediately catch up on the last few
// events instead of only seeing whatever happens to arrive after it
// subscribes. Backed by eventRing below: a fixed-capacity ring buffer, so
// evicting the oldest entry once full is O(1) -- unlike the slice-based
// `append` + reslice this would otherwise need on every single publish.
const replayBufferSize = 200

// eventRing is a small, fixed-capacity ring-buffer FIFO of ChangeEvent --
// the same "ring buffer instead of slice+append" idea a queue library like
// eapache/queue is built on, kept in-tree here because that library's
// generics (v2) subpath isn't published as an independently-versioned Go
// module (no resolvable tag/pseudo-version via the module proxy), so
// depending on it isn't reproducible from a clean `go get`. Not
// goroutine-safe on its own -- callers (ChangeBus) provide the lock.
type eventRing struct {
	buf   []ChangeEvent
	head  int
	count int
}

func newEventRing(capacity int) *eventRing {
	return &eventRing{buf: make([]ChangeEvent, capacity)}
}

// Add appends ev, overwriting the oldest entry once the ring is at
// capacity -- exactly the "drop the oldest, O(1)" behavior the replay
// buffer wants.
func (r *eventRing) Add(ev ChangeEvent) {
	if r.count < len(r.buf) {
		r.buf[(r.head+r.count)%len(r.buf)] = ev
		r.count++
		return
	}
	r.buf[r.head] = ev
	r.head = (r.head + 1) % len(r.buf)
}

func (r *eventRing) Length() int { return r.count }

// Get returns the i-th oldest element currently in the ring (0 = oldest).
func (r *eventRing) Get(i int) ChangeEvent {
	return r.buf[(r.head+i)%len(r.buf)]
}

type subscriber struct {
	id    uint64
	ch    chan ChangeEvent
	db    string // "" = all databases
	block string // "" = all blocks (within db, if db != "")
}

// ChangeBus is a lightweight, in-process pub/sub hub for real-time change
// notifications. It intentionally has no cross-node fan-out or durable
// persistence -- those are legitimate future extensions -- but it does
// keep a short in-memory replay buffer (see replayBufferSize) so a
// dashboard or sync client that just subscribed isn't left blind until
// the next write happens. The goal is the common case: "give me a live
// feed of writes to db.block as they happen" for dashboards, cache
// invalidation, sync clients, etc., without paying for a poll loop
// against the engine.
//
// Design choices that matter for real-time/high-throughput use:
//   - Publish() never blocks the write path. Each subscriber has its own
//     bounded channel; a full channel means that subscriber drops the
//     event (and its DroppedCount is incremented) rather than the writer
//     stalling. A slow watcher can never become a throughput ceiling for
//     inserts/updates/deletes.
//   - Fan-out is O(matching subscribers), and subscriber lookup is a
//     single RLock over a small slice -- cheap even at high event rates,
//     and there's normally far fewer watchers than writers.
type ChangeBus struct {
	mu       sync.RWMutex
	subs     map[uint64]*subscriber
	nextID   uint64
	// dropped is per-subscriber dropped-event counters. Publish only ever
	// takes mu.RLock() (many concurrent publishers, by design -- see the
	// Publish doc comment), so these counters must be their own
	// atomic.Int64 rather than a plain int64 incremented in place: a
	// plain *int64 mutated under a shared RLock is a data race the
	// moment two insert workers publish concurrently, which is the
	// normal case under GENERATE/bulk load.
	dropped  map[uint64]*atomic.Int64
	closedCh chan struct{}

	// bulkHint mirrors the engine's turbo bulk-load flag (see
	// SetBulkModeHint). While on and no subscriber is attached, Publish
	// skips its replay-buffer append as a deliberate throughput/history
	// trade-off -- see Publish's doc comment for what that costs a
	// client that subscribes right after a bulk load finishes.
	bulkHint atomic.Bool

	replayMu sync.Mutex
	replay   *eventRing
}

func NewChangeBus() *ChangeBus {
	return &ChangeBus{
		subs:     make(map[uint64]*subscriber),
		dropped:  make(map[uint64]*atomic.Int64),
		closedCh: make(chan struct{}),
		replay:   newEventRing(replayBufferSize),
	}
}

// Subscribe registers a new watcher. db == "" matches every database;
// block == "" (with a non-empty db) matches every block in that database.
// The returned channel is closed and the subscription torn down when the
// provided ctxDone channel fires (typically an HTTP request's ctx.Done())
// — callers should always arrange for that to happen, or call Unsubscribe
// directly, to avoid leaking subscriber channels.
func (b *ChangeBus) Subscribe(db, block string) (id uint64, events <-chan ChangeEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id = b.nextID
	sub := &subscriber{
		id:    id,
		ch:    make(chan ChangeEvent, subscriberBufferSize),
		db:    db,
		block: block,
	}
	b.subs[id] = sub
	b.dropped[id] = &atomic.Int64{}
	return id, sub.ch
}

// Unsubscribe removes a watcher and closes its channel. Safe to call more
// than once for the same id.
func (b *ChangeBus) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sub, ok := b.subs[id]; ok {
		close(sub.ch)
		delete(b.subs, id)
		delete(b.dropped, id)
	}
}

// SetBulkModeHint mirrors the engine-level BULK MODE flag into the bus
// (call this from the same place that flips turbo.BulkMode, alongside
// DBPool.SetBulkModeHint -- see Engine.SetBulkMode). Purely advisory: it
// only ever lets Publish take an early exit that is safe precisely
// because it re-checks the live subscriber count on every call rather
// than caching a decision.
func (b *ChangeBus) SetBulkModeHint(on bool) {
	if b == nil {
		return
	}
	b.bulkHint.Store(on)
}

// Publish fans an event out to every matching subscriber without ever
// blocking on a slow reader, and appends it to the short replay buffer
// (see RecentEvents) so a subscriber that connects moments later can
// still catch up on it.
//
// Called once per document from the synchronous post-commit path (see
// insertBatchLocalDetailed), so at bulk-GENERATE volumes this runs
// millions of times in a row from many concurrent insert workers. The
// replay-buffer append used to take replayMu.Lock() -- a single mutex
// shared by the whole engine, exclusive, unconditionally -- every single
// call, which serialized every concurrent insert batch's post-commit
// step behind one global lock regardless of which block/shard each
// batch belonged to.
//
// When bulkHint is set and nobody is currently subscribed, Publish skips
// both the fan-out (nothing to fan out to) and the replay-buffer append.
// Skipping the fan-out is free -- there's no observer, full stop. Skipping
// the replay append is a real, deliberate trade-off, not a free lunch: a
// client that subscribes right after a bulk load finishes will see an
// empty catch-up buffer instead of the load's last ~200 events, whereas
// today it wouldn't. That's the same kind of trade this codebase already
// makes for BULK MODE elsewhere (relaxed WAL fsync via WALSyncInterval,
// TierLarge storage hints) -- it only ever applies while BULK MODE is
// explicitly on, and reverts the instant a subscriber connects (checked
// fresh on every call, nothing cached), so normal/non-bulk operation and
// any bulk load someone is actively watching keep the full guarantee.
func (b *ChangeBus) Publish(ev ChangeEvent) {
	if b.bulkHint.Load() {
		b.mu.RLock()
		empty := len(b.subs) == 0
		b.mu.RUnlock()
		if empty {
			return
		}
	}

	b.replayMu.Lock()
	b.replay.Add(ev)
	b.replayMu.Unlock()

	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.subs) == 0 {
		return // hot path when nobody is watching: one RLock, no allocation
	}

	for id, sub := range b.subs {
		if sub.db != "" && sub.db != ev.DB {
			continue
		}
		if sub.block != "" && sub.block != ev.Block {
			continue
		}
		select {
		case sub.ch <- ev:
		default:
			if counter, ok := b.dropped[id]; ok {
				counter.Add(1)
			}
			metrics.MetricChangeEventsDropped.Inc()
		}
	}
}

// RecentEvents returns up to the last replayBufferSize events that
// matched db/block (same "" == wildcard rules as Subscribe), oldest
// first, so a newly-subscribed watcher can immediately catch up instead
// of only seeing events published after it connected.
func (b *ChangeBus) RecentEvents(db, block string) []ChangeEvent {
	b.replayMu.Lock()
	defer b.replayMu.Unlock()

	n := b.replay.Length()
	out := make([]ChangeEvent, 0, n)
	for i := 0; i < n; i++ {
		ev := b.replay.Get(i)
		if db != "" && ev.DB != db {
			continue
		}
		if block != "" && ev.Block != block {
			continue
		}
		out = append(out, ev)
	}
	return out
}

// PublishInsert/PublishUpdate/PublishDelete are small conveniences called
// from the mutation paths (ops_local.go, ops_insert.go) so those call
// sites stay one-liners. bus may be nil (e.g. engine shutting down); all
// three are no-ops in that case.
func (b *ChangeBus) PublishInsert(db, block, id, shardID string, data map[string]any) {
	if b == nil {
		return
	}
	b.Publish(ChangeEvent{Op: "insert", DB: db, Block: block, ID: id, ShardID: shardID, Data: data, Timestamp: time.Now().UnixNano()})
}

func (b *ChangeBus) PublishUpdate(db, block, id, shardID string, data map[string]any) {
	if b == nil {
		return
	}
	b.Publish(ChangeEvent{Op: "update", DB: db, Block: block, ID: id, ShardID: shardID, Data: data, Timestamp: time.Now().UnixNano()})
}

func (b *ChangeBus) PublishDelete(db, block, id, shardID string) {
	if b == nil {
		return
	}
	b.Publish(ChangeEvent{Op: "delete", DB: db, Block: block, ID: id, ShardID: shardID, Timestamp: time.Now().UnixNano()})
}

// Stats reports subscriber counts and per-subscriber drop totals, exposed
// via /status for observability.
func (b *ChangeBus) Stats() map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var totalDropped int64
	for _, d := range b.dropped {
		totalDropped += d.Load()
	}
	b.replayMu.Lock()
	replayLen := b.replay.Length()
	b.replayMu.Unlock()

	return map[string]any{
		"subscribers":    len(b.subs),
		"dropped_events": totalDropped,
		"replay_buffered": replayLen,
	}
}
