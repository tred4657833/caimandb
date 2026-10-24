package turbo

import "sync"

// Priority controls dispatch order in Scheduler. Higher-priority work is
// always dispatched before lower-priority work when both are ready to run.
type Priority int

const (
	// PriorityBulk is for large background loads (BULK MODE, imports)
	// that should use spare capacity but never starve interactive work.
	PriorityBulk Priority = iota
	// PriorityLow is for background maintenance (compaction, index
	// rebuilds, secondary-index catch-up).
	PriorityLow
	// PriorityNormal is the default for ordinary write batches.
	PriorityNormal
	// PriorityHigh is for latency-sensitive, user-facing work.
	PriorityHigh
)

const numPriorities = int(PriorityHigh) + 1

type task struct {
	key string
	fn  func()
}

// Scheduler dispatches work onto an AdaptiveWorkerPool with two properties
// plain pool.Submit doesn't give you:
//
//  1. Priority ordering: when multiple tasks are ready to run, higher
//     priority tasks are dispatched first.
//  2. Key affinity: tasks sharing a non-empty key never run concurrently
//     with each other (they run in submission order), while tasks with
//     different keys (or no key) run fully in parallel. This is what lets
//     a caller safely batch/serialize per-block or per-document work
//     without needing its own extra locking on top.
//
// The zero value is not usable; construct with NewScheduler.
type Scheduler struct {
	pool *AdaptiveWorkerPool

	mu      sync.Mutex
	queues  [numPriorities][]*task
	keyBusy map[string]bool

	notify chan struct{}
	stopCh chan struct{}
}

// NewScheduler creates a Scheduler that dispatches onto pool. The caller
// keeps ownership of pool (Scheduler does not close it).
func NewScheduler(pool *AdaptiveWorkerPool) *Scheduler {
	s := &Scheduler{
		pool:    pool,
		keyBusy: make(map[string]bool),
		notify:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	go s.dispatchLoop()
	return s
}

// Submit enqueues fn at the given priority. If key is non-empty, fn is
// guaranteed not to run concurrently with any other currently-queued or
// currently-running task sharing the same key.
func (s *Scheduler) Submit(priority Priority, key string, fn func()) {
	if priority < 0 {
		priority = PriorityNormal
	}
	if int(priority) >= numPriorities {
		priority = PriorityHigh
	}
	s.mu.Lock()
	s.queues[priority] = append(s.queues[priority], &task{key: key, fn: fn})
	s.mu.Unlock()
	s.wake()
}

func (s *Scheduler) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Scheduler) dispatchLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.notify:
			s.drainReady()
		}
	}
}

// drainReady dispatches every task that's currently eligible to run (i.e.
// whose key, if any, isn't already busy), highest priority first.
func (s *Scheduler) drainReady() {
	for {
		t := s.popNext()
		if t == nil {
			return
		}
		key := t.key
		fn := t.fn
		s.pool.Submit(func() {
			defer s.finish(key)
			fn()
		})
	}
}

func (s *Scheduler) popNext() *task {
	s.mu.Lock()
	defer s.mu.Unlock()

	for p := numPriorities - 1; p >= 0; p-- {
		q := s.queues[p]
		for i, t := range q {
			if t.key != "" && s.keyBusy[t.key] {
				continue
			}
			s.queues[p] = append(q[:i], q[i+1:]...)
			if t.key != "" {
				s.keyBusy[t.key] = true
			}
			return t
		}
	}
	return nil
}

func (s *Scheduler) finish(key string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	delete(s.keyBusy, key)
	s.mu.Unlock()
	s.wake() // a task queued behind this key may now be eligible
}

// Stats returns a snapshot of queue depth per priority level.
func (s *Scheduler) Stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"queued_bulk":   len(s.queues[PriorityBulk]),
		"queued_low":    len(s.queues[PriorityLow]),
		"queued_normal": len(s.queues[PriorityNormal]),
		"queued_high":   len(s.queues[PriorityHigh]),
		"busy_keys":     len(s.keyBusy),
	}
}

// Close stops the dispatch loop. It does not close the underlying pool.
func (s *Scheduler) Close() {
	close(s.stopCh)
}
