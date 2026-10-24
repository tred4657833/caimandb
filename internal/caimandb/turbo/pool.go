package turbo

import (
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// idleTimeout is how long a worker waits for a job before it becomes a
// candidate for self-termination when the pool is above its minimum size.
const idleTimeout = 2 * time.Second

// sampleInterval is how often the pool re-evaluates whether it needs more
// workers.
const sampleInterval = 200 * time.Millisecond

// AdaptiveWorkerPool is a goroutine pool whose worker count floats between
// [min, max] based on sampled load, instead of being fixed for the life of
// the process:
//
//   - Workers that sit idle past idleTimeout self-terminate once the pool
//     is above its minimum size, so a burst of work doesn't leave behind a
//     permanently oversized pool of goroutines doing nothing.
//   - A periodic monitor spawns additional workers when the job queue is
//     backing up and every current worker is busy, up to max.
//
// The zero value is not usable; construct with NewAdaptiveWorkerPool.
type AdaptiveWorkerPool struct {
	jobs chan func()

	min int
	max int

	current   atomic.Int64
	active    atomic.Int64
	submitted atomic.Int64
	completed atomic.Int64
	panics    atomic.Int64

	wg        sync.WaitGroup
	stopCh    chan struct{}
	closeOnce sync.Once
}

// NewAdaptiveWorkerPool creates a pool with worker count bounded to
// [min, max]. A min/max of 0 or less resolves to a sensible default derived
// from GOMAXPROCS: min defaults to GOMAXPROCS, max defaults to
// GOMAXPROCS*8. The pool starts at min workers and grows on demand.
func NewAdaptiveWorkerPool(min, max int) *AdaptiveWorkerPool {
	procs := runtime.GOMAXPROCS(0)
	if min <= 0 {
		min = procs
	}
	if max <= 0 {
		max = procs * 8
	}
	if max < min {
		max = min
	}

	p := &AdaptiveWorkerPool{
		jobs:   make(chan func(), max*64),
		min:    min,
		max:    max,
		stopCh: make(chan struct{}),
	}

	for i := 0; i < min; i++ {
		p.spawnWorker()
	}

	go p.monitor()

	return p
}

// Submit enqueues fn to run on the pool. It blocks only if the internal
// queue is completely full (backpressure), which under normal operation
// means "the pool couldn't grow fast enough for a very sudden burst" -- a
// deliberate trade-off over unbounded queueing, which would just move
// memory pressure from here to somewhere worse.
func (p *AdaptiveWorkerPool) Submit(fn func()) {
	p.submitted.Add(1)
	select {
	case p.jobs <- fn:
	case <-p.stopCh:
	}
}

// TrySubmit is the non-blocking variant of Submit: it returns false instead
// of blocking when the queue is full or the pool is closed.
func (p *AdaptiveWorkerPool) TrySubmit(fn func()) bool {
	select {
	case p.jobs <- fn:
		p.submitted.Add(1)
		return true
	default:
		return false
	}
}

func (p *AdaptiveWorkerPool) spawnWorker() {
	p.current.Add(1)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer p.current.Add(-1)

		timer := time.NewTimer(idleTimeout)
		defer timer.Stop()

		for {
			select {
			case fn, ok := <-p.jobs:
				if !ok {
					return
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				p.runJob(fn)
				timer.Reset(idleTimeout)

			case <-timer.C:
				if p.current.Load() > int64(p.min) {
					return
				}
				timer.Reset(idleTimeout)

			case <-p.stopCh:
				return
			}
		}
	}()
}

func (p *AdaptiveWorkerPool) runJob(fn func()) {
	p.active.Add(1)
	defer func() {
		p.active.Add(-1)
		p.completed.Add(1)
		if r := recover(); r != nil {
			p.panics.Add(1)
			// Deliberately swallow the stack rather than crash the whole
			// pool; callers that care about panics should recover inside
			// their own fn and report through their own error channel.
			_ = debug.Stack()
		}
	}()
	fn()
}

// monitor periodically checks whether the pool is under pressure (queue
// backing up while every worker is busy) and, if so, grows it.
func (p *AdaptiveWorkerPool) monitor() {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.rebalance()
		case <-p.stopCh:
			return
		}
	}
}

func (p *AdaptiveWorkerPool) rebalance() {
	cur := int(p.current.Load())
	act := int(p.active.Load())
	queued := len(p.jobs)

	if queued == 0 || act < cur {
		return // no backlog, or there's slack capacity already
	}
	if cur >= p.max {
		return
	}

	grow := queued
	if room := p.max - cur; grow > room {
		grow = room
	}
	if grow < 1 {
		grow = 1
	}
	for i := 0; i < grow; i++ {
		p.spawnWorker()
	}
}

// Stats returns a snapshot of pool activity, suitable for STATUS/metrics.
func (p *AdaptiveWorkerPool) Stats() map[string]any {
	return map[string]any{
		"workers":   p.current.Load(),
		"active":    p.active.Load(),
		"queued":    len(p.jobs),
		"min":       p.min,
		"max":       p.max,
		"submitted": p.submitted.Load(),
		"completed": p.completed.Load(),
		"panics":    p.panics.Load(),
	}
}

// Close stops accepting new work and waits for in-flight jobs to finish.
// Jobs still sitting in the queue when Close is called are dropped (not
// executed) -- callers that need graceful drain should stop submitting and
// wait for Stats().queued/active to hit zero before calling Close.
func (p *AdaptiveWorkerPool) Close() {
	p.closeOnce.Do(func() {
		close(p.stopCh)
		p.wg.Wait()
	})
}
