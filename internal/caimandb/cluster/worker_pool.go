package cluster

import (
	"context"
	"caimandb/internal/caimandb/logging"
	"go.uber.org/zap"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type job struct {
	fn   func()
	ctx  context.Context
	name string
}

type WorkerPool struct {
	jobs   chan job
	wg     sync.WaitGroup
	mu     sync.Mutex
	done   bool
	// stopCh is closed by Close instead of jobs. jobs is never closed:
	// closing it would race with an in-flight Submit that already passed
	// the p.done check and is about to send, which can panic with "send
	// on closed channel". Both Submit and worker select on stopCh
	// alongside jobs, so a submission racing with Close is safely
	// dropped instead of ever reaching a closed channel.
	stopCh chan struct{}
	active atomic.Int64
	queued atomic.Int64
}

func NewWorkerPool(n int) *WorkerPool {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0) * 64
	}
	p := &WorkerPool{
		jobs:   make(chan job, n*256),
		stopCh: make(chan struct{}),
	}
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *WorkerPool) Submit(ctx context.Context, name string, fn func()) {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	p.queued.Add(1)

	select {
	case p.jobs <- job{
		fn:   fn,
		ctx:  ctx,
		name: name,
	}:
		p.queued.Add(-1)

	case <-ctx.Done():
		p.queued.Add(-1)
		logging.Log().Warn("Job submission cancelled",
			zap.String("job", name),
			zap.Error(ctx.Err()),
		)

	case <-p.stopCh:
		// Close() ran concurrently with this Submit; drop the job
		// instead of risking a send on a (potentially closed) channel.
		p.queued.Add(-1)
	}
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			logging.Log().Error("Worker panic recovered",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())),
			)
		}
	}()
	for {
		var j job
		select {
		case j = <-p.jobs:
		case <-p.stopCh:
			return
		}
		p.active.Add(1)
		func() {
			defer func() {
				if r := recover(); r != nil {
					logging.Log().Error("Job panic recovered",
						zap.Any("panic", r),
						zap.String("job", j.name),
						zap.String("stack", string(debug.Stack())),
					)
				}
				p.active.Add(-1)
			}()
			select {
			case <-j.ctx.Done():
				logging.Log().Warn("Job cancelled", zap.String("job", j.name))
				return
			default:
				j.fn()
			}
		}()
	}
}

// Close stops accepting new work and waits for in-flight/already-queued
// jobs to be picked up or dropped. It never closes the jobs channel (see
// the stopCh comment on WorkerPool) so a concurrent Submit can never
// panic sending on a closed channel; both sides race safely on stopCh
// instead.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	if !p.done {
		p.done = true
		close(p.stopCh)
	}
	p.mu.Unlock()
	p.wg.Wait()
}
