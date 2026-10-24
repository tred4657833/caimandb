package turbo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveWorkerPool_RunsAllJobs(t *testing.T) {
	p := NewAdaptiveWorkerPool(2, 8)
	defer p.Close()

	const n = 500
	var count atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		p.Submit(func() {
			count.Add(1)
			wg.Done()
		})
	}
	wg.Wait()

	if got := count.Load(); got != n {
		t.Fatalf("expected %d jobs to run, got %d", n, got)
	}
}

func TestAdaptiveWorkerPool_GrowsUnderLoad(t *testing.T) {
	p := NewAdaptiveWorkerPool(1, 16)
	defer p.Close()

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(8)

	for i := 0; i < 8; i++ {
		p.Submit(func() {
			started.Done()
			<-release
		})
	}

	started.Wait()
	// Give the monitor a couple of sample ticks to react.
	time.Sleep(sampleInterval * 3)

	if got := p.current.Load(); got <= 1 {
		t.Fatalf("expected pool to grow beyond min=1 under load, current=%d", got)
	}

	close(release)
}

func TestAdaptiveWorkerPool_PanicDoesNotKillPool(t *testing.T) {
	p := NewAdaptiveWorkerPool(1, 2)
	defer p.Close()

	p.Submit(func() { panic("boom") })

	var ran atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	p.Submit(func() {
		ran.Store(true)
		wg.Done()
	})
	wg.Wait()

	if !ran.Load() {
		t.Fatal("expected pool to keep working after a job panics")
	}
	if p.panics.Load() < 1 {
		t.Fatal("expected panic to be recorded")
	}
}

func TestAdaptiveWorkerPool_CloseStopsAcceptingWork(t *testing.T) {
	p := NewAdaptiveWorkerPool(1, 2)
	p.Close()

	var ran atomic.Bool
	done := make(chan struct{})
	go func() {
		p.Submit(func() { ran.Store(true) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Submit did not return after Close")
	}
	if ran.Load() {
		t.Fatal("job should not have run after Close")
	}
}
