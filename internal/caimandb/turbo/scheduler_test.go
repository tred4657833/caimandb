package turbo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler_KeyAffinitySerializesSameKey(t *testing.T) {
	pool := NewAdaptiveWorkerPool(4, 8)
	defer pool.Close()
	s := NewScheduler(pool)
	defer s.Close()

	var running atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup

	const n = 20
	wg.Add(n)
	for i := 0; i < n; i++ {
		s.Submit(PriorityNormal, "same-key", func() {
			defer wg.Done()
			cur := running.Add(1)
			for {
				m := maxConcurrent.Load()
				if cur <= m || maxConcurrent.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
		})
	}
	wg.Wait()

	if got := maxConcurrent.Load(); got != 1 {
		t.Fatalf("expected same-key tasks to never run concurrently, max concurrent=%d", got)
	}
}

func TestScheduler_DifferentKeysRunInParallel(t *testing.T) {
	pool := NewAdaptiveWorkerPool(4, 16)
	defer pool.Close()
	s := NewScheduler(pool)
	defer s.Close()

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(4)

	for i := 0; i < 4; i++ {
		key := string(rune('a' + i))
		s.Submit(PriorityNormal, key, func() {
			started.Done()
			<-release
		})
	}

	done := make(chan struct{})
	go func() {
		started.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected differently-keyed tasks to all start concurrently")
	}
	close(release)
}

func TestScheduler_HighPriorityDispatchedFirst(t *testing.T) {
	pool := NewAdaptiveWorkerPool(1, 1) // single worker forces strict ordering
	defer pool.Close()
	s := NewScheduler(pool)
	defer s.Close()

	gate := make(chan struct{})
	var order []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Occupy the single worker so subsequent submissions queue up.
	wg.Add(1)
	s.Submit(PriorityNormal, "", func() {
		<-gate
		wg.Done()
	})
	time.Sleep(20 * time.Millisecond) // let it start and block on gate

	record := func(name string) func() {
		return func() {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			wg.Done()
		}
	}

	wg.Add(3)
	s.Submit(PriorityBulk, "", record("bulk"))
	s.Submit(PriorityLow, "", record("low"))
	s.Submit(PriorityHigh, "", record("high"))
	time.Sleep(20 * time.Millisecond) // let them all queue up behind the gate

	close(gate)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "high" {
		t.Fatalf("expected high priority task to run first, got order=%v", order)
	}
}
