package turbo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatcher_CoalescesConcurrentSubmits(t *testing.T) {
	var flushCalls atomic.Int64
	var totalItems atomic.Int64

	b := NewBatcher[int](50, 20*time.Millisecond, func(items []int) []error {
		flushCalls.Add(1)
		totalItems.Add(int64(len(items)))
		errs := make([]error, len(items))
		return errs
	})

	const n = 300
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(v int) {
			defer wg.Done()
			if err := b.Submit(v); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := totalItems.Load(); got != n {
		t.Fatalf("expected %d total items flushed, got %d", n, got)
	}
	// With 300 concurrent submits and a batch size of 50, coalescing should
	// produce meaningfully fewer than 300 flush calls.
	if calls := flushCalls.Load(); calls >= n {
		t.Fatalf("expected batching to reduce flush calls well below %d, got %d", n, calls)
	}
}

// An isolated Submit (nothing else arrives to batch with) should flush
// after the short adaptive quiet window, not sit out the whole maxWait
// cap -- that's the whole point of making the batcher adaptive instead of
// a fixed-window batcher.
func TestBatcher_FlushesQuicklyWhenIsolated(t *testing.T) {
	var flushed []int
	var mu sync.Mutex

	maxWait := 200 * time.Millisecond // deliberately large: proves we did NOT wait for it
	b := NewBatcher[int](100, maxWait, func(items []int) []error {
		mu.Lock()
		flushed = append(flushed, items...)
		mu.Unlock()
		return make([]error, len(items))
	})

	start := time.Now()
	if err := b.Submit(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	// quietWait for a 200ms maxWait is 25ms (maxWait/8); give generous
	// scheduling slack but stay well under maxWait to prove the quiet
	// window -- not the cap -- is what fired.
	if elapsed >= maxWait/2 {
		t.Fatalf("expected isolated submit to flush via the quiet window, not wait near maxWait=%v; took %v", maxWait, elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(flushed) != 1 || flushed[0] != 1 {
		t.Fatalf("expected single item flushed, got %v", flushed)
	}
}

// A sustained stream of arrivals (each one landing before the previous
// item's quiet window expires) keeps resetting the quiet timer and should
// grow the batch instead of flushing item-by-item -- but must still get
// flushed by the hard maxWait cap rather than growing forever.
func TestBatcher_QuietWindowResetsUnderSustainedArrivals(t *testing.T) {
	var flushCalls atomic.Int64
	var totalItems atomic.Int64

	maxWait := 40 * time.Millisecond
	b := NewBatcher[int](10000, maxWait, func(items []int) []error {
		flushCalls.Add(1)
		totalItems.Add(int64(len(items)))
		return make([]error, len(items))
	})

	// quietWait derives to 5ms here; send one item every 1ms for longer
	// than maxWait so the quiet timer never gets the chance to fire on
	// its own -- only the cap should force the flush.
	start := time.Now()
	var wg sync.WaitGroup
	const n = 60
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			time.Sleep(time.Duration(v) * time.Millisecond)
			_ = b.Submit(v)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if got := totalItems.Load(); got != n {
		t.Fatalf("expected %d total items flushed, got %d", n, got)
	}
	// Everything arrived within ~60ms of steady traffic against a 40ms
	// cap: expect it to have taken at least one full cap cycle (i.e. not
	// all flushed in a single batch), and comfortably more than one flush.
	if calls := flushCalls.Load(); calls < 2 {
		t.Fatalf("expected the hard cap to force more than one flush over %v of steady traffic, got %d flush(es)", elapsed, calls)
	}
}

func TestBatcher_PerItemErrorsRouteCorrectly(t *testing.T) {
	b := NewBatcher[int](10, 10*time.Millisecond, func(items []int) []error {
		errs := make([]error, len(items))
		for i, v := range items {
			if v%2 == 0 {
				errs[i] = fmt.Errorf("even value not allowed: %d", v)
			}
		}
		return errs
	})

	var wg sync.WaitGroup
	results := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = b.Submit(idx)
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		if i%2 == 0 && err == nil {
			t.Errorf("expected error for even value %d", i)
		}
		if i%2 == 1 && err != nil {
			t.Errorf("expected no error for odd value %d, got %v", i, err)
		}
	}
}

func TestBatcher_SizeTriggerFlushesImmediately(t *testing.T) {
	flushed := make(chan int, 10)
	b := NewBatcher[int](3, time.Hour, func(items []int) []error {
		flushed <- len(items)
		return make([]error, len(items))
	})

	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			_ = b.Submit(1)
		}()
	}
	wg.Wait()

	select {
	case n := <-flushed:
		if n != 3 {
			t.Fatalf("expected batch of 3, got %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("expected size-triggered flush, timed out waiting")
	}
}

func TestBatcherGroup_SeparatesKeys(t *testing.T) {
	var mu sync.Mutex
	perKey := map[string]int{}

	g := NewBatcherGroup[int](10, 10*time.Millisecond, func(key string, items []int) []error {
		mu.Lock()
		perKey[key] += len(items)
		mu.Unlock()
		return make([]error, len(items))
	})

	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				_ = g.Submit(k, 1)
			}(key)
		}
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if perKey["a"] != 5 || perKey["b"] != 5 {
		t.Fatalf("expected 5 items per key, got %v", perKey)
	}
}
