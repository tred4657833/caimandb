package storage

import (
	"sync"
	"sync/atomic"
)

// BufferPool is a concurrency-safe pool of reusable byte slices. It is used
// on hot write paths (WAL flushing, batch inserts) to avoid allocating a new
// backing array per operation under high, sustained throughput.
type BufferPool struct {
	pool    sync.Pool
	maxSize int
	getCnt  atomic.Int64
	putCnt  atomic.Int64
	missCnt atomic.Int64 // Get() calls that had to allocate fresh (pool was empty)
}

// bufferPoolInitialCap is the starting capacity for a freshly allocated
// buffer on a pool miss. maxSize is the ceiling for what Put keeps in the
// pool (see Put), not a size to allocate up front -- callers append into
// these buffers, so Go's normal slice growth handles anything bigger than
// this starting point. Pre-allocating the full maxSize (historically up to
// 128MB) on every miss meant a burst of concurrent misses against an empty
// pool -- e.g. several goroutines flushing small WAL batches at once, well
// before anything had been Put back -- could each try to reserve 128MB at
// once, demanding hundreds of MB+ of committed memory for what was actually
// a few KB of real data. That's wasteful anywhere, and fatal on a host with
// a small page file (Windows VirtualAlloc simply fails instead of
// overcommitting the way Linux does).
const bufferPoolInitialCap = 4 << 10 // 4KB

func NewBufferPool(maxSize int) *BufferPool {
	bp := &BufferPool{maxSize: maxSize}
	initialCap := maxSize
	if initialCap > bufferPoolInitialCap {
		initialCap = bufferPoolInitialCap
	}
	bp.pool.New = func() interface{} {
		bp.missCnt.Add(1)
		return make([]byte, 0, initialCap)
	}
	return bp
}

// Get returns a zero-length slice backed by pooled (or freshly allocated)
// capacity. Callers must return it via Put when done to keep the pool warm.
func (bp *BufferPool) Get() []byte {
	bp.getCnt.Add(1)
	return bp.pool.Get().([]byte)[:0]
}

// Put returns a buffer to the pool. Oversized buffers (larger than maxSize)
// are dropped instead of pooled so a single huge allocation can't bloat the
// pool's steady-state memory footprint forever.
func (bp *BufferPool) Put(b []byte) {
	if cap(b) <= bp.maxSize {
		bp.pool.Put(b[:0]) //nolint:staticcheck // intentional: reset length, keep capacity
		bp.putCnt.Add(1)
	}
}

func (bp *BufferPool) Stats() map[string]any {
	return map[string]any{
		"gets":      bp.getCnt.Load(),
		"puts":      bp.putCnt.Load(),
		"pool_miss": bp.missCnt.Load(),
		"max_size":  bp.maxSize,
	}
}
