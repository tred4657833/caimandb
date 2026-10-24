// Package turbo provides the generic, engine-agnostic building blocks for
// CaimanDB's faster execution path ("turbo engine"):
//
//   - AdaptiveWorkerPool: a goroutine pool that grows and shrinks its worker
//     count based on sampled queue/backlog pressure instead of running a
//     fixed number of goroutines for the life of the process.
//   - Scheduler: a small priority scheduler on top of an AdaptiveWorkerPool
//     that (a) lets high-priority work (e.g. interactive queries) jump
//     ahead of low-priority background work (e.g. bulk loads, compaction),
//     and (b) serializes work that shares a "key" (e.g. the same block)
//     while letting unrelated keys run fully in parallel.
//   - Batcher / BatcherGroup: a generic "group commit" coalescer. Concurrent
//     Submit calls arriving within a short time window (or until a size
//     threshold is hit) are handed to the caller's flush function as one
//     batch, then each caller's result is delivered back individually. This
//     is the same trick databases use to turn many small fsync'd writes
//     into a few larger ones.
//   - BulkMode: a simple on/off switch a caller can flip to widen batch
//     windows and relax durability settings during large bulk loads, then
//     restore normal behavior afterward.
//
// None of these types know anything about CaimanDB's storage format, WAL,
// or documents -- they're deliberately generic so they can be reused
// anywhere the engine needs "do less work per unit of throughput" without
// pulling in a dependency on the caimandb package itself (which would
// create an import cycle, since caimandb depends on turbo, not the other
// way around).
package turbo
