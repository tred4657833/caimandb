# Storage AI: per-block adaptive Badger sizing (July 2026)

**Important, same as in the other documents in this pass:** this
environment had no network access nor a Go toolchain installed, so what
follows was written and reviewed by hand (types, signatures, brace/parenthesis
balance) but **was not compiled**. Run
`go build ./... && go vet ./...` before relying on this, and especially
before deploying it with real data.

## The reported problem

> All blocks weigh about the same (~49 MB), even with very few documents.
> This indicates the size comes from BadgerDB pre-allocation, not from
> stored data.

Correct. Before this change, `storage/badger_pool.go` opened **all**
blocks (`__data`, `__index`, `__users`, `__system`, regardless of how
many documents they had) with exactly the same fixed BadgerDB options,
defined in `storage/constants.go`:

```go
badgerValueLogSize       = 64 << 20   // 64MB
badgerMemTableSize       = 128 << 20  // 128MB
badgerNumMemTables       = 4          // → up to 512MB of memtables
badgerBlockCacheSize     = 512 << 20  // 512MB block cache
```

Badger reserves/creates its memtable and value-log files according to
those numbers when opening, regardless of how many documents will actually
live there. A brand new block with 3 documents pays the same disk footprint
as one with 300,000. With thousands of small blocks, this becomes a real
disk space problem (and RAM, due to the 512MB × N `BlockCacheSize` for
simultaneously open blocks).

## The implemented solution: tiers + shared RAM budget

`internal/caimandb/storage/adaptive.go` (new) introduces 4 **tiers** of
Badger configuration:

| Tier | MemTable | NumMemtables | ValueLog | BlockCache | Intended for |
|---|---|---|---|---|---|
| `micro` | 4MB | 2 | 8MB | 8MB | New or nearly empty block |
| `small` | 16MB | 3 | 16MB | 32MB | Some data, far from "big data" |
| `standard` | 128MB | 4 | 64MB | 512MB | The original fixed values |
| `large` | 256MB | 6 | 128MB | 1GB | Block already with serious volume |

`storage/badger_pool.go` (`DBPool.pickTierLocked`), when opening a block:

1. **Looks at what's already on disk** for that block (`onDiskFootprintBytes`):
   a block that doesn't exist yet, or exists but is empty, falls into
   `micro` — this directly fixes the "~49MB with few documents" issue.
   A block that already has, say, 200MB on disk is reopened at `standard`
   or `large`, not `micro`.
2. **Adjusts it against a shared RAM budget** across all blocks currently
   open in the process (`DBPool.budgetTotal`, default 50% of detected RAM
   via `/proc/meminfo` on Linux, or 2GB by default if undetectable —
   configurable with `storage_ai_ram_fraction` / `storage_ai_max_budget_mb` /
   `CAIMANDB_STORAGE_AI_MAX_BUDGET_MB`). If the "ideal" tier for that block
   doesn't fit in the remaining budget, it's opened at the largest tier
   that does fit — a block is never refused due to lack of budget; in the
   worst case it opens at `micro`.

This is exactly what was requested: *"small blocks should take little
space and large blocks should maximize performance"*, with RAM and load
(number of blocks open at once) as inputs to the decision.

`storage_ai_enabled=false` in the config (or `CAIMANDB_STORAGE_AI_ENABLED=false`)
disables all of this and reproduces the original fixed behavior
byte-for-byte (`standard` tier for everything).

## Concurrency and query speed: what already existed and what was adjusted

- **Each block is already an independent BadgerDB instance**
  (`pool.OpenBlock` → `openBlock` by `(db, block)`), so concurrency
  between blocks was already real before this change: two different
  blocks read/write without contending with each other.
- Within the same block, Badger already handles its own concurrent MVCC
  transactions (snapshot reads don't block writes).
- What's new: the `large` tier raises `NumCompactors` (more parallel
  compaction goroutines) and `NumLevelZeroTables` (more headroom before
  writes have to wait for L0 compaction), designed so a large block under
  sustained concurrent write load doesn't stall as quickly.
  `micro`/`small` stay with fewer compactors because with so little data,
  there's nothing to compact in parallel — it would be pure overhead.
- `AdaptiveStats()` in `DBPool` exposes how many blocks are open in each
  tier and the used/total RAM budget, to verify in production that the
  classification is behaving as expected (not yet connected to any HTTP
  endpoint/metric — see "Next step" below).

## What this does NOT do (by design)

**There is no hot re-tiering of a block that's already serving traffic.**
A block's tier is decided once, the first time it's opened in a given
process (usually at startup, or the first time something touches it if
the process opens it lazily). If a block is born in `micro` and grows a
lot while the process is still running, it stays in `micro` until the
next restart/reopen of that block — at that point `onDiskFootprintBytes`
will see the real size and open it at the appropriate tier.

This was decided not out of oversight: upgrading a **live** block's tier
means closing its `*badger.DB` and reopening it with different options,
and in this repo that's not safe without more work. Checking who uses the
pool:

- `ops_insert.go`, `ops_local.go`, `transaction.go` do take
  `e.lockManager.Lock(<db>/<block>)` before writing.
- **`ops_find.go` (reads) does not take that lock** — it relies on
  Badger's MVCC isolation, which assumes the `*badger.DB` handle stays
  alive for the duration of the transaction/iterator.

Closing the Badger handle while there are iterators or read transactions
in flight in another goroutine is not a documented safe operation in
Badger. To do it properly would require, at minimum, that the resize
takes the same lock already used by writers (blocking new writes during
the swap) **and** some way to wait for in-flight readers to finish
(something that doesn't exist today, because today nothing needs to wait
for a reader to finish). This is exactly the kind of change this repo
already avoided doing "blindly" elsewhere (see
`docs/known-limitations.md`) due to not having a compiler or concurrency
tests available to verify it.

## Reasonable next step (not implemented here)

If true hot upgrade is desired, the safest path is to extend the
`maintenance.go` sweep (which is already single-goroutine, already
pauses between blocks, and already knows which blocks are cold/hot):

1. Add something like `AdaptiveResizeEnabled` +
   `AdaptiveResizeCheckInterval` to the config.
2. In each `runMaintenanceSweep` pass, for each block: compare
   `db.Size()` (actual current LSM+vlog size, which Badger exposes)
   against the tier it was opened with (`DBPool.tierOf`).
3. If it exceeded its tier's ceiling: take `e.lockManager.Lock(key)` (the
   same one writers use) to block new writes to that block, close and
   reopen the handle with the new tier, release the lock. This blocks
   writers during the swap (acceptable, it's brief and infrequent) but
   **does not protect readers that don't take that lock today** —
   you'd first need to decide whether `ops_find.go` starts taking an
   `RLock()` on that same `shardedLockManager` (it currently has
   `RLock`/`RUnlock` already implemented and unused — see `locks.go`)
   before you can call this truly safe under concurrent load.

Each of those three steps needs to be compiled and run against real
concurrent traffic before trusting it — not implemented blindly in this
pass for the same reason as the rest of the limitations documented in
`docs/known-limitations.md`.
