# Load Testing Findings

This document records the output of real load/concurrency testing run
against this checkout (`zzz_manual_load_test.go`,
`zzz_manual_bench_test.go` -- both gated behind
`CAIMANDB_MANUAL_BENCH=1`, not part of the normal `go test ./...`
suite), not projections or estimates. Every number below is from an
actual run, on a severely constrained single-vCPU / ~3.9GB-RAM
sandbox -- real production hardware will do better in absolute terms,
but the *relative* gap between operation types (the actual finding
here) is architectural, not a hardware artifact, and will reproduce on
any hardware under enough concurrent write load.

## Bulk import: 200,000 documents in 9.45s

```
IMPORT users FROM FILE '...users_200k.ndjson' FORMAT NDJSON BATCH 20000
Import complete: 200000 decoded, 200000 inserted, 0 errors, took 9.452000164s
heap alloc after GC: 871.1 MB (before GC: 1422.1 MB)
```

~21,200 docs/sec on a single core. Healthy, and consistent with the
project's own stated design intent for bulk import (LineDat/BULK MODE,
FLEX-COLUMN indexing deferred). Not a concern.

## FIXED (this pass): single-ID fast path for UPDATE/DELETE -- see "Fix applied" below

The full-scan half of the finding below is fixed: `updateLocal`/
`deleteLocal` (`ops_local.go`) now recognize a WHERE clause that
reduces to exactly `id = "<literal>"` and route it through a direct
`txn.Get(docKey(...))` -- the same O(1) lookup `FindByID` already used
-- instead of scanning every document in the block. The **lock
granularity** half is not: `UPDATE`/`DELETE` (of any shape, fast-path
or not) still take the same single whole-block lock they always did,
so concurrent writers still queue behind each other; see "Fix applied"
for measured before/after numbers and exactly what's still open.

## Original finding: UPDATE and DELETE serialize on a whole-block lock and full-scan on every call, regardless of the WHERE clause

50 concurrent workers, mixed workload (60% `FIND <block> <id>`, 20%
`INSERT`, 10% `UPDATE ... WHERE id = "..."`, 10% `DELETE ... WHERE id
= "..."`) against a block seeded with 5,000 documents, for 20 seconds:

```
find  :    2204 ops,     0 errs, p50=21.125µs    p95=63.174µs    p99=82.179µs
insert:     715 ops,     0 errs, p50=2.983836ms  p95=8.220107ms  p99=19.894601ms
update:     354 ops,     0 errs, p50=1.469966638s p95=1.558514522s p99=1.583742963s
delete:     370 ops,     0 errs, p50=1.466281704s p95=1.556320428s p99=1.58004377s
total: 3643 ops over 20s = 182 ops/sec
goroutines: 104 before load, 104 after (no leak)
```

`FIND <block> <id>` (a direct key lookup, `FindByID`'s O(1) path) is
fast: p99 82µs. `UPDATE`/`DELETE` with an equally simple `WHERE id =
"..."` -- logically the same "find one document by its ID" operation
-- are **~19,000x slower at p50** (1.47 seconds vs 21 microseconds),
even though every one of these test's `WHERE` clauses matched exactly
one document.

A second run with less contention (10 workers instead of 50, 90s
instead of 20s) confirms the latency scales with concurrency, as the
root cause below predicts -- lower, but still catastrophically slower
than `FIND`, and this was still only 10 concurrent writers:

```
find  :    7361 ops,     0 errs, p50=24.932µs     p95=73.082µs    p99=100.528µs
insert:    2522 ops,     0 errs, p50=3.10617ms    p95=7.580686ms  p99=10.75033ms
update:    1280 ops,     0 errs, p50=360.161542ms p95=433.24012ms p99=536.008046ms
delete:    1215 ops,     0 errs, p50=352.681682ms p95=424.757324ms p99=458.57936ms
total: 12378 ops over 1m30s = 138 ops/sec
goroutines: 104 before load, 104 after (no leak over a 90s sustained run)
```

Even at 10 concurrent writers, `UPDATE`/`DELETE` p50 is ~360ms --
roughly **14,000x** `FIND`'s p50. No goroutine growth across either
run (104 -> 104 both times), which is a real, if short (~2 minutes of
combined sustained load across both runs), soak signal against a leak
in the request path itself -- it says nothing about longer-horizon
concerns like Badger compaction behavior over hours/days, which
neither run was long enough to observe.

### Root cause (confirmed by reading the code, not just inferred from the numbers)

Both `updateLocal` and `deleteLocal` (`ops_local.go`):

1. Take a **single lock keyed by `db+block+shardID`**
   (`e.lockManager.Lock(lockKey)`) -- not per-document, not per-key
   range. Every concurrent `UPDATE`/`DELETE` against the same block
   queues behind this one lock, regardless of whether they'd touch
   different documents.
2. Once inside the lock, **scan every document key in the block**
   (`prefix := docKeyPrefix(block)`, then iterate) and evaluate
   `matchesWhere` per document -- there is no fast path that
   recognizes `WHERE id = "<literal>"` (or `WHERE _id = "<literal>"`)
   as a direct single-key lookup the way `FindByID`/`FIND <block>
   <id>` already has.

Under concurrent write load, these two combine: each `UPDATE`/`DELETE`
call holds the block-wide lock for the duration of a full scan, so
the Nth queued call waits for N-1 full scans to finish first, and the
scan cost itself grows as the block grows (concurrent `INSERT`s in
this same test kept adding documents throughout the run). This is a
classic head-of-line-blocking pattern, and it will get worse, not
better, on a larger block or under higher write concurrency -- this
test only ran ~350-370 `UPDATE`/`DELETE` calls in 20 seconds; a
production workload with steady write traffic on a hot block would
see this queue grow without bound.

### Why this matters for "is it production ready"

`FIND`'s read path is genuinely fast and doesn't show this problem.
But any workload that does `UPDATE`/`DELETE` by ID under real
concurrent write load -- which is an extremely common pattern (user
profile updates, status toggles, session deletes, etc.) -- will hit
this. A single hot block under moderate concurrent write traffic could
see request latency climb into the seconds, not milliseconds, purely
from this lock+scan pattern, independent of hardware.

### Fix applied: single-ID fast path (point 1 below), point 2 still open

Implemented: `singleIDEquality` (`query_filter.go`) recognizes a WHERE
clause -- flat `[]Filter` or the parsed `*parse.Expr` tree, whichever
the caller has -- that reduces to exactly one condition, `id =
"<literal>"`, with nothing else AND'd, OR'd, or NOT'd around it (both
parsers normalize `_id` to `id` before this ever runs, so `WHERE _id =
...` qualifies too). `updateLocal`/`deleteLocal` check this first and,
when it matches, do a single `txn.Get(docKey(block, id))` instead of
opening an iterator over the whole block -- the exact per-document
match/mutate/index/WAL/cache logic is unchanged and shared verbatim
between the fast path and the full scan (`updateOneItem` /
`deleteCandidateFromItem`), so a document reached either way is
updated/deleted identically; the fast path only changes *how a
candidate is found*, never what happens to it once found.

**What this does not change**: the whole-block lock
(`e.lockManager.Lock(dbName+block+shardID)`) is still taken up front,
by both paths, for the reason noted below -- it may be protecting
index consistency against other concurrent writers, not just guarding
the scan, and changing its scope needs its own design review before
anyone touches it. So concurrent `UPDATE`/`DELETE` calls against the
same block still fully serialize against each other; what's gone is
the O(block size) scan each one used to do *while* holding that lock.

**Measured impact** (same box, same `TestManualConcurrentCRUDLoad`,
before numbers from the "Original finding" section above):

| workers | duration | metric | before | after | change |
|---|---|---|---|---|---|
| 50 | 20s | update p50 | 1.470s | 87.2ms | ~16.9x faster |
| 50 | 20s | delete p50 | 1.466s | 85.2ms | ~17.2x faster |
| 50 | 20s | total ops/sec | 182 | 2,764 | ~15.2x |
| 10 | 90s | update p50 | 360ms | 15.2ms | ~23.7x faster |
| 10 | 90s | delete p50 | 353ms | 13.6ms | ~25.9x faster |
| 10 | 90s | total ops/sec | 138 | 2,749 | ~19.9x |

Both re-runs: 0 errors, goroutine count unchanged after the run (104
-> 104), same as the original baseline -- the fix didn't trade
correctness or leak-safety for speed.

`update`/`delete` p50 (~13-87ms) is still far above `find`'s (~20µs) --
expected, and exactly what the "what this does not change" paragraph
above predicts: with the scan gone, every concurrent `UPDATE`/`DELETE`
against the block is now queueing on lock acquisition alone, not on
scan time, but it's still queueing. Closing that remaining gap is
point 2 below, unattempted here on purpose.

### What's still open: finer-grained locking

The block-wide lock exists presumably to protect the scan-and-batch-
commit sequence from racing with other writers -- for the fast path
that's now really just "protect the single Get+Set+index-update from
racing," but that still needs a real design review before narrowing
(could be protecting secondary-index consistency against a concurrent
insert into the same block, not just data races on the same
document), not a rename fix. Reducing the lock's scope from "the
whole block" to something finer (per-key-range, or per-shard-slice
locking within a block) would let concurrent writes to *different*
documents proceed in parallel, which is the remaining throughput fix
beyond the single-ID fast path above. Whatever that fix ends up being,
it needs the same before/after `TestManualConcurrentCRUDLoad` treatment
this fix got -- not just a green functional test suite -- to prove it
actually moved the numbers.

## Side finding from re-measuring this: the per-client rate limit is now reachable

`DefaultConfig`'s `RateLimit` (10,000 requests/minute, keyed per
`authUser` -- `ratelimit.go`/`dsl_parser.go`) is a real per-client
production safety limit, not a measurement artifact -- but re-running
`TestManualConcurrentCRUDLoad` unmodified after the fix above hits it
almost immediately (all 50 workers share one bucket, `authUser
"admin"`), which surfaces as a wall of near-instant `"rate limited"`
errors that looks exactly like a correctness regression at first
glance (near-100% error rate, sub-microsecond p50s) but isn't one --
it's the limiter doing its job once the engine is fast enough to reach
it. Before this fix, `UPDATE`/`DELETE`'s own scan cost kept this
workload's real throughput (138-182 ops/sec measured above) far under
10,000/minute, so the limiter was structurally unreachable and this
was never visible. The benchmark (`zzz_manual_load_test.go`) now sets
`cfg.RateLimit` very high specifically so it keeps measuring
engine/lock/scan latency rather than an unrelated per-client throttle
-- a real deployment would size or disable that limit deliberately,
not have this benchmark silently paper over it.

## How to re-run these

```bash
export CAIMANDB_MANUAL_BENCH=1
# Import benchmark (needs an NDJSON file; generate one however you like):
export CAIMANDB_BENCH_FILE=/path/to/users.ndjson
go test ./internal/caimandb/ -run TestManualImportBenchmark1M -v -timeout 300s

# Mixed CRUD load test:
export CAIMANDB_LOAD_WORKERS=50      # optional, default 50
export CAIMANDB_LOAD_DURATION=20s    # optional, default 20s
go test ./internal/caimandb/ -run TestManualConcurrentCRUDLoad -v -timeout 120s
```
