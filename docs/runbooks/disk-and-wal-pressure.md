# Runbook: Disk Filling Up, WAL Growing, or Write Latency Climbing

## Symptoms
- Disk usage on a node's `DataRoot` climbing steadily.
- `caimandb_wal_size` (Prometheus metric, `metrics.go`) growing
  without bound.
- Write latency climbing on an otherwise-idle cluster.
- **If you're also seeing `UPDATE`/`DELETE` latency specifically climb
  under concurrent write load**, check `docs/load-testing-findings.md`
  first -- that's a known, documented architectural issue (whole-block
  locking + full scan on every `UPDATE`/`DELETE`, confirmed by real
  load testing), not necessarily a disk/WAL problem. Don't chase disk
  space if the real cause is that.

## Step 1: is it the WAL, or the actual data?

```sql
CLUSTER STATUS
```

and check `caimandb_wal_size` vs. total disk usage on `DataRoot`.

- **WAL disproportionately large relative to actual document data**:
  the WAL isn't being checkpointed/compacted fast enough. Check
  `WALSyncPolicy` and whatever periodic checkpoint/compaction interval
  your config uses -- a WAL that only grows means writes are landing
  but the durable-then-truncate cycle isn't keeping up.
- **Actual document/index data is the bulk of it**: this is capacity
  planning, not an incident -- see "Step 3" below.

## Step 2: if the WAL is the problem

1. Check for a stuck or crashed WAL writer/checkpoint goroutine in the
   logs -- this would explain checkpoints not advancing.
2. Confirm nothing is holding a long-running transaction open
   (`TX LIST`, `TX STATUS`) -- an abandoned open transaction can block
   WAL truncation past that transaction's start point, the same way a
   long-open transaction blocks vacuum/checkpoint in other databases.
3. If a `BEGIN` was left open and abandoned (a client crashed mid
   transaction without `COMMIT`/`ROLLBACK`), `ABORT` it explicitly
   rather than waiting for a timeout, if your deployment doesn't have
   one configured.
4. As a last resort on a single node that's about to run out of disk:
   stop writes to that node (route around it if clustered; if
   standalone, this is now a hard outage -- prioritize accordingly),
   free space by whatever means available, restart, and let recovery
   run (`wal_recovery.go`'s recovery path is exercised by
   `restart_test.go`/`crash_recovery_test.go`, so a clean restart
   after freeing space should recover normally).

## Step 3: if it's genuine data growth (capacity planning, not an incident)

- Check `caimandb_shard_distribution` (per-shard document counts, see
  the CHANGELOG for when this started actually being populated) to
  see whether growth is concentrated on one shard/node (a hot-key
  problem, possibly needing `SHARD REBALANCE`/`SHARD SCALE`) or spread
  evenly (genuine overall growth needing more disk or nodes).
- `COMPACT` is available for Badger-level compaction if fragmentation
  (not raw data volume) is contributing -- check disk usage before and
  after to confirm it actually helped before treating it as the fix.

## Prevention

None of this is automated in this repo as of this session -- there's
no built-in disk-usage alerting or automatic WAL-pressure backoff.
Wire up alerting on `caimandb_wal_size` and node disk usage externally
(Prometheus alerting rules) rather than relying on someone noticing
this runbook's symptoms manually.
