# Runbook: A Cluster Node Won't Start, or Stopped Responding

## Symptoms
- A node process exited and won't restart cleanly.
- A node is running but doesn't respond on its admin/query port.
- `CLUSTER STATUS` (run against a *different*, healthy node) shows a
  node as missing or unreachable.
- `caimandb_raft_replication_lag{node="<the node>"}` has been
  climbing and hasn't reset to near-zero.

## First: is this a single node, or is quorum actually at risk?

Run `CLUSTER STATUS` against any node you *can* still reach. If a
majority of voters are still up, the cluster keeps serving writes
(Raft only needs a quorum, not every node) -- this is urgent but not
an outage. If you've lost quorum (more than half the voters are down),
writes are blocked cluster-wide until quorum is restored -- treat this
as the highest priority.

## Diagnosing why the node won't start

1. **Check the process logs first.** This engine logs via
   `zap` (see `logging.go`) -- look for the last few lines before
   exit. Common real causes in this codebase:
   - **Port already in use** -- another process (or a stuck previous
     instance) is holding the admin/query/Raft port.
   - **TLS cert/key load failure** -- if `TLSEnabled`/`RaftTLSEnabled`
     is on and a cert file is missing, unreadable, or expired,
     startup fails at `buildServerTLSConfig`/`raftTLSConfig`
     (`tls_mtls.go`/`tls_raft.go`). Check the cert files' paths and
     permissions match `Config.TLSCertFile`/`TLSKeyFile`/
     `RaftTLSCertFile`/`RaftTLSKeyFile`/`RaftTLSCAFile` exactly.
   - **Badger open failure** -- often disk full, a stale `LOCK` file
     from an unclean shutdown, or filesystem permissions on
     `Config.DataRoot`.
   - **Raft log store corruption** after an unclean shutdown (a real
     `kill -9` mid-write scenario -- this is exactly what
     `crash_recovery_test.go` and `transaction_crash_recovery_test.go`
     exercise, so recovery on restart is expected to work; if it
     doesn't, that's a bug worth reporting with the exact log output).

2. **If it's a stale lock/lockfile from an unclean shutdown**: confirm
   no other process actually holds the data directory
   (`ps aux | grep caimandb` on that host) before removing any lock
   file by hand -- removing a lock file out from under a still-running
   process is how you get real corruption, not recover from it.

3. **If the node starts but won't rejoin the cluster**: check that its
   `RaftBind`/advertise address hasn't changed (e.g. a new pod IP in a
   container orchestrator) in a way the rest of the cluster's
   configuration doesn't already know about -- you'll need to
   `RemoveServer` the old identity and re-add the node under its new
   address; see `docs/nql-reference.md`'s cluster commands.

## If the node's data is unrecoverable

Don't try to hand-repair Badger/Raft on-disk state. Recovery here is:

1. Wipe that node's `DataRoot` entirely.
2. Start it fresh with the same node ID (or a new one, if the old ID
   is being retired).
3. Let it rejoin and catch up via Raft snapshot + log replay from the
   current leader -- this is exactly what `restart_test.go`'s crash
   recovery already exercises in the test suite. It'll re-sync from
   the cluster, not from local disk.
4. Watch `caimandb_raft_replication_lag` for that node drop back
   toward zero as it catches up.

## After recovery

- Confirm `CLUSTER STATUS` shows the node healthy and
  `caimandb_shard_health` is back to 1 for shards it's the primary
  for.
- If this was caused by something systemic (disk full, OOM), see
  `disk-and-wal-pressure.md` before considering this closed.
