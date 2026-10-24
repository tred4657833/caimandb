# Runbook: Rolling Upgrade Without Downtime

**Caveat before anything else**: this repo is v0.0.1 beta with no
formal versioning/compatibility policy yet, and (as of this session)
no test coverage specifically for "old node talking to new node during
a rolling upgrade" -- the chaos/crash tests all run a single version
across all nodes. Treat this runbook as the reasonable default
procedure, not a guarantee this codebase has been proven safe for.

## Before starting

1. **Read the CHANGELOG entry for the version you're upgrading to.**
   Look specifically for anything touching the WAL entry format, the
   `RaftCommand` shape, or the on-disk backup format -- a change there
   means old and new nodes may not be able to replicate to each other
   mid-upgrade, which changes this from "rolling" to "requires a full
   stop".
2. **Take a fresh backup first regardless.**
   ```sql
   BACKUP <db> FULL
   VERIFY BACKUP <db>
   ```
3. **Confirm current cluster health** (`CLUSTER STATUS`,
   `caimandb_raft_replication_lag` near zero on every node) before
   touching anything -- don't start an upgrade on a cluster that's
   already degraded.

## Rolling procedure (one node at a time)

For each node, starting with a **follower**, never the leader first:

1. Stop the node cleanly (send a graceful shutdown signal, don't
   `kill -9` -- an unclean stop is fine for this codebase's crash
   recovery to handle, but there's no reason to test that path during
   a routine upgrade).
2. Deploy the new binary/version.
3. Start it back up.
4. Wait for it to fully rejoin: `CLUSTER STATUS` shows it healthy, and
   `caimandb_raft_replication_lag` for that node drops back near zero
   (it needs to catch up on whatever committed during its downtime).
5. **Don't proceed to the next node until step 4 is confirmed.**
   Upgrading two nodes simultaneously risks losing quorum if anything
   goes wrong with either one.

Repeat for every follower. **Upgrade the current leader last**: when
its turn comes, a graceful shutdown triggers a normal leader election
among the already-upgraded followers first, so you get a clean
handover instead of an abrupt loss of leadership mid-upgrade.

## After every node is upgraded

1. `CLUSTER STATUS` -- confirm every node reports the new version (if
   your build exposes a version string; check whatever endpoint your
   deployment uses).
2. Confirm all four cluster/shard metrics look normal:
   `caimandb_cluster_status` (exactly one leader),
   `caimandb_raft_replication_lag` (near zero everywhere),
   `caimandb_shard_health` (all 1), `caimandb_shard_distribution`
   (no sudden imbalance).
3. Run a smoke-test write and read against the cluster, not just a
   health check -- confirm the actual data path works end to end on
   the new version before considering this done.

## If something goes wrong mid-rollout

Stop the rollout. Do not upgrade further nodes while the cluster is in
a mixed-version state and unhealthy -- diagnose first (this is where
`node-down.md` and `split-brain-suspicion.md` apply if the symptoms
match). If you need to roll back a node, the same one-at-a-time
procedure applies in reverse, and the fresh backup from "Before
starting" is your fallback if a mixed-version state has caused data
inconsistency you can't otherwise resolve.
