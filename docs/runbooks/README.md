# Operational Runbooks

These are written against what's actually in this codebase as of this
session (real command names, real config fields), not generic advice.
Every command referenced here is real and documented in
`docs/nql-reference.md` or `docs/configuration.md` -- cross-check there
if something looks unfamiliar. None of these have been rehearsed
against a real incident; they're a first draft based on reading the
dry-run and refine, not as something to trust blind the first time you
need one at 3am.

- [`node-down.md`](node-down.md) -- a cluster node won't start, or has
  stopped responding.
- [`panic-restore.md`](panic-restore.md) -- you need to restore from
  backup right now, including to a point in time before a bad write.
- [`split-brain-suspicion.md`](split-brain-suspicion.md) -- more than
  one node believes it's the Raft leader, or writes seem inconsistent
  across nodes.
- [`disk-and-wal-pressure.md`](disk-and-wal-pressure.md) -- disk
  filling up, WAL growing unbounded, or write latency climbing.
- [`cert-rotation-and-expiry.md`](cert-rotation-and-expiry.md) -- a
  TLS certificate (admin/query API or Raft mTLS) is expiring or needs
  rotating.
- [`rolling-upgrade.md`](rolling-upgrade.md) -- deploying a new
  version across a cluster without downtime.

## Before you need these

A few things worth having ready *before* an incident, none of which
exist yet in this repo as of this session:

- **A tested backup schedule.** `BACKUP <db>` (incremental) and
  `BACKUP <db> FULL` exist and work (see
  `docs/nql-reference.md`'s "Encrypted Backup / Restore" section), but
  nothing in this repo runs them on a schedule for you --
  `backup_scheduler.go` exists but check your `configs/` for whether
  it's actually enabled and how often it runs in your deployment.
  A backup you've never test-restored is not a backup you can rely on.
- **Monitoring wired to the metrics that now exist.** As of this
  session, `caimandb_raft_replication_lag`,
  `caimandb_shard_health`, `caimandb_cluster_status`, and
  `caimandb_shard_distribution` are all real, populated Prometheus
  gauges (see the CHANGELOG for when each was added) -- but nothing in
  this repo ships a Grafana dashboard or alerting rules for them yet.
  Alerting on `caimandb_raft_replication_lag` staying elevated is a
  reasonable first alert to set up; it's exactly the signal
  `split-brain-suspicion.md` and `node-down.md` below both point back
  to.
- **A known-good `RestoreDB` test.** Practice `RESTORE <db> AS OF
  <timestamp>` against a real (non-production) backup before you need
  it in a panic -- see `panic-restore.md`.
