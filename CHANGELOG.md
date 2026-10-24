# Changelog

## [Unreleased] — Single Backups Directory, Outside the Data Directory

Continuing directly from the five entries below (real Go 1.24.4
toolchain, `go build`/`go vet`/`go test` verified for every claim
here).

**The problem**: this project used to create two separate backup
directories, both nested inside the data directory:
`<dataRoot>/backups/` (EXPORT and the older, unencrypted
Badger-native `BACKUP`/`RESTORE` command) and `<dataRoot>/backup/arw/`
(the newer encrypted-JSONL automatic backup system,
`ops_backup_jsonl.go`) -- two similarly-named directories that were
easy to confuse, both buried inside the data directory rather than
being independently manageable (e.g. for separate backup/retention
policies, or mounting backups on different storage than live data).

**The fix**: `storage.DirectoryManager` gained a `BackupRoot()`
method returning a single backups directory that's a **sibling** of
`DataRoot()`, not nested inside it (e.g. `DataRoot` `./data` gets
`BackupRoot` `./backups` by default). Both backup systems now share
this one root: EXPORT/legacy `BACKUP` files live directly under it
(`BackupsPath()`, unchanged externally), and the encrypted-JSONL
system's files live under `<BackupRoot>/arw/<db>/` (an internal
subdirectory *within* the one backups directory, not a second
top-level one -- needed only so the two systems' filenames can never
collide with each other). `Config.BackupRoot` (env var
`CAIMANDB_BACKUP_ROOT`) lets an operator pin an explicit path instead
of accepting the sibling-of-`DataRoot` default -- e.g. to put backups
on different storage entirely.

Every function in `internal/caimandb/backup` (`ManifestPath`,
`FullDir`, `IncrDir`, `LoadManifest`, `Manifest.Save`,
`LoadOrCreateKey`) had its parameter renamed from `dataRoot` to
`backupRoot` and its path-joining updated to drop the now-redundant
`"backup"` path segment (since the parameter itself is now the
backups root, not the data root). `ops_backup_jsonl.go`'s four
functions that call into that package (`BackupDB`, `RestoreDB`,
`VerifyBackup`, `restoreDocsFromBackup`) now fetch
`e.dirMgr.BackupRoot()` instead of `e.dirMgr.DataRoot()` before
calling them. `LoadOrCreateKey`'s backup-encryption key/salt files
(`.key`/`.salt`) moved with it -- they now live directly under
`BackupRoot`, not under a `<dataRoot>/backup/` subdirectory.

Verified both by the existing backup/restore/export test suite
passing unchanged (`backup_pitr_test.go`,
`view_export_where_tree_test.go`, `TestHandleRestoreJSONL_AsOfParsing`,
etc. -- none of them depend on the exact on-disk layout, only on the
`Engine`-level API, so this was a clean internal refactor) and by
directly inspecting the resulting directory tree from a real run:

```
<root>/data/            <root>/backups/
  __users/                .key
  __raft/                 arw/
  __system/                 demo/
  demo/                  <export/backup files>.csv, .json, ...
```

`docs/configuration.md` and `configs/caimandb.conf.example` document
the new `backup_root`/`CAIMANDB_BACKUP_ROOT` config option.

## [Unreleased] — Toward Production Readiness: Fixed Orphaned Metrics, Extended ACL Coverage, Migrated the Last WHERE Holdout, Real Load Testing (Critical Finding), Runbooks

Continuing directly from the four entries below (real Go 1.24.4
toolchain, `go build`/`go vet`/`go test` verified for every claim
here, except where explicitly marked as a documented finding rather
than a fix).

**`TxManager.Update`/`Delete` (in-transaction BEGIN/COMMIT mutations)
migrated to the real WHERE tree** (`transaction.go`): took `where
*parse.Expr` alongside `filters`, same "tree if present" pattern as
everywhere else, using `Engine.Find`'s existing `Query.Where` support.
While investigating this, found that this code path is **not reachable
from the real `UPDATE`/`DELETE` commands at all** -- `handleUpdate`/
`handleDelete` always call `Engine.Update`/`Engine.Delete` directly,
regardless of whether a transaction is active. `TxManager.Update`/
`Delete` are only exercised by `cmd/txcrashtestworker` (the external
crash-test harness) and the `TxManagerForTestUpdate`/
`TxManagerForTestDelete` Go-test bridges, whose own exported signatures
were kept unchanged (they now just pass `nil` for `where` internally)
so the external worker binary isn't affected.

**Fixed the two orphaned metrics flagged as a known gap in an earlier
entry** (`cluster.go`, `shard_manager.go`): `MetricClusterStatus` and
`MetricShardDistribution` were defined and registered from early in
this project's history but never once had `.Set()` called on them
anywhere in the codebase. `reportClusterStatusMetric` (called from the
same periodic loop as `reportRaftLagMetric`) now sets
`caimandb_cluster_status{node,role}` to 1 for whichever Raft role
(`leader`/`follower`/`candidate`/`shutdown`) the node currently is, 0
for the other three. `reportShardHealthMetrics` now also sets
`caimandb_shard_distribution`'s `DocCount` per shard -- with `db`/
`block` labels left as `""`, documented honestly in the code: this
version of `ShardManager` tracks document counts per shard across the
whole node, not broken down by database/block, so there's no truthful
per-db/per-block figure to report yet. 
values in `observability_metrics_test.go`
(`TestReportClusterStatusMetric`, `TestReportShardHealthMetrics_SetsDistribution`).

**Granular ACL (`authorizeBlockAccess`) extended to aggregations/
`GROUP BY`, `EXPORT`, and `VIEW`** (`cmd_aggregate.go`, `cmd_admin.go`,
`cmd_view.go`) -- previously scoped to `FIND`/`SEARCH`/`INSERT`/
`UPDATE`/`DELETE` only. Tested end to end
(`TestGrant_CoversAggregatesExportView`, `acl_test.go`): a user with no
role-level access to a database, granted `read` on one specific block,
can now `COUNT`/aggregate, `EXPORT`, and `VIEW CREATE`/query against
that block through the real command strings.

**Real load testing against the production entrypoint (`parseAndExec`),
not synthetic benchmarks** -- results and full analysis in the new
`docs/load-testing-findings.md`:

- **Bulk import**: 200,000 documents in 9.45s (~21,200 docs/sec, single
  vCPU sandbox) via the existing `TestManualImportBenchmark1M` (fixed
  a session-unrelated bug in that test while running it: its `Session`
  had no `authDatabase` set, so `CREATE DB` was rejected by the role
  hierarchy -- this test had apparently never been run in this
  environment before). Healthy, no concerns.
- **New: `TestManualConcurrentCRUDLoad`** (`zzz_manual_load_test.go`,
  same `CAIMANDB_MANUAL_BENCH=1` opt-in gate) -- 50 (then 10, for a
  second data point) concurrent goroutines running a realistic mixed
  60% read / 20% insert / 10% update / 10% delete workload through
  `parseAndExec` against a pre-seeded block, reporting real ops/sec and
  p50/p95/p99 latency per operation type, plus a goroutine-count
  before/after check (no leak detected across ~2 minutes of combined
  sustained load).
- **Critical finding**: `UPDATE`/`DELETE` with a simple `WHERE id =
  "<literal>"` -- logically the same "look up one document by ID"
  operation `FIND <block> <id>` already does in ~21 microseconds at
  p50 -- took **~1.47 seconds at p50 under 50 concurrent workers** (and
  still ~360ms at only 10 concurrent workers), roughly 19,000x and
  14,000x slower respectively. Root cause confirmed by reading the
  code, not just inferred from the numbers: `updateLocal`/
  `deleteLocal` (`ops_local.go`) take a single lock keyed by the whole
  block (not per-document) and always do a full prefix scan of every
  document in the block, with no fast path recognizing a single-ID
  equality the way `FindByID` already has. This is a real
  production-readiness blocker for any workload with concurrent
  writes to the same block by ID -- not fixed in this pass (a genuine
  architectural change to the write-locking/scan path, judged too
  risky to rush blind under this session's remaining budget), but
  documented in full with a scoped fix proposal in
  `docs/load-testing-findings.md`.

**Runbooks** (`docs/runbooks/`): a first-draft operational runbook set
-- node down/won't start, panic restore from backup (including
point-in-time), split-brain suspicion, disk/WAL pressure, TLS
cert rotation and expiry, and rolling upgrade. Written against real
commands and config fields that exist in this codebase as of this
session (`RESTORE ... AS OF`, `CLUSTER STATUS`, the new
`caimandb_raft_replication_lag`/`caimandb_cluster_status` metrics,
`RaftTLSCertFile` etc.), explicitly marked as unrehearsed first drafts
own caveats section.

**Explicitly not done in this pass, and not fixable by more code
changes alone** -- see the conversation this session for the fuller
discussion: a real external security audit, and the kind of
battle-testing that only comes from real users hitting real edge
cases over time. No amount of additional self-review substitutes for
either.

## [Unreleased] — Granular Access Grants: GRANT / REVOKE / SHOW GRANTS

Continuing directly from the three entries below (real Go 1.24.4
toolchain, `go build`/`go vet`/`go test` verified for every claim
here). This adds the last remaining item from the "what's needed for
10/10" list: a granular access layer on top of the fixed role
hierarchy in `roles.go`.

**Design** (`acl.go`): a `Grant{Username, DB, Block, Actions,
CreatedAt, GrantedBy}` record, stored in the same Badger-backed users
database `User` records already live in (`e.usersDB()`,
`users_auth.go`), under keys `acl/<username>/<db>/<block>` (`Block`
`"*"` means every block in that database). Grants are strictly
additive: they can extend a user's access to a specific block in a
database their role alone wouldn't cover at all, but a missing grant
never removes access a role already provides (`ADMIN_GENERAL`, or a
DB's own `ADMIN`/`SUBADMIN`/`DEVELOPER`/`USER` within their assigned
database, are unaffected either way).

**Commands** (`cmd_grant.go`): `GRANT read[,write] ON <db>.<block> TO
<user>`, `REVOKE ... FROM <user>`, `SHOW GRANTS [FOR <user>]`. Only an
admin (`ADMIN`/`ADMIN_GENERAL`) can `GRANT`/`REVOKE`; a scoped `ADMIN`
can only grant/revoke within their own database (the same isolation
`requireDBAccess` already enforces elsewhere). `SHOW GRANTS FOR
<user>` needs admin to inspect someone else's; your own is always
visible.

**Enforcement**: `authorizeBlockAccess` (`acl.go`) is the new
block-level gate, called from `handleFind`/`handleSearch` (action
`"read"`) and `handleInsert`/`handleUpdate`/`handleDelete` (action
`"write"`) right after each confirms the block exists. It layers on
top of, never below, the existing DB-level role gate: if the caller's
role already grants DB access, this always allows the action; a grant
only matters for extending access to a database/block the role
wouldn't otherwise cover.

**A real architectural gap found and closed while wiring this up, not
originally planned:** a grant to a block in a different database
would have been completely unreachable through the normal command
flow. Two independent, pre-existing central gates both check DB
access purely by role before a grant is ever consulted:

1. The `USE`/`CD` command handler (`dsl_parser.go`) -- a user can't
   even switch into a database their role doesn't cover, so they'd
   never reach the point of touching a granted block in it.
2. `enforceSessionIsolation` (`hierarchy.go`), a second, independent
   check run on *every* command via `parseAndExec`
   (`dsl_parser.go`) specifically to close off `sess.currentDB` being
   set some other way than `USE` (e.g. an HTTP endpoint's `?db=`
   param) -- this would have re-blocked every subsequent command even
   after a hypothetical successful `USE`.

Both now also check `Engine.hasAnyGrantInDB` (a lightweight "does this
user have any grant at all in this database" check) as an additional
way in, alongside the existing role check.
`enforceSessionIsolation` changed signature to take `*Engine` to reach
this check; its one call site (`parseAndExec`) was updated to match.
Without this, `GRANT`/`REVOKE`/`SHOW GRANTS` would have compiled,
looked correct in isolation, and simply never worked for the
cross-database case that's the actual point of the feature -- caught
by `TestGrant_ExtendsAccessBeyondRole` failing against the
initial implementation before this fix, not by inspection.

**Tests** (`acl_test.go`), all through real command strings
(`parseAndExec`) against a real Engine/Badger instance with real users
created via `CreateUserWithOpts`, not hand-built `Grant` structs or
direct `authorizeBlockAccess` calls: a `USER` scoped to one database by
role, denied `USE` into an unrelated database before any grant and a
non-admin's own attempt to `GRANT` denied; after an admin `GRANT read`,
the user can `USE` into the other database and `FIND` the granted
block but still can't `INSERT` (no write grant yet); after `GRANT
write`, the insert succeeds; after `REVOKE read`, reads are denied
again while the write grant remains untouched. A second test confirms
granting one user access to a block has no effect on a completely
unrelated admin's own isolation from that database. A third covers
`SHOW GRANTS FOR` self-vs-other visibility.

**Not done in this pass** -- see `docs/known-limitations.md`'s updated
gap list: enforcement only covers the primary CRUD surface (not
aggregations, `EXPORT`, `VIEW`, or admin commands), and there's no
grantable `admin` action tier (DB-admin capability stays role-only).

## [Unreleased] — Real WHERE Clause Everywhere: UPDATE, VIEW CREATE, EXPORT Migrated (Two Real Bugs Found and Fixed)

Continuing directly from the two entries below (real Go 1.24.4
toolchain, `go build`/`go vet`/`go test` verified for every claim
here). This closes out the WHERE-clause migration entirely: every
command with a `WHERE` now goes through the real parenthesized/NOT/
AND-OR expression tree (`parse.ParseWhere`/`parseWhereClause`), not
the old flat left-to-right `parseFilters`. See
`docs/known-limitations.md`'s rewritten "WHERE AST" section for the
full technical rundown; this entry focuses on what changed and what
was found.

**`UPDATE` migrated** (`cmd_update.go`, `ops_update.go`,
`raft_fsm.go`): `Engine.Update`/`updateLocal` now take `where
*parse.Expr` alongside `filters`, reusing `RaftCommand.Where` (added
for `DELETE` in an earlier entry) and `matchesWhere`. No WAL-shape
change was needed, unlike `DELETE`'s own migration: `UPDATE`'s WAL
entry is already the fully-computed new document body, not a filter
description to re-match at recovery time.

**A serious pre-existing bug found and fixed while migrating
`UPDATE`, unrelated to `parseFilters` vs `parseWhereClause` and
present long before this pass touched the file:** `UPDATE <block>
WHERE <condition> SET ...` -- the single most common form of the
command -- was silently ignoring the WHERE condition and updating
**every document in the block**, whenever the condition didn't happen
to hit the `id =`/`ID` shortcut path. The bug: an earlier block in
`handleUpdate` already detects a leading `"WHERE"` token and advances
the parser index past it, but the WHERE-parsing block further down
unconditionally re-scanned the remaining tokens looking for a literal
`"WHERE"` -- which no longer existed, having already been consumed.
That left both `filters` and `where` `nil`, which
`matchesWhere`/`matchesFilters` correctly (and, here, catastrophically)
treat as "no restriction, match everything". Nothing had ever
exercised `UPDATE ... WHERE ...` through the real command string
before (`update_where_tree_test.go` does now -- both a plain-equality
case and a parenthesized-OR case, and both would have failed or
misbehaved against the old code). Fixed by parsing from the
already-advanced index directly when the leading `"WHERE"` was already
detected, instead of re-searching for a token that was already gone.

**`VIEW CREATE` migrated** (`cmd_view.go`): `ViewDefinition` gained a
`Where *parse.Expr` field alongside the existing `Filters []Filter`,
so a view's `WHERE` keeps real `OR`/`NOT`/grouping when the view is
later queried -- the `default` case in `handleViewCmd` (querying a
view) now builds `Query{Filters, Where}` the same way `FIND` already
does, instead of only ever passing the flat (and, for a grouped WHERE,
empty) `Filters`.

**`EXPORT` migrated** (`cmd_admin.go`, `handleExport`) -- the last
command still on `parseFilters`; `parseFilters` now has zero remaining
callers in any command handler. Found and fixed the same class of bug
`DELETE`/`Count` already had fixed in an earlier entry:
`handleExport`'s guard was `if len(filters) > 0` before applying the
WHERE to the export, but `parseWhereClause`'s flat `[]Filter` is
empty/nil for any WHERE that isn't a pure AND-chain (an `OR` group,
for instance) -- so a grouped `EXPORT ... WHERE (...)` would have
silently exported the **entire block**, ignoring the condition, the
exact same failure shape as the `UPDATE` bug above but caught before
it shipped rather than after. Fixed as part of the same migration:
`if len(filters) > 0 || where != nil`.

**Tests:** `update_where_tree_test.go` (simple-equality and
parenthesized-OR cases, both through the real `UPDATE` command
string), `view_export_where_tree_test.go` (a parenthesized-OR `VIEW
CREATE` whose later query correctly excludes non-matching documents,
and a parenthesized-OR `EXPORT` whose output file correctly excludes
them too) -- all against a real Engine/Badger instance via
`parseAndExec`, not hand-built `Query`/`UpdateOp`/`ViewDefinition`
structs, specifically because the bugs above only showed up when
going through the real command-parsing path.

## [Unreleased] — Raft mTLS + Raft-Lag/Shard-Health Metrics

Continuing directly from the entry below (real Go 1.24.4 toolchain,
`go build`/`go vet`/`go test` verified for every claim here).

**Mutual TLS for inter-node Raft traffic (`tls_raft.go`):**

- The previous entry's mTLS only covered the admin/query HTTP APIs.
  This closes the other half: `Config.RaftTLSEnabled` (plus
  `RaftTLSCertFile`/`RaftTLSKeyFile`/`RaftTLSCAFile`, with matching
  `CAIMANDB_RAFT_TLS_*` env vars) makes every Raft RPC between cluster
  nodes -- vote requests, AppendEntries, snapshot install, everything
  hashicorp/raft's `raft.NetworkTransport` sends -- travel over mutual
  TLS instead of plain TCP. Implemented as a custom `raft.StreamLayer`
  (`tlsStreamLayer`) wired into `cluster.go`'s
  `initRaftWithCustomLogStore` in place of the plain
  `raft.NewTCPTransport` call, alongside the existing
  test-only stream-layer hook (both now go through the same
  `raft.NewNetworkTransport(stream, ...)` constructor).
- Every node in a cluster needs a certificate signed by the same
  `RaftTLSCAFile`; hostname/SAN verification is intentionally skipped
  (Raft addresses are `IP:port`, not hostnames a cert would name) in
  favor of still-enforced chain verification against that CA via a
  manual `VerifyPeerCertificate` callback.
- Reuses `rotatingCertStore` from `tls_mtls.go` for the same hot
  cert-rotation behavior the HTTP servers already had -- a node's Raft
  identity certificate can be rotated without a restart (and therefore
  without forcing a leadership disruption).
  nodes joined into a cluster via `AddVoter` (same pattern
  `cluster_failover_test.go` already uses), with real, in-process
  X.509 certificates -- a document inserted on the leader is confirmed
  to replicate to the follower purely by querying the follower after
  the insert, proving the mTLS stream layer actually carries Raft's
  real traffic correctly end to end, not just that it can complete a
  handshake. A separate unit-level test confirms a client certificate
  signed by an unrelated CA is rejected at the TLS handshake against
  `newTLSStreamLayer` directly.
- One bug caught and fixed while writing the cluster test: the test
  initially failed silently (data never replicated, no error) because
  it hadn't called `(*ClusterManager).refreshSingleNodeStatus()` after
  the two-node config change landed -- the leader was still treating
  itself as a single-node cluster and applying writes locally only,
  entirely bypassing Raft (and therefore the new TLS transport) for
  the actual data path. Fixed in the test, not in engine code (this is
  existing, intentional single-node-fast-path behavior elsewhere too,
  e.g. `cluster_failover_test.go` already calls
  `refreshSingleNodeStatus()` for the same reason) -- but worth calling
  out here since it's exactly the kind of thing that makes an
  insufficiently end-to-end test pass for the wrong reason.

**Raft replication lag + shard health metrics (`metrics/metrics.go`):**

- `caimandb_raft_replication_lag{node}`: seconds since this node's
  Raft instance last had successful contact with its leader, via
  hashicorp/raft's own `(*raft.Raft).LastContact()` (the same value
  the library uses internally for election timeouts) -- 0 on the
  leader itself. This is deliberately a wall-clock contact-staleness
  metric, not a log-index-distance one: hashicorp/raft's public API
  doesn't expose a leader's view of each follower's match/next index,
  so a "log entries behind the leader" metric isn't honestly
  computable from outside the library without extra peer-to-peer
  probing that wasn't built here. `(*ClusterManager).reportRaftLagMetric`
  in `cluster.go`.
- `caimandb_shard_health{shard,node}`: 1 if a shard's `Status` isn't
  `"degraded"` and its primary node's own `Status` is `"active"`, 0
  otherwise -- reusing the exact same `ShardInfo.Status`/`NodeInfo.Status`
  fields `HandleNodeFailure` already sets on a real node-loss event,
  rather than inventing a parallel health signal.
  `(*ShardManager).reportShardHealthMetrics` in `shard_manager.go`.
- Both are polled every 10 seconds from a new background goroutine
  started in `app.go`'s `RunServer` (`observability_metrics_loop`),
  stopped via the same deferred-shutdown sequence as the admin/query
  servers. Both report functions nil-check their receiver, so the loop
  runs safely even in standalone (non-cluster, non-sharded) mode --
  nothing to report, no panic.
- Also noticed and worth flagging: `MetricClusterStatus` and
  `MetricShardDistribution`, both defined and registered in
  `metrics.go` since before this session, were never actually `.Set()`
  anywhere in the codebase -- they've been silently stuck at their
  zero value the entire time. Not fixed in this pass (out of scope for
  "Raft lag + shard health" specifically), but flagged here since it's
  exactly the kind of gap `docs/known-limitations.md` exists to track
  rather than let quietly persist.
  (read back via `prometheus_client_model`'s `.Write(&dto.Metric{})`,
  not `testutil.ToFloat64` -- that package isn't yet a direct
  `go.mod` dependency here, and this avoids a `go mod tidy` just for
  a test): a real single-node Raft cluster reports 0 lag as its own
  leader; a real `ShardManager` reports 1 before a simulated node
  failure and 0 after `HandleNodeFailure` marks the shard degraded
  with no replica to take over; nil-receiver safety for both report
  functions.

## [Unreleased] — Point-in-Time Restore, Mutual TLS + Certificate Rotation

Continuing directly from the two entries below (real Go 1.24.4
toolchain, `go build`/`go vet`/`go test` verified for every claim
here).

**Point-in-time restore (`RESTORE <db> AS OF <ts>`):**

- `Engine.RestoreDB` (`ops_backup_jsonl.go`) now takes an `asOf int64`
  parameter (Unix seconds; `0` means "latest", the previous
  unconditional behavior). When set, only document versions with
  `Meta.UpdatedAt <= asOf` are replayed from the backup history, so a
  document changed after `asOf` reverts to whatever version it held at
  that moment. Filtering happens per document line, not per backup
  file, since one incremental backup's file can span a range of
  `UpdatedAt` values straddling `asOf`. An `asOf` older than the oldest
  full backup returns a clear error instead of silently restoring zero
  documents.
- New command surface: `RESTORE <db> AS OF <unix-seconds>` and
  `RESTORE <db> AS OF <RFC3339>` (`cmd_backup_jsonl.go`). Fixed the
  dispatcher (`dsl_parser.go`) routing for this at the same time: it
  previously sent anything with more than 2 tokens after `RESTORE` to
  the *older*, unrelated `RESTORE <db> FROM <file>` command
  (`ops_backup.go`'s Badger-native snapshot restore),
  which would have swallowed `RESTORE users AS OF ...` silently.
- `docs/nql-reference.md` gained a dedicated section on the encrypted
  JSONL backup/restore system (it previously only documented the older
  `BACKUP ... TO`/`RESTORE ... FROM` command).
  real on-disk encrypted backups (not a hand-built `Manifest`): take a
  full backup of one document version, update the document, take an
  incremental backup of the new version, then confirm `RESTORE ... AS
  OF` between those two backups brings back the OLD version even
  though the live database and the incremental backup both currently
  hold the new one. Also covers the too-old-`asOf` error case and the
  command-parsing/dispatch surface.

**Mutual TLS + certificate rotation (`tls_mtls.go`):**

- `Config.TLSClientCAFile`: when set alongside the existing
  `TLSEnabled`/`TLSCertFile`/`TLSKeyFile`, the admin (`http_admin.go`)
  and query (`http_query.go`) HTTP servers now require and verify a
  client certificate signed by that CA
  (`tls.RequireAndVerifyClientCert`) on every connection, on top of
  the server-side TLS `TLSEnabled` already provided. Leaving it unset
  preserves the previous server-only-TLS behavior exactly. New env var
  `CAIMANDB_TLS_CLIENT_CA_FILE`; `CAIMANDB_TLS_CERT_FILE` and
  `CAIMANDB_TLS_KEY_FILE` also got env var wiring in `defaults.go`,
  which they were missing entirely before (only `TLSEnabled` had one).
- `Config.TLSCertReloadInterval` (default 10 minutes,
  `CAIMANDB_TLS_CERT_RELOAD_INTERVAL` env var): both servers now serve
  their TLS certificate through a `GetCertificate` callback backed by
  an atomically-swapped `*tls.Certificate`
  (`rotatingCertStore` in `tls_mtls.go`), with a background goroutine
  that re-stats the cert/key files on disk every interval and reloads
  them in place if either file's mtime changed. New TLS handshakes
  pick up the rotated certificate; existing connections are
  unaffected; a failed reload (e.g. an operator mid-write on a new
  cert file) logs a warning and keeps serving the previous certificate
  rather than taking the server down.
  `AdminServer` listening on a real TCP port, with real X.509
  certificates generated in-process (self-signed CA + leaf, `crypto/x509`,
  not a mocked TLS layer): confirms a client with no
  certificate is rejected at the handshake, a client certificate
  signed by an unrelated CA is rejected, a client certificate signed
  by the configured client CA is accepted end to end through a real
  HTTP GET, and that overwriting the cert/key files on disk causes a
  subsequent TLS connection to present the new certificate (checked by
  its Subject CommonName) without restarting the server.
- Not done in this pass: mTLS for the inter-node cluster/Raft HTTP
  transport (`cluster.go`) -- only the admin and query servers got it.
  Also not done: exposing `TLSClientCAFile`/cert-rotation
  configuration through `configs/`-directory example config files, if
  this project has any that should be updated to mention it.

## [Unreleased] — `FIND ... DISTINCT`, Aggregations/`GROUP BY` Migrated to the Real WHERE Tree

Continuing from the previous entry's real Go 1.24.4 toolchain setup
(`golang-1.24-go` via `apt`, `GOPROXY=direct` against GitHub) — every
claim below is `go build`/`go vet`/`go test` output again, not manual
review.

**`FIND <block> DISTINCT`, new:**

- `FIND <block> DISTINCT field1,field2 ...` dedupes the result set,
  keeping the first document seen for each distinct combination of
  values at those fields (dot-paths like `address.city` work the same
  as elsewhere).
- `FIND <block> DISTINCT` with no field list dedupes on the entire
  projected row instead (runs after `SELECT`/`EXCLUDE`, so it sees
  exactly what the caller would see in the output).
- Not available on `SEARCH`; not combinable with `GROUP BY` (grouping
  already collapses to one row per group). The FlexColumn columnar
  fast path (`flexcolumn.go`) defers to the normal scan/index flow
  when either form of `DISTINCT` is set, the same way it already does
  for `GROUP BY`/`HAVING`.
- `ops_find.go` (`Query.Distinct`/`Query.DistinctAll`,
  `distinctByFields`/`distinctByRow`), `cmd_find.go` (parsing;
  `handleFind`'s `FIND <block> <id>` shortcut also had to learn
  `DISTINCT` as a modifier keyword, or it would've read
  `FIND users DISTINCT city` as a lookup for a document literally
  named `"DISTINCT"`), `help.go`/`docs/nql-reference.md` (docs).
  Covered by `find_distinct_test.go` (3 tests, real Engine/Badger, not
  synthetic `Query` structs).

**Aggregations/`GROUP BY` migrated to `parseWhereClause`/`parse.Expr`:**

The known limitation that `SUM`/`AVG`/`COUNT`/etc. and `GROUP BY`
still used `parseFilters` (the original flat parser — no parentheses,
no `NOT`, left-to-right only) is closed. `Engine.Aggregate` and
`Engine.Group` (`ops_aggregate.go`) now take a `where *parse.Expr`
parameter alongside the existing `filters []Filter`, using
`matchesWhere` (already shared with `Delete`/`Count`) instead of
`matchesFilters` directly. `cmd_aggregate.go`'s two handlers
(`handleAggregate`, `handleGroup`) now call `parseWhereClause` instead
of `parseFilters`. Being entirely read-only, this needed none of the
`RaftCommand`/WAL-shape changes `DELETE`'s earlier migration did.

Remaining commands still on the flat parser: `UPDATE`, `VIEW CREATE`,
and admin commands — see `docs/known-limitations.md` for the updated
migration order.

`docs/known-limitations.md` also got a broader pass: it previously
described `internal/caimandb` as an unsplit ~15,000-line package with
no compiler available to verify a split, which is now stale on both
counts (real subpackages exist — `storage/`, `wal/`, `raft/`,
`cache/`, `cluster/`, `backup/`, `turbo/`, `parse/`, `query/` — and
`golang-1.24-go` is installable via `apt` in this sandbox).


**This entry is different from every one before it in one respect: this session had a real Go 1.24.4 toolchain (`golang-1.24-go` via `apt`, exact version match for this repo's `toolchain go1.24.4` directive) and, after working around this sandbox's blocked `proxy.golang.org`, real network access to fetch every real dependency from GitHub directly (`GOPROXY=direct`). Every claim below is `go build`/`go vet`/`go test` output, not manual review.**

**The FlexColumn OOM (previous entry's top finding) is now fixed**, using exactly the "cheapest / immediate" option that entry documented:

- **`flexcolumn.go`:** `InsertWithColumnar`/`UpdateWithColumnar` now check a new `FlexColumnEngine.bulkModeActive()` (nil-safe — a `FlexColumnEngine` built without an owning `*Engine`, as the existing concurrency/benchmark tests do, never reads as bulk mode) and skip indexing entirely while `BULK MODE` is on, instead of doing one map-insert per scalar field per document into the unbounded `docFields`/`index` maps documented in the previous entry. The actual indexing logic was pulled out into a new unexported `indexDoc`, called directly by the new reindex path below (bypassing the bulk-mode gate — an explicit admin action should still work even if bulk mode happens to still be on).
- **`block_repair.go`:** new `Engine.ReindexFlexColumn(dbName, block)` — the explicit backfill step: clears whatever's indexed for that block, then streams every document on disk through `indexDoc` one at a time (never materializing the whole block in memory at once, same discipline as the existing `RebuildBlock`).
- **`cmd_admin_extra.go`:** new `FLEX REINDEX <db> <block>` command.
- **`cmd_import.go`:** `IMPORT`'s result string now says plainly when FLEX-COLUMN indexing was skipped and gives the exact `FLEX REINDEX` command to run next.
- **`help.go`:** documents the above.

**Real 1M-document benchmark, this session, same shape file as before (`id`, `email`, and 7 other scalar fields — the exact high-cardinality shape that OOM-killed the process in the previous entry):** `FlexColumn.Enabled` at its **default (`true`)**, no config changes, on the same 1 vCPU / 3.9GB sandbox that OOM-killed twice in a row before:

```
Import complete: 1000000 decoded, 1000000 inserted, 0 errors, took 1m25s
Note: FLEX-COLUMN indexing was skipped during this import (BULK MODE). Run FLEX REINDEX benchdb users to rebuild it.
COUNT: 1000000 documents in users
heap alloc after GC: 1283.7 MB (before GC: 2035.8 MB)
```

No crash, no OOM, and materially faster than even the original working-but-slow 6m18s report, on the exact configuration (`FlexColumn.Enabled=true`) that previously couldn't finish at all on hardware this size.

**New tests (`flexcolumn_bulk_reindex_test.go`), against a real Engine/Badger instance, not a synthetic `FlexColumnEngine` in isolation:**
- `TestFlexColumn_BulkModeSkipsIndexing_ReindexBackfills`: inserts under `SetBulkMode(true)`, confirms the documents are durably readable (`Count` matches) but `FlexColumnEngine.Stats()["indexed_documents"]` is `0` and `QueryWithColumnar` declines to accelerate (`nil, nil`); then calls `ReindexFlexColumn` and confirms the index is fully populated and `QueryWithColumnar` now serves the query (`IndexUsed: "flex-column:status"`, correct `Total`).
- `TestFlexColumn_ReindexCommand`: same shape, but through the actual NQL surface (`FLEX REINDEX <db> <block>` via `parseAndExec`) instead of calling the Go method directly — the way an operator running `IMPORT` would actually use this.

**Full verification run, this session, real toolchain:**
```
go build ./...                    -> clean, no output
go vet ./...                      -> clean, no output
go test ./internal/caimandb/... -race -count=1   -> PASS (see caveat below)
```
All FlexColumn tests (new and pre-existing), all `parse` tests, all `turbo` tests, and both pre-existing `shard_manager` concurrency tests pass clean under `-race`. **One pre-existing, already-documented environment caveat reproduced, not introduced by this session:** running `TestEngine_InsertBatch_ConcurrentAcrossBlocks` and `TestEngine_InsertBatch_ConcurrentSameBlock` together in one `-race` process gets `signal: killed` (OOM) on this same 1 vCPU/3.9GB box — confirmed to be exactly the race-detector memory multiplication those tests' own file header already warns about (two real mmap'd Badger engines under TSan shadow memory in one process), not a regression: both pass individually (`go test -run TestEngine_InsertBatch_ConcurrentAcrossBlocks -race` and `-run TestEngine_InsertBatch_ConcurrentSameBlock -race` separately, both green), and the rest of the suite (including this session's new tests) passes fine in the same process as either one of them.

**`go.mod`, an update to the earlier note:** with a real Go 1.24.4 toolchain and `GOPROXY=direct`, every dependency in this repo resolves and builds straight from GitHub **except** the handful already flagged as sandbox-only workarounds: `go.opentelemetry.io/otel` (+ `/trace`, `/metric`), `go.opentelemetry.io/auto/sdk`, `gopkg.in/yaml.v3`, and `cloud.google.com/go/compute/metadata`, whose `replace` lines point at `/home/claude/vendor/...` — a path that exists only inside this specific sandbox container (recreated fresh, and only partially, this session, since the previous session's vendor tree didn't persist). **These four `replace` lines will fail on your machine** (`replacement directory ... does not exist`) because that path is sandbox-local. This wasn't fixed by editing `go.mod` (would need a real, portable module cache for a domain blocked here, `go.opentelemetry.io`) — the safe move on your side is: delete those `replace` lines for `go.opentelemetry.io/*`, `gopkg.in/yaml.v3`, and `cloud.google.com/go/compute/metadata`, then `go mod tidy` with your normal internet access; every one of those is a correctly-published module at exactly the path it's imported by, so on a machine that isn't blocking `proxy.golang.org`/`go.opentelemetry.io`, nothing should need a replace at all. The `go.uber.org/zap`→`github.com/uber-go/zap`-style replaces (module path genuinely differs from repo host) are unrelated and fine to keep.

---

## [Unreleased] — Real 1M-Document Benchmark Results, GOMAXPROCS-Gated Pipeline, and a Newly-Found Memory Bug (FlexColumn)

Compiled with `go1.24.4` and actually ran (not just reviewed) against a real 1,000,000-document NDJSON `IMPORT`, same shape as `users_1000000.ndjson` from the original report. `go build ./...` and `go vet ./...` are clean; `go test ./internal/caimandb/... -race` passes for every test that fits this environment's memory (see caveat below).

**Test hardware caveat, stated up front because it matters for every number below:** the sandbox this ran in has **1 vCPU and 3.9GB RAM**. That is a real constraint on the numbers, not a rounding error — see the GOMAXPROCS finding right below, which exists *because of* that constraint.

**New finding — the decode/insert pipeline (previous entry) needed a GOMAXPROCS guard:** measured head-to-head against the unpatched baseline, same process, same machine, `-count=1` (bypassing Go's test cache) for a fair sample each time:

| Version | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Baseline (unpatched) | 94.1s | 90.9s | — |
| Patched, pipeline always on | 139.3s | 95.3s | 152.9s |
| Patched, GOMAXPROCS-gated (this fix) | 92.0s | — | — |

The pipeline's whole premise is a spare core for batch N+1's decode to overlap with batch N's commit. On 1 vCPU there is no spare core — Go just time-slices the same core between the two goroutines — so the pipeline paid for goroutine/channel/atomic bookkeeping with no offsetting benefit, and came out slower *and* more erratic than the original synchronous loop. `BulkImportReader` now checks `runtime.GOMAXPROCS(0) > 1` once at the start and only builds the channel/goroutine pipeline when there's a real second core to use it; otherwise it runs the original synchronous decode-then-commit loop. Re-measured after the guard: 92.0s, back in line with baseline. The guarded-off fallback is exercised by the same `BulkImportReader` tests; the pipelined branch itself was separately re-verified under `-race` with `GOMAXPROCS=4` forced (50k docs, to fit this machine's memory under the race detector's overhead) — 0 errors, no races reported. **This fix's benefit is specifically for machines with more than one core** (essentially all real deployment targets) — it could not be demonstrated as a win on this particular single-core test box, and the numbers above say so plainly instead of only reporting the config where it looks good.

**New finding — FlexColumn's columnar index is unbounded, un-evicted, in-process RAM, and it's what actually OOMs a large IMPORT:** running the 1M-doc import with default config OOM-killed the process twice in a row (confirmed via `dmesg`: killed at ~3.9GB RSS, ~115s in) — with *or without* this session's concurrency patches applied, so this is a pre-existing bug, not something introduced here. Root cause, in `flexcolumn.go`: `FlexColumnEngine`/`flexShard` keeps `docFields[db][block][docID] = field -> value` (every scalar field of every document, forever) and `index[db][block][field][value] = set of docIDs` (a full inverted index) purely in memory, with no disk backing, no eviction, and no cap tied to document count (`MaxColumnarFields` caps *fields per document*, not total documents or total index size). For a schema with several high-cardinality fields (a unique `id`, a near-unique `email`), this is on the order of one map entry per document per indexed field — for 1M docs across ~8 scalar fields, comfortably enough to exhaust a few GB of RAM with nothing else running. Confirmed by direct A/B on identical hardware: **the exact same 1M-doc import that OOM-crashes with `FlexColumn.Enabled` at its default (`true`) completes cleanly in ~92-95s with `FlexColumn.Enabled = false`** (1,000,000 decoded, 1,000,000 inserted, 0 errors, `COUNT` matches exactly), both on baseline and patched code.

This was **not fixed in this pass** — it's a real architectural gap (an in-memory-only secondary index with no spill-to-disk or eviction strategy), not a small patch, and deserves its own design discussion rather than a rushed fix. Concrete options worth evaluating, roughly cheapest-to-most-durable:
- **Cheapest / immediate:** skip `InsertWithColumnar` while bulk mode is on (there's already a `bulkHint` the storage tier logic reads — `insertBatchLocalDetailed`'s post-commit loop could check the same flag) and require an explicit `REINDEX`/backfill pass after a large `IMPORT` finishes. Turns "OOM mid-import" into "explicit, visible, resumable indexing step" — the safest short-term fix.
- **Bounded memory:** cap `docFields`/`index` by total entries (LRU-evict cold field-values, similar in spirit to the L1/L2 doc cache that already exists) rather than only capping fields-per-document.
- **Durable fix:** persist the columnar index to Badger (its own key prefix, or a dedicated column-family-style block) instead of a bare in-memory map, so it survives restarts and isn't bounded by RAM at all — the direction the `DocumentCacheMB` field on `FlexColumnConfig` hints was already intended, but nothing currently spills `docFields`/`index` themselves to it.
- Regardless of which fix lands, `handleImportCmd`'s doc comment and any user-facing `IMPORT` documentation should say plainly that FlexColumn is active during bulk loads and scales with total row count kept in memory, so this isn't a surprise found only at OOM time in production.

**What this means for the original 6m18s / 1M-doc report:** since the reporting machine evidently has enough RAM to not OOM (it finished, over 6 minutes, with FlexColumn on), that specific run's slowness is still best explained by the three concurrency issues fixed earlier in this file (serial decode/insert, block lock held too long, autoscale thrashing) rather than by the FlexColumn finding — but the FlexColumn issue is real, was found by actually running the code the way this session was asked to, and will bite on any host with less headroom or a larger document count than that one had.

**Note on `go.mod`:** this sandbox blocks most of Go's usual module domains (`go.dev`, `gopkg.in`, `golang.org/x/*`, `google.golang.org`), so `go.mod` picked up a block of `replace` directives pointing old transitive dependencies at GitHub mirrors purely to get a build working *here*. On a normal machine with unrestricted internet access these `replace` lines are almost certainly unnecessary (plain `go mod tidy` there should resolve the real modules directly) — they're left in only because removing them isn't safe to do blind without re-tidying, and this pass didn't touch dependency versions in a way that changes behavior, only *where* they're fetched from.

---

## [Unreleased] — IMPORT Throughput and Lock/Autoscale Concurrency Fixes

**Motivating symptom:** `IMPORT users FROM FILE 'users_1000000.ndjson' BATCH 20000` took 6m18s for 1,000,000 docs (~2650 docs/s), and the server log showed an unprompted `"Scaling down shards" old:16 new:8` about 9 seconds into a run that was still actively inserting.

**`ops_bulk_import.go` — `BulkImportReader` decode/insert pipeline:** Decoding a batch (CPU-bound JSON parsing) and committing it (`insertBatchLocalDetailed`: WAL append + Badger `WriteBatch`, largely I/O-bound) ran strictly sequentially on one goroutine — decode 20000 docs, block waiting on the commit, decode the next 20000. CPU sat idle during every commit and disk sat idle during every decode, for the whole file. Now a small buffered channel (capacity 2) connects a decode goroutine to a dedicated inserter goroutine, so batch N+1 decodes while batch N commits. This does not add parallelism *within* a commit (still correctly serialized per block), it removes artificial serialization *between* two phases that never needed to wait on each other. Counters (`Decoded`/`Inserted`/`Errors`) are now `atomic.Int64` and `ErrorSamples` is guarded by a mutex, since both goroutines can touch error/progress bookkeeping concurrently now (they couldn't before).

**`ops_insert.go` — `insertBatchLocalDetailed` block-lock scope:** The per-block exclusive lock (`lockManager.Lock`) was held for the entire function via a top-level `defer`, including the post-commit bookkeeping loop over every document in the batch (`shardMgr.UpdateShardStats`, `changeBus.PublishInsert`, `flexEngine.InsertWithColumnar`) — none of which touch the WAL/Badger state the lock actually protects; each already does its own internal synchronization. For a 20000-doc batch this meant any other reader/writer of the block queued behind not just one WAL+WriteBatch commit but ~20000 iterations of unrelated, individually-locked work. The lock is now released right after `EnsureBlockMeta`/`SaveBlockMeta` (the one piece of that tail that genuinely needs it — an unguarded read-modify-write over a JSON file), before the bookkeeping loop runs.

**`shard_manager.go` — autoscale grace period + stat-preserving rescale:** `checkAndScale` judged shard load purely from `totalDocs/shardCount`, so a few seconds after a fresh `IMPORT` starts — when writes genuinely haven't caught up to disk yet — it read as "way under `MinShardDocs`" and fired an immediate, spurious `scaleDown`. New `scaleGracePeriod` (60s from `ShardManager` construction) holds off any scale decision until there's been time for real signal. Separately, `scaleUp`/`scaleDown` called `InitializeShards()` directly, which rebuilds the shard map from scratch and zeroes every `DocCount`/`SizeBytes` — so even a *correct* scale decision discarded every stat `UpdateShardStats` had accumulated, which could then cause the next `checkAndScale` tick to fire again off the now-zeroed counters. New `rescaleLocked` sums the aggregate doc/size totals before calling `InitializeShards()` and redistributes them evenly across the new shard set afterward (still under `sm.mu.Lock()`, same as before) — an even split isn't as precise as re-deriving true per-shard counts from block metadata would be, but it's a correct aggregate with zero added I/O on the scale-event path, and self-corrects further as real inserts keep landing.

**Not touched:** `insertBatchLocalDetailed`'s existing parallel prep phase (JSON marshal/compression across `GOMAXPROCS` workers, unlocked, before the block lock is even acquired) was already correct and is unchanged. The WAL group-commit/fsync policy and Storage AI adaptive tiering (see the entries below) were reviewed but not modified in this pass.

**Not Verified with `go build`/`go vet`/`go test`** — no Go toolchain or network in this environment (same caveat as every other entry in this file). Reviewed manually for balanced braces/parens, matching lock acquire/release pairs on every exit path, and absence of new data races (the two goroutines introduced in `BulkImportReader` never touch the same field of `BulkImportResult` without going through the new atomics/mutex). Confirm with `go build ./... && go vet ./... && go test ./... -race` before deploying, and ideally re-run the same `IMPORT` benchmark to confirm the wall-clock improvement.

---

## [Unreleased] — WAL: Prevents Double Application of Entries After a Second Crash

**Fixed Issue:** After WAL recovery on startup (`RecoverWAL`), the engine only pruned old segments (`PruneToLastSegment`) but left the *active* segment — the same one it had just read and applied — as the destination for new writes. If the process crashed again without a clean `Close()` in between, the next startup would re-read that segment and reapply the same entries a second time. This was harmless for `insert` (idempotent by ID) but corrupts non-idempotent `update` operations (`$inc`, etc.).

**Fix:** New `WAL.RotateAndPruneFresh()` method (`internal/caimandb/wal/wal.go`), used by `RecoverWAL` (`internal/caimandb/wal_recovery.go`) instead of direct `PruneToLastSegment()`: rotates to a new empty segment before pruning, ensuring no already-applied entries remain on disk to be re-read. See `docs/corruption-fixes-2026-07.md` (section 6) for complete details.

**Not Touched:** The rest of the startup/shutdown/cleanup pipeline was already well covered — `.clean` marker invalidated on startup (point 1 of July document), graceful shutdown with `SIGINT`/`SIGTERM`/`SIGHUP` and `http.Server.Shutdown` with timeout, adaptive worker pool that auto-terminates idle goroutines (`internal/caimandb/turbo/pool.go`), and dedicated `tx_cleanup_loop` that aborts and purges abandoned/expired transactions (`internal/caimandb/transaction.go`). This change reviewed that entire path without touching it because it already worked correctly; the only actual bug found was the one above.

---

## [Unreleased] — Storage AI: Adaptive BadgerDB Sizing by Block

**Fixed Issue:** Every block (new or existing) was opened with the same fixed BadgerDB options (`storage/constants.go`), so a nearly empty block occupied the same disk/RAM as one with hundreds of thousands of documents (~49MB reported). With many small blocks, this becomes a real space and memory problem.

**`internal/caimandb/storage/adaptive.go` (new):** 4 tiers of BadgerDB configuration (`micro`/`small`/`standard`/`large`). A block's tier is chosen, the first time it is opened in the process, based on what already exists on disk for that block (empty → `micro`) and adjusted against a shared RAM budget across all simultaneously opened blocks (default 50% of detected RAM via `/proc/meminfo`, configurable).

**`storage/badger_pool.go`:** `DBPool` now performs this selection when opening each block (`OpenDataPath`/`OpenBlock`, without changing its public signature, so none of the ~20 existing call sites were touched) and releases the corresponding budget on close. New `DBPool.AdaptiveStats()` for observability (block count per tier, budget used/total).

**New Configuration:** `storage_ai_enabled` (default `true`), `storage_ai_ram_fraction` (default `0.5`), `storage_ai_max_budget_mb` (default `0` = no explicit cap), with corresponding environment variables `CAIMANDB_STORAGE_AI_*`. `storage_ai_enabled=false` reproduces the exact original fixed behavior.

**What It Does NOT Do Yet, Intentionally:** Does not hot-readjust a block that is already serving traffic (only classified on open). See [`docs/storage-ai-adaptive-sizing.md`](docs/storage-ai-adaptive-sizing.md) for complete analysis of why (reads in `ops_find.go` do not take the write lock, so closing/reopening the Badger handle under concurrent read traffic is not safe without more) and the recommended path to implement it with a compiler at hand.

---

## [Unreleased] — `SHOW AUTORELATIONS`: `WHERE` and `ORDER BY` with Direction

**`WHERE <expression>`** (`cmd_show_autorelations.go`): Synonym for `FILTER` with the same condition syntax used by `FIND`/`SEARCH`/`VIEW`, on `doc_id`, `user_id`, `access_count`, `relevance`, `last_seen`, `first_seen`. `FILTER` remains for backward compatibility.

**`ORDER BY ... ASC|DESC`:** `ORDER BY` now accepts an explicit optional direction, and sortable fields are extended from `DEGREE|ID|NAME` to also include `ACCESS_COUNT`, `RELEVANCE`, `LAST_SEEN`, and `FIRST_SEEN`. Without `ASC`/`DESC`, each field retains its default direction (descending for `DEGREE`/`ACCESS_COUNT`/`RELEVANCE`/`LAST_SEEN`/`FIRST_SEEN`, ascending for `ID`/`NAME`).

**Combined Example:** `SHOW AUTORELATIONS products FROM p145 DEPTH 6 DIRECTION BOTH WHERE relevance >= 0.75 ORDER BY ACCESS_COUNT DESC LIMIT 100 FORMAT TREE STATS VERBOSE;`

---

## [Unreleased] — Temporal Auto-Relations Based on Access Patterns

**`relations_auto.go`, new `AutoRelationManager`:** When the same user repeatedly reads the same document (default: 5 reads in 10 minutes), CaimanDB automatically creates an auto-relation (self-relation) between that user and the document — without requiring an explicit `RELATE`. Each auto-relation stores `access_count`, `last_seen`, a relevance score (`relevance`, combining frequency and recency), and a small sample of key document metadata (`key_metadata`, e.g., `name`/`title`).

**These are temporal data by design:** Each new access extends their expiration (`auto_relation_ttl`, 24h by default); if the user-document pair stops being accessed, the relation expires on its own and a background sweep (`autoRelationCleanupLoop`, every 5 min) removes it. This is not durable data — it's a live signal of "what is relevant right now."

**`FIND` and `GET <id>` feed the detector automatically** (`cmd_find.go`): every document read by a session is reported to the `AutoRelationManager` with the authenticated user of that session.

**`SHOW AUTORELATIONS <block>`** (`cmd_show_autorelations.go`, `cmd_show_autorelations_render.go`, `relations_auto_graph.go`): Full graph-like query command over that set of auto-relations (bipartite and directed: `user -> read document`), with modifiers `FROM`, `TO`, `DEPTH`, `DIRECTION IN|OUT|BOTH`, `FORMAT TABLE|TREE|GRAPH|JSON`, `ORDER BY DEGREE|ID|NAME`, `LIMIT`, `OFFSET`, `FILTER <expression>` (reuses the same WHERE evaluator as `FIND`), `STATS`, `SUMMARY`, `PATHS` (BFS traversal tree from `FROM`), `ORPHANS` (isolated pairs), `CYCLES` (detected with union-find), and `BROKEN` (relations whose document was deleted).

**New Configuration Options:** `auto_relations_enabled`, `auto_relation_threshold`, `auto_relation_window`, `auto_relation_ttl`.

---

## [Unreleased] — Web Console (ui/) with Real Status Panel, Connected to Live Node Data

**`ui/` Console Rewritten** with the look and feel of a Beekeeper Studio-style database client: icon rail with two views (Query Editor / Dashboard), real query tabs, sidebar with databases and node blocks, and status panel with KPIs and charts. All content comes from real HTTP requests to the node itself — no sample data or `Math.random` on the client.

**`GET /entities`** (`http_query.go`), new query server endpoint: lists databases (`Engine.ShowDBs`) and, for the active database, its blocks with real document counts and sizes (`Engine.DescribeBlock`) — feeding the sidebar and block document chart.

**`POST /query` now also returns `rows`/`columns`** for `FIND`/`GET` commands: a structured document grid (re-executing the same read against the engine), in addition to the result text that all commands already returned. This allows the client to render a real table instead of parsing text output designed for the terminal.

**`L1Cache.Stats()` exposes raw bytes** (`used_bytes`, `max_bytes`, `hit_ratio_pct`) alongside already-formatted strings, so the dashboard can calculate a real cache usage percentage without parsing strings like "128.00 MB".

---

## [Unreleased] — High-Performance Engine: Sharded Cache, WAL with Group Commit, and Real-Time Change Streams

**L1/L2 Cache with Sharding (32 partitions)** (`cache.go`). Previously each cache had **a single global mutex** protecting the entire map + LRU list, so any read/write serialized all other threads — the classic bottleneck under sustained concurrent load. Now each cache is split into 32 independent shards (using `fnv32a` hash already used by `shardedLockManager`), each with its own mutex, map, and LRU list/byte budget, reducing contention from "global" to "1/32 of keys." A **real data race** was also fixed: `L2IndexCache.get()` took `RLock` but mutated shared fields (`LastUsed`, `Frequency`) without exclusive lock. Direct access to `l1Cache.mu`/`l1Cache.items` in `RenameDB` (which also had a **latent deadlock**: took `Lock()` then called `del()`, which re-acquires the same mutex) was replaced with the new `L1Cache.DeleteByPrefix` method.

**WAL with Buffered I/O + Configurable fsync (Group Commit)** (`wal.go`). The WAL never performed `fsync` — data remained in the OS buffer without real durability guarantees despite being called "Write-Ahead Log." Each entry was also written with `json.Encoder.Encode` directly against the `*os.File`, without buffering (one syscall per entry). Now each segment uses a `bufio.Writer`, batches are encoded in memory (reusing the buffer pool, which was previously allocated but never used) to emit **a single `Write()` per batch**, and there is a configurable sync policy (`wal_sync_policy`: `always` / `interval` [default, group commit] / `off`) that caps the maximum data loss window without paying an fsync per operation.

**`BufferPool` Without Data Race** (`buffer_pool.go`): counters were plain `int64` mutated from multiple goroutines without atomics.

**Real-Time Change Streams** (`changestream.go`): New `ChangeBus` (in-process pub/sub) that publishes insert/update/delete events as they occur, with non-blocking `Publish()` (a slow subscriber never slows the write path — its event is dropped and counted in `caimandb_change_events_dropped_total`). Exposed via SSE at the new endpoint `GET /watch?db=...&block=...` (`http_query.go`), with the connection exempted from the global server `WriteTimeout` (which would otherwise cut the stream at 30s) and a heartbeat every 15s to keep it alive through proxies/load balancers.

---

## [Unreleased] — New `EXPLAIN FIND ...` / `EXPLAIN SEARCH ...` Command

(`cmd_explain.go`). "EXPLAIN ANALYZE" style: builds the query with the same functions used by `FIND`/`SEARCH` (`buildFindQuery`/`buildSearchQuery`, extracted from `cmd_find.go` so EXPLAIN can never desynchronize from what the actual command does), executes it for real, and reports measured metrics: rows scanned, rows found, what access was actually used (`Actual Access`), what index the optimizer would have chosen for top-level equalities/IN (`Planned`, via `QueryOptimizer.AnalyzeQuery`) -- and the parsed `WHERE` tree (`parse.Expr.String()`), useful now that it supports parentheses and precedence. Does not invent numbers it didn't measure (no "memory used" or fictional "cost").

**`SHOW DBS` and `SHOW BLOCKS` Now Accept One or More Names** (`cmd_show_size.go`): `SHOW DBS`, `SHOW DBS name`, `SHOW DBS name1 name2`; same for `SHOW BLOCKS [<db>] [name1 name2 ...]`. If any name doesn't exist, it's reported on a "Not found: ..." line instead of failing the entire command. `SHOW DBS` now also includes on-disk size per database (previously only showed blocks/docs) and a total aggregate at the end; `SHOW BLOCKS` already showed size per block and now also aggregates a total.

`docs/known-limitations.md` and `docs/nql-reference.md`: unchanged in this pass (see previous block for AST status); `help.go` documents the new `EXPLAIN` and `SHOW DBS/BLOCKS` syntax.

**Scope of This Pass, Explicitly:** Only `EXPLAIN` and the requested `SHOW` improvements were implemented. The broader NQL vision discussed (`USING`/`EXPAND` for relations without global `RELATE` state, `GROUP BY` inside `FIND`, inline aggregation functions (`COUNT()`/`SUM()`/...), `DISTINCT`, `PAGE`/`SIZE`, `FIND ... IDS`, `FIND ... COUNT`, `FIND ... CACHE`, `ANALYZE FIND ...`) is a significantly larger language redesign and was not touched in this pass to avoid risking extensive changes without being able to compile; it remains pending if it should be approached incrementally.

**Not Verified with `go build`/`go vet`** — same caveat as the rest of this changelog: no Go or network in this environment. Manually reviewed with special attention to: that `buildFindQuery`/`buildSearchQuery` extracted from `cmd_find.go` preserve the exact behavior of `handleFind`/`handleSearch` (they are the same code, only moved to a function parameterized by starting index), that no unused imports remain after moving code between files (`time` in `cmd_create_show.go`), and that no function names are duplicated. Confirm with `go build ./... && go vet ./... && go test ./...` before deploying.

---

## [Unreleased] — Real AST for WHERE (Parentheses, Precedence, NOT)

New `internal/caimandb/parse/ast.go`: a recursive-descent parser that compiles a `WHERE` clause into a real expression tree (`Expr`) -- `KindCondition` (leaf) / `KindAnd` / `KindOr` / `KindNot` -- instead of the flat `[]Filter` list with one `Logic` per element that was evaluated strictly left-to-right. Supports:
- Parentheses for grouping (`(a = 1 OR b = 2) AND c = 3`).
- Standard precedence `AND` > `OR` (previously nonexistent: the only possible semantics was "left-to-right", so `a=1 OR b=2 AND c=3` couldn't be written with its usual meaning).
- `NOT` before a condition or a complete group.
- Same operators and same value syntax as always (quotes, JSON arrays/objects, `BETWEEN x AND y`, `IS [NOT] NULL`, two-word operators) -- intentionally preserved, including a peculiarity of the original parser (a quoted value still goes through the same numeric/boolean coercion as an unquoted one).

`internal/caimandb/parse/tokenizer.go`: `(` and `)` are now always emitted as proper tokens (previously only brackets `[]` and braces `{}` were recognized), so AST grouping works without requiring exact spaces around parentheses.

Connected to `FIND` and `SEARCH` (`cmd_find.go`): new `parseWhereClause` in `cmd_filters_util.go` builds the tree and, additionally, a best-effort flat list (only when the tree is a pure chain of top-level `AND`) to avoid touching the existing index planner (`QueryOptimizer.AnalyzeQuery`). `Query` (`ops_find.go`) has a new field `Where *parse.Expr`; `matchesQuery`/`evalExpr` (`query_filter.go`) evaluate the tree when present and otherwise fall back to `matchesFilters` as before -- by design, everything else that constructs `[]Filter` directly (`JOIN`, transactions, `VIEW`, admin) remains exactly as before this change.

`docs/nql-reference.md` documents the new syntax with examples; `docs/known-limitations.md` documents which commands already use the AST and which remain on the flat parser, with a migration path.

**Not Verified with `go build`/`go vet`** — this environment also had no access to a Go compiler or network (same caveat as the rest of this changelog). Manually reviewed with special care at integration points (signature of `Query`, the two call-sites of `matchesFilters`, and that no existing positional `Query{...}` literal uses positional fields), but confirm with `go build ./... && go vet ./...` before deploying.

---

## [Unreleased] — configs/caimandb.conf and First Subpackage (parse/)

`caimandb.conf` now loads/creates from `configs/caimandb.conf` instead of the root working directory (`internal/caimandb/constants.go`, `internal/caimandb/defaults.go`); `SaveToFile` creates the `configs/` folder itself if needed. Updated `help.go`, `README.md`, `docs/configuration.md`, `docs/architecture.md`, `examples/quickstart.md`, and `.gitignore` to reflect this.

New subpackage `internal/caimandb/parse`: the NQL syntax tokenizer (`tokenize` → `parse.Tokenize`) was moved here — the only engine file with no dependencies on `Engine`/`Document`/`Config`/`Session`/`Filter`/`Transaction`. `dsl_parser.go` and `cmd_view.go` now import it as `caimandb/internal/caimandb/parse`.

`docs/known-limitations.md` expanded: documents this first safe split, includes a `grep` filter for finding more "leaf" file candidates, and maintains the recommended path (`httpapi`, `cluster`, leaving `raft_fsm.go`/`transaction.go` in the core) for anyone who wants to continue splitting the package with a Go compiler at hand.

---

## [Unreleased] — Repository Reorganization

New folder structure at repo level: `docs/` (with `docs/api/`), `deployments/docker/`, `scripts/`, `configs/`, `examples/`, `test/` (with `test/integration/` and `test/fixtures/`), `.github/workflows/`.

New documentation: architecture (`docs/architecture.md`), complete NQL reference (`docs/nql-reference.md`), HTTP API (`docs/api/http-api.md`), configuration (`docs/configuration.md`), known limitations (`docs/known-limitations.md`).

Configuration template (`configs/caimandb.conf.example`) generated from the actual `Config` struct.

`Dockerfile` + `docker-compose.yml` for running CaimanDB in a container.

Build/test/run scripts (`scripts/*.sh`) and `Makefile`.

GitHub Actions CI workflow (`go build`, `go vet`, `go test`, `go mod tidy` check).

`internal/caimandb` was intentionally left intact: it is a single Go package with over 40 files coupled to the same `Engine` via unexported fields; truly splitting it requires exporting much of that state and verifying with a compiler, something that could not be confirmed in this environment (see `docs/known-limitations.md`).

---

## Previous Pass — Compilation Fixes

`cmd/` + `internal/caimandb/` structure instead of a flat directory of 68 files in the root.

Removed duplicate redeclaration of `views`/`viewsMu`.

Implemented 11 NQL handlers that `dsl_parser.go` referenced but did not exist (`handleDrop`, `handleRename`, `handleInfo`, `handleDescribe`, `handleStats`, `handleSize`, `handleRebuild`, `handleCheck`, `handleRepair`, `handleFlexCommand`, `handleTransaction`), in `cmd_admin_extra.go`.

Removed unused imports of `github.com/hashicorp/raft` and `errors` in 11 files.
