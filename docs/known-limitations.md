# Known Limitations

## The WHERE AST: now used by every command that has a WHERE clause

`internal/caimandb/parse/ast.go` adds a real expression tree for
`WHERE` (`ParseWhere`): supports parentheses, `NOT`, and standard
`AND`/`OR` precedence (previously none of these three existed — see
the comment at the top of that file for why). Every command that
takes a `WHERE` clause now goes through it via `parseWhereClause`
(`cmd_filters_util.go`) and `matchesQuery`/`matchesWhere`/`evalExpr`
(`query_filter.go`): `FIND`/`SEARCH`, `DELETE`, `CLEAR`/`COUNT`,
aggregations/`GROUP BY`, `UPDATE`, `VIEW CREATE`, and `EXPORT` (the
last admin command still on the old parser). The original flat
parser, `parseFilters` (`cmd_filters_util.go`), has no remaining
callers in command handlers — it's kept only as a fallback path
`matchesWhere`/`matchesQuery` use for callers that never had a tree in
the first place (e.g. `EmptyBlock`'s empty-filter "match everything"),
and as the input the query optimizer's index-selection heuristic
(`AnalyzeQuery`) looks at, tree or no tree.

`DELETE` (`cmd_delete.go`), `CLEAR`/`COUNT`
(`cmd_clear_count.go`), aggregations/`GROUP BY` (`cmd_aggregate.go`),
`UPDATE` (`cmd_update.go`/`ops_update.go`), `VIEW CREATE`
(`cmd_view.go`), and `EXPORT` (`cmd_admin.go`) were migrated to
`parseWhereClause` across several passes. `DELETE`'s migration touched
more than the command handler itself, since `DELETE` (unlike
`FIND`/`SEARCH`/aggregations) writes and replicates:

- `Engine.Delete` and `Engine.Count` (`ops_delete.go`) now take an
  additional `where *parse.Expr` parameter alongside the existing
  `filters []Filter`, matching `Query.Where`/`Query.Filters` in
  `query_filter.go`. `where`, when non-nil, always wins; `filters`
  is kept as the fallback for callers that never had a tree (e.g.
  `EmptyBlock`'s empty-filter "match everything") and as the only
  input the query optimizer's index-selection heuristic
  (`AnalyzeQuery`) ever looks at, tree or no tree.
- `matchesWhere` (`query_filter.go`) is the write-path counterpart of
  `matchesQuery`: same "tree if present, else flat" fallback, used by
  `deleteLocal`, `Count`, `updateLocal`, and aggregations.
- `RaftCommand` (`raft_fsm.go`) gained a `Where *parse.Expr` field so
  the tree survives a `raft.Apply()` round-trip and gets replayed
  identically on every node, not just re-derived from `Filters` (which
  would silently lose OR/NOT/grouping on replay). `Engine.Update`
  reuses this same field.
- The per-document WAL entry for `delete` (`ops_local.go`,
  `wal_recovery.go`) changed shape from a bare `[]Filter` JSON array to
  `walDeleteEntry{Filters, Where}`. `RecoverWAL` tries the new shape
  first and falls back to the old bare-array shape for WAL segments
  written before this migration, so recovery of pre-migration WALs is
  unaffected. `UPDATE` needed no equivalent WAL-shape change: its
  per-document WAL entry is already the fully-computed new document
  body (`ops_local.go`'s `e.wal.Write("update", ...)`), not a filter
  description to re-match at recovery time -- `wal_recovery.go`'s
  `"update"` case just replays the document bytes directly.

Aggregations (`Engine.Aggregate`/`Engine.Group` in `ops_aggregate.go`)
took the same `where *parse.Expr` parameter alongside `filters`, using
`matchesWhere` instead of `matchesFilters`. Being read-only, this
needed none of the `RaftCommand`/WAL-shape changes `DELETE` did.
`VIEW CREATE`'s migration was similarly read-only, but did add a new
`ViewDefinition.Where *parse.Expr` field alongside the legacy
`ViewDefinition.Filters []Filter`, so a view whose `WHERE` has real
`OR`/`NOT`/grouping doesn't silently lose that condition when the view
is later queried (the `default` case in `handleViewCmd` builds a
`Query{Filters, Where}` the same way `FIND` does). `EXPORT`'s
migration (`handleExport` in `cmd_admin.go`) was the last command on
the old parser.

Three real, pre-existing bugs were found and fixed along the way,
none specific to this WHERE migration itself but all caught while
doing it:

- Both `Count` and `handleDelete`'s "no condition given" guard used to
  treat `len(filters) == 0` as "match/require nothing". That's only
  true when there's no tree either — a WHERE with OR/NOT/parens
  (anything that isn't a pure top-level AND-chain) makes
  `parseWhereClause` return a nil flat list *on purpose* even though
  the query is a real, non-empty condition. Both now check
  `len(filters) == 0 && where == nil`. `handleExport` had the same
  bug (`if len(filters) > 0` before applying the WHERE to the export)
  and got the same fix (`if len(filters) > 0 || where != nil`) as part
  of its own migration.
- `handleUpdate` (`cmd_update.go`) had a more serious version of the
  same class of bug, unrelated to `parseFilters` vs `parseWhereClause`
  and present in the code long before this migration touched it:
  the block that parses `WHERE` used to always re-scan
  `tokens[i:]` looking for a literal `"WHERE"` token before parsing —
  but for the by far most common form, `UPDATE <block> WHERE
  <condition> SET ...`, an earlier block in the same function (the
  initial "detect ID or field" token check) already consumes the
  leading `"WHERE"` token and advances the index past it. That left
  the WHERE-parsing block scanning for a `"WHERE"` that no longer
  existed in the remaining tokens, finding nothing, and leaving both
  `filters` and `where` `nil` — which `matchesWhere`/`matchesFilters`
  correctly treat as "no restriction, match every document in the
  block". In other words, `UPDATE <block> WHERE <condition> SET ...`
  was silently updating the **entire block**, ignoring the WHERE
  condition, whenever the condition didn't happen to hit the
  `isIDFilter` shortcut. This had never been caught because nothing
  exercised `UPDATE ... WHERE ...` through the real command string
  before (`update_where_tree_test.go` does now, and would have failed
  against the old code). Fixed by parsing directly from the
  already-advanced index when `hasWhere` was detected early, instead
  of re-searching for a token that was already consumed.

## `internal/caimandb` is now split into subpackages

An earlier version of this document described `internal/caimandb` as a
single ~15,000-line Go package with no compiler available in this
sandbox to verify a split. Both of those are now out of date:

- A Go 1.24.4 toolchain is available in this sandbox (`apt-get install
  golang-1.24-go`), and `GOPROXY=direct` reaches module dependencies
  directly over `git`/`https` via `github.com`/`codeload.github.com`,
  which this sandbox's network allowlist does permit — `go build ./...`
  and `go vet ./...` now both run clean, and the real test suite
  (`go test ./...`) runs and passes, chaos/Raft tests included.
- The core package has since been split into real subpackages:
  `storage/`, `wal/`, `raft/`, `cache/`, `cluster/`, `backup/`,
  `turbo/`, `parse/`, `query/`. `internal/caimandb` itself remains the
  largest package (it still owns `Engine` and the bulk of command
  handling), but it's no longer the monolith this section originally
  described.

This is now stale historical context, kept for anyone still deciding
whether to split `raft_fsm.go`/`transaction.go` out further: an earlier
investigation (grep for access to unexported `.field`/`.method` across
files) found those two touch more than ten unexported `Engine` fields
from different subsystems (`engine.pool`, `engine.dirMgr`,
`engine.cacheKey`, `engine.lockManager`, `engine.l1Cache`,
`engine.shardMgr`, `engine.externalStore`, `engine.intelEngine`,
`engine.flexEngine`, `engine.buildSecondaryIndex`, ...), and several
other subsystems (`cluster.go`, `dist_query.go`, `flexcolumn.go`,
`http_admin.go`, `http_query.go`, `shard_manager.go`, `transaction.go`)
hold a reference back to `*Engine` itself — moving those to their own
packages while `Engine` keeps them as fields would create an import
cycle, requiring the dependency to be inverted with interfaces first.
Real, mechanical work; just not attempted in this pass. With a working
compiler now available (see above), this is safe to attempt
incrementally, verifying with `go build ./... && go vet ./...` after
each subsystem moved — starting with `raft_fsm.go`/`transaction.go`
last, since they're the most deeply coupled.

### What was already separated: `internal/caimandb/parse`

`tokenizer.go` (the `tokenize` function, now `parse.Tokenize`) had no
references to `Engine`, `Document`, `Config`, `Session`, `Filter` or
`Transaction` — only standard library `strings`. Being a true "leaf"
file (zero coupling, not just low coupling), it was moved without
needing to export anything from the engine or risking an import cycle.
The two places that called it (`dsl_parser.go`, `cmd_view.go`) now
import `caimandb/internal/caimandb/parse`.

### If you want to go further

`storage/`, `wal/`, `raft/`, `cache/`, `cluster/`, `backup/`, `turbo/`,
`query/` are already split out (see above). Remaining candidates still
living in `internal/caimandb` itself, roughly least to most coupled
with `Engine`:

- `internal/httpapi` — `http_admin.go`/`http_query.go`; would need
  exporting a handful of `Engine` fields (`nodeID`, `startupTime`,
  `opCount`, `l1Cache`, `metrics`, `tokenManager`, `cluster`,
  `flexEngine`, `shardMgr`, `config`) via getters.
- Leave `raft_fsm.go`/`transaction.go` in the core package last — see
  the import-cycle note above.

Each step should end with `go build ./... && go vet ./...` before
moving to the next.

## `go.sum`

`go mod tidy` hasn't been run against a network with unrestricted
module-proxy access (only `GOPROXY=direct` via `github.com` has been
exercised here, which was enough to build/test successfully — see
above). Worth running `go mod tidy` once in an environment with full
proxy access, purely to confirm `go.sum` matches what a normal `go get`
would produce.

## Test suite

`go test ./...` passes end-to-end in this sandbox (Go 1.24.4, see
above), 25+ test files including real chaos/Raft/network-partition
tests (`crash_recovery_test.go`, `network_partition_test.go`,
`cluster_failover_test.go`, etc.) — see the CHANGELOG for what each
covers. mTLS, certificate rotation (both for the admin/query HTTP
APIs and, since the most recent CHANGELOG entry, for inter-node Raft
traffic too via `tls_raft.go`), point-in-time restore,
Raft-lag/shard-health Prometheus metrics, the real-WHERE-clause
migration for every remaining command (`UPDATE`/`VIEW
CREATE`/`EXPORT`, plus the in-transaction `TxManager.Update`/`Delete`
found and migrated in a later pass), and granular per-resource ACL
(`acl.go`, `GRANT`/`REVOKE`/`SHOW GRANTS`, layered on top of the fixed
role hierarchy in `roles.go`) have all since been added -- see the
CHANGELOG and the "WHERE AST" section above.

`authorizeBlockAccess` enforcement now also covers aggregations/`GROUP
BY`, `EXPORT`, and `VIEW` (both `CREATE` and querying an existing
view), on top of the original `FIND`/`SEARCH`/`INSERT`/`UPDATE`/
`DELETE` surface -- see the CHANGELOG entry that extended it.
Remaining ACL gap: there's still no `admin` action tier in `GRANT`
(only `read`/`write`), so a grant can never hand out DB-admin-level
capability (backup/restore, user management) the way the role
hierarchy's `ADMIN` tier does -- that stays role-only. `MetricClusterStatus`
and `MetricShardDistribution`, flagged above as defined-but-never-set,
are now also fixed -- see the CHANGELOG.

## Critical performance finding from real load testing: `UPDATE`/`DELETE` under concurrent write load

See `docs/load-testing-findings.md` for the full writeup, real numbers,
and root-cause analysis. Summary: `updateLocal`/`deleteLocal`
(`ops_local.go`) take a single lock keyed by the whole block (not
per-document) and do a full prefix scan of every document in the
block on every call, with no fast path for a simple `WHERE id =
"<literal>"` equality the way `FindByID`/`FIND <block> <id>` already
has. Under real concurrent load testing (50 workers, mixed
read/write), this produced `UPDATE`/`DELETE` p50 latency of ~1.47
seconds versus `FIND`'s 21 microseconds for a logically equivalent
"look up one document by ID" operation -- roughly 19,000x slower, and
the gap scales with concurrency (confirmed at ~360ms p50 with only 10
concurrent workers too). This is a real production-readiness blocker
for any workload with concurrent writes to the same block by ID (a
very common pattern), not a hardware artifact of the sandbox it was
measured on. Not fixed in this pass -- it's a genuine architectural
change to the write-locking/scan path, out of scope to rush blind; see
the load-testing doc for a scoped fix proposal (single-ID fast path,
then finer-grained locking) and what would need to be re-measured to
confirm any fix actually worked.

