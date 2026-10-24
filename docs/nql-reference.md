# NQL Reference (CaimanDB Query Language)

This document summarizes the content of the interactive `HELP` command
(`internal/caimandb/help.go`), organized by category.

## Databases

| Command | Description |
|---|---|
| `CREATE DB <name>` | Create a database |
| `DROP DB <name>` | Delete a database |
| `RENAME DB <old> TO <new>` | Rename a database |
| `USE <name>` | Switch active database |
| `SHOW DBS` | List databases |
| `INFO DB <name>` / `DESCRIBE DB <name>` | Details / schema |
| `STATS DB [<name>]` / `SIZE DB [<name>]` | Statistics / size |
| `COMPACT <db>` | Garbage collection |
| `ANALYZE DB` / `OPTIMIZE DB` | Analysis and optimization |
| `BACKUP <db> TO <file>` / `RESTORE <db> FROM <file>` | Backups |
## Blocks (collections)

| Command | Description |
|---|---|
| `CREATE BLOCK [<db>] <name>` | Create a block |
| `DROP BLOCK [<db>] <name>` | Delete a block |
| `RENAME BLOCK [<db>] <old> TO <new>` | Rename |
| `SHOW BLOCKS [<db>]` | List blocks |
| `EMPTY BLOCK [<db>] <name>` / `CLEAR [<db>] <name>` | Empty block |
| `REBUILD BLOCK` / `CHECK BLOCK` / `REPAIR BLOCK` | Index/integrity maintenance |

## Documents — INSERT

```sql
INSERT users {"name": "John", "age": 30}
INSERT users name: "John", age: 30
INSERT users {"name": "John"}; {"name": "Jane"}
INSERT users [{"name": "John"}, {"name": "Jane"}]
INSERT users FROM "file.json"
```

## Documents — FIND

```sql
FIND users WHERE age > 18 AND status = "active"
FIND users SELECT name, age WHERE age > 18
FIND users ORDER name, age:DESC WHERE age > 18
FIND users WHERE age > 18 LIMIT 50 OFFSET 100
GET users <id>
```

### WHERE with parentheses, precedence and NOT (FIND / SEARCH)

`FIND` and `SEARCH` compile the `WHERE` clause into a real expression
tree (AST) in `internal/caimandb/parse/ast.go`, instead of the flat
left-to-right chaining still used by the rest of the commands
(`UPDATE`, `DELETE`, `COUNT`, aggregations, `VIEW`, admin). This allows:

- **Parentheses** for grouping and explicit precedence forcing:

  ```sql
  FIND users WHERE (status = "active" OR status = "trial") AND age >= 18
  ```

- **`AND` with higher precedence than `OR`** (like in SQL): in
  `a = 1 OR b = 2 AND c = 3`, `b = 2 AND c = 3` is evaluated as a
  unit before the `OR`. Previously, without the AST, this query was
  evaluated strictly left-to-right and there was no way to express
  standard precedence without explicit parentheses on every term.

- **`NOT`** before a condition or a group:

  ```sql
  FIND users WHERE NOT (status = "banned" OR status = "suspended")
  ```

The condition operators (`=`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`,
...) are the same as always; the only new thing is how they combine.
For indexes, the query planner still uses the flat list of top-level
conditions when the `WHERE` is a pure `AND` chain (the common case);
if an `OR`, a `NOT` or an explicit group appears at the top level,
there is simply no single condition to index and a full scan is
performed — still correct, just without that shortcut.

## Documents — SEARCH (full-text)

```sql
SEARCH articles "search text"
SEARCH articles "exact phrase" EXACT
SEARCH articles "~fuzzy" FUZZY
SEARCH articles "text" WITH SCORE WITH MATCHES
```

## Documents — UPDATE / DELETE

```sql
UPDATE users WHERE _id = "abc" SET name = "John", age = 30
UPDATE users WHERE _id = "abc" INC views = 1
UPDATE users WHERE _id = "abc" PUSH tags = "newtag"
UPDATE ALL users SET status = "archived"

DELETE users WHERE age < 18
DELETE ALL users
```

## Aggregations and GROUP BY

```sql
COUNT users WHERE active = true
SUM orders amount WHERE status = "completed"
AVG products price
GROUP orders BY status SUM amount
```

Available functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `MEDIAN`,
`MODE`, `STDDEV`.

## DISTINCT (FIND only)

```sql
FIND users DISTINCT city WHERE country = "MX"
FIND orders DISTINCT customer_id, status
FIND users DISTINCT WHERE active = true
```

`FIND <block> DISTINCT <field>[, <field>...]` keeps the first document
seen for each distinct combination of values at those fields (dot-paths
like `address.city` work the same as elsewhere). `FIND <block> DISTINCT`
with no field list dedupes on the whole projected row instead. Not
available on `SEARCH`, and not combinable with `GROUP BY` (grouping
already collapses rows to one per group).

## ACID Transactions

```sql
BEGIN mydb users
INSERT users {"name": "John"}
COMMIT
-- or ROLLBACK / ABORT

TX STATUS
TX LIST
TX ISOLATION
```

Isolation levels: `read_committed`, `repeatable_read`
(default), `serializable`.

## JOIN and Views

```sql
JOIN orders WITH customers ON orders.customer_id = customers._id

VIEW CREATE <name> AS FIND <block> WHERE ...
VIEW DROP <name>
VIEW SHOW
```

## Export / Import

```sql
EXPORT users TO "file.json"
EXPORT users WHERE age > 18 TO "file.csv"
IMPORT users FROM "file.json"
```

## Encrypted Backup / Restore (with point-in-time restore)

This is a separate system from the older `BACKUP <db> TO <file>` /
`RESTORE <db> FROM <file>` above (which uses Badger's native
uncompressed, unencrypted snapshot format). This one is
AES-256-GCM-encrypted, gzip-compressed, incremental, and admin-gated:

```sql
BACKUP users              -- incremental (or first-ever full)
BACKUP users FULL         -- force a full snapshot
RESTORE users             -- replay full + incrementals (latest state)
RESTORE users AS OF 1717000000                      -- Unix seconds
RESTORE users AS OF 2024-05-29T12:00:00Z             -- RFC3339
VERIFY BACKUP users       -- checksum + decrypt/decode check, read-only
```

`RESTORE <db> AS OF <ts>` is a point-in-time restore: only document
versions with `Meta.UpdatedAt` at or before `<ts>` are replayed, so a
document changed after that moment reverts to whatever version it held
at `<ts>` (or is skipped entirely if it didn't exist yet). `<ts>`
before the oldest full backup fails with an error rather than silently
restoring nothing. This format has no delete tombstones: a document
deleted from the live database after being captured in a backup comes
back on restore (see `ops_backup_jsonl.go`'s package doc comment).

```sql
CREATE USER alice PASSWORD "..." ROLE readwrite
SHOW USERS

SHARD STATUS
SHARD REBALANCE
SHARD SCALE mydb 8

CLUSTER STATUS
```

## Granular Access Grants (GRANT / REVOKE)

Layered on top of (never instead of) the fixed role hierarchy
(`ADMIN_GENERAL`/`ADMIN`/`SUBADMIN`/`DEVELOPER`/`USER`): a grant can
extend a specific user's access to one specific block, including a
block in a database their role alone wouldn't let them touch at all.
A grant never takes away access a role already provides.

```sql
GRANT read ON mydb.customers TO alice
GRANT read,write ON mydb.customers TO alice
GRANT read ON mydb TO alice          -- every block in mydb
REVOKE write ON mydb.customers FROM alice
SHOW GRANTS FOR alice
SHOW GRANTS                          -- your own grants
```

Only an admin (`ADMIN` or `ADMIN_GENERAL`) can run `GRANT`/`REVOKE`; a
scoped `ADMIN` can only grant/revoke access to their own database, not
someone else's. `SHOW GRANTS FOR <user>` needs admin to inspect
another user's grants; your own is always visible to you. Actions are
`read` and `write` only -- there's no grantable `admin` tier; DB-admin
capability (backup/restore, user management) stays role-only. Grant
enforcement currently covers the primary CRUD surface
(`FIND`/`SEARCH`/`INSERT`/`UPDATE`/`DELETE`); see
`docs/known-limitations.md` for what it doesn't cover yet.

## Navigation / System

`PWD`, `LS`, `LS <db>`, `CD <db>`, `TREE`, `STATUS`, `HEALTH`,
`VERSION`, `PING`, `HELP`, `EXIT` / `QUIT`.

## Filter Operators

`=`/`==`, `!=`/`<>`, `>`, `<`, `>=`, `<=`, `LIKE`, `CONTAINS`,
`EXISTS`, `IN`, `NOT IN`, `BETWEEN`, `STARTS WITH`, `ENDS WITH`,
`AND`, `OR`.
