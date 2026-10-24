# CaimanDB NQL — Complete Syntax Reference (English)

Other languages: [Español](./NQL_SYNTAX.es.md) · [Deutsch](./NQL_SYNTAX.de.md)

This document covers the NQL command surface implemented by CaimanDB's console/query engine, including the complete top-level command index, syntax variants, and examples.

**Conventions used below:**
- `<block>` — a block name, optionally as `<db>.<block>` for a cross-database reference.
- `<db>` — a database name.
- `[...]` — optional part. `<a|b>` — choose one. `...` — repeatable.
- Statement tokens are case-insensitive (`insert`, `INSERT`, `Insert` all work);
  this reference uses UPPERCASE for keywords by convention.

## Table of contents
- [Database commands](#database-commands)
- [Block commands](#block-commands)
- [Named indexes](#named-indexes)
- [INSERT — all variants](#insert--all-variants)
- [FIND / GET — query](#find--get--query)
- [SEARCH — full-text](#search--full-text)
- [UPDATE](#update)
- [DELETE](#delete)
- [ADD FIELD / DROP FIELD / TRUNCATE / UPSERT](#add-field--drop-field--truncate--upsert)
- [Aggregations (COUNT/SUM/AVG/...)](#aggregations-countsumavg)
- [GROUP BY](#group-by)
- [ACID Transactions](#acid-transactions)
- [TURBO / BULK loading](#turbo--bulk-loading)
- [JOIN](#join)
- [RELATE](#relate)
- [AUTORELATIONS](#autorelations)
- [Views](#views)
- [EXPORT / IMPORT](#export--import)
- [RENAME FIELD — rename a field across every document](#rename-field--rename-a-field-across-every-document)
- [FLEX-COLUMN](#flex-column)
- [User management](#user-management)
- [Access control (GRANT / REVOKE / SHOW GRANTS)](#access-control-grant--revoke--show-grants)
- [Shard management](#shard-management)
- [Cluster](#cluster)
- [CHECKPOINT](#checkpoint)
- [Navigation & system](#navigation--system)
- [Filter operators](#filter-operators)
- [Full worked example](#full-worked-example)

---

### Database commands

```
CREATE DB <name>                    Create a new database
DROP DB <name>                      Delete a database
RENAME DB <old> TO <new>            Rename a database
USE <name>                          Switch to a database (sets the session's current DB)
SHOW DBS                            List all databases (blocks/docs/size)
SHOW DBS <name> [<name2> ...]       List only the named database(s)
INFO DB <name>                      Show database details
DESCRIBE DB <name>                  Show database schema (inferred field types)
STATS DB [<name>]                   Show database statistics
SIZE DB [<name>]                    Show database size on disk
COMPACT <db>                        Run garbage collection / reclaim space
ANALYZE DB [<name>]                 Analyze database performance
OPTIMIZE DB [<name>]                Optimize database (indexes, storage tiers)
BACKUP <db> TO <file>               Backup database to a file
RESTORE <db> FROM <file>            Restore database from a backup file
```

```sql
CREATE DB shop
USE shop
SHOW DBS
SHOW DBS shop analytics
INFO DB shop
STATS DB shop
BACKUP shop TO "shop_2026-08.bak"
RESTORE shop FROM "shop_2026-08.bak"
DROP DB old_shop
```

### Block commands

A block is CaimanDB's equivalent of a table/collection — a named, schemaless
container of documents inside a database.

```
CREATE BLOCK [<db>] <name>          Create a new block
CREATE BLOCK [<db>] (<n1>, <n2>, ...) Create several blocks in one statement
DROP BLOCK [<db>] <name>            Delete a block
RENAME BLOCK [<db>] <old> TO <new>  Rename a block
RENAME BLOCK [<db>] (<o1>,<o2>,...) TO (<n1>,<n2>,...)
                                     Rename several blocks in parallel, by position
SHOW BLOCKS [<db>]                  List all blocks (docs/size/shards)
SHOW BLOCKS <db> <name> [<name2>]   List only the named block(s)
INFO BLOCK [<db>] <name>            Show block details
DESCRIBE BLOCK [<db>] <name>        Show block schema
EMPTY BLOCK [<db>] <name>           Delete all documents from the block
CLEAR [<db>] <name>                 Alias for EMPTY BLOCK
ANALYZE BLOCK [<db>] <name>         Analyze block performance
OPTIMIZE BLOCK [<db>] <name>        Optimize block
REBUILD BLOCK [<db>] <name>         Rebuild all indexes
CHECK BLOCK [<db>] <name>           Check block integrity
REPAIR BLOCK [<db>] <name>          Repair a corrupted block
SIZE BLOCK [<db>] <name>            Show block size on disk
```

```sql
CREATE BLOCK products
CREATE BLOCK shop products          -- explicit db, without USE
CREATE BLOCK (products, customers, orders)          -- 3 blocks, current db (USE)
CREATE BLOCK shop (products, customers, orders)     -- 3 blocks, explicit db
SHOW BLOCKS
SHOW BLOCKS shop products inventory
DESCRIBE BLOCK products
REBUILD BLOCK products              -- e.g. after changing indexed fields
EMPTY BLOCK products                -- keeps the block, deletes its documents
CLEAR products                      -- same as above
RENAME BLOCK product TO new_product
RENAME BLOCK (product, customer, shelf) TO (products, customers, shelves)
```

**Structure notes — `CREATE BLOCK` / `RENAME BLOCK` with a parenthesized list:**
- The `(<a>, <b>, ...)` list is parsed by counting balanced parentheses
  (`parseParenList`), not by fixed token position: it can contain
  spaces, and names may be quoted (quotes are stripped automatically).
- Each block is created/renamed in an **independent** call — this is
  not one atomic transaction. If one fails (e.g. it already exists, or
  the old name doesn't exist), the result reports exactly which
  succeeded and which failed, and the rest of the list is still
  processed.
- In `RENAME BLOCK (...) TO (...)`, the old-name list and the new-name
  list must have the **same length**; renaming happens by position
  (`old[i] -> new[i]`). A length mismatch returns an error before
  renaming anything.
- A block has no fixed columns or types to declare at creation time
  (CaimanDB is a document engine, not relational): a block's
  "structure" emerges from the documents it holds, and can be
  inspected afterward with `DESCRIBE BLOCK` (inferred field types) —
  there is no `CREATE BLOCK ... (field TYPE, ...)` clause.

### Named indexes

```
CREATE INDEX <name> ON <block> (<field>)
CREATE UNIQUE INDEX <name> ON <block> (<field>)
CREATE INDEX <name> ON <block> (<field>) WHERE <condition>
DROP INDEX <name> ON <block>
SHOW INDEXES ON <block>
```

```sql
CREATE INDEX idx_price ON products (price)
CREATE UNIQUE INDEX idx_sku ON products (sku)
CREATE INDEX idx_active ON products (status) WHERE status = "active"
SHOW INDEXES ON products
DROP INDEX idx_price ON products
```

- `UNIQUE` is enforced on plain `INSERT` (rejects a duplicate value).
- The `WHERE` clause is stored and echoed back by `SHOW INDEXES ON`, but is
  not yet enforced during query planning — every document is indexed
  regardless of the condition (a partial index today, full index in
  practice).
- Only a single field per index is supported: `(<field>)`.
- This is separate from `SHOW INDEXES [<db>]` (§2), which lists the
  flat set of indexed field names at the database level; `SHOW INDEXES
  ON <block>` lists the named `CREATE INDEX` definitions for one block.

### INSERT — all variants

```
INSERT <block> [<id>] <json-object>
INSERT <block> [<id>] key: value, key2: value2, ...
INSERT <block> [<id>] key = value, key2 = value2, ...
INSERT <block> <doc1>; <doc2>; <doc3>; ...
INSERT <block> [<json-array-of-docs>]
INSERT <block> FROM "<file.json|file.csv>"
INSERT <block> GENERATE <n> [WORKERS <w>]
```

**Structure notes:**
- `<id>` is optional and, when present, must be the token right after the block
  name and must not look like `{`, `[`, `"`, `NULL`, or a reserved keyword
  (`FROM`, `GENERATE`, `TO`, `WHERE`, `SET`, `LIMIT`, `ORDER`, `SELECT`) — those
  are parsed as the start of the document/clause instead of an id.
- With an explicit id, the document is inserted with that exact `_id` (via
  `insertWithID`) instead of an auto-generated one.
- `key: value` and `key = value` both work for flat documents; values are
  auto-typed: things that parse as a number become a number, `{...}` becomes a
  nested object, everything else is a trimmed string.
- Multiple `;`-separated documents and a top-level JSON array (`[...]`) both
  insert a batch in one call; if a custom id is given, it's applied only to the
  first document, the rest get generated ids.

**JSON document:**
```sql
INSERT products {"name": "Keyboard", "price": 49.90, "in_stock": true}
INSERT products {"user": {"name": "John", "age": 30}}
```

**JSON document with an explicit id (the id token comes right after the block name):**
```sql
INSERT products kb001 {"name": "Keyboard", "price": 49.90, "in_stock": true}
-- -> Inserted document: kb001 (ID: kb001, shard: shard_7)
```

**Key:Value format:**
```sql
INSERT products name: "Mouse", price: 19.90, in_stock: true
INSERT products mouse001 name: "Mouse", price: 19.90, in_stock: true
```

**Key=Value format:**
```sql
INSERT products name = "Monitor", price = 199.00
```

**Multiple documents (semicolon-separated), same statement:**
```sql
INSERT products {"name": "A"}; {"name": "B"}; {"name": "C"}
INSERT products name: "A"; name: "B"; name: "C"
```

**Batch insert (JSON array):**
```sql
INSERT products [{"name": "A"}, {"name": "B"}, {"name": "C"}]
```

**Import from file (blocking, reads the whole file):**
```sql
INSERT products FROM "products.json"
INSERT products FROM "products.csv"
```

**GENERATE — synthetic data for benchmarking / seeding:**
```sql
INSERT products GENERATE 1000000              -- auto-scaled workers
INSERT products GENERATE 200000 WORKERS 8     -- fixed worker count (up to 64)
```
- Without `WORKERS <n>`, an internal watchdog (`runRateWatchdog`) samples
  throughput every 2s and adds 2 workers at a time whenever the rate drops
  below 85% of the best rate seen so far, capped at
  `GOMAXPROCS * GenerateAutoScaleMaxMultiplier` (default multiplier: 4) and a
  hard ceiling of 64 workers, with a cooldown between increases.
- With an explicit `WORKERS <n>`, that exact worker count is used for the
  whole run (up to the same hard ceiling of 64) and the watchdog is disabled.
- Large `GENERATE` runs automatically switch to BULK MODE for their duration.

### FIND / GET — query

```
FIND <block> [SELECT <field>[,<field>...] | <field> AS <alias> | COUNT(<field>) AS <alias> | <field>/<n> AS <alias>]
             [WHERE <condition>]
             [DISTINCT [<field>[,<field>...]]]
             [GROUP BY <field>[,<field>...]] [HAVING <condition>]
             [ORDER <field>[:ASC|:DESC][,<field>...]]
             [LIMIT <n>] [OFFSET <n>]
             [--type:table]

GET <block> <id>
GET <block> @ <id>

EXPLAIN FIND ...     -- runs the query for real and reports what happened
EXPLAIN SEARCH ...   -- (like EXPLAIN ANALYZE, not just a plan estimate)
```

**Basic find / by id:**
```sql
FIND products WHERE _id = "abc123"
GET products abc123
GET products @ abc123
```

**Filters (see full operator table in §21):**
```sql
FIND products WHERE price > 20 AND in_stock = true
FIND products WHERE name LIKE "%board%" OR name CONTAINS "Mon"
FIND products WHERE price BETWEEN 10 AND 100
FIND products WHERE status IN ("active", "pending")
FIND products WHERE tags IN ["go", "database", "nosql"]
```

**Grouping/precedence with parentheses and NOT (FIND/SEARCH only):**
```sql
FIND products WHERE (status = "active" OR status = "trial") AND price >= 18
FIND products WHERE NOT (status = "banned" OR status = "suspended")
```

**Projection (SELECT):**
```sql
FIND products SELECT name, price WHERE price > 20
```

**Computed SELECT fields — COUNT(field), simple arithmetic AS alias:**
```sql
FIND movies SELECT title, COUNT(actors) as actors_count, year
FIND movies SELECT title, duration_minutes / 60 as hours
```

**GROUP BY / HAVING (FIND only):**
```sql
FIND movies SELECT title, COUNT(actors) as actors_count, year
  WHERE year >= 2000
  GROUP BY title, year
  HAVING COUNT(actors) >= 5
```

**DISTINCT — deduplicate results:**
```sql
FIND products DISTINCT                    -- dedupe on the whole projected row
FIND products DISTINCT category           -- dedupe on one field's value
FIND products DISTINCT category, status   -- dedupe on the combination
FIND products SELECT category DISTINCT category WHERE price > 10
```
`FIND <block> DISTINCT` with no fields keeps the first document for each
distinct row it would otherwise return; `DISTINCT <field>,...` keeps the
first document for each distinct combination of those field values.

**Filtering through a RELATE alias (see §13):**
```sql
RELATE movies USE directors
FIND movies SELECT title, directors.name
  WHERE directors.name == "Christopher Nolan"
```

**Sorting, pagination, table output:**
```sql
FIND products ORDER name, price:DESC WHERE price > 18
FIND products WHERE price > 18 LIMIT 50 OFFSET 100
FIND products WHERE price > 18 --type:table
```

**EXPLAIN:**
```sql
EXPLAIN FIND products WHERE price > 18 ORDER price:DESC LIMIT 10
EXPLAIN SEARCH products "wireless keyboard"
```

### SEARCH — full-text

```
SEARCH <block> "<text>" [EXACT | FUZZY]
                         [WITH SCORE] [WITH MATCHES]
                         [WHERE <condition>] [LIMIT <n>] [ORDER <field>]
```

```sql
SEARCH products "wireless keyboard"
SEARCH products "exact phrase" EXACT
SEARCH products "~keybord" FUZZY
SEARCH products "keyboard" WITH SCORE WITH MATCHES
SEARCH products "+must_include -must_exclude optional"
SEARCH products "keyboard" WHERE price > 18 LIMIT 50 ORDER name
```

### UPDATE

```
UPDATE <block> WHERE <condition> SET <field> = <value>[, <field2> = <value2> ...]
UPDATE <block> WHERE <condition> INC <field> = <n>
UPDATE <block> WHERE <condition> DEC <field> = <n>
UPDATE <block> WHERE <condition> PUSH <field> = <value>
UPDATE <block> WHERE <condition> PULL <field> = <value>
UPDATE ALL <block> SET <field> = <value>[, ...]
```

- `SET` replaces field values. `INC`/`DEC` add/subtract a number from a numeric
  field. `PUSH`/`PULL` append/remove a value from an array field.
- `UPDATE ALL` applies to every document in the block, no `WHERE` needed.
- Clauses can combine multiple assignments and functions such as `now()`.

```sql
UPDATE products WHERE _id = "kb001" SET name = "Mechanical Keyboard", price = 55
UPDATE products WHERE _id = "kb001" INC views = 1
UPDATE products WHERE _id = "kb001" DEC stock = 5
UPDATE products WHERE _id = "kb001" PUSH tags = "on_sale"
UPDATE products WHERE _id = "kb001" PULL tags = "discontinued"
UPDATE ALL products SET status = "archived"
UPDATE products WHERE status = "draft" SET status = "published", published_at = now()
```

### DELETE

```
DELETE <block> WHERE <condition>
DELETE ALL <block>
EMPTY BLOCK [<db>] <name>     -- alias for DELETE ALL
CLEAR [<db>] <name>           -- alias for DELETE ALL / EMPTY BLOCK
```

```sql
DELETE products WHERE _id = "kb001"
DELETE products WHERE price < 5 OR in_stock = false
DELETE ALL products
```

### ADD FIELD / DROP FIELD / TRUNCATE / UPSERT

```
ADD FIELD <block>.<field> DEFAULT <value>[, <field2> DEFAULT <value2> ...]
DROP FIELD <block>.<field>[, <field2> ...] [WHERE <condition>]
TRUNCATE <block>
UPSERT <block> <id> SET <field>=<value>[, <field2>=<value2> ...]
UPSERT <block> <id> <json-object>
```

```sql
ADD FIELD products.discount DEFAULT 0
ADD FIELD products.discount DEFAULT 0, products.featured DEFAULT false

DROP FIELD products.discount
DROP FIELD products.discount, products.featured
DROP FIELD products.legacy_sku WHERE category = "discontinued"

TRUNCATE products                    -- empties the block, no confirmation prompt

UPSERT products kb001 SET name = "Mechanical Keyboard", price = 55
UPSERT products kb001 {"name": "Mechanical Keyboard", "price": 55}
```

- `ADD FIELD`: only the first field carries the `<block>.` prefix; every
  field after a comma is assumed to belong to the same block. A document
  that already has the field (even with a falsy/zero/empty value) is left
  untouched — it only fills in documents where the field is missing.
- `DROP FIELD` without `WHERE` runs across the whole block in one pass;
  with `WHERE` it evaluates the full condition tree (ORs/NOTs/parentheses
  included, same as `FIND`) and updates only the matching documents.
- `TRUNCATE` is unconditional and instant, functionally equivalent to
  `CLEAR`/`EMPTY BLOCK` but modeled on SQL `TRUNCATE` semantics (no
  confirmation prompt).
- `UPSERT` updates the document if `<id>` already exists, otherwise
  inserts a new one with that id. Accepts either comma-separated
  `SET field=value` assignments or a raw JSON object body.

### Aggregations (COUNT/SUM/AVG/...)

```
COUNT  <block> [WHERE <condition>]
SUM    <block> <field> [WHERE <condition>]
AVG    <block> <field> [WHERE <condition>]
MIN    <block> <field> [WHERE <condition>]
MAX    <block> <field> [WHERE <condition>]
MEDIAN <block> <field> [WHERE <condition>]
MODE   <block> <field> [WHERE <condition>]
STDDEV <block> <field> [WHERE <condition>]
```

```sql
COUNT products WHERE in_stock = true
SUM orders amount WHERE status = "completed"
AVG products price WHERE category = "electronics"
MIN products price
MAX products price
MEDIAN salaries amount
MODE products category
STDDEV scores value
```

### GROUP BY

```
GROUP <block> BY <field> [COUNT | SUM | AVG | MIN | MAX] [<field>] [WHERE <condition>]
```

```sql
GROUP users BY city COUNT
GROUP orders BY status SUM amount
GROUP products BY category AVG price WHERE price > 10
GROUP logs BY level COUNT WHERE timestamp > "2024-01-01"
```

### ACID Transactions

```
BEGIN [<db> <block>]
  <INSERT|UPDATE|DELETE statements...>
COMMIT
ROLLBACK | ABORT

TX STATUS       Show current transaction details
TX LIST         List active transactions
TX ISOLATION    Show isolation level
```

Isolation levels (configured, not selected per-statement):
`read_committed`, `repeatable_read` (default), `serializable`.

```sql
BEGIN shop products
  INSERT products {"name": "Webcam", "price": 39.90}
  UPDATE products WHERE _id = "kb001" SET price = 45
  DELETE products WHERE _id = "old001"
COMMIT
```
```sql
BEGIN shop products
  INSERT products {"name": "Bad idea"}
ROLLBACK
```

### TURBO / BULK loading

```
BULK MODE ON            Wider batch windows, relaxed WAL fsync policy
BULK MODE OFF           Restore normal low-latency batching/fsync policy
BULK STATUS             Show turbo engine stats (worker pool, batching)

IMPORT <block> FROM FILE '<path>' [FORMAT NDJSON|ARRAY] [BATCH <n>]
```

- Plain `INSERT`s already auto-batch concurrent writes to the same block;
  `BULK MODE` widens that further for large loads.
- `IMPORT ... FROM FILE` streams a file into `<block>` in large batches
  (default 20000 docs/batch) without holding the whole file in memory.
  `FORMAT NDJSON` (default) reads one JSON object per line; `FORMAT ARRAY`
  reads a single top-level JSON array. A `.gz`-suffixed path is decompressed
  on the fly. Runs under BULK MODE automatically for the duration of the load.

```sql
BULK MODE ON
INSERT products GENERATE 2000000
BULK MODE OFF

IMPORT products FROM FILE '/data/products.ndjson' FORMAT NDJSON BATCH 50000
IMPORT products FROM FILE '/data/products.json.gz' FORMAT ARRAY
BULK STATUS
```

### JOIN

```
JOIN <block1> WITH <block2> ON <block1>.<field> = <block2>.<field>
```

```sql
JOIN orders WITH customers ON orders.customer_id = customers._id
JOIN posts WITH users ON posts.author_id = users._id
```

### RELATE

Register once how a block relates to other blocks (optionally in other
databases); `FIND` then resolves the relation automatically instead of
repeating a `JOIN` condition in every query.

```
RELATE <block> USE <target1>[,<target2>,...]
```

- Each target is a bare block name (same database) or `db.block` (cross-database).
- Match convention: the source document must have a `<target>_id` field (the
  singular of the target block name) holding the target's id — a single id
  for one-to-one/many-to-one, or an array of ids for one-to-many.
- After relating, select target fields with dot notation, or the bare alias
  for the whole related document.

```sql
RELATE movies USE directors,actors,genres
RELATE sales USE crm.customers,inventory.products,accounting.invoices

FIND movies SELECT title,directors.name,actors.name
FIND sales SELECT customers.name,products.name,invoices.total
```

### AUTORELATIONS

CaimanDB watches its own read access: when the same user reads the same
document repeatedly in a short window (default: 5 reads in 10 minutes), it
automatically creates a self-relation between that user and the document — no
`RELATE` needed. Each auto-relation carries `access_count`/`last_seen`,
a `relevance` score, and a small `key_metadata` sample of the doc's fields.

Unlike `RELATE` (explicit, permanent), auto-relations are temporal: every
further access slides their expiry forward (default TTL: 24h); once a
document stops being read by that user, the relation expires and is swept
away by a background pass. The resulting graph is bipartite and directed
(`user -> document read`).

```
SHOW AUTORELATIONS <block>
    [FROM <id>] [TO <id>] [DEPTH <n>] [DIRECTION IN|OUT|BOTH]
    [FORMAT TABLE|TREE|GRAPH|JSON]
    [WHERE|FILTER <expression>]
    [ORDER BY DEGREE|ID|NAME|ACCESS_COUNT|RELEVANCE|LAST_SEEN|FIRST_SEEN [ASC|DESC]]
    [LIMIT <n>] [OFFSET <n>]
    [STATS] [PATHS] [ORPHANS] [CYCLES] [BROKEN] [SUMMARY] [VERBOSE];
```

| Modifier | Meaning |
|---|---|
| `FROM <id>` | Start from this document or user id (auto-detected) |
| `TO <id>` | Keep only relations touching this id as the other end |
| `DEPTH <n>` | Hops to traverse from FROM (default 1) |
| `DIRECTION` | `OUT` = reads outward from a user; `IN` = readers inward into a doc; `BOTH` (default) |
| `FORMAT` | `TABLE` (default), `TREE` (needs FROM), `GRAPH` (adjacency list), `JSON` |
| `WHERE`/`FILTER` | Condition over `doc_id`, `user_id`, `access_count`, `relevance`, `last_seen`, `first_seen` |
| `ORDER BY` | `DEGREE`, `ID`, `NAME`, `ACCESS_COUNT`, `RELEVANCE`, `LAST_SEEN`, `FIRST_SEEN` (+`ASC`/`DESC`) |
| `LIMIT`/`OFFSET` | Pagination over the final, sorted result |
| `STATS`/`SUMMARY` | Prepend an aggregate report |
| `PATHS` | Render the FROM traversal as an indented tree |
| `ORPHANS` | Only isolated pairs |
| `CYCLES` | Only relations closing a cycle |
| `BROKEN` | Only relations whose document was later deleted |
| `VERBOSE` | Add first_seen/expires/full metadata |

```sql
SHOW AUTORELATIONS products;
SHOW AUTORELATIONS products FROM p_042;
SHOW AUTORELATIONS products FROM p_042 DEPTH 3 DIRECTION BOTH FORMAT TREE;
SHOW AUTORELATIONS products STATS;
SHOW AUTORELATIONS products WHERE access_count > 10 ORDER BY DEGREE LIMIT 20;
SHOW AUTORELATIONS products FROM p145 DEPTH 6 DIRECTION BOTH
  WHERE relevance >= 0.75 ORDER BY ACCESS_COUNT DESC LIMIT 100
  FORMAT TREE STATS VERBOSE;
SHOW AUTORELATIONS products CYCLES;
SHOW AUTORELATIONS products BROKEN;
```

### Views

```
VIEW CREATE <name> AS FIND <block> WHERE <condition>
VIEW DROP <name>
VIEW SHOW
VIEW INFO <name>
<view_name>                    -- executes the view
```

```sql
VIEW CREATE active_users AS FIND users WHERE active = true
VIEW SHOW
VIEW INFO active_users
active_users
VIEW DROP active_users
```

### EXPORT / IMPORT

`EXPORT` always writes **both** a `.csv` and a `.json` file into
`<data_root>/backups/`, using the base name you give it. Every exported
row/document includes both `_id` and `id`.

```
EXPORT <block> [WHERE <condition>] TO "<file>"
IMPORT <block> FROM "<file.json>"        -- also looks inside backups/
IMPORT <block> FROM "<file.csv>"
```

```sql
EXPORT products TO "products_export"
-- -> writes backups/products_export.csv and backups/products_export.json
EXPORT products WHERE price > 18 TO "expensive_products"

IMPORT products FROM "products_export.json"
IMPORT products FROM "products_export.csv"
```

### RENAME FIELD — rename a field across every document

```
RENAME FIELD [<db>] <block>.<field> TO <new_field>
```

Unlike `RENAME BLOCK`/`RENAME DB` (which only rename a directory entry
and are instant), `RENAME FIELD` is a **bulk rewrite**: every document
in the block that has `<field>` is read, has its key changed, and is
written back, along with that document's secondary index and
FLEX-COLUMN entries.

```sql
RENAME FIELD product.name TO product_name
RENAME FIELD shop product.name TO product_name     -- explicit db
```

**Structure notes:**
- The first argument after `RENAME FIELD` is **a single token** shaped
  like `<block>.<field>` (block and field separated by exactly one
  dot); the next token must be `TO`, and the last token is the new
  field name.
- The database is optional and, if given, goes **before**
  `<block>.<field>` as its own token — detected because that token
  contains no dot (`RENAME FIELD mydb product.name TO product_name`).
  Without it, the current database (`USE`) is used.
- A document that has neither `<field>` nor already has `<new_field>`
  is left untouched. A document that already has `<new_field>` (from a
  previous partial run, or a genuine name collision) is skipped rather
  than overwritten, and reported separately as a conflict.
- The operation is **idempotent and restartable**: it does not run
  inside the ACID transaction manager's WAL, but as a direct Badger
  operation in fixed-size batches with a cursor. A crash mid-run
  leaves some documents renamed and others not (not atomic
  all-or-nothing) — re-running the exact same `RENAME FIELD` finishes
  the job where it left off, since an already-renamed document simply
  no longer has `<field>` and is skipped.
- Takes an exclusive lock per `<db>/<block>` (`rename_field`) while it
  runs, so it doesn't collide with another concurrent `RENAME FIELD`
  on the same block.
- Returns the number of documents actually changed:
  `Field renamed: product.name -> product_name in database: shop (128 document(s))`.

### FLEX-COLUMN

Admin/diagnostic sub-commands for the flexible-column engine (adaptive
indexing of hot fields).

```
FLEX STATS                     Global FLEX-COLUMN engine statistics
FLEX HOT [<block>]             "Hot" fields detected (most read/filtered)
FLEX REINDEX [<db>] <block>    Rebuild a block's FLEX-COLUMN index
```

```sql
FLEX STATS
FLEX HOT products
FLEX REINDEX products
FLEX REINDEX shop products
```

### User management

```
CREATE USER <name> IDENTIFIED BY "<pass>"
    [ROLE <ADMIN|SUBADMIN|DEVELOPER|USER>]
    [STATUS <ACTIVE|DISABLED>]
    [DEFAULT DATABASE <db>]
    [DEFAULT BLOCK <block>]
    [PASSWORD EXPIRE <NEVER|DAYS <n>>]
    [ACCOUNT LOCK <ON|OFF>]
    [COMMENT "<description>"]
DROP USER <name>
SHOW USERS

CREATE SERVER <name> ADMIN [<username>] IDENTIFIED BY "<pass>"
```

**Structure notes — `CREATE USER`:**
- Every clause after the username is optional except `IDENTIFIED BY`,
  and they may be given **in any order**.
- The legacy short form is also accepted: `CREATE USER <user>
  "<pass>" [ROLE <role>]` (positional password instead of
  `IDENTIFIED BY`).
- `ROLE` defaults to `USER` if omitted.
- `PASSWORD EXPIRE DAYS <n>` requires an integer `>= 0`; `PASSWORD
  EXPIRE NEVER` disables expiration.

**Structure notes — `CREATE SERVER`:**
- Creates a new logical server: its own data directory, isolated from
  other servers, with its own admin user — all servers share the same
  port/process; what distinguishes each server is only its admin
  credentials.
- `<username>` (the admin's name for that server) is optional; if
  omitted, `admin` is used.

```sql
CREATE USER analyst IDENTIFIED BY "s3cret!" ROLE readonly
CREATE USER carla IDENTIFIED BY "anotherPass!" ROLE DEVELOPER STATUS ACTIVE
    DEFAULT DATABASE shop DEFAULT BLOCK products
    PASSWORD EXPIRE DAYS 90 ACCOUNT LOCK OFF
    COMMENT "CI integration account"
SHOW USERS
DROP USER analyst

CREATE SERVER north_branch ADMIN IDENTIFIED BY "adminPass!"
CREATE SERVER south_branch ADMIN south_admin IDENTIFIED BY "adminPass2!"
```

### Access control (GRANT / REVOKE / SHOW GRANTS)

```
GRANT read[,write] ON <db>.<block> TO <user>
REVOKE read[,write] ON <db>.<block> FROM <user>
SHOW GRANTS [FOR <user>]
```

```sql
GRANT read ON shop.products TO analyst
GRANT read,write ON shop.* TO carla        -- every block in shop
GRANT read ON shop TO alice                -- bare db == shop.* (equivalent to shop.*)
REVOKE write ON shop.products FROM carla
SHOW GRANTS
SHOW GRANTS FOR analyst
```

- `<block>` may be `*` for "every block in `<db>`"; a bare `<db>` with no
  dot after `ON` is treated the same as `<db>.*`.
- Only an admin (role `ADMIN` or `ADMIN_GENERAL`) may run `GRANT` or
  `REVOKE`. A scoped `ADMIN` (not `ADMIN_GENERAL`) can only grant/revoke
  access within their own database — the same isolation `USE`/`CD`
  enforce elsewhere.
- A granular `GRANT` to a specific block is enough for that user to
  `USE`/`CD` into the database even if their role alone wouldn't allow
  it; per-block/per-action checks still apply once they touch a block.
- `SHOW GRANTS` with no `FOR` shows the caller's own grants. `SHOW
  GRANTS FOR <user>` for someone else's grants requires admin.

### Shard management

```
SHARD STATUS
SHARD REBALANCE
SHARD SCALE <db> <shards>
```

```sql
SHARD STATUS
SHARD REBALANCE
SHARD SCALE shop 32
```

### Cluster management

```
CLUSTER STATUS
```

### CHECKPOINT

```
CHECKPOINT        Forces a checkpoint: syncs every database to disk and
                  rotates the WAL if applicable
```

```sql
CHECKPOINT
-- -> Checkpoint completed: 3 database(s) synced, WAL rotated: true, took 42ms
```

### Navigation & system

```
PWD               Show current path
LS                List databases
LS <db>           List blocks in database
CD <db>           Change to database
TREE              Show full directory tree
STATUS            Show system status
HEALTH            Show health check
VERSION           Show version
PING              Ping the engine
HELP              Show help
EXIT, QUIT        Exit the shell
```

### Filter operators (usable in every `WHERE`)

| Operator | Meaning |
|---|---|
| `=`, `==` | Equal |
| `!=`, `<>` | Not equal |
| `>`, `<` | Greater than / Less than |
| `>=`, `<=` | Greater or equal / Less or equal |
| `LIKE` | Pattern matching (`%` wildcard) |
| `CONTAINS` | Substring contains |
| `EXISTS` | Field exists |
| `IN` | Value in list |
| `NOT IN` | Value not in list |
| `BETWEEN` | Range between two values |
| `STARTS WITH` | String starts with |
| `ENDS WITH` | String ends with |
| `AND` | Logical AND (default when omitted) |
| `OR` | Logical OR |

### Full worked example

```sql
CREATE DB shop
USE shop
CREATE BLOCK products

INSERT products kb001 {"name": "Keyboard", "price": 49.90, "in_stock": true}
INSERT products {"name": "Mouse", "price": 19.90, "in_stock": true}
INSERT products name: "Monitor", price: 199.00, in_stock: true

FIND products WHERE price > 20
FIND products WHERE name LIKE "%oard%" ORDER price:DESC
FIND products WHERE price BETWEEN 20 AND 200 SELECT name, price

SEARCH products "keyboard" WITH SCORE

UPDATE products WHERE _id = "kb001" SET price = 45
UPDATE products WHERE price < 30 INC price = 1

COUNT products WHERE in_stock = true
AVG products price
GROUP products BY in_stock COUNT

DELETE products WHERE _id = "kb001"

BEGIN shop products
  INSERT products {"name": "Webcam", "price": 39.90}
  UPDATE products WHERE name = "Mouse" SET price = 17.90
COMMIT

VIEW CREATE cheap_products AS FIND products WHERE price < 25
cheap_products

EXPORT products TO "products_backup"
STATUS
STATS DB shop
```


## Complete command index

The query engine dispatches the following top-level commands. The detailed sections above define their arguments and variants.

| Command | Syntax |
|---|---|
| `VERSION` | `VERSION` |
| `PING` | `PING` |
| `STATUS` | `STATUS` |
| `HEALTH` | `HEALTH` |
| `HELP` | `HELP` |
| `EXIT / QUIT` | `EXIT | QUIT | \Q` |
| `PWD` | `PWD` |
| `CD` | `CD <db>` |
| `LS` | `LS [<db>]` |
| `TREE` | `TREE` |
| `CREATE` | `CREATE DB <db> | CREATE BLOCK [<db>] <block> | CREATE USER ... | CREATE INDEX ...` |
| `DROP` | `DROP DB <db> | DROP BLOCK <block> | DROP FIELD <block>.<field> | DROP INDEX ...` |
| `RENAME` | `RENAME DB <old> TO <new> | RENAME BLOCK ...` |
| `GRANT` | `GRANT <actions> ON <db>.<block> TO <user>` |
| `REVOKE` | `REVOKE <actions> ON <db>.<block> FROM <user>` |
| `SHOW` | `SHOW DBS | SHOW BLOCKS ... | SHOW USERS | SHOW GRANTS [FOR <user>] | SHOW INDEXES ON <block> | SHOW ...` |
| `INFO` | `INFO DB <db> | INFO BLOCK <block>` |
| `DESCRIBE` | `DESCRIBE DB <db> | DESCRIBE BLOCK <block>` |
| `STATS` | `STATS [DB|BLOCK] [<name>]` |
| `SIZE` | `SIZE [DB|BLOCK] [<name>]` |
| `USE` | `USE <db>` |
| `INSERT` | `INSERT <block> <json> | INSERT <block> SET <field>=<value>[, ...] | INSERT ...` |
| `FIND` | `FIND <block> [WHERE <expr>] [ORDER <field>[:ASC|DESC]] [LIMIT <n>] [OFFSET <n>] [DISTINCT <field>]` |
| `EXPLAIN` | `EXPLAIN FIND ... | EXPLAIN SEARCH ...` |
| `GET` | `GET <block> <id>` |
| `SEARCH` | `SEARCH <block> <text> [WHERE ...] [LIMIT ...]` |
| `UPDATE` | `UPDATE <block> WHERE <expr> SET <field>=<value>[, ...] | UPDATE ALL <block> SET ...` |
| `DELETE` | `DELETE <block> WHERE <expr> | DELETE ALL <block>` |
| `CLEAR / EMPTY` | `CLEAR <block> | EMPTY <block>` |
| `COUNT` | `COUNT <block> [WHERE <expr>]` |
| `SUM / AVG / MIN / MAX / MEDIAN / MODE / STDDEV` | `<AGGREGATE> <block> <field> [WHERE <expr>]` |
| `GROUP` | `GROUP <block> BY <field>[, ...] [WHERE <expr>] [HAVING <expr>]` |
| `JOIN` | `JOIN <block> WITH <block> ON <field>=<field> ...` |
| `RELATE` | `RELATE ...` |
| `EXPORT` | `EXPORT <block> TO <file> [FORMAT ...]` |
| `IMPORT` | `IMPORT <block> FROM FILE <file> [FORMAT ...] [BATCH ...] | IMPORT <block> <file>` |
| `BACKUP` | `BACKUP <db> [FULL] | BACKUP <db> <file>` |
| `RESTORE` | `RESTORE <db> [AS OF <timestamp>] | RESTORE <db> FROM <file>` |
| `VERIFY BACKUP` | `VERIFY BACKUP <db>` |
| `COMPACT` | `COMPACT <db>` |
| `CHECKPOINT` | `CHECKPOINT` |
| `ANALYZE` | `ANALYZE DB|BLOCK ...` |
| `OPTIMIZE` | `OPTIMIZE DB|BLOCK ...` |
| `REBUILD` | `REBUILD BLOCK [<db>] <block>` |
| `CHECK` | `CHECK ...` |
| `REPAIR` | `REPAIR ...` |
| `SHARD` | `SHARD ...` |
| `CLUSTER` | `CLUSTER ...` |
| `VIEW` | `VIEW ...` |
| `FLEX` | `FLEX ...` |
| `BEGIN` | `BEGIN [<db>] [<block>]` |
| `COMMIT` | `COMMIT` |
| `ROLLBACK / ABORT` | `ROLLBACK | ABORT` |
| `TX / TRANSACTION` | `TX [STATUS|LIST|STATS] | TRANSACTION [STATUS|LIST|STATS]` |
| `BULK` | `BULK ...` |
| `TRUNCATE` | `TRUNCATE <block>` |
| `UPSERT` | `UPSERT <block> <id> SET <field>=<value>[, ...] | UPSERT <block> <id> <json>` |
| `ADD FIELD` | `ADD FIELD <block>.<field> DEFAULT <value> [, <field2> DEFAULT <value2> ...]` |
