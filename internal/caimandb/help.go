package caimandb

func helpText() string {
	return `
===============================================================================
  CaimanDB V1.0 "DataEngine" - Enterprise Big Data Engine
  OPTIMIZED FOR LARGE DOCUMENTS - PRODUCTION READY
===============================================================================

DIRECTORY STRUCTURE:
  ./data/
  ├── <db_name>/                    <- Database directory
  │   ├── __meta/                   <- Database metadata (db.json)
  │   ├── <block_name>/             <- Block directory
  │   │   ├── __data/               <- BadgerDB data (documents)
  │   │   ├── __index/              <- Secondary indexes
  │   │   └── __meta/               <- Block metadata (block.json)
  │   └── ... more blocks
  ├── __users/                      <- User authentication (Argon2id)
  ├── __raft/                       <- Raft cluster state
  ├── __shards/                     <- Shard management data
  ├── __cluster/                    <- Cluster node information
  ├── __system/                     <- System data
  │   └── wal/                      <- Write-Ahead Log (WAL)
  └── __external/                   <- Large documents (>5MB) external storage


===============================================================================
  ADVANCED FEATURES
===============================================================================

  SECURITY:
    - Argon2id password hashing (OWASP recommended)
    - JWT Authentication with configurable expiry
    - Basic Auth fallback (HTTP)
    - Rate limiting per user (configurable)
    - Account lockout after failed attempts
    - Audit logging (JSON format)
    - TLS/HTTPS support
    - Risk engine for operation blocking

  DATA MANAGEMENT:
    - ACID Transactions (BEGIN/COMMIT/ROLLBACK)
    - Write-Ahead Logging (WAL) for crash recovery
    - External storage for large documents (>5MB)
    - Automatic compression (Zstd/Gzip) for >100KB
    - Secondary indexes with lazy loading
    - Multi-level caching (L1 + L2)
    - Consistent hashing for sharding
    - Automatic shard splitting/merging
    - Auto-scaling based on data growth

  PERFORMANCE:
    - Cost-based query optimizer
    - Vectorized query processing
    - Distributed query execution
    - Buffer pooling for reduced GC pressure
    - Streaming WAL support
    - Fast startup mode (skip WAL replay)
    - Parallel batch operations
    - Lazy index loading on demand

  CLUSTER:
    - Auto-cluster initialization (Raft consensus)
    - Auto-node discovery and joining
    - Auto-rebalancing with predictive analytics
    - Auto-failover and leader election
    - Distributed query executor with shard pruning
    - Auto-scale to millions of nodes and shards

  INTELLIGENT ENGINE:
    - Predictive scaling (machine learning)
    - Auto-index optimization
    - Auto-cache adaptation
    - Self-healing with zero downtime
    - Predictive resource allocation
    - Intelligent query routing


===============================================================================
  DATABASE COMMANDS
===============================================================================

  CREATE DB <name>                    Create a new database
  DROP DB <name>                      Delete a database
  RENAME DB <old> TO <new>            Rename a database
  USE <name>                          Switch to a database
  SHOW DBS                            List all databases (blocks/docs/size)
  SHOW DBS <name> [<name2> ...]       List only the named database(s)
  INFO DB <name>                      Show database details
  DESCRIBE DB <name>                  Show database schema
  STATS DB [<name>]                   Show database statistics
  SIZE DB [<name>]                    Show database size
  COMPACT <db>                        Run garbage collection
  ANALYZE DB [<name>]                 Analyze database performance
  OPTIMIZE DB [<name>]                Optimize database
  BACKUP <db> TO <file>               Backup database
  RESTORE <db> FROM <file>            Restore database


===============================================================================
  BLOCK COMMANDS
===============================================================================

  CREATE BLOCK [<db>] <name>          Create a new block
  CREATE BLOCK [<db>] (<n1>, <n2>...) Create several blocks at once
  DROP BLOCK [<db>] <name>            Delete a block
  RENAME BLOCK [<db>] <old> TO <new>  Rename a block
  RENAME BLOCK [<db>] (<o1>,<o2>...)  Rename several blocks at once, by
    TO (<n1>,<n2>...)                   position (old/new lists must match)
  RENAME FIELD [<db>] <block>.<field> Rename a field across every document
    TO <new_field>                      in <block>
  SHOW BLOCKS [<db>]                  List all blocks (docs/size/shards)
  SHOW BLOCKS <db> <name> [<name2>]   List only the named block(s)
  INFO BLOCK [<db>] <name>            Show block details
  DESCRIBE BLOCK [<db>] <name>        Show block schema
  EMPTY BLOCK [<db>] <name>           Delete all documents from block
  CLEAR [<db>] <name>                 Alias for EMPTY BLOCK
  ANALYZE BLOCK [<db>] <name>         Analyze block performance
  OPTIMIZE BLOCK [<db>] <name>        Optimize block
  REBUILD BLOCK [<db>] <name>         Rebuild all indexes
  CHECK BLOCK [<db>] <name>           Check block integrity
  REPAIR BLOCK [<db>] <name>          Repair corrupted block
  SIZE BLOCK [<db>] <name>            Show block size


===============================================================================
  DOCUMENT OPERATIONS - INSERT
===============================================================================

  JSON format:
    INSERT <block> {"field": "value", "number": 123, "boolean": true}
    INSERT <block> {"user": {"name": "John", "age": 30}}

  Key:Value format:
    INSERT <block> name: "John", age: 30, active: true

  Key=Value format:
    INSERT <block> name = "John", age = 30, active = true

  Multiple documents (semicolon separated):
    INSERT <block> {"name": "John"}; {"name": "Jane"}; {"name": "Bob"}

  Batch insert (JSON array):
    INSERT <block> [{"name": "John"}, {"name": "Jane"}, {"name": "Bob"}]

  Import from file:
    INSERT <block> FROM "file.json"
    INSERT <block> FROM "file.csv"


===============================================================================
  DOCUMENT OPERATIONS - FIND / QUERY
===============================================================================

  Basic find:
    FIND <block> WHERE _id = "abc123"

  With filters:
    FIND <block> WHERE age > 18 AND status = "active"
    FIND <block> WHERE name LIKE "%John%" OR email CONTAINS "@gmail.com"
    FIND <block> WHERE age BETWEEN 18 AND 65
    FIND <block> WHERE status IN ("active", "pending")
    FIND <block> WHERE tags IN ["go", "database", "nosql"]

  With grouping and precedence (FIND/SEARCH only -- see WHERE below):
    FIND <block> WHERE (status = "active" OR status = "trial") AND age >= 18
    FIND <block> WHERE NOT (status = "banned" OR status = "suspended")

  With projection:
    FIND <block> SELECT name, age, email WHERE age > 18

  With computed SELECT fields (COUNT(field), simple arithmetic AS alias):
    FIND movies SELECT title, COUNT(actors) as actors_count, year
    FIND movies SELECT title, duration_minutes / 60 as hours

  With grouping (FIND only):
    FIND movies SELECT title, COUNT(actors) as actors_count, year
      WHERE year >= 2000
      GROUP BY title, year
      HAVING COUNT(actors) >= 5

  With DISTINCT (FIND only -- deduplicates the result set; put it right
  after the block name, before WHERE/SELECT/etc.):
    FIND users DISTINCT city WHERE country = "MX"
    FIND orders DISTINCT customer_id, status
    FIND users DISTINCT WHERE active = true

  Filtering through a RELATE alias (see RELATE below):
    RELATE movies USE directors
    FIND movies SELECT title, directors.name
      WHERE directors.name == "Christopher Nolan"

  With sorting:
    FIND <block> ORDER name, age:DESC WHERE age > 18

  With pagination:
    FIND <block> WHERE age > 18 LIMIT 50 OFFSET 100

  Table output (human readable):
    FIND <block> WHERE age > 18 --type:table

  Explain (runs the query for real and reports what happened -- like
  EXPLAIN ANALYZE, not just an estimate):
    EXPLAIN FIND <block> WHERE age > 18 ORDER age:DESC LIMIT 10
    EXPLAIN SEARCH <block> "some text"

  Get by ID:
    GET <block> <id>
    GET <block> @ <id>


===============================================================================
  DOCUMENT OPERATIONS - SEARCH (Full-Text)
===============================================================================

  Basic search:
    SEARCH <block> "search text"

  Phrase search (exact match):
    SEARCH <block> "exact phrase" EXACT

  Fuzzy search:
    SEARCH <block> "~fuzzy" FUZZY

  With score and matches:
    SEARCH <block> "search text" WITH SCORE WITH MATCHES

  Operator search:
    SEARCH <block> "+must_include -must_exclude optional"

  With filters and pagination:
    SEARCH <block> "text" WHERE age > 18 LIMIT 50 ORDER name


===============================================================================
  DOCUMENT OPERATIONS - UPDATE
===============================================================================

  Set fields:
    UPDATE <block> WHERE _id = "abc" SET name = "John", age = 30

  Increment (numeric):
    UPDATE <block> WHERE _id = "abc" INC views = 1

  Decrement (numeric):
    UPDATE <block> WHERE _id = "abc" DEC stock = 5

  Push to array:
    UPDATE <block> WHERE _id = "abc" PUSH tags = "newtag"

  Pull from array:
    UPDATE <block> WHERE _id = "abc" PULL tags = "oldtag"

  Update ALL documents:
    UPDATE ALL <block> SET status = "archived"

  Combined operations:
    UPDATE <block> WHERE status = "draft" SET status = "published", published_at = now()


===============================================================================
  DOCUMENT OPERATIONS - DELETE
===============================================================================

  Delete with filters:
    DELETE <block> WHERE _id = "abc"
    DELETE <block> WHERE age < 18 OR status = "inactive"

  Delete ALL documents (with confirmation):
    DELETE ALL <block>

  Empty block (alias for DELETE ALL):
    EMPTY BLOCK [<db>] <name>
    CLEAR [<db>] <name>


===============================================================================
  AGGREGATIONS
===============================================================================

  COUNT <block> [WHERE ...]
    COUNT users WHERE active = true

  SUM <block> <field> [WHERE ...]
    SUM orders amount WHERE status = "completed"

  AVG <block> <field> [WHERE ...]
    AVG products price WHERE category = "electronics"

  MIN <block> <field> [WHERE ...]
    MIN products price

  MAX <block> <field> [WHERE ...]
    MAX products price

  MEDIAN <block> <field> [WHERE ...]
    MEDIAN salaries amount

  MODE <block> <field> [WHERE ...]
    MODE products category

  STDDEV <block> <field> [WHERE ...]
    STDDEV scores value


===============================================================================
  GROUP BY
===============================================================================

  GROUP <block> BY <field> [COUNT|SUM|AVG|MIN|MAX] [<field>] [WHERE ...]

  Examples:
    GROUP users BY city COUNT
    GROUP orders BY status SUM amount
    GROUP products BY category AVG price WHERE price > 10
    GROUP logs BY level COUNT WHERE timestamp > "2024-01-01"


===============================================================================
  ACID TRANSACTIONS
===============================================================================

  Start a transaction:
    BEGIN [<db> <block>]
    BEGIN mydb users

  Transaction operations (within BEGIN/COMMIT):
    INSERT users {"name": "John", "email": "john@example.com"}
    UPDATE users WHERE _id = "abc" SET email = "new@example.com"
    DELETE users WHERE _id = "xyz"

  Commit (persist all changes atomically):
    COMMIT

  Rollback (discard all changes):
    ROLLBACK
    ABORT

  Transaction status:
    TX STATUS          - Show current transaction details
    TX LIST            - List active transactions
    TX ISOLATION       - Show isolation level

  Isolation levels (configurable):
    read_committed     - Read only committed data
    repeatable_read    - Consistent snapshot (default)
    serializable       - Strictest isolation


===============================================================================
  TURBO / BULK LOADING
===============================================================================

  Plain INSERTs already auto-batch concurrent writes to the same block
  (see TurboEnabled in config); BULK MODE widens that further for large
  loads:

    BULK MODE ON       - Wider batch windows, relaxed WAL fsync policy
    BULK MODE OFF      - Restore normal low-latency batching/fsync policy
    BULK STATUS        - Show turbo engine stats (worker pool, batching)

  IMPORT <block> FROM FILE '<path>' [FORMAT NDJSON|ARRAY] [BATCH <n>]
                       Stream-load a file into <block> in large batches
                       (default 20000 docs/batch) without holding the
                       whole file in memory. FORMAT NDJSON (default) reads
                       one JSON object per line; FORMAT ARRAY reads a
                       single top-level JSON array. A .gz-suffixed path is
                       decompressed on the fly. Runs under BULK MODE
                       automatically for the duration of the load.
                       Embedders can call Engine.BulkImportReader directly
                       for progress callbacks and structured results.
                       NOTE: FLEX-COLUMN indexing (see below) is skipped
                       for the whole block while BULK MODE is on, so a
                       block loaded via IMPORT is not FLEX-COLUMN-indexed
                       until you run FLEX REINDEX <db> <block> afterward.


===============================================================================
  JOIN
===============================================================================

  JOIN <block1> WITH <block2> ON block1.field = block2.field

  Examples:
    JOIN orders WITH customers ON orders.customer_id = customers._id
    JOIN posts WITH users ON posts.author_id = users._id


===============================================================================
  RELATE
===============================================================================

  Register once how a block relates to other blocks (optionally in other
  databases), then FIND resolves the relation automatically -- no more
  repeating a JOIN condition in every query.

  RELATE <block> USE <target1>[,<target2>,...]

  Each target is either a bare block name (same database) or "db.block"
  for a cross-database relation. The match is by convention: the source
  document must have a "<target>_id" field (singular of the target block
  name) holding the target document's id -- a single id for a one-to-one/
  many-to-one relation, or an array of ids for a one-to-many relation.

  Examples:
    RELATE movies USE directors,actors,genres
    RELATE sales USE crm.customers,inventory.products,accounting.invoices

  Once related, select the target's fields with dot notation, or select
  the bare alias to get the whole related document:
    FIND movies SELECT title,directors.name,actors.name
    FIND sales SELECT customers.name,products.name,invoices.total


===============================================================================
  AUTORELATIONS (automatic, temporal self-relations)
===============================================================================

  CaimanDB watches read access on its own: whenever the same user reads
  the same document repeatedly in a short window (default: 5 reads in
  10 minutes), it automatically creates a self-relation between that
  user and the document -- no RELATE needed. Each auto-relation carries:

    - access_count / last_seen  -- how often and how recently it was read
    - relevance                 -- a score combining frequency and recency
    - key_metadata              -- a small sample of the doc's own fields
                                    (e.g. name/title), for readability

  Unlike RELATE (explicit, permanent), auto-relations are temporal: every
  further access slides their expiry forward (default TTL: 24h); once a
  document stops being read by that user, the relation quietly expires
  and is swept away by a background cleanup pass. Nothing here is
  durable data -- it's a live "what's hot right now" signal.

  The resulting graph is bipartite and directed: every edge is
  "user -> document read". FROM/TO/DEPTH/DIRECTION/PATHS/CYCLES/ORPHANS
  below all reason about that graph literally (see relations_auto_graph.go).

  SHOW AUTORELATIONS <block>
      [FROM <id>] [TO <id>] [DEPTH <n>] [DIRECTION IN|OUT|BOTH]
      [FORMAT TABLE|TREE|GRAPH|JSON]
      [WHERE|FILTER <expression>]
      [ORDER BY DEGREE|ID|NAME|ACCESS_COUNT|RELEVANCE|LAST_SEEN|FIRST_SEEN [ASC|DESC]]
      [LIMIT <n>] [OFFSET <n>]
      [STATS] [PATHS] [ORPHANS] [CYCLES] [BROKEN] [SUMMARY] [VERBOSE];

  Modifiers:
    FROM <id>        Start from this document or user id (auto-detected).
    TO <id>          Keep only relations touching this id as the other end.
    DEPTH <n>         Hops to traverse from FROM (default 1).
    DIRECTION         OUT = follow reads outward from a user; IN = follow
                      readers inward into a document; BOTH (default) =
                      either, which is what makes DEPTH > 1 useful.
    FORMAT            TABLE (default), TREE (needs FROM), GRAPH (plain
                      adjacency list), or JSON.
    WHERE <expr>      Condition over doc_id, user_id, access_count,
                      relevance, last_seen, first_seen -- same syntax as
                      FIND's WHERE. FILTER is accepted as a synonym.
    ORDER BY          DEGREE (connections), ID (doc id), NAME (sampled
                      name/title), ACCESS_COUNT, RELEVANCE, LAST_SEEN, or
                      FIRST_SEEN, optionally followed by ASC or DESC.
                      Default direction: descending for DEGREE,
                      ACCESS_COUNT, RELEVANCE, LAST_SEEN, and FIRST_SEEN;
                      ascending for ID and NAME. With no ORDER BY at all,
                      results come back relevance-sorted, descending.
    LIMIT / OFFSET    Pagination over the final, sorted result.
    STATS / SUMMARY   Prepend an aggregate report (nodes, relations,
                      average degree, cycles, orphans, broken links).
    PATHS             Render the FROM traversal as an indented tree
                      (implies TREE-like output even without FORMAT TREE).
    ORPHANS           Show only relations that are an isolated pair
                      (doc and user connect to nothing else).
    CYCLES            Show only relations that close a cycle in the
                      access graph (detected via union-find).
    BROKEN            Show only relations whose document was deleted
                      after the auto-relation was created.
    VERBOSE           Add first_seen/expires/full metadata to the output.

  Examples:
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


===============================================================================
  VIEWS
===============================================================================

  Create view:
    VIEW CREATE <name> AS FIND <block> WHERE ...

  Drop view:
    VIEW DROP <name>

  Show views:
    VIEW SHOW

  View info:
    VIEW INFO <name>

  Execute view:
    <view_name>


===============================================================================
  EXPORT / IMPORT
===============================================================================

  EXPORT always writes BOTH a .csv and a .json file into the <data_root>/backups/
  directory, using the name you give it (any path/extension you pass is
  reduced to its base name). Every exported row/document includes both the
  "_id" field and the "id" field (never just one of them).

    EXPORT <block> TO "file"
    EXPORT <block> WHERE age > 18 TO "file"
    -> writes backups/file.csv and backups/file.json

  Import from JSON (also looks inside backups/ if a bare name is given):
    IMPORT <block> FROM "file.json"

  Import from CSV:
    IMPORT <block> FROM "file.csv"


===============================================================================
  USER MANAGEMENT
===============================================================================

  Create user:
    CREATE USER <name> PASSWORD "<pass>" [ROLE admin|readwrite|readonly]

  Drop user:
    DROP USER <name>

  List users:
    SHOW USERS


===============================================================================
  SHARD MANAGEMENT
===============================================================================

  View shard status:
    SHARD STATUS

  Trigger rebalancing:
    SHARD REBALANCE

  Scale shards:
    SHARD SCALE <db> <shards>


===============================================================================
  CLUSTER MANAGEMENT
===============================================================================

  View cluster status:
    CLUSTER STATUS


===============================================================================
  NAVIGATION & SYSTEM
===============================================================================

  PWD               Show current path
  LS                List databases
  LS <db>           List blocks in database
  CD <db>           Change to database
  TREE              Show full directory tree
  STATUS            Show system status
  HEALTH            Show health check
  VERSION           Show version
  PING              Ping the engine
  HELP              Show this help
  EXIT, QUIT        Exit the shell


===============================================================================
  FILTER OPERATORS
===============================================================================

  =, ==            Equal
  !=, <>           Not equal
  >, <             Greater than / Less than
  >=, <=           Greater or equal / Less or equal
  LIKE             Pattern matching (% wildcard)
  CONTAINS         Substring contains
  EXISTS           Field exists
  IN               Value in list
  NOT IN           Value not in list
  BETWEEN          Range between two values
  STARTS WITH      String starts with
  ENDS WITH        String ends with
  AND              Logical AND (default)
  OR               Logical OR


===============================================================================
  ENVIRONMENT VARIABLES
===============================================================================

  CAIMANDB_DATA              Data root directory (default: ./data)
  CAIMANDB_CACHE             L1 cache MB (default: 2048)
  CAIMANDB_L2_CACHE          L2 index entries (default: 4096)
  CAIMANDB_WORKERS           Worker goroutines (default: auto)
  CAIMANDB_LOG_LEVEL         debug|info|warn|error (default: info)
  CAIMANDB_METRICS           Metrics port (default: 9090)
  CAIMANDB_NODE_ID           Node identifier (default: auto)
  CAIMANDB_RAFT_PORT         Raft port (default: 2335)
  CAIMANDB_RAFT_BIND         Raft bind address (default: 0.0.0.0)
  CAIMANDB_QUERY_PORT        HTTP Query port (default: 1555)
  CAIMANDB_ADMIN_PORT        Admin API port (default: 1556)
  CAIMANDB_UI_DIR            Path to the ui/ static console (default: ui)
  CAIMANDB_SHARD_COUNT       Number of shards (default: 16)
  CAIMANDB_REPLICA_COUNT     Replication factor (default: 3)
  CAIMANDB_FAST_STARTUP      Enable fast startup (default: true)
  CAIMANDB_COMPRESS_LARGE    Compress large docs (default: true)
  CAIMANDB_MAX_DOC_SIZE      Max document size in bytes (default: 10485760)
  CAIMANDB_JWT_SECRET        JWT secret key (auto-generated)
  CAIMANDB_TLS_ENABLED       Enable TLS (default: false)
  CAIMANDB_TLS_CERT_FILE     TLS certificate file
  CAIMANDB_TLS_KEY_FILE      TLS private key file


===============================================================================
  CONFIGURATION FILE (configs/caimandb.conf)
===============================================================================

{
  "data_root": "./data",
  "cache_mb": 2048,
  "l2_cache_mb": 4096,
  "workers": 0,
  "log_level": "info",
  "metrics_port": 9090,
  "raft_port": 2335,
  "raft_bind": "0.0.0.0",
  "query_port": 1555,
  "admin_port": 1556,
  "query_user": "admin",
  "query_password": "auto-generated",
  "ui_dir": "ui",
  "shard_count": 16,
  "replica_count": 3,
  "wal_segments": 64,
  "auto_cluster": true,
  "fast_startup": true,
  "lazy_index_loading": true,
  "preload_cache": false,
  "compress_large_docs": true,
  "skip_large_indexes": true,
  "use_streaming_wal": true,
  "max_shards_per_node": 1000000,
  "max_cluster_nodes": 10000000,
  "auto_scale_interval": 10,
  "prediction_window": 48,
  "min_shard_docs": 10000,
  "max_shard_docs": 1000000,
  "scale_up_cooldown": 60,
  "scale_down_cooldown": 300,
  "auto_merge_enabled": true,
  "auto_split_enabled": true,
  "predictive_scaling": true,
  "aggressive_scaling": true,
  "max_replicas": 10,
  "min_replicas": 1,
  "max_document_size": 10485760,
  "compress_threshold": 102400,
  "index_skip_threshold": 1048576,
  "large_doc_threshold": 5242880,
  "max_concurrent_queries": 100000,
  "query_timeout": 60,
  "transaction_timeout": 30,
  "max_query_depth": 10,
  "max_batch_size": 10000,
  "rate_limit": 10000,
  "jwt_secret": "your-secret-key",
  "token_expiry": "24h",
  "max_login_attempts": 5,
  "lockout_duration": "15m",
  "tls_enabled": false,
  "tls_cert_file": "",
  "tls_key_file": ""
}


===============================================================================
  HTTP API ENDPOINTS
===============================================================================

  Query API (Port 1555):
    GET  /query?db=<db>&query=<command>
    GET  /status
    GET  /health

  Admin API (Port 1556):
    POST   /api/v1/token           Generate JWT token (Basic Auth)
    POST   /api/v1/auth/login      Login (Basic Auth)
    POST   /api/v1/auth/logout     Logout
    GET    /api/v1/health          Health check
    GET    /api/v1/status          System status
    GET    /api/v1/dbs             List databases
    POST   /api/v1/dbs             Create database
    GET    /api/v1/db/{name}       Get database info
    DELETE /api/v1/db/{name}       Delete database
    POST   /api/v1/query           Execute query
    POST   /api/v1/backup          Backup database
    POST   /api/v1/restore         Restore database
    POST   /api/v1/compact         Compact database
    GET    /api/v1/users           List users
    POST   /api/v1/users           Create user
    GET    /metrics                Prometheus metrics


===============================================================================
  EXAMPLES
===============================================================================

  Database operations:
    CREATE DB mydb
    USE mydb
    CREATE BLOCK users
    SHOW BLOCKS

  Creating and renaming several blocks at once:
    CREATE BLOCK (producto, cliente, estante)
    RENAME BLOCK producto TO producto_nuevo
    RENAME BLOCK (producto, cliente, estante) TO (productos, clientes, estantes)
    RENAME FIELD producto.nombre TO nombre_producto

  Insert documents:
    INSERT users {"name": "Alice", "age": 25, "email": "alice@example.com"}
    INSERT users {"name": "Bob", "age": 30, "email": "bob@example.com"}
    INSERT users {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
    INSERT users name: "Diana", age: 28, active: true

  Query documents:
    FIND users WHERE age > 25
    FIND users WHERE name LIKE "%a%" ORDER age:DESC
    FIND users WHERE age BETWEEN 25 AND 35 SELECT name, age

  Search:
    SEARCH users "alice" WITH SCORE
    SEARCH users "bob" EXACT

  Update:
    UPDATE users WHERE name = "Alice" SET age = 26
    UPDATE users WHERE age < 30 INC age = 1

  Aggregations:
    COUNT users WHERE age > 25
    AVG users age
    GROUP users BY age COUNT

  Delete:
    DELETE users WHERE name = "Bob"

  Transaction (ACID):
    BEGIN mydb users
      INSERT users {"name": "Eve", "age": 22}
      UPDATE users WHERE name = "Alice" SET age = 27
      DELETE users WHERE name = "Bob"
    COMMIT

  View:
    VIEW CREATE active_users AS FIND users WHERE active = true
    VIEW SHOW
    active_users

  Export:
    EXPORT users TO "users.json"
    EXPORT users WHERE age > 25 TO "adults.json"

  System:
    STATUS
    STATS DB mydb
    COMPACT mydb


===============================================================================
  THE MOTTO: "Big Data, Zero Touch - Optimized for Large Documents"
===============================================================================
`
}
