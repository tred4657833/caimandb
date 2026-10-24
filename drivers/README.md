# CaimanDB Drivers

Lightweight clients to connect to CaimanDB from different languages,
all without external dependencies (only each language's standard
library, except for PHP which requires the `curl` extension, very
common in any installation).

| Folder | Language | Requires |
|---|---|---|
| `js/` | JavaScript / Node.js | Node 18+ (uses native `fetch`) |
| `go/` | Go | Go 1.21+ |
| `python/` | Python | Python 3.8+ |
| `php/` | PHP | PHP 7.4+, ext-curl |
| `java/` | Java | Java 11+ |

## What they do

All five speak the same protocol — CaimanDB's protocol documented in
`docs/api/http-api.md` from the main repo — and expose the same API
shape in each language:

- `login(username, password)` — authenticates against the admin server and stores the JWT
- `query(nql, db)` — executes any raw NQL command (`FIND`, `INSERT`, `UPDATE`, `JOIN`, transactions, whatever)
- Convenience wrappers: `insert`, `get`, `find`, `search`, `update`, `delete`, `count`
- `health()`, `status()`
- `watch(...)` — subscribes to the real-time change stream (Server-Sent Events)

All support both Basic Auth (username/password) and JWT Bearer
token (via `login()` or passing the token directly).

## What they DON'T do (yet)

- They don't include a language-typed query builder — `WHERE`/`SET`/`ORDER`
  clauses are passed as raw NQL text. This is the simplest option and
  makes the fewest assumptions about the exact syntax your version of
  CaimanDB supports; building a DSL per language on top of this is a
  good next step if you need it.
- They don't handle connection pooling or retries — they're thin
  clients, intended as a starting point.

## Verification

This sandbox didn't have compilers for all languages available.
Actual status of each:

- **JavaScript**: syntax verified with `node --check`.
- **Python**: syntax and a smoke test (import + instantiation) verified with `python3`.
- **Java**: compiled and actually run (including assembling NQL commands
  for `find`/`insert`/`update` against a dead port, to confirm the logic
  reaches the network layer intact).
- **Go**: no Go toolchain available to compile — written carefully and
  reviewed by hand, but run `go build ./...` before trusting it and let
  me know if any error shows up.
- **PHP**: no PHP interpreter available — same case as Go, run
  `php -l` before trusting it.

If anything doesn't compile as-is, paste me the error and I'll fix it.
