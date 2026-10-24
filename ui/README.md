# CaimanDB Studio (ui/)

Web administration console for CaimanDB, served directly by the node
itself (`internal/caimandb/http_query.go` serves this directory at
`/` when `config.UIDir` points here — it does by default).

It's pure HTML/CSS/JS: no build step, no npm, no framework. The only
external dependencies are CDNs (Tabler Icons, Google Fonts,
CodeMirror and Chart.js), loaded from `index.html`.

## What each file does

- `index.html` — app structure: connection screen, icon rail
  (Query / Dashboard), entity sidebar, query editor with tabs,
  results panel and dashboard panel.
- `style.css` — dark theme. All visual variables live in `:root`.
- `app.js` — all the logic. **There are no example data nor
  `Math.random` anywhere**: every number, row or chart comes from a
  real HTTP request against the node:
  - `GET /entities?db=` — real databases and blocks, with document
    count, for the sidebar and the "Documents by block" chart.
  - `POST /query` — executes real NQL. In addition to the result
    text (which any command produces), for `FIND`/`GET` it also
    returns structured `rows`/`columns` — a real grid, not a parsed
    ASCII table.
  - `GET /status` — real process metrics (databases, shards,
    operations, L1 cache, query metrics). The "Dashboard" panel polls
    this endpoint every 5s and builds its own ops/s and latency
    history from the real deltas between samples — it doesn't
    simulate time series.
  - `GET /watch` — Server-Sent Events with real changes
    (insert/update/delete) from the node, shown in "Live events".

## Authentication

HTTP Basic Auth with the credentials entered in the connection
form — the same user/role created with `CREATE USER` inside
CaimanDB. The password only lives in the script's memory (a JS
variable); it's never written to localStorage, sessionStorage or
cookies, and disappears on reload or disconnect.

## Local development

No build required. Just start the node (`make run` or
`./bin/caimandb`) and open `http://localhost:<query_port>/` — the
query server itself serves this directory at the root, same origin as
the API, with no CORS issues.
