# HTTP API

CaimanDB exposes two independent HTTP servers, each on its own
configurable port.

## Admin Server (`internal/caimandb/http_admin.go`)

Default port: `admin_port` (`CAIMANDB_ADMIN_PORT`, 1556 if not
configured).

| Endpoint | Description |
|---|---|
| `GET /api/v1/health` | Health check |
| `GET /api/v1/status` | Engine status |
| `GET /api/v1/dbs` | List databases |
| `/api/v1/db/...` | Operations on a specific database |
| `POST /api/v1/query` | Execute an NQL query |
| `POST /api/v1/backup` | Backup a database |
| `POST /api/v1/restore` | Restore a database |
| `POST /api/v1/compact` | Compaction |
| `/api/v1/users` | User management |
| `POST /api/v1/auth/login` | Login (returns JWT) |
| `POST /api/v1/auth/logout` | Logout |
| `/api/v1/token` | Token management |
| `GET /api/v1/metrics` | Prometheus metrics (`promhttp`) |

## Query Server (`internal/caimandb/http_query.go`)

Default port: `query_port` (`CAIMANDB_QUERY_PORT`, 1555 if not
configured).

| Endpoint | Description |
|---|---|
| `POST /query` | Execute an NQL query. For `FIND`/`GET`, the JSON response additionally includes `rows`/`columns` with the already structured documents (not just the result text) |
| `GET /status` | Engine status |
| `GET /health` | Health check |
| `GET /entities?db=` | Actual databases and blocks (with document count and size) for the indicated database — used by the `ui/` sidebar |
| `GET /watch?db=&block=` | Real-time change stream (Server-Sent Events). `db` and `block` are optional filters; with none, streams all changes from the node. Example: `curl -N "http://host:port/watch?db=mydb&block=users"`. The connection remains open until the client closes it; a `change` event (JSON with `op`/`db`/`block`/`id`/`data`/`timestamp`) is sent for each insert/update/delete, and a `: heartbeat` comment every 15s. A slow subscriber never blocks writes: if its buffer fills up, its events are dropped (counter `caimandb_change_events_dropped_total`). |

## Authentication

Both servers support JWT (`auth_jwt.go`) and Basic Auth as an
alternative. Default credentials are configured with
`query_user` / `query_password` (or `CAIMANDB_QUERY_USER`) and the JWT
secret with `jwt_secret` (or `CAIMANDB_JWT_SECRET`).
