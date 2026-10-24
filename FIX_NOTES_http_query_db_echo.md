# Fix: /query response echoed the wrong "db"

## File changed
`internal/caimandb/http_query.go` (`handleQuery`)

## Bug
The JSON response of every `/query` call includes a `"db"` field. It was
set from the `db` variable read from the request's `?db=` query
parameter (before execution), not from `sess.currentDB` after
`parseAndExec` ran. For almost every command these are the same value,
but `USE`/`CD` mutate `sess.currentDB` as their entire purpose -- and the
response kept echoing back the OLD, pre-statement db, silently hiding
the fact that the switch happened.

This matters because the Studio editor's "Run all" (`renderer/js/studio.js`,
`runActiveTab`) trusts `res.db` to track which database subsequent
statements in the same run should target, specifically so a `USE <db>`
mid-script can hand off context to everything after it. Since the field
never actually reflected a `USE`, the editor could only recover via a
secondary regex that pattern-matches `USE <db>` in the statement text
itself -- which is what actually saved most runs, but left the `db`
field itself lying about the session state to any other client that
reads it literally.

## Fix
Echo `sess.currentDB` (post-execution) instead of the pre-execution `db`
variable. No behavior change for the common case; `USE`/`CD` now report
their real resulting database.
