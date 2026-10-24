# Fix: UPDATE/DELETE/UPSERT "matched nothing" on the id-equality fast path

## File changed
`internal/caimandb/ops_local.go`

## Bug
`updateLocal` and `deleteLocal` have a fast path (`singleIDEquality`) that
triggers whenever a WHERE clause reduces to exactly `id = "<literal>"`
(this is also what `UPSERT`'s internal `eng.Update(...)` call uses). That
fast path does a direct `txn.Get(docKey(block, id))` — which already
conclusively proves the document exists at that key — but then it STILL
called `matchesWhere(doc, where, filters)` again before applying the
change.

The problem: that re-check evaluates the filter `id = "<literal>"`
against `doc.Data["id"]`, a normal, mutable data field that gets mirrored
onto every document at insert time (see `ops_insert.go`), NOT against
`doc.ID`/`_id`, the actual storage key that was just matched. If that
data field is ever missing, stale, renamed away (`RENAME FIELD`),
dropped (`DROP FIELD`), or simply wasn't set on an older document, the
fast path silently reports "0 rows matched" for a document that
unambiguously exists at that exact key.

This is exactly what produced:

    UPSERT animals newborn001 {...}
    ERROR: UPSERT found document newborn001 but the update matched nothing

`FindByID` (used by UPSERT to decide insert-vs-update) resolves by the
real key and succeeded; the subsequent `eng.Update(...)` call re-derives
the same id, takes the same fast path, and failed the second check.

## Fix
Added an `idMatched bool` parameter to `updateOneItem` and
`deleteCandidateFromItem`. The two fast-path call sites (one in
`updateLocal`, one in `deleteLocal`) now pass `true`, skipping the
redundant/unsafe `matchesWhere` re-check since identity is already
proven by the key lookup. The full block-scan call sites pass `false`
and behave exactly as before (they still need the real check, since
there they haven't matched anything about this specific document yet).

No other callers of either function exist in the codebase.

## What to verify once you can build
- `UPSERT <block> <id> SET ...` / `UPSERT <block> <id> <json>` against an
  existing document that has no `"id"` data field (or a stale one) now
  succeeds instead of returning "found ... but matched nothing".
- `UPDATE <block> WHERE _id = "<id>" SET ...` and
  `DELETE <block> WHERE _id = "<id>"` still behave correctly for normal
  documents (regression check — the fast path should still update/delete
  exactly one document, never zero when the key exists, never more than
  one).
- `UPDATE ALL` / any WHERE with more than one condition (which never hits
  the fast path) is unaffected — still goes through the real
  `matchesWhere` check.
