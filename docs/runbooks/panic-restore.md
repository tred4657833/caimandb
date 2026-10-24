# Runbook: Panic Restore From Backup

Use this when data needs to come back *right now* -- a bad write,
accidental `DELETE ALL`, or corruption, and you need the database back
to a known-good state.

## Step 0: stop the bleeding first

If the bad write is still happening (a runaway job, a bug actively
corrupting data), stop the source before restoring -- otherwise you're
restoring into a database that's still being damaged.

## Step 1: figure out what "good" means -- latest, or a point in time?

- If the damage was caused by something that happened at a known
  moment (a bad deploy, an operator mistake at a specific time),
  you want **point-in-time restore**, to just before that moment.
- If you just need the most recent full+incremental backup state
  (e.g. recovering a node that lost its data entirely, not undoing a
  bad write), you want a **latest restore**.

## Step 2: check what backups actually exist

```sql
SHOW GRANTS FOR <yourself>   -- confirm you have admin on this db first
VERIFY BACKUP <db>
```

`VERIFY BACKUP` checksums and decrypts/decodes the backup chain
without restoring anything -- run this first so you're not discovering
a corrupt backup file mid-restore. `RESTORE` is gated to admin roles
(`ADMIN`/`ADMIN_GENERAL`) -- see `cmd_backup_jsonl.go`.

## Step 3: restore

Latest state:
```sql
RESTORE <db>
```

Point in time (Unix seconds or RFC3339 -- both are accepted):
```sql
RESTORE <db> AS OF 2026-08-20T14:30:00Z
```

Read the response carefully -- it reports how many documents were
replayed. Zero replayed for a point-in-time restore usually means your
`AS OF` timestamp predates the oldest full backup (`RestoreDB` returns
an explicit error for this case rather than silently restoring
nothing -- if you see that error, your usable history doesn't go back
that far).

## Important: what restore does and doesn't do

- **Restoring into a block that still has data doesn't wipe it
  first.** Documents are replayed through the normal insert path,
  which overwrites by ID -- if you need a guaranteed-clean state
  (not just "the backed-up documents are correct, but anything
  inserted since isn't touched"), `TRUNCATE` the block *before*
  restoring.
- **No delete tombstones.** If a document was deleted after being
  captured in a backup, restoring brings it back. If the "bad write"
  you're undoing was itself a bad *delete*, restore will actually fix
  it (the deleted doc comes back) -- but if you deliberately deleted
  something and are now restoring for an unrelated reason, that
  deleted document will reappear too. Check what you're restoring
  reintroduces, not just what it fixes.
- **Point-in-time restore filters per document, not per file.** An
  incremental backup taken after your `AS OF` timestamp can still
  contribute documents to the restore, if those specific documents
  hadn't changed since before your cutoff. This is correct behavior
  (see `docs/nql-reference.md`), just worth knowing so a restore
  including a "too-recent-looking" backup file isn't alarming on its
  own.

## Step 4: verify before declaring victory

```sql
COUNT <block>
FIND <block> WHERE <spot-check a few known documents>
```

Compare against what you expect. If this is a multi-node cluster,
confirm the restore propagated (it goes through the normal write path,
so it replicates via Raft like any other write) -- check
`caimandb_raft_replication_lag` settles back down across all nodes.

## Practice this before you need it

The single best thing you can do for this runbook is to have actually
run `RESTORE <db> AS OF <timestamp>` against a non-production backup
at least once, so step 3 isn't the first time you've typed that
command under pressure.
