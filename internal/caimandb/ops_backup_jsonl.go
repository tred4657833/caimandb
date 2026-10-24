// ops_backup_jsonl.go implements CaimanDB's encrypted, compressed,
// incremental backup system: JSONL documents, gzip-compressed, then
// AES-256-GCM encrypted (see internal/caimandb/backup), stored under
// <backupRoot>/arw/<db>/{full,incr}/ with a manifest.json tracking
// which files exist and what each one covers. backupRoot
// (storage.DirectoryManager.BackupRoot()) is a single top-level
// backups directory that's a sibling of the data directory, not
// nested inside it, and is shared with EXPORT/legacy BACKUP's files
// (see BackupsPath()) -- there's exactly one backups directory on
// disk, not two.
//
// Backup/Restore in ops_backup.go (Badger's native binary snapshot
// format, uncompressed unless ".gz", unencrypted) -- that command still
// works exactly as before; this one is reachable via BACKUP/RESTORE/
// VERIFY BACKUP in cmd_backup_jsonl.go, which is admin-gated (see the
// package doc comment there for why).
//
// Known limitation, stated plainly: this format has no delete tombstones
// -- it only ever records "this document existed with this data as of
// this timestamp". A document deleted from the live database after being
// captured in a full or incremental backup is NOT removed from a
// subsequent restore or from automatic post-corruption recovery
// (block_repair.go); it comes back. Treat a restore as "recover what
// existed," not "reproduce the exact live state," until this format
// grows tombstones.
package caimandb

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"

	"caimandb/internal/caimandb/backup"
)

// backupLine is one JSONL record inside a backup file. A database backup
// spans every block, all in the same file, so each line records which
// block the document came from.
type backupLine struct {
	Block string   `json:"block"`
	Doc   Document `json:"doc"`
}

// BackupDB writes an encrypted, gzip-compressed JSONL backup of dbName.
// If force is true, or dbName has never been backed up before, a FULL
// snapshot of every document in every block is written and the manifest
// is reset (a new full backup makes every prior incremental irrelevant,
// since the full backup already contains everything they captured).
// Otherwise an INCREMENTAL is written: only documents whose
// Meta.UpdatedAt is at or after the manifest's current cutoff (see
// backup.Manifest.NextCutoff) -- so an established database that gets a
// small trickle of new/changed documents keeps producing small backup
// files instead of a full copy every time.
//
// Returns ("", nil) with no file written when an incremental finds
// nothing new, so a quiet database doesn't accumulate empty backup files
// on every scheduler tick (see backup_scheduler.go).
func (e *Engine) BackupDB(dbName string, force bool) (string, error) {
	if !e.dirMgr.DBExists(dbName) {
		return "", ErrDBNotFound
	}

	backupRoot := e.dirMgr.BackupRoot()
	manifest, err := backup.LoadManifest(backupRoot, dbName)
	if err != nil {
		return "", err
	}

	full := force || manifest.Full == nil
	var cutoff int64
	if !full {
		cutoff = manifest.NextCutoff()
	}
	scanStarted := time.Now().Unix()

	blocks, err := e.dirMgr.ListBlocks(dbName)
	if err != nil {
		return "", err
	}

	// JSONL -> gzip -> a plain temp file first, so only the (much
	// smaller) compressed bytes are ever held anywhere other than disk;
	// encryption happens as a second streaming pass over that temp file
	// into the real destination. Bounded memory regardless of how big
	// the database is.
	tmp, err := os.CreateTemp("", "caimandb-backup-*.jsonl.gz")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	gz := gzip.NewWriter(tmp)
	enc := json.NewEncoder(gz)
	var docCount int64

	for _, block := range blocks {
		db, err := e.pool.OpenBlock(dbName, block)
		if err != nil {
			gz.Close()
			tmp.Close()
			return "", fmt.Errorf("opening block %s: %w", block, err)
		}
		prefix := docKeyPrefix(block)
		scanErr := txHelper.SafeView(db, func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				if item == nil {
					continue
				}
				var doc Document
				if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
					// Already corrupted; CheckBlock/RepairBlock is the
					// place that surfaces and fixes this, not backup.
					log().Warn("Skipping corrupted document during backup",
						zap.String("db", dbName), zap.String("block", block), zap.Error(err))
					continue
				}
				if !full && doc.Meta.UpdatedAt < cutoff {
					continue
				}
				if err := enc.Encode(backupLine{Block: block, Doc: doc}); err != nil {
					return err
				}
				docCount++
			}
			return nil
		})
		if scanErr != nil {
			gz.Close()
			tmp.Close()
			return "", scanErr
		}
	}

	if err := gz.Close(); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}

	if !full && docCount == 0 {
		return "", nil
	}

	key, err := backup.LoadOrCreateKey(backupRoot)
	if err != nil {
		return "", err
	}

	destDir := backup.IncrDir(backupRoot, dbName)
	fileName := fmt.Sprintf("incr_%d.jsonl.gz.enc", scanStarted)
	if full {
		destDir = backup.FullDir(backupRoot, dbName)
		fileName = fmt.Sprintf("full_%d.jsonl.gz.enc", scanStarted)
	}
	if err := os.MkdirAll(destDir, 0750); err != nil {
		return "", err
	}
	destPath := filepath.Join(destDir, fileName)

	if err := encryptFile(destPath, tmpPath, key); err != nil {
		os.Remove(destPath)
		return "", err
	}

	checksum, err := backup.FileChecksum(destPath)
	if err != nil {
		return "", err
	}

	entry := backup.Entry{
		File:     fileName,
		Full:     full,
		At:       time.Now().Unix(),
		Cutoff:   scanStarted,
		Docs:     docCount,
		Checksum: checksum,
	}
	if full {
		manifest.Full = &entry
		manifest.Incrementals = nil
	} else {
		manifest.Incrementals = append(manifest.Incrementals, entry)
	}
	if err := manifest.Save(backupRoot); err != nil {
		return "", err
	}

	log().Info("Backup completed",
		zap.String("db", dbName), zap.Bool("full", full),
		zap.String("file", fileName), zap.Int64("docs", docCount))

	return destPath, nil
}

func encryptFile(destPath, srcPath string, key []byte) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(destPath)
	if err != nil {
		return err
	}
	if err := backup.EncryptStream(dst, src, key); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}

// RestoreDB restores dbName from its encrypted JSONL backup history: the
// most recent full snapshot, then every incremental recorded since, in
// order (backup.Manifest.AllEntriesInOrder). The database (and any block
// a restored document belongs to) is created if it doesn't already
// exist. Documents are restored through the normal insert path
// (insertWithID); restoring into a block that still holds the original
// documents follows INSERT's own ID-collision behavior rather than
// silently overwriting -- TRUNCATE the block first (see cmd_ddl_extra.go)
// for a guaranteed-clean restore.
//
// asOf implements point-in-time restore: 0 means "latest" (every backed
// up document, unfiltered -- the only behavior this function had before
// PITR). A positive asOf is a Unix-seconds cutoff: only documents whose
// Meta.UpdatedAt is at or before asOf are replayed, so the trailing
// portion of the backup history newer than asOf is skipped document by
// document rather than file by file. That per-document (not per-file)
// filtering matters because one incremental's JSONL file spans a range
// of UpdatedAt values -- an incremental taken at T+2h can still contain
// a document last updated at T+5min, which a valid "restore to T+1h"
// must include even though the file it lives in overall postdates
// asOf. Entries (full or incremental) are still replayed in
// chronological order, so for any document ID with multiple versions
// at or before asOf, the last one applied (via insertWithID's
// overwrite-on-Set behavior in insertWithIDLocal) is the most recent
// one as of asOf -- exactly the state a point-in-time restore should
// reproduce. This does NOT reconstruct documents that were deleted
// before asOf and then backed up again under the same ID later (no
// tombstones -- see this file's package doc comment); it only bounds
// which document *versions* are eligible, same known limitation as an
// unbounded restore already has.
func (e *Engine) RestoreDB(dbName string, asOf int64) (int64, error) {
	backupRoot := e.dirMgr.BackupRoot()
	manifest, err := backup.LoadManifest(backupRoot, dbName)
	if err != nil {
		return 0, err
	}
	entries := manifest.AllEntriesInOrder()
	if len(entries) == 0 {
		return 0, fmt.Errorf("no backup found for database %s", dbName)
	}
	if asOf > 0 && len(entries) > 0 && asOf < entries[0].At-1 {
		// entries[0] is always the full snapshot (AllEntriesInOrder
		// puts it first when present). If asOf predates even the full
		// snapshot's own completion time by more than a second (a
		// small allowance since a full snapshot's own documents can
		// have UpdatedAt equal to or just before the moment the scan
		// started), there is no state to restore to: every document
		// this database has ever backed up was captured at or after
		// that full snapshot, so nothing could have existed "as of"
		// an earlier point. Surface that plainly instead of silently
		// restoring zero documents, which would look like an empty
		// backup rather than an out-of-range request.
		return 0, fmt.Errorf("asOf %d predates the oldest backup for %s (full snapshot at %d)", asOf, dbName, entries[0].At)
	}

	if !e.dirMgr.DBExists(dbName) {
		if err := e.CreateDB(dbName); err != nil {
			return 0, err
		}
	}

	key, err := backup.LoadOrCreateKey(backupRoot)
	if err != nil {
		return 0, err
	}

	var restored int64
	for _, entry := range entries {
		path := entryPath(backupRoot, dbName, entry)
		var n int64
		err := scanBackupFile(path, key, func(line backupLine) {
			if asOf > 0 && line.Doc.Meta.UpdatedAt > asOf {
				return
			}
			if !e.dirMgr.BlockExists(dbName, line.Block) {
				if err := e.CreateBlock(dbName, line.Block); err != nil {
					log().Warn("Failed to create block while restoring",
						zap.String("db", dbName), zap.String("block", line.Block), zap.Error(err))
					return
				}
			}
			if _, err := e.insertWithID(dbName, line.Block, line.Doc.ID, line.Doc.Data); err != nil {
				log().Warn("Failed to restore document",
					zap.String("db", dbName), zap.String("block", line.Block),
					zap.String("id", line.Doc.ID), zap.Error(err))
				return
			}
			n++
		})
		restored += n
		if err != nil {
			return restored, fmt.Errorf("replaying %s: %w", entry.File, err)
		}
	}

	if asOf > 0 {
		log().Info("Point-in-time restore completed",
			zap.String("db", dbName), zap.Int64("asOf", asOf), zap.Int64("docs", restored))
	} else {
		log().Info("Restore completed", zap.String("db", dbName), zap.Int64("docs", restored))
	}
	return restored, nil
}

// VerifyBackup checks every backup file recorded in dbName's manifest,
// without writing anything back to the database: it recomputes each
// file's SHA-256 and compares it to the checksum recorded at backup time
// (catches on-disk bit-rot or truncation even if the file would still
// decrypt), then decrypts, decompresses, and JSON-decodes every line
// (catches a wrong/rotated key, tampering, or a corrupted compressed
// stream that a checksum mismatch alone wouldn't explain). Safe to run
// anytime, including unattended on a schedule alongside BackupDB.
func (e *Engine) VerifyBackup(dbName string) (map[string]any, error) {
	backupRoot := e.dirMgr.BackupRoot()
	manifest, err := backup.LoadManifest(backupRoot, dbName)
	if err != nil {
		return nil, err
	}
	entries := manifest.AllEntriesInOrder()

	key, err := backup.LoadOrCreateKey(backupRoot)
	if err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0, len(entries))
	ok := true
	for _, entry := range entries {
		path := entryPath(backupRoot, dbName, entry)
		res := map[string]any{"file": entry.File, "full": entry.Full, "docs_recorded": entry.Docs}

		sum, err := backup.FileChecksum(path)
		if err != nil {
			res["status"] = "MISSING"
			res["error"] = err.Error()
			ok = false
			results = append(results, res)
			continue
		}
		if sum != entry.Checksum {
			res["status"] = "CHECKSUM_MISMATCH"
			ok = false
			results = append(results, res)
			continue
		}

		var docs int64
		decErr := scanBackupFile(path, key, func(backupLine) { docs++ })
		if decErr != nil {
			res["status"] = "DECRYPT_FAILED"
			res["error"] = decErr.Error()
			ok = false
			results = append(results, res)
			continue
		}
		res["docs_verified"] = docs
		if docs != entry.Docs {
			res["status"] = "DOC_COUNT_MISMATCH"
			ok = false
		} else {
			res["status"] = "OK"
		}
		results = append(results, res)
	}

	return map[string]any{"db": dbName, "ok": ok, "entries": results}, nil
}

// restoreDocsFromBackup looks up specific document IDs within one block
// across dbName's whole backup history (full snapshot, then every
// incremental, in order -- so a later incremental's version of a
// document wins over an earlier one, matching normal restore semantics)
// and re-inserts whichever of them are found. Used by RepairBlock
// (block_repair.go) to automatically pull back documents that had to be
// quarantined because they were corrupted on disk -- "the database asks
// for the backup data back" when it notices loss/corruption, rather than
// requiring an operator to run RESTORE manually.
func (e *Engine) restoreDocsFromBackup(dbName, block string, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	backupRoot := e.dirMgr.BackupRoot()
	manifest, err := backup.LoadManifest(backupRoot, dbName)
	if err != nil {
		return 0, err
	}
	entries := manifest.AllEntriesInOrder()
	if len(entries) == 0 {
		return 0, fmt.Errorf("no backup available for %s", dbName)
	}
	key, err := backup.LoadOrCreateKey(backupRoot)
	if err != nil {
		return 0, err
	}

	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	found := make(map[string]map[string]any)

	for _, entry := range entries {
		path := entryPath(backupRoot, dbName, entry)
		if err := scanBackupFile(path, key, func(line backupLine) {
			if line.Block == block && want[line.Doc.ID] {
				found[line.Doc.ID] = line.Doc.Data
			}
		}); err != nil {
			return 0, fmt.Errorf("scanning %s: %w", entry.File, err)
		}
	}

	var restored int64
	for id, data := range found {
		if _, err := e.insertWithID(dbName, block, id, data); err != nil {
			log().Warn("Failed to auto-restore document from backup",
				zap.String("db", dbName), zap.String("block", block), zap.String("id", id), zap.Error(err))
			continue
		}
		restored++
	}
	return restored, nil
}

func entryPath(backupRoot, dbName string, entry backup.Entry) string {
	dir := backup.IncrDir(backupRoot, dbName)
	if entry.Full {
		dir = backup.FullDir(backupRoot, dbName)
	}
	return filepath.Join(dir, entry.File)
}

// scanBackupFile decrypts, decompresses, and JSONL-decodes one backup
// file, calling fn once per document line. Decryption runs in a
// goroutine feeding an io.Pipe so gzip and JSON decoding can consume it
// as a stream rather than requiring the whole decrypted file in memory
// at once -- the same "never materialize the whole backup in RAM"
// discipline BackupDB's write side follows.
func scanBackupFile(path string, key []byte, fn func(backupLine)) error {
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()

	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(backup.DecryptStream(pw, src, key))
	}()

	gz, err := gzip.NewReader(pr)
	if err != nil {
		return err
	}
	defer gz.Close()

	dec := json.NewDecoder(gz)
	for dec.More() {
		var line backupLine
		if err := dec.Decode(&line); err != nil {
			return err
		}
		fn(line)
	}
	return nil
}
