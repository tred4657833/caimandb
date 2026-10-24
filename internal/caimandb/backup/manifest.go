package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Entry records one backup file: a full snapshot or one incremental
// delta on top of the most recent full snapshot.
type Entry struct {
	File     string `json:"file"`     // filename only, relative to its full/ or incr/ subdirectory
	Full     bool   `json:"full"`     // true for a full snapshot, false for an incremental delta
	At       int64  `json:"at"`       // unix seconds when this backup finished
	Cutoff   int64  `json:"cutoff"`   // the doc.Meta.UpdatedAt cutoff this backup captured up to (see Manifest.NextCutoff)
	Docs     int64  `json:"docs"`     // number of document lines written
	Checksum string `json:"checksum"` // SHA-256 of the encrypted file, for VerifyBackup
}

// Manifest is one database's backup history: caimandb keeps exactly one
// of these per database, at <backupRoot>/arw/<db>/manifest.json.
// backupRoot is the single top-level backups directory
// (storage.DirectoryManager.BackupRoot(), a sibling of the data
// directory, not nested inside it) -- "arw" is a subdirectory within
// it so this system's files never collide with EXPORT/legacy BACKUP's
// files, which live directly under backupRoot (see
// storage.DirectoryManager.BackupsPath()). Restoring a database
// replays the most recent Full entry followed by every Incremental
// entry recorded after it, in order -- see ops_backup_jsonl.go's
// RestoreDB.
type Manifest struct {
	DB           string  `json:"db"`
	Full         *Entry  `json:"full,omitempty"`
	Incrementals []Entry `json:"incrementals,omitempty"`
}

// ManifestPath returns where db's manifest lives under backupRoot.
func ManifestPath(backupRoot, db string) string {
	return filepath.Join(backupRoot, "arw", db, "manifest.json")
}

// FullDir/IncrDir are where db's full and incremental backup files
// themselves live (the manifest only records filenames, not full paths,
// precisely so a whole <backupRoot>/arw/<db>/ directory can be
// moved/renamed without editing it).
func FullDir(backupRoot, db string) string { return filepath.Join(backupRoot, "arw", db, "full") }
func IncrDir(backupRoot, db string) string { return filepath.Join(backupRoot, "arw", db, "incr") }

// LoadManifest reads db's manifest, returning an empty (zero-Full,
// zero-Incrementals) Manifest -- not an error -- if none exists yet,
// since "no backup has ever been taken" is the normal starting state for
// every database, not a failure.
func LoadManifest(backupRoot, db string) (*Manifest, error) {
	data, err := os.ReadFile(ManifestPath(backupRoot, db))
	if os.IsNotExist(err) {
		return &Manifest{DB: db}, nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest for %s is corrupted: %w", db, err)
	}
	return &m, nil
}

// Save persists m, creating <backupRoot>/arw/<db>/ if needed.
func (m *Manifest) Save(backupRoot string) error {
	dir := filepath.Dir(ManifestPath(backupRoot, m.DB))
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ManifestPath(backupRoot, m.DB), data, 0640)
}

// NextCutoff returns the doc.Meta.UpdatedAt value the next incremental
// backup should use as its lower bound (documents with UpdatedAt strictly
// less than this were already captured by an earlier backup): the Cutoff
// of the most recent entry (whichever of Full/last-Incremental is more
// recent), or 0 if no backup has ever run, meaning "capture everything".
func (m *Manifest) NextCutoff() int64 {
	if len(m.Incrementals) > 0 {
		return m.Incrementals[len(m.Incrementals)-1].Cutoff
	}
	if m.Full != nil {
		return m.Full.Cutoff
	}
	return 0
}

// AllEntriesInOrder returns the full snapshot (if any) followed by every
// incremental, in the order RestoreDB must replay them.
func (m *Manifest) AllEntriesInOrder() []Entry {
	var out []Entry
	if m.Full != nil {
		out = append(out, *m.Full)
	}
	out = append(out, m.Incrementals...)
	return out
}
