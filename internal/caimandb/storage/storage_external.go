package storage

import (
	"bytes"
	"github.com/google/uuid"
	natomic "github.com/natefinch/atomic"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// newID generates a UUIDv4 ID for externally-stored blobs. Duplicated
// (rather than imported) from the root package's misc_utils.go to keep
// this package dependency-free.
func newID() string {
	return uuid.New().String()
}

type ExternalStorage interface {
	Save(data []byte) (string, error)
	Load(id string) ([]byte, error)
	Delete(id string) error
	Exists(id string) bool
	// List enumerates every blob currently on disk along with its last
	// modification time, for orphan garbage collection (a blob a
	// transaction saved during staging that never became referenced by
	// any committed document, e.g. because the process crashed between
	// the blob Save() and the transaction's commit -- see the GC sweep
	// in blob_gc.go). ModTime lets a sweep apply an age safety margin so
	// it never races a transaction that is still actively staging.
	List() ([]BlobInfo, error)
}

// BlobInfo describes one blob on disk, as returned by List().
type BlobInfo struct {
	ID      string
	ModTime time.Time
}

type FileSystemStorage struct {
	basePath string
	mu       sync.RWMutex
}

func NewFileSystemStorage(basePath string) (*FileSystemStorage, error) {
	if err := os.MkdirAll(basePath, 0750); err != nil {
		return nil, err
	}
	return &FileSystemStorage{basePath: basePath}, nil
}

func (fs *FileSystemStorage) Save(data []byte) (string, error) {
	id := newID()
	path := filepath.Join(fs.basePath, id[:2], id)
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return "", err
	}
	// natefinch/atomic writes to a temp file in the same directory and
	// renames it into place, so a crash or concurrent Load() mid-write
	// can never observe a partially-written blob at `path` -- os.WriteFile
	// offers no such guarantee (a truncate + partial write is visible to
	// readers the instant it starts).
	if err := natomic.WriteFile(path, bytes.NewReader(data)); err != nil {
		return "", err
	}
	return id, nil
}

func (fs *FileSystemStorage) Load(id string) ([]byte, error) {
	path := filepath.Join(fs.basePath, id[:2], id)
	return os.ReadFile(path)
}

func (fs *FileSystemStorage) Delete(id string) error {
	path := filepath.Join(fs.basePath, id[:2], id)
	return os.Remove(path)
}

func (fs *FileSystemStorage) Exists(id string) bool {
	path := filepath.Join(fs.basePath, id[:2], id)
	_, err := os.Stat(path)
	return err == nil
}

// List walks the sharded (2-char prefix directory) blob layout Save()
// writes into and returns every blob found. Best-effort: a shard
// directory that vanishes mid-walk (e.g. Delete() racing this) is
// treated as "no entries there", not an error, since List() is used by a
// background GC sweep that already tolerates the blob set changing
// concurrently.
func (fs *FileSystemStorage) List() ([]BlobInfo, error) {
	shards, err := os.ReadDir(fs.basePath)
	if err != nil {
		return nil, err
	}
	var blobs []BlobInfo
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(fs.basePath, shard.Name()))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				continue
			}
			blobs = append(blobs, BlobInfo{ID: entry.Name(), ModTime: info.ModTime()})
		}
	}
	return blobs, nil
}
