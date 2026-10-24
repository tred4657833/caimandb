// DBPool manages a bounded set of open Badger handles, one per block.
package storage

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"caimandb/internal/caimandb/logging"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DBPool struct {
	mu     sync.Mutex
	dbs    map[string]*badger.DB
	cache  any // *cache.L1Cache; kept as `any` to avoid an import cycle (unused internally)
	l2     any // *cache.L2IndexCache; kept as `any` to avoid an import cycle (unused internally)
	dirMgr *DirectoryManager
	closed bool

	// Storage AI: adaptive per-block sizing (see adaptive.go). adaptive.Enabled
	// == false reproduces the original fixed-size behavior exactly.
	adaptive    AdaptiveConfig
	budgetTotal int64
	budgetUsed  int64
	tierOf      map[string]SizeTier

	// bulkHint mirrors the engine's turbo.BulkMode flag (see SetBulkModeHint).
	// It only ever influences the ONE-TIME tier decision a fresh/empty block
	// gets in pickTierLocked -- it deliberately never touches an already-open
	// *badger.DB. Hot-swapping a live block's tuning options mid-flight
	// would need to coordinate with every in-flight reader/writer of that
	// block, which is real, unverified-here concurrency work (see the
	// package doc comment in adaptive.go) -- not something to bolt on
	// blind. This is the safe subset: reuse a signal the caller already
	// gives us (BULK MODE ON, or IMPORT's automatic bulk mode) to make a
	// smarter choice at the one moment a tier decision is naturally already
	// being made and already serialized behind p.mu.
	bulkHint atomic.Bool
}

// SetBulkModeHint mirrors the engine-level BULK MODE flag into the pool so
// pickTierLocked can use it. Call this from the same place that flips
// turbo.BulkMode (Engine.SetBulkMode) -- it's purely advisory and safe to
// call at any time; it never touches a block that's already open.
func (p *DBPool) SetBulkModeHint(on bool) {
	p.bulkHint.Store(on)
}

// NewDBPool constructs a pool with Storage AI enabled and a default RAM
// budget (50% of detected system memory). Use NewDBPoolWithAdaptive to
// customize or disable adaptive sizing.
func NewDBPool(cache any, l2 any, dirMgr *DirectoryManager) *DBPool {
	return NewDBPoolWithAdaptive(cache, l2, dirMgr, AdaptiveConfig{Enabled: true})
}

func NewDBPoolWithAdaptive(cache any, l2 any, dirMgr *DirectoryManager, adaptive AdaptiveConfig) *DBPool {
	p := &DBPool{
		dbs:      make(map[string]*badger.DB),
		cache:    cache,
		l2:       l2,
		dirMgr:   dirMgr,
		adaptive: adaptive,
		tierOf:   make(map[string]SizeTier),
	}
	if adaptive.Enabled {
		p.budgetTotal = adaptive.budgetBytes()
	}
	return p
}

func (p *DBPool) OpenDataPath(path string) (*badger.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("db pool is closed")
	}

	if db, ok := p.dbs[path]; ok {
		return db, nil
	}

	if err := os.MkdirAll(path, 0750); err != nil {
		return nil, err
	}

	tier, profile := p.pickTierLocked(path)

	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		WithCompression(2).
		WithValueLogFileSize(profile.ValueLogFileSize).
		WithMemTableSize(profile.MemTableSize).
		WithNumMemtables(profile.NumMemtables).
		WithNumLevelZeroTables(profile.NumLevelZero).
		WithBlockCacheSize(profile.BlockCacheSize).
		WithNumCompactors(profile.NumCompactors).
		WithNumVersionsToKeep(1).
		WithSyncWrites(false).
		WithDetectConflicts(true).
		WithValueThreshold(badgerValueThreshold)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening badger at %s: %w", path, err)
	}

	p.dbs[path] = db
	logging.Log().Info("BadgerDB opened",
		zap.String("path", path),
		zap.String("storage_tier", tier.String()))
	return db, nil
}

// pickTierLocked chooses a Badger tuning tier for path. Must be called with
// p.mu already held (it mutates budget/tier bookkeeping). When Storage AI is
// disabled, it always returns TierStandard -- byte-for-byte what this pool
// used before adaptive sizing existed.
func (p *DBPool) pickTierLocked(path string) (SizeTier, tierProfile) {
	if !p.adaptive.Enabled {
		return TierStandard, tierProfiles[TierStandard]
	}

	requested := classifyByOnDiskBytes(onDiskFootprintBytes(path))

	// A brand-new/near-empty block normally starts at TierMicro purely
	// because it has ~0 bytes on disk yet -- exactly right for "lots of
	// small blocks", exactly wrong for "about to receive a huge bulk
	// load into a fresh block", which looks identical from on-disk bytes
	// alone. BULK MODE (or an automatic-bulk-mode IMPORT) is the caller
	// telling us which situation this actually is, so use it to raise the
	// floor -- still just a smarter starting *guess*, still subject to
	// the RAM budget check right below like any other tier request.
	//
	// The floor used to be TierStandard, but TierStandard is also just
	// "the normal tier a block eventually earns on its own once it's big
	// enough" -- for a sustained bulk load (hundreds of thousands to
	// millions of docs written back-to-back into ONE block, one open
	// *badger.DB handle for the whole run) that's too small: its L0
	// table count (8) and memtable budget (128MB x 4) get saturated
	// early into the run, and every write after that has to wait on
	// LSM compaction to keep up (Badger's write-stall behavior when L0
	// is full). That's what produces a throughput curve that decays
	// over the course of a single large GENERATE/IMPORT (measured:
	// ~2700 docs/s in the first minutes down to ~1000 docs/s by the
	// end of a 1M-doc run) even though every batch is the same size and
	// shape -- it's not the insert path getting slower, it's LSM
	// compaction falling behind a fixed tier's headroom as the block
	// grows. TierLarge (256MB x 6 memtables, 12 L0 tables, 4
	// compactors) gives a bulk load much more headroom before it hits
	// that wall, so raise the bulk floor there instead. Still subject
	// to the RAM budget check right below like any other tier request,
	// so a memory-constrained deployment still safely falls back to a
	// smaller tier via downgradeToFit instead of over-committing.
	if p.bulkHint.Load() && requested < TierLarge {
		requested = TierLarge
	}

	remaining := p.budgetTotal - p.budgetUsed
	tier := requested
	if p.budgetTotal > 0 {
		tier = downgradeToFit(requested, remaining)
	}

	profile := tierProfiles[tier]
	p.tierOf[path] = tier
	p.budgetUsed += profile.approxFootprintBytes()
	return tier, profile
}

// releaseBudgetLocked returns the budget previously charged to path (if
// any) back to the pool. Must be called with p.mu held. Safe to call for a
// path that was never adaptively opened (no-op).
func (p *DBPool) releaseBudgetLocked(path string) {
	tier, ok := p.tierOf[path]
	if !ok {
		return
	}
	p.budgetUsed -= tierProfiles[tier].approxFootprintBytes()
	if p.budgetUsed < 0 {
		p.budgetUsed = 0
	}
	delete(p.tierOf, path)
}

// AdaptiveStats reports current Storage AI budget usage, for observability
// (e.g. an admin/metrics endpoint). Safe to call on a pool with Storage AI
// disabled -- returns zero values.
type AdaptiveStats struct {
	Enabled     bool
	BudgetTotal int64
	BudgetUsed  int64
	TierCounts  map[string]int
}

func (p *DBPool) AdaptiveStats() AdaptiveStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	counts := make(map[string]int, 4)
	for _, tier := range p.tierOf {
		counts[tier.String()]++
	}
	return AdaptiveStats{
		Enabled:     p.adaptive.Enabled,
		BudgetTotal: p.budgetTotal,
		BudgetUsed:  p.budgetUsed,
		TierCounts:  counts,
	}
}

func (p *DBPool) OpenBlock(dbName, blockName string) (*badger.DB, error) {
	path := p.dirMgr.BlockDataPath(dbName, blockName)
	return p.OpenDataPath(path)
}

func (p *DBPool) OpenUserDB() (*badger.DB, error) {
	path := filepath.Join(p.dirMgr.dataRoot, "__users")
	return p.OpenDataPath(path)
}

func (p *DBPool) OpenSystemDB() (*badger.DB, error) {
	path := filepath.Join(p.dirMgr.dataRoot, "__system")
	return p.OpenDataPath(path)
}

func (p *DBPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for path, db := range p.dbs {
		if err := db.Close(); err != nil {
			logging.Log().Error("Failed to close BadgerDB", zap.String("path", path), zap.Error(err))
		}
		p.releaseBudgetLocked(path)
	}
	p.dbs = make(map[string]*badger.DB)
}

// SyncAll fuerza un fsync de todas las bases Badger actualmente abiertas
// en el pool (checkpoint). Devuelve cuántas se sincronizaron
// correctamente; si alguna falla, se sigue con el resto y se devuelve el
// último error encontrado, para que un solo handle problemático no deje
// sin sincronizar al resto.
func (p *DBPool) SyncAll() (int, error) {
	p.mu.Lock()
	dbs := make(map[string]*badger.DB, len(p.dbs))
	for path, db := range p.dbs {
		dbs[path] = db
	}
	p.mu.Unlock()

	synced := 0
	var lastErr error
	for path, db := range dbs {
		if err := db.Sync(); err != nil {
			logging.Log().Warn("Checkpoint: failed to sync BadgerDB", zap.String("path", path), zap.Error(err))
			lastErr = err
			continue
		}
		synced++
	}
	return synced, lastErr
}

func (p *DBPool) CloseAndRemove(path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if db, ok := p.dbs[path]; ok {
		if err := db.Close(); err != nil {
			return err
		}
		delete(p.dbs, path)
		p.releaseBudgetLocked(path)
	}
	return nil
}

func (p *DBPool) CloseAndRemoveForce(path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if db, ok := p.dbs[path]; ok {
		done := make(chan struct{})
		logging.SafeGo("badger_close_force", func() {
			defer close(done)
			if err := db.Close(); err != nil {
				logging.Log().Error("Failed to close DB in force mode",
					zap.String("path", path),
					zap.Error(err))
			}
		})

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			logging.Log().Warn("DB close timeout, forcing removal",
				zap.String("path", path))
		}

		delete(p.dbs, path)
		p.releaseBudgetLocked(path)
	}

	lockFile := filepath.Join(path, "LOCK")
	if _, err := os.Stat(lockFile); err == nil {
		if err := os.Remove(lockFile); err != nil {
			logging.Log().Warn("Failed to remove LOCK file",
				zap.String("file", lockFile),
				zap.Error(err))
		}
	}

	files, _ := filepath.Glob(filepath.Join(path, "*.mem"))
	for _, f := range files {
		os.Remove(f)
	}

	return nil
}

// CloseAndRemoveForceByPrefix force-closes every open Badger handle whose
// path is pathPrefix itself or lives underneath it (e.g. a block's
// __data and __index Badger instances when pathPrefix is the block's
// root, or every block's handles when pathPrefix is a database root).
// It never returns an error -- individual close failures are logged and
// the handle is dropped from the pool regardless, because the caller's
// goal is "make sure nothing still has these files open before the OS
// rename," not "guarantee a clean Badger shutdown." It returns the list
// of paths that were closed, purely for logging.
func (p *DBPool) CloseAndRemoveForceByPrefix(pathPrefix string) []string {
	p.mu.Lock()

	sep := string(os.PathSeparator)
	normPrefix := pathPrefix
	if !strings.HasSuffix(normPrefix, sep) {
		normPrefix += sep
	}

	var toClose []string
	for path := range p.dbs {
		if path == pathPrefix || strings.HasPrefix(path, normPrefix) {
			toClose = append(toClose, path)
		}
	}

	type closeJob struct {
		path string
		db   *badger.DB
	}
	var jobs []closeJob
	for _, path := range toClose {
		jobs = append(jobs, closeJob{path: path, db: p.dbs[path]})
		delete(p.dbs, path)
		p.releaseBudgetLocked(path)
	}
	p.mu.Unlock()

	for _, j := range jobs {
		done := make(chan struct{})
		dbCopy, pathCopy := j.db, j.path
		logging.SafeGo("badger_close_force_by_prefix", func() {
			defer close(done)
			if err := dbCopy.Close(); err != nil {
				logging.Log().Error("Failed to close BadgerDB in force-by-prefix mode",
					zap.String("path", pathCopy),
					zap.Error(err))
			}
		})

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			logging.Log().Warn("DB close timeout during force-by-prefix, proceeding anyway",
				zap.String("path", j.path))
		}

		lockFile := filepath.Join(j.path, "LOCK")
		if _, err := os.Stat(lockFile); err == nil {
			if err := os.Remove(lockFile); err != nil {
				logging.Log().Warn("Failed to remove LOCK file",
					zap.String("file", lockFile),
					zap.Error(err))
			}
		}

		memFiles, _ := filepath.Glob(filepath.Join(j.path, "*.mem"))
		for _, f := range memFiles {
			os.Remove(f)
		}
	}

	return toClose
}

func (p *DBPool) forceClose(path string) error {
	if db, ok := p.dbs[path]; ok {
		db.Close()
		if runtime.GOOS == "windows" {
			time.Sleep(500 * time.Millisecond)
			runtime.GC()
		}
	}
	return nil
}

func (p *DBPool) close(path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if db, ok := p.dbs[path]; ok {
		if err := db.Close(); err != nil {
			return err
		}
		delete(p.dbs, path)
		p.releaseBudgetLocked(path)
	}
	return nil
}

func (p *DBPool) get(path string) (*badger.DB, bool) {
	p.mu.Lock()
	db, ok := p.dbs[path]
	p.mu.Unlock()
	return db, ok
}
