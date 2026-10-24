package caimandb

import (
	"caimandb/internal/caimandb/storage"
	"time"

	"go.uber.org/zap"
)

// maintenanceLoop runs the autonomous, self-healing background sweep: for
// every block in every database it (a) verifies integrity and, if
// configured, auto-repairs any corruption found (see block_repair.go —
// RepairBlock now actually removes and logs corrupted documents instead
// of silently leaving them in place), and (b) reclaims disk space from
// blocks that haven't been written to in a while.
//
// This is deliberately a single pass per block rather than two separate
// full scans: it reuses the storage.BlockMeta already read by CheckBlock to
// decide coldness, and it sleeps MaintenanceBlockPause between every
// block regardless of what it did, so a sweep's CPU/IO footprint stays
// low and proportional, never a burst that competes with live queries.
// waitForBulkModeClear blocks the maintenance sweep while the engine is in
// BULK MODE (see Engine.SetBulkMode / turbo.BulkMode). A bulk load already
// widens batches and relaxes WAL fsync to maximize disk throughput; letting
// a maintenance sweep's CheckBlock run at the same time on the very block
// being bulk-loaded defeats that entirely by putting sweep I/O back on the
// hot path. It polls rather than blocking forever so it still notices
// shutdown promptly.
func (e *Engine) waitForBulkModeClear() {
	if e.turbo == nil {
		return
	}
	for e.turbo.Bulk.Enabled() {
		select {
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// RunMaintenanceNow triggers one maintenance sweep immediately, blocking
// until it completes. Intended for manual/administrative use (e.g. an
// admin command or HTTP endpoint) on top of the automatic periodic sweep
// started by maintenanceLoop; both paths share the same throttled
// implementation so a manual run has the same bounded CPU/IO footprint.
func (e *Engine) RunMaintenanceNow() {
	cfg := e.config
	if cfg == nil {
		return
	}
	e.runMaintenanceSweep(cfg)
}

func (e *Engine) maintenanceLoop() {
	cfg := e.config
	if cfg == nil || !cfg.MaintenanceEnabled {
		return
	}

	interval := cfg.MaintenanceInterval
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run one pass shortly after startup, not immediately: give the
	// engine time to finish WAL recovery and settle so the very first
	// sweep doesn't compete with recovery I/O, but still surface any
	// pre-existing corruption soon rather than waiting a full interval
	// (which defaults to hours).
	initial := time.NewTimer(2 * time.Minute)
	defer initial.Stop()

	for {
		select {
		case <-initial.C:
			e.runMaintenanceSweep(cfg)
		case <-ticker.C:
			e.runMaintenanceSweep(cfg)
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) runMaintenanceSweep(cfg *Config) {
	pause := cfg.MaintenanceBlockPause
	if pause <= 0 {
		pause = 200 * time.Millisecond
	}
	coldAfter := cfg.MaintenanceColdAfter
	if coldAfter <= 0 {
		coldAfter = 24 * time.Hour
	}

	e.waitForBulkModeClear()

	dbs, err := e.dirMgr.ListDatabases()
	if err != nil {
		log().Warn("Maintenance sweep: failed to list databases", zap.Error(err))
		return
	}

	start := time.Now()
	var scanned, corruptBlocks, repaired, repairFailed, compacted int

	for _, dbName := range dbs {
		if e.isRenaming(dbName) {
			log().Debug("Maintenance sweep: skipping database, rename in progress",
				zap.String("db", dbName))
			continue
		}

		blocks, err := e.dirMgr.ListBlocks(dbName)
		if err != nil {
			log().Warn("Maintenance sweep: failed to list blocks",
				zap.String("db", dbName), zap.Error(err))
			continue
		}

		for _, block := range blocks {
			select {
			case <-e.stopChan:
				return
			case <-e.ctx.Done():
				return
			default:
			}

			if e.isDBOrBlockRenaming(dbName, block) {
				log().Debug("Maintenance sweep: skipping block, rename in progress",
					zap.String("db", dbName), zap.String("block", block))
				continue
			}

			if e.turbo != nil && e.turbo.Bulk.Enabled() {
				log().Info("Maintenance sweep: pausing mid-sweep, bulk mode active",
					zap.String("db", dbName), zap.String("block", block))
				e.waitForBulkModeClear()
			}

			scanned++
			result, err := e.CheckBlock(dbName, block)
			if err != nil {
				log().Warn("Maintenance sweep: integrity check failed",
					zap.String("db", dbName), zap.String("block", block), zap.Error(err))
				time.Sleep(pause)
				continue
			}

			if corrupted, _ := result["corrupted"].(int64); corrupted > 0 {
				corruptBlocks++
				log().Warn("Maintenance sweep: corruption detected",
					zap.String("db", dbName), zap.String("block", block),
					zap.Int64("corrupted_docs", corrupted))

				if cfg.MaintenanceAutoRepair {
					if err := e.RepairBlock(dbName, block); err != nil {
						repairFailed++
						log().Error("Maintenance sweep: auto-repair failed",
							zap.String("db", dbName), zap.String("block", block), zap.Error(err))
					} else {
						repaired++
						log().Info("Maintenance sweep: auto-repaired block",
							zap.String("db", dbName), zap.String("block", block))
					}
				}
			}

			if cfg.MaintenanceAutoCompact {
				if meta, ok := result["meta"].(storage.BlockMeta); ok && meta.UpdatedAt > 0 {
					age := time.Since(time.Unix(0, meta.UpdatedAt))
					if age >= coldAfter {
						if rounds, err := e.CompactBlock(dbName, block); err != nil {
							log().Warn("Maintenance sweep: cold-block compact failed",
								zap.String("db", dbName), zap.String("block", block), zap.Error(err))
						} else if rounds > 0 {
							compacted++
							log().Info("Maintenance sweep: compacted cold block",
								zap.String("db", dbName), zap.String("block", block),
								zap.Duration("age", age), zap.Int("gc_rounds", rounds))
						}
					}
				}
			}

			// Throttle unconditionally: this must stay a background,
			// low-priority job, not a burst of CPU/IO that competes with
			// live traffic on every block regardless of whether anything
			// needed doing above.
			time.Sleep(pause)
		}
	}

	log().Info("Maintenance sweep completed",
		zap.Int("blocks_scanned", scanned),
		zap.Int("blocks_with_corruption", corruptBlocks),
		zap.Int("blocks_repaired", repaired),
		zap.Int("blocks_repair_failed", repairFailed),
		zap.Int("blocks_compacted", compacted),
		zap.Duration("duration", time.Since(start)))
}
