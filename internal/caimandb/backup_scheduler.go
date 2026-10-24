// backup_scheduler.go runs the automatic backup sweep: on
// BackupInterval, every database gets an incremental backup (or a full
// one, the first time, or every BackupFullEvery-th run) via BackupDB
// (ops_backup_jsonl.go). This is what makes backups "automatic" per the
// requirement that the database keep its own compressed/encrypted
// backups without an operator having to remember to run BACKUP --
// separate from the admin-gated manual BACKUP/RESTORE/VERIFY BACKUP
// commands (cmd_backup_jsonl.go), which stay admin-only because they're
// a person acting through a session, not the engine acting on its own.
package caimandb

import (
	"time"

	"go.uber.org/zap"
)

// RunBackupSweepNow triggers one backup pass immediately for every
// database, blocking until it completes. Intended for manual/
// administrative use on top of the automatic periodic sweep started by
// backupSchedulerLoop; both share this same implementation.
func (e *Engine) RunBackupSweepNow() {
	cfg := e.config
	if cfg == nil {
		return
	}
	e.runBackupSweep(cfg, true)
}

func (e *Engine) backupSchedulerLoop() {
	cfg := e.config
	if cfg == nil || !cfg.BackupAutoEnabled {
		return
	}

	interval := cfg.BackupInterval
	if interval <= 0 {
		interval = 4 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Like maintenanceLoop (maintenance.go), wait a bit after startup
	// before the first automatic backup: let WAL recovery and the
	// initial maintenance sweep finish first, so the very first backup
	// isn't competing with recovery I/O or reading documents mid-repair.
	initial := time.NewTimer(5 * time.Minute)
	defer initial.Stop()

	var runCount int
	for {
		select {
		case <-initial.C:
			runCount++
			e.runBackupSweep(cfg, runCount%cfg.effectiveBackupFullEvery() == 0)
		case <-ticker.C:
			runCount++
			e.runBackupSweep(cfg, runCount%cfg.effectiveBackupFullEvery() == 0)
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		}
	}
}

// effectiveBackupFullEvery guards against a zero/negative configured
// value causing a divide-by-zero in the modulo above.
func (c *Config) effectiveBackupFullEvery() int {
	if c.BackupFullEvery <= 0 {
		return 6
	}
	return c.BackupFullEvery
}

func (e *Engine) runBackupSweep(cfg *Config, forceFull bool) {
	dbs, err := e.dirMgr.ListDatabases()
	if err != nil {
		log().Warn("Backup sweep: failed to list databases", zap.Error(err))
		return
	}

	for _, db := range dbs {
		select {
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		default:
		}

		path, err := e.BackupDB(db, forceFull)
		if err != nil {
			log().Warn("Automatic backup failed",
				zap.String("db", db), zap.Bool("forced_full", forceFull), zap.Error(err))
			continue
		}
		if path == "" {
			continue // no changes since the last backup; nothing to log
		}
		log().Info("Automatic backup completed",
			zap.String("db", db), zap.String("file", path), zap.Bool("full", forceFull))

		// Throttle between databases for the same reason
		// runMaintenanceSweep (maintenance.go) pauses between blocks:
		// this is a background sweep, not a burst that should compete
		// with live traffic for disk/CPU.
		pause := cfg.MaintenanceBlockPause
		if pause <= 0 {
			pause = 200 * time.Millisecond
		}
		select {
		case <-time.After(pause):
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		}
	}
}
