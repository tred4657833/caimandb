package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"go.uber.org/zap"
	"time"
)

func (e *Engine) Compact(dbName string) (int, error) {
	start := time.Now()

	if !e.dirMgr.DBExists(dbName) {
		return 0, ErrDBNotFound
	}

	if e.isRenaming(dbName) {
		log().Debug("Compact skipped: database rename in progress", zap.String("db", dbName))
		return 0, nil
	}

	db, err := e.pool.OpenDataPath(e.dirMgr.DBPath(dbName))
	if err != nil {
		return 0, err
	}

	rounds := 0
	for {
		err := db.RunValueLogGC(0.5)
		if err != nil {
			break
		}
		rounds++
	}

	log().Info("Compact completed",
		zap.String("db", dbName),
		zap.Int("rounds", rounds),
		zap.Duration("duration", time.Since(start)))

	metrics.MetricCompactions.Inc()
	return rounds, nil
}

// CompactBlock reclaims disk space for a single block's BadgerDB instance
// (value-log garbage collection), the same operation Compact performs at
// whole-database granularity. It exists so the autonomous maintenance
// sweep (see maintenance.go) can compact individual cold blocks without
// touching every other block in the database, keeping the CPU/IO cost of
// a sweep proportional to how much is actually cold.
func (e *Engine) CompactBlock(dbName, block string) (int, error) {
	start := time.Now()

	if e.isDBOrBlockRenaming(dbName, block) {
		log().Debug("CompactBlock skipped: rename in progress",
			zap.String("db", dbName), zap.String("block", block))
		return 0, nil
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	rounds := 0
	for {
		if err := db.RunValueLogGC(0.5); err != nil {
			break
		}
		rounds++
	}

	log().Debug("Block compact completed",
		zap.String("db", dbName),
		zap.String("block", block),
		zap.Int("rounds", rounds),
		zap.Duration("duration", time.Since(start)))

	if rounds > 0 {
		metrics.MetricCompactions.Inc()
	}
	return rounds, nil
}
