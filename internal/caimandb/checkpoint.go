// checkpoint.go implementa checkpoints: periódicamente (o a pedido, vía
// el comando CHECKPOINT) se fuerza un fsync de todas las bases Badger
// abiertas y se sincroniza y rota el WAL de aplicación a un segmento
// nuevo. El punto en el que ocurre el checkpoint es, a partir de ese
// momento, la base desde la que arranca un REDO (RecoverWAL, en
// wal_recovery.go) tras un fallo -- así la recuperación solo tiene que
// reproducir lo escrito después del último checkpoint, no todo el
// historial desde el arranque del proceso.
package caimandb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/natefinch/atomic"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// CheckpointStats resume lo que hizo un checkpoint, para el comando
// CHECKPOINT y para el log del checkpoint periódico.
type CheckpointStats struct {
	ID         string        `json:"id"`
	SyncedDBs  int           `json:"synced_dbs"`
	Duration   time.Duration `json:"duration_ms"`
	Rotated    bool          `json:"wal_rotated"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
}

// checkpointMetaPath is where the most recent checkpoint's metadata is
// persisted, atomically -- so an admin (or a future startup check) can
// see when/whether the last checkpoint actually completed without
// depending on log retention.
func (e *Engine) checkpointMetaPath() string {
	return filepath.Join(e.config.DataRoot, ".checkpoint")
}

// Checkpoint fuerza un punto de recuperación seguro: sincroniza a disco
// todas las bases Badger abiertas y luego sincroniza y rota el WAL a un
// segmento nuevo (los datos que el WAL protegía ya quedaron durmiendo en
// disco vía el fsync anterior, así que ese WAL ya no hace falta para
// recuperarlos). Es la misma operación que corre periódicamente en
// segundo plano (ver checkpointLoop) y que dispara el comando manual
// CHECKPOINT.
func (e *Engine) Checkpoint() (CheckpointStats, error) {
	stats := CheckpointStats{ID: uuid.New().String(), StartedAt: time.Now()}

	synced, syncErr := e.pool.SyncAll()
	stats.SyncedDBs = synced
	if syncErr != nil {
		log().Warn("Checkpoint: not all databases synced cleanly", zap.Error(syncErr))
	}

	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return stats, fmt.Errorf("checkpoint: WAL sync failed: %w", err)
		}
		if err := e.wal.RotateAndPruneFresh(); err != nil {
			return stats, fmt.Errorf("checkpoint: WAL rotation failed: %w", err)
		}
		stats.Rotated = true
	}

	stats.FinishedAt = time.Now()
	stats.Duration = stats.FinishedAt.Sub(stats.StartedAt)

	// Best-effort: a failure to persist the metadata marker doesn't
	// invalidate the checkpoint itself (the WAL/Badger state is already
	// durable at this point), so this only logs, never returns an error.
	if data, err := json.Marshal(stats); err == nil {
		if werr := atomic.WriteFile(e.checkpointMetaPath(), bytes.NewReader(data)); werr != nil {
			log().Warn("Failed to persist checkpoint metadata", zap.Error(werr))
		}
	}

	log().Info("Checkpoint completed",
		zap.String("id", stats.ID),
		zap.Int("synced_dbs", stats.SyncedDBs),
		zap.Bool("wal_rotated", stats.Rotated),
		zap.Duration("took", stats.Duration))

	return stats, nil
}

// checkpointLoop dispara Checkpoint() periódicamente en segundo plano,
// con el mismo patrón (ticker + parada limpia por stopChan/ctx) que
// maintenanceLoop.
func (e *Engine) checkpointLoop() {
	cfg := e.config
	if cfg == nil || !cfg.CheckpointEnabled {
		return
	}

	interval := cfg.CheckpointInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := e.Checkpoint(); err != nil {
				log().Warn("Periodic checkpoint failed", zap.Error(err))
			}
		case <-e.stopChan:
			return
		case <-e.ctx.Done():
			return
		}
	}
}
