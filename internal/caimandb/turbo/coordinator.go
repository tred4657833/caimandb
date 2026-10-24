package turbo

import "time"

// Config controls the sizing of a Coordinator's pool and default batch
// thresholds. Zero values resolve to sensible defaults (see DefaultConfig).
type Config struct {
	MinWorkers int
	MaxWorkers int

	// BatchMaxSize/BatchMaxWait are the group-commit thresholds used in
	// normal operation.
	BatchMaxSize int
	BatchMaxWait time.Duration

	// BulkBatchMaxSize/BulkBatchMaxWait are used instead while BulkMode is
	// enabled: bigger batches, longer windows, for maximum throughput
	// during large loads where a little extra per-item latency doesn't
	// matter.
	BulkBatchMaxSize int
	BulkBatchMaxWait time.Duration
}

// DefaultConfig returns reasonable defaults: worker bounds derived from
// GOMAXPROCS (see NewAdaptiveWorkerPool), a batch window small enough to
// stay invisible to a single interactive client (a few milliseconds) but
// large enough to coalesce meaningfully under concurrent load, and a much
// larger/slower window for bulk loads.
func DefaultConfig() Config {
	return Config{
		BatchMaxSize:     200,
		BatchMaxWait:     3 * time.Millisecond,
		BulkBatchMaxSize: 2000,
		BulkBatchMaxWait: 20 * time.Millisecond,
	}
}

// Coordinator wires together an AdaptiveWorkerPool, a Scheduler on top of
// it, and a BulkMode flag -- the shared "turbo engine" infrastructure an
// Engine embeds and hands out to its batchers.
type Coordinator struct {
	Pool      *AdaptiveWorkerPool
	Scheduler *Scheduler
	Bulk      *BulkMode

	cfg Config
}

// New creates a Coordinator from cfg (see DefaultConfig for zero-value
// resolution of the worker bounds).
func New(cfg Config) *Coordinator {
	if cfg.BatchMaxSize <= 0 {
		cfg.BatchMaxSize = DefaultConfig().BatchMaxSize
	}
	if cfg.BatchMaxWait <= 0 {
		cfg.BatchMaxWait = DefaultConfig().BatchMaxWait
	}
	if cfg.BulkBatchMaxSize <= 0 {
		cfg.BulkBatchMaxSize = DefaultConfig().BulkBatchMaxSize
	}
	if cfg.BulkBatchMaxWait <= 0 {
		cfg.BulkBatchMaxWait = DefaultConfig().BulkBatchMaxWait
	}

	pool := NewAdaptiveWorkerPool(cfg.MinWorkers, cfg.MaxWorkers)
	return &Coordinator{
		Pool:      pool,
		Scheduler: NewScheduler(pool),
		Bulk:      &BulkMode{},
		cfg:       cfg,
	}
}

// BatchThresholds returns the (maxSize, maxWait) a batcher should currently
// use: the bulk thresholds while BulkMode is enabled, the normal ones
// otherwise. Callers typically feed this into BatcherGroup.SetThresholds
// whenever bulk mode is toggled.
func (c *Coordinator) BatchThresholds() (int, time.Duration) {
	if c.Bulk.Enabled() {
		return c.cfg.BulkBatchMaxSize, c.cfg.BulkBatchMaxWait
	}
	return c.cfg.BatchMaxSize, c.cfg.BatchMaxWait
}

// Stats returns a snapshot combining pool, scheduler, and bulk-mode state.
func (c *Coordinator) Stats() map[string]any {
	return map[string]any{
		"pool":      c.Pool.Stats(),
		"scheduler": c.Scheduler.Stats(),
		"bulk_mode": c.Bulk.Enabled(),
	}
}

// Close shuts down the scheduler and pool.
func (c *Coordinator) Close() {
	c.Scheduler.Close()
	c.Pool.Close()
}
