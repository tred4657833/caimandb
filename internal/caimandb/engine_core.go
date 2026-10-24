package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"go.uber.org/zap"
	"sync"
	"time"
)

type IntelligentEngine struct {
	mu           sync.RWMutex
	configs      map[string]*DBConfig
	stats        map[string]*DBStats
	queryHistory []*QueryRecord
	autoTune     bool
	autoScale    bool
	ticker       *time.Ticker
	scaleTicker  *time.Ticker
	stopChan     chan struct{}
	predictor    *PredictiveAnalyzer
	config       *Config
}

type DBConfig struct {
	Name        string   `json:"name"`
	ShardCount  int      `json:"shard_count"`
	Replicas    int      `json:"replicas"`
	CacheSizeMB int64    `json:"cache_size_mb"`
	Compression string   `json:"compression"`
	AutoTune    bool     `json:"auto_tune"`
	AutoScale   bool     `json:"auto_scale"`
	Indexes     []string `json:"indexes"`
	// NamedIndexes holds explicit CREATE INDEX / CREATE UNIQUE INDEX
	// definitions, keyed by index name (unique within the database).
	// The underlying secondary-index storage (index_secondary.go)
	// already indexes every simple-typed field on every document
	// unconditionally, so a named index doesn't need to trigger a
	// separate build pass -- it mainly (a) records the name/block/
	// uniqueness so SHOW INDEXES / DROP INDEX have something to look
	// up, (b) makes sure the field is also present in Indexes so the
	// query optimizer's index-selection heuristic considers it, and
	// (c) is consulted by checkUniqueConstraints (ops_insert.go) to
	// enforce UNIQUE on INSERT. See known-limitations note there for
	// what UNIQUE does NOT yet cover (UPDATE, bulk/turbo insert paths)
	// and for the WHERE clause, which is stored but not yet used to
	// restrict which documents are considered indexed (a "partial"
	// index today behaves like a full one).
	NamedIndexes map[string]NamedIndexDef `json:"named_indexes,omitempty"`
	Custom       map[string]any           `json:"custom"`
	CreatedAt    int64                    `json:"created_at"`
	UpdatedAt    int64                    `json:"updated_at"`
	Stats        *DBStats                 `json:"stats,omitempty"`
}

// NamedIndexDef is one CREATE INDEX / CREATE UNIQUE INDEX definition.
type NamedIndexDef struct {
	Name   string `json:"name"`
	Block  string `json:"block"`
	Field  string `json:"field"`
	Unique bool   `json:"unique"`
	// Where is the raw text of an optional partial-index WHERE clause.
	// NamedIndexes comment above for the current (unenforced) status.
	Where string `json:"where,omitempty"`
}

type DBStats struct {
	DocCount      int64     `json:"doc_count"`
	AvgDocSize    int64     `json:"avg_doc_size"`
	QueryCount    int64     `json:"query_count"`
	AvgQueryTime  float64   `json:"avg_query_time_ms"`
	IndexHitRate  float64   `json:"index_hit_rate"`
	CacheHitRate  float64   `json:"cache_hit_rate"`
	ShardSkew     float64   `json:"shard_skew"`
	GrowthRate    float64   `json:"growth_rate_per_sec"`
	LastOptimized time.Time `json:"last_optimized"`
	TotalShards   int       `json:"total_shards"`
	TotalNodes    int       `json:"total_nodes"`
}

type QueryRecord struct {
	DB        string
	Block     string
	Filters   []Filter
	Duration  time.Duration
	Timestamp time.Time
	IndexUsed string
	Scanned   int64
	Returned  int64
}

type PredictiveAnalyzer struct {
	mu              sync.RWMutex
	growthRates     map[string][]float64
	queryPatterns   map[string][]string
	predictions     map[string]Prediction
	windowSize      int
	predictionHoriz time.Duration
}

type Prediction struct {
	ExpectedGrowth    float64   `json:"expected_growth"`
	PeakTime          time.Time `json:"peak_time"`
	RecommendedShards int       `json:"recommended_shards"`
	RecommendedNodes  int       `json:"recommended_nodes"`
	Confidence        float64   `json:"confidence"`
	Trend             string    `json:"trend"`
}

func NewIntelligentEngine(config *Config) *IntelligentEngine {
	ie := &IntelligentEngine{
		configs:      make(map[string]*DBConfig),
		stats:        make(map[string]*DBStats),
		queryHistory: make([]*QueryRecord, 0, 200000),
		autoTune:     true,
		autoScale:    true,
		stopChan:     make(chan struct{}),
		predictor:    NewPredictiveAnalyzer(),
		config:       config,
	}

	ie.ticker = time.NewTicker(30 * time.Second)
	ie.scaleTicker = time.NewTicker(time.Duration(config.AutoScaleInterval) * time.Second)
	safeGoLoop("intel_auto_tune_loop", ie.autoTuneLoop)
	safeGoLoop("intel_auto_scale_loop", ie.autoScaleLoop)

	return ie
}

func NewPredictiveAnalyzer() *PredictiveAnalyzer {
	return &PredictiveAnalyzer{
		growthRates:     make(map[string][]float64),
		queryPatterns:   make(map[string][]string),
		predictions:     make(map[string]Prediction),
		windowSize:      1000,
		predictionHoriz: 72 * time.Hour,
	}
}

func (ie *IntelligentEngine) autoTuneLoop() {
	for {
		select {
		case <-ie.ticker.C:
			ie.runAutoTune()
		case <-ie.stopChan:
			return
		}
	}
}

func (ie *IntelligentEngine) autoScaleLoop() {
	for {
		select {
		case <-ie.scaleTicker.C:
			ie.runAutoScale()
		case <-ie.stopChan:
			return
		}
	}
}

func (ie *IntelligentEngine) runAutoTune() {
	if !ie.autoTune {
		return
	}

	ie.mu.RLock()
	configs := make([]*DBConfig, 0, len(ie.configs))
	for _, cfg := range ie.configs {
		if cfg.AutoTune {
			configs = append(configs, cfg)
		}
	}
	ie.mu.RUnlock()

	for _, cfg := range configs {
		ie.analyzeAndOptimize(cfg)
	}
}

func (ie *IntelligentEngine) runAutoScale() {
	if !ie.autoScale {
		return
	}

	ie.mu.RLock()
	configs := make([]*DBConfig, 0, len(ie.configs))
	for _, cfg := range ie.configs {
		if cfg.AutoScale {
			configs = append(configs, cfg)
		}
	}
	ie.mu.RUnlock()

	for _, cfg := range configs {
		ie.checkAndScale(cfg)
	}
}

func (ie *IntelligentEngine) analyzeAndOptimize(cfg *DBConfig) {
	stats, ok := ie.stats[cfg.Name]
	if !ok {
		return
	}

	if stats.ShardSkew > 0.25 {
		log().Info("Auto-tuning: high shard skew detected",
			zap.String("db", cfg.Name),
			zap.Float64("skew", stats.ShardSkew))
	}

	if stats.CacheHitRate < 0.5 && cfg.CacheSizeMB < 32768 {
		log().Info("Auto-tuning: low cache hit rate, increasing cache",
			zap.String("db", cfg.Name),
			zap.Float64("hit_rate", stats.CacheHitRate))
		cfg.CacheSizeMB = cfg.CacheSizeMB * 2
	}

	if stats.IndexHitRate < 0.3 {
		log().Info("Auto-tuning: low index hit rate, analyzing queries",
			zap.String("db", cfg.Name),
			zap.Float64("index_hit_rate", stats.IndexHitRate))
		ie.suggestIndexes(cfg)
	}

	cfg.UpdatedAt = time.Now().Unix()
}

func (ie *IntelligentEngine) checkAndScale(cfg *DBConfig) {
	stats, ok := ie.stats[cfg.Name]
	if !ok {
		return
	}

	if stats.DocCount > int64(cfg.ShardCount)*ie.config.MaxShardDocs {
		newShards := cfg.ShardCount * 2
		if newShards < cfg.ShardCount+1 {
			newShards = cfg.ShardCount + 1
		}
		log().Info("Auto-scaling: splitting shards",
			zap.String("db", cfg.Name),
			zap.Int("current_shards", cfg.ShardCount),
			zap.Int("new_shards", newShards),
			zap.Int64("doc_count", stats.DocCount))
		cfg.ShardCount = newShards
		cfg.UpdatedAt = time.Now().Unix()
		metrics.MetricAutoScaleEvents.Inc()
	}

	if stats.DocCount < int64(cfg.ShardCount)*ie.config.MinShardDocs &&
		cfg.ShardCount > 4 && ie.config.AutoMergeEnabled {
		newShards := cfg.ShardCount / 2
		if newShards < 4 {
			newShards = 4
		}
		log().Info("Auto-scaling: merging shards",
			zap.String("db", cfg.Name),
			zap.Int("current_shards", cfg.ShardCount),
			zap.Int("new_shards", newShards),
			zap.Int64("doc_count", stats.DocCount))
		cfg.ShardCount = newShards
		cfg.UpdatedAt = time.Now().Unix()
		metrics.MetricAutoScaleEvents.Inc()
	}

	if ie.config.PredictiveScaling {
		if pred, ok := ie.predictor.GetPrediction(cfg.Name); ok {
			if pred.ExpectedGrowth > 1.0 && pred.Confidence > 0.6 {
				recommended := pred.RecommendedShards
				if recommended > cfg.ShardCount {
					log().Info("Predictive scaling: increasing shards based on growth prediction",
						zap.String("db", cfg.Name),
						zap.Int("current", cfg.ShardCount),
						zap.Int("recommended", recommended),
						zap.Float64("confidence", pred.Confidence),
						zap.String("trend", pred.Trend))
					cfg.ShardCount = recommended
					cfg.UpdatedAt = time.Now().Unix()
					metrics.MetricAutoScaleEvents.Inc()
				}
			}
		}
	}

	if stats != nil {
		stats.TotalShards = cfg.ShardCount
	}
}

func (ie *IntelligentEngine) suggestIndexes(cfg *DBConfig) {
	fieldCount := make(map[string]int)
	total := 0

	ie.mu.RLock()
	history := ie.queryHistory
	ie.mu.RUnlock()

	for _, q := range history {
		if q.DB != cfg.Name {
			continue
		}
		for _, f := range q.Filters {
			if f.Op == "=" || f.Op == "==" || f.Op == "IN" {
				fieldCount[f.Field]++
				total++
			}
		}
	}

	if total < 100 {
		return
	}

	for field, count := range fieldCount {
		if float64(count)/float64(total) > 0.05 {
			if !ie.hasIndex(cfg, field) {
				log().Info("Auto-tuning: suggesting new index",
					zap.String("db", cfg.Name),
					zap.String("field", field),
					zap.Int("usage_count", count))
				ie.addIndex(cfg, field)
			}
		}
	}
}

func (ie *IntelligentEngine) hasIndex(cfg *DBConfig, field string) bool {
	for _, idx := range cfg.Indexes {
		if idx == field {
			return true
		}
	}
	return false
}

func (ie *IntelligentEngine) addIndex(cfg *DBConfig, field string) {
	cfg.Indexes = append(cfg.Indexes, field)
	cfg.UpdatedAt = time.Now().Unix()
}

func (ie *IntelligentEngine) GetConfig(dbName string) (*DBConfig, bool) {
	ie.mu.RLock()
	defer ie.mu.RUnlock()
	cfg, ok := ie.configs[dbName]
	return cfg, ok
}

func (ie *IntelligentEngine) SetConfig(cfg *DBConfig) {
	ie.mu.Lock()
	defer ie.mu.Unlock()
	cfg.UpdatedAt = time.Now().Unix()
	ie.configs[cfg.Name] = cfg
}

func (ie *IntelligentEngine) UpdateStats(dbName string, stats *DBStats) {
	ie.mu.Lock()
	defer ie.mu.Unlock()
	ie.stats[dbName] = stats
}

func (ie *IntelligentEngine) RecordQuery(record *QueryRecord) {
	ie.mu.Lock()
	defer ie.mu.Unlock()
	ie.queryHistory = append(ie.queryHistory, record)
	if len(ie.queryHistory) > 200000 {
		ie.queryHistory = ie.queryHistory[20000:]
	}
}

func (p *PredictiveAnalyzer) RecordGrowth(db string, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.growthRates[db]; !ok {
		p.growthRates[db] = make([]float64, 0, p.windowSize)
	}
	rates := p.growthRates[db]
	rates = append(rates, rate)
	if len(rates) > p.windowSize {
		rates = rates[1:]
	}
	p.growthRates[db] = rates
	p.updatePrediction(db)
}

func (p *PredictiveAnalyzer) RecordQuery(db, pattern string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.queryPatterns[db]; !ok {
		p.queryPatterns[db] = make([]string, 0, p.windowSize)
	}
	patterns := p.queryPatterns[db]
	patterns = append(patterns, pattern)
	if len(patterns) > p.windowSize {
		patterns = patterns[1:]
	}
	p.queryPatterns[db] = patterns
}

func (p *PredictiveAnalyzer) updatePrediction(db string) {
	rates := p.growthRates[db]
	if len(rates) < 20 {
		return
	}

	var sumX, sumY, sumXY, sumX2 float64
	n := float64(len(rates))
	for i, r := range rates {
		x := float64(i)
		sumX += x
		sumY += r
		sumXY += x * r
		sumX2 += x * x
	}
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)

	expGrowth := sumY > 0 && sumY/float64(len(rates)) > 1000

	pred := p.predictions[db]
	pred.ExpectedGrowth = slope
	pred.PeakTime = time.Now().Add(p.predictionHoriz)

	if slope > 0 {
		if expGrowth || slope > 1000000 {
			pred.RecommendedShards = int(float64(len(rates)) * slope * 2 / 1000000)
			pred.RecommendedNodes = int(float64(len(rates)) * slope / 10000000)
			pred.Trend = "exponential"
		} else {
			pred.RecommendedShards = int(float64(len(rates)) * slope / 1000000)
			pred.RecommendedNodes = int(float64(len(rates)) * slope / 5000000)
			pred.Trend = "linear"
		}
	}

	if pred.RecommendedShards < 4 {
		pred.RecommendedShards = 4
	}
	if pred.RecommendedNodes < 1 {
		pred.RecommendedNodes = 1
	}
	pred.Confidence = 1.0 - (1.0 / float64(len(rates)))
	p.predictions[db] = pred
}

func (p *PredictiveAnalyzer) GetPrediction(db string) (Prediction, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pred, ok := p.predictions[db]
	return pred, ok
}

func (ie *IntelligentEngine) Stop() {
	close(ie.stopChan)
	if ie.ticker != nil {
		ie.ticker.Stop()
	}
	if ie.scaleTicker != nil {
		ie.scaleTicker.Stop()
	}
}
