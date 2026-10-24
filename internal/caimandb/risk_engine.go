package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"fmt"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

type RiskEngine struct {
	mu      sync.RWMutex
	rules   []RiskRule
	blocked map[string]time.Time
	config  RiskConfig
}

type RiskRule struct {
	Name     string
	Pattern  string
	Action   string // "block", "warn", "log"
	Severity int
}

type RiskConfig struct {
	Enabled            bool
	BlockOnSuspicious  bool
	AlertOnAnomaly     bool
	MaxAnomalyScore    float64
	EvaluationInterval time.Duration
}

func NewRiskEngine() *RiskEngine {
	return &RiskEngine{
		rules: []RiskRule{
			{Name: "drop_database", Pattern: `(?i)drop\s+db`, Action: "block", Severity: 10},
			{Name: "delete_all", Pattern: `(?i)delete\s+all`, Action: "warn", Severity: 8},
			{Name: "mass_update", Pattern: `(?i)update\s+.*\s+where\s+`, Action: "warn", Severity: 6},
			{Name: "drop_block", Pattern: `(?i)drop\s+block`, Action: "warn", Severity: 7},
			{Name: "clear_block", Pattern: `(?i)empty\s+block`, Action: "warn", Severity: 5},
		},
		blocked: make(map[string]time.Time),
		config: RiskConfig{
			Enabled:            true,
			BlockOnSuspicious:  true,
			AlertOnAnomaly:     true,
			MaxAnomalyScore:    70.0,
			EvaluationInterval: 1 * time.Minute,
		},
	}
}

func (re *RiskEngine) Evaluate(query string, user string) (bool, string, error) {
	if !re.config.Enabled {
		return false, "", nil
	}

	re.mu.Lock()
	defer re.mu.Unlock()

	if blockedUntil, ok := re.blocked[user]; ok {
		if time.Now().Before(blockedUntil) {
			return true, "user temporarily blocked due to suspicious activity", ErrRiskBlocked
		} else {
			delete(re.blocked, user)
		}
	}

	score := 0.0
	var reasons []string

	for _, rule := range re.rules {
		if strings.Contains(strings.ToLower(query), strings.ToLower(rule.Pattern)) {
			score += float64(rule.Severity)
			reasons = append(reasons, rule.Name)
		}
	}

	if score >= re.config.MaxAnomalyScore {
		if re.config.BlockOnSuspicious {
			re.blocked[user] = time.Now().Add(5 * time.Minute)
			metrics.MetricRiskBlocks.Inc()
			return true, fmt.Sprintf("risk score %.2f triggered by: %v", score, reasons), ErrRiskBlocked
		}
	}

	if score > 30 && re.config.AlertOnAnomaly {
		log().Warn("Risk engine detected anomalous operation",
			zap.String("user", user),
			zap.Float64("score", score),
			zap.Strings("reasons", reasons))
	}

	return false, "", nil
}
