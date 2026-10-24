package caimandb

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type QueryOptimizer struct {
	mu              sync.RWMutex
	indexStats      map[string]*IndexStats
	tableStats      map[string]*TableStats
	lastAnalyze     time.Time
	analyzeInterval time.Duration
}

type IndexStats struct {
	Field       string
	Size        int64
	Selectivity float64
	LastUsed    time.Time
	UseCount    int64
}

type TableStats struct {
	TotalDocs     int64
	AvgDocSize    int64
	IndexedFields []string
}

func NewQueryOptimizer() *QueryOptimizer {
	return &QueryOptimizer{
		indexStats:      make(map[string]*IndexStats),
		tableStats:      make(map[string]*TableStats),
		analyzeInterval: 5 * time.Minute,
	}
}

func (qo *QueryOptimizer) UpdateIndexStats(field string, size int64, selectivity float64) {
	qo.mu.Lock()
	defer qo.mu.Unlock()
	if stat, ok := qo.indexStats[field]; ok {
		stat.Size = size
		stat.Selectivity = selectivity
		stat.LastUsed = time.Now()
		stat.UseCount++
	} else {
		qo.indexStats[field] = &IndexStats{
			Field:       field,
			Size:        size,
			Selectivity: selectivity,
			LastUsed:    time.Now(),
			UseCount:    1,
		}
	}
}

// IndexCandidate is one equality/IN filter AnalyzeQuery considers usable
// as a secondary-index lookup.
type IndexCandidate struct {
	Field string
	Value string
}

type QueryPlan struct {
	UseIndex   bool
	IndexField string
	IndexValue string
	// Candidates holds every equality/IN filter this plan considers
	// index-eligible, ordered best (most selective known field) first.
	// IndexField/IndexValue above always mirror Candidates[0], kept for
	// callers written against the original single-field plan (Count,
	// EXPLAIN). Callers that can act on more than one indexed field
	// (Find) should walk Candidates and intersect the resulting doc-ID
	// sets instead of only ever probing a single field.
	Candidates    []IndexCandidate
	ScanAll       bool
	EstimatedCost int64
}

// AnalyzeQuery decides which equality/IN filters, if any, are worth
// probing against the secondary index instead of a full block scan.
//
// Previously this only offered a field up as UseIndex when indexStats
// already held a measured Selectivity < 0.3 for it -- but nothing in the
// engine ever called UpdateIndexStats for a real document field (the only
// call site fed it the constant, meaningless pair ("_id", 0, 0) on every
// single-document insert). In practice that meant indexStats stayed empty
// for every field a query might actually filter on, `ok` was always
// false, bestIndex stayed nil, and FIND fell back to ScanAll=true no
// matter how many documents a secondary index could have ruled out --
// even though buildSecondaryIndexInto (index_secondary.go) had already
// been indexing essentially every field on every insert the whole time.
// The index existed on disk; the planner just never knew to ask for it.
//
// Since shouldIndexField indexes any field by default (skipping only
// oversized values), any equality/IN filter is a reasonable index
// candidate whether or not we have measured stats for it yet: the
// lookup itself (lookupByIndex) is cheap to try and always falls back to
// a full scan safely if it comes back empty, so offering a candidate up
// costs nothing when the guess is wrong and saves a full scan when it's
// right. Measured selectivity (populated opportunistically by
// lookupByIndex itself -- see index_secondary.go) is used only to choose
// the best order among several candidates, never to gate whether
// indexing is attempted at all.
func (qo *QueryOptimizer) AnalyzeQuery(filters []Filter) QueryPlan {
	qo.mu.RLock()
	defer qo.mu.RUnlock()

	plan := QueryPlan{ScanAll: true}

	type candidate struct {
		field       string
		value       string
		selectivity float64
		known       bool
	}
	var cands []candidate

	for _, f := range filters {
		if f.Op != "=" && f.Op != "==" && f.Op != "IN" {
			continue
		}
		c := candidate{field: f.Field, value: fmt.Sprint(f.Value), selectivity: 1.0}
		if stats, ok := qo.indexStats[f.Field]; ok {
			c.selectivity = stats.Selectivity
			c.known = true
		}
		cands = append(cands, c)
	}

	if len(cands) == 0 {
		return plan
	}

	// Fields with real measured selectivity sort first (most selective,
	// i.e. smallest fraction of the block, at the very front); fields
	// never measured yet still get offered, just after anything with
	// actual evidence behind it.
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].known != cands[j].known {
			return cands[i].known
		}
		return cands[i].selectivity < cands[j].selectivity
	})

	for _, c := range cands {
		plan.Candidates = append(plan.Candidates, IndexCandidate{Field: c.field, Value: c.value})
	}

	best := cands[0]
	plan.UseIndex = true
	plan.ScanAll = false
	plan.IndexField = best.field
	plan.IndexValue = best.value
	if stats, ok := qo.indexStats[best.field]; ok {
		plan.EstimatedCost = int64(float64(stats.Size) * stats.Selectivity)
	}

	return plan
}
