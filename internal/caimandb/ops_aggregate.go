package caimandb

import (
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"math"
	"sort"
	"strings"

	"caimandb/internal/caimandb/parse"
)

// Aggregate and Group both take `where` alongside the historical `filters`
// so callers built from parseWhereClause (parentheses/NOT/precedence, see
// docs/known-limitations.md) get correct matching instead of the flat
// left-to-right evaluation -- matchesWhere below handles the "tree if
// present, else flat" fallback, matching Delete/Count's existing pattern
// (query_filter.go). Both are read-only (no WAL/Raft replication), so
// unlike DELETE this migration didn't need any wire-format changes.
func (e *Engine) Aggregate(dbName, block, op, field string, where *parse.Expr, filters []Filter) (float64, error) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return 0, ErrBlockNotFound
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	prefix := docKeyPrefix(block)
	var (
		sum   float64
		count int64
		min_v = math.MaxFloat64
		max_v = -math.MaxFloat64
		// MEDIAN and STDDEV can't be folded into a running total like
		// SUM/AVG/MIN/MAX -- both need the whole distribution (STDDEV
		// needs every deviation from the mean; MEDIAN needs the sorted
		// middle value(s)) -- so every value is kept here too. Only
		// populated for the ops that actually need it would save
		// memory, but the field is scalar per document (already
		// converted with getNestedF64 below) and block sizes here are
		// modest, so keeping it unconditionally keeps this function
		// simple with one collection pass.
		values []float64
	)

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				continue
			}
			if !matchesWhere(doc, where, filters) {
				continue
			}
			v := getNestedF64(doc.Data, field)
			sum += v
			count++
			values = append(values, v)
			if v < min_v {
				min_v = v
			}
			if v > max_v {
				max_v = v
			}
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	switch strings.ToUpper(op) {
	case "SUM":
		return sum, nil
	case "AVG":
		if count == 0 {
			return 0, nil
		}
		return sum / float64(count), nil
	case "MIN":
		if count == 0 {
			return 0, nil
		}
		return min_v, nil
	case "MAX":
		if count == 0 {
			return 0, nil
		}
		return max_v, nil
	case "COUNT":
		return float64(count), nil
	case "MEDIAN":
		if count == 0 {
			return 0, nil
		}
		sorted := append([]float64(nil), values...)
		sort.Float64s(sorted)
		mid := len(sorted) / 2
		if len(sorted)%2 == 1 {
			return sorted[mid], nil
		}
		return (sorted[mid-1] + sorted[mid]) / 2, nil
	case "STDDEV":
		// Population standard deviation (divides by N, not N-1): with
		// no WHERE the field's whole set of documents already IS "the
		// population" from this database's point of view, not a
		// sample drawn from some larger one.
		if count == 0 {
			return 0, nil
		}
		mean := sum / float64(count)
		var variance float64
		for _, v := range values {
			d := v - mean
			variance += d * d
		}
		variance /= float64(count)
		return math.Sqrt(variance), nil
	}
	return 0, fmt.Errorf("unknown aggregation: %s", op)
}

// AggregateMode returns the most frequent value of field across the
// documents in dbName.block matching where/filters, plus how many times
// it occurred. Unlike Aggregate's other operations, MODE is meaningful
// on non-numeric fields too (status categories, names...), so this
// keeps each value in its original JSON type instead of coercing
// through getNestedF64 -- ties break in favor of the value that was
// seen first (stable iteration order), same convention as GROUP BY's
// COUNT ordering.
func (e *Engine) AggregateMode(dbName, block, field string, where *parse.Expr, filters []Filter) (any, int64, error) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return nil, 0, ErrBlockNotFound
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, 0, err
	}

	prefix := docKeyPrefix(block)
	counts := map[string]int64{}
	firstValue := map[string]any{}
	var order []string

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				continue
			}
			if !matchesWhere(doc, where, filters) {
				continue
			}
			v := getNested(doc.Data, field)
			if v == nil {
				continue
			}
			key := fmt.Sprint(v)
			if _, seen := firstValue[key]; !seen {
				firstValue[key] = v
				order = append(order, key)
			}
			counts[key]++
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	var bestKey string
	var bestCount int64
	for _, key := range order {
		if counts[key] > bestCount {
			bestCount = counts[key]
			bestKey = key
		}
	}
	if bestKey == "" {
		return nil, 0, nil
	}
	return firstValue[bestKey], bestCount, nil
}

type GroupResult struct {
	Key   string  `json:"key"`
	Count int64   `json:"count,omitempty"`
	Sum   float64 `json:"sum,omitempty"`
	Avg   float64 `json:"avg,omitempty"`
	Min   float64 `json:"min,omitempty"`
	Max   float64 `json:"max,omitempty"`
}

func (e *Engine) Group(dbName, block, byField, aggOp, aggField string, where *parse.Expr, filters []Filter) ([]GroupResult, error) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return nil, ErrBlockNotFound
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, err
	}

	prefix := docKeyPrefix(block)
	type acc struct {
		count int64
		sum   float64
		min   float64
		max   float64
	}
	groups := make(map[string]*acc, 128)

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) }); err != nil {
				continue
			}
			if !matchesWhere(doc, where, filters) {
				continue
			}
			key := fmt.Sprint(getNested(doc.Data, byField))
			if _, ok := groups[key]; !ok {
				groups[key] = &acc{
					min: math.MaxFloat64,
					max: -math.MaxFloat64,
				}
			}
			groups[key].count++
			if aggField != "" {
				v := getNestedF64(doc.Data, aggField)
				groups[key].sum += v
				if v < groups[key].min {
					groups[key].min = v
				}
				if v > groups[key].max {
					groups[key].max = v
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	out := make([]GroupResult, 0, len(groups))
	for k, a := range groups {
		gr := GroupResult{Key: k, Count: a.count}
		if strings.ToUpper(aggOp) == "SUM" {
			gr.Sum = a.sum
		}
		if strings.ToUpper(aggOp) == "AVG" && a.count > 0 {
			gr.Avg = a.sum / float64(a.count)
		}
		if strings.ToUpper(aggOp) == "MIN" {
			gr.Min = a.min
		}
		if strings.ToUpper(aggOp) == "MAX" {
			gr.Max = a.max
		}
		if strings.ToUpper(aggOp) == "COUNT" {
			gr.Count = a.count
		}
		out = append(out, gr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out, nil
}
