package caimandb

import (
	"caimandb/internal/caimandb/metrics"
	"caimandb/internal/caimandb/query"
	"caimandb/internal/caimandb/storage"
	"context"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"sync"
	"time"

	"caimandb/internal/caimandb/parse"
)

type Query struct {
	Filters []Filter
	// Where, when set, is the full WHERE-clause expression tree (see
	// internal/caimandb/parse/ast.go) and takes precedence over Filters
	// for matching: it supports parentheses and correct AND/OR/NOT
	// precedence, which a flat []Filter cannot express. Filters is
	// still populated alongside it (best-effort, top-level AND-chain
	// only) purely so index selection in QueryOptimizer.AnalyzeQuery
	// keeps working unchanged. Callers that don't build a Where tree
	// (JOIN, transactions, admin commands not yet migrated) leave this
	// nil and get the historical flat-list evaluation, unchanged.
	Where  *parse.Expr
	Select []string
	// SelectItems is the parsed form of Select: each entry is classified
	// as a plain field/path, a COUNT(field) aggregate, or a simple
	// two-operand arithmetic expression (see select_expr.go). Populated
	// by buildFindQuery alongside Select; Select itself is left
	// unchanged so callers that only care about plain projection (or
	// that build a Query without going through buildFindQuery) keep
	// working exactly as before.
	SelectItems []query.SelectItem
	Exclude     []string
	Sort        []SortField
	// GroupBy/Having implement FIND ... GROUP BY <fields> [HAVING ...].
	// When GroupBy is set, postProcessDocs collapses the matched
	// documents into one row per distinct GroupBy value combination
	// (see group_by.go) instead of returning them individually.
	GroupBy []string
	Having  *parse.Expr
	// Distinct implements "FIND <block> DISTINCT <fields> ...": when set,
	// postProcessDocs collapses the result set to one row per distinct
	// combination of these field values (first-seen order preserved),
	// applied after Select/computed fields so it sees exactly what a
	// caller would see in the output, and before Sort/Limit. An empty
	// Distinct with DistinctAll=true means "whole projected row" (every
	// field in the final Data, not just a chosen subset).
	Distinct    []string
	DistinctAll bool
	Limit       int
	Offset      int
	UseIndex    bool
	PreferScan  bool
	ShardID     string
	Timeout     time.Duration
}

type Filter struct {
	Field  string
	Op     string
	Value  any
	Value2 any
	Values []any
	Logic  string
}

type SortField struct {
	Field string
	Desc  bool

	// NullsFirst/NullsLast implement "ORDER BY <field> NULLS FIRST" /
	// "... NULLS LAST": at most one is true. Neither set preserves the
	// original behavior of comparing a missing value as its numeric/
	// string zero value, exactly as before this field existed.
	NullsFirst bool
	NullsLast  bool

	// Random implements "ORDER BY RANDOM()": sortDocs (sort_utils.go)
	// shuffles instead of comparing when this is the (only) sort key.
	Random bool
}

type QueryResult struct {
	Docs      []Document    `json:"docs"`
	Total     int64         `json:"total"`
	Scanned   int64         `json:"scanned"`
	IndexUsed string        `json:"index_used"`
	Took      time.Duration `json:"took"`
}

func (e *Engine) Find(dbName, block string, q *Query) (*QueryResult, error) {
	start := time.Now()
	defer func() {
		e.metrics.RecordQuery(time.Since(start))
		metrics.MetricActiveQueries.Dec()
	}()
	metrics.MetricActiveQueries.Inc()

	if !e.dirMgr.BlockExists(dbName, block) {
		return nil, ErrBlockNotFound
	}

	// Try FLEX-COLUMN acceleration first
	if e.flexEngine != nil && e.flexEngine.config.Enabled {
		result, err := e.flexEngine.QueryWithColumnar(dbName, block, q)
		if err == nil && result != nil && len(result.Docs) > 0 {
			return result, nil
		}
	}

	ctx := context.Background()
	if q.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, q.Timeout)
		defer cancel()
	} else if e.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.QueryTimeout)
		defer cancel()
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, err
	}

	// Block-level read lock, matching the write lock InsertBatch takes on
	// the same key (dbName + block). Reads previously took no lock at
	// all here, which is fine while a Badger handle just stays open for
	// its whole life -- but it means any future mechanism that closes and
	// reopens a block's handle in place (e.g. adaptive resize) has no
	// read lock to synchronize against, and could yank the handle out
	// from under an in-flight Find. This doesn't block concurrent reads
	// against each other (RWMutex), only against a future writer of that
	// kind.
	lockKey := dbName + "\x00" + block
	e.lockManager.RLock(lockKey)
	defer e.lockManager.RUnlock(lockKey)

	var docIDs []string
	var scanAll bool
	var indexUsed string

	if !q.PreferScan && len(q.Filters) > 0 {
		plan := e.queryOpt.AnalyzeQuery(q.Filters)
		if plan.UseIndex && (q.UseIndex || !q.PreferScan) {
			pageSize := q.Limit
			if pageSize <= 0 {
				pageSize = 10000
			}
			result, err := e.lookupByIndex(dbName, block, plan.IndexField, plan.IndexValue, 0, pageSize)
			if err == nil && len(result.DocIDs) > 0 {
				ids := result.DocIDs
				used := plan.IndexField

				// Fold in a bounded number of the next-best index
				// candidates by intersecting doc-ID sets, instead of
				// only ever probing the single top-ranked field and
				// leaving every other indexed equality filter to be
				// re-checked later the expensive way (fetch the
				// document, then matchesQuery). Each extra lookup here
				// is a real Badger prefix scan, so this stops once the
				// set is already small or a small cap of extra
				// candidates has been tried, whichever comes first.
				const maxExtraCandidates = 2
				tried := 0
				for _, c := range plan.Candidates[1:] {
					if len(ids) <= 1 || tried >= maxExtraCandidates {
						break
					}
					tried++
					extra, err := e.lookupByIndex(dbName, block, c.Field, c.Value, 0, pageSize)
					if err != nil || len(extra.DocIDs) == 0 {
						continue
					}
					ids = intersectIDs(ids, extra.DocIDs)
					used += "+" + c.Field
				}

				docIDs = ids
				scanAll = false
				indexUsed = used
			}
		}
	}

	if scanAll || len(docIDs) == 0 {
		docIDs = nil
		indexUsed = "full_scan"
		result, err := e.scanBlock(ctx, dbName, block, q, db)
		if err == nil {
			e.intelEngine.RecordQuery(&QueryRecord{
				DB:        dbName,
				Block:     block,
				Filters:   q.Filters,
				Duration:  result.Took,
				Timestamp: time.Now(),
				IndexUsed: result.IndexUsed,
				Scanned:   result.Scanned,
				Returned:  result.Total,
			})
			e.updateDBStats(dbName, result)
			metrics.MetricQueryLatency.WithLabelValues(dbName, block, "scan").Observe(result.Took.Seconds())
		}
		return result, err
	}

	var docs []Document
	var scanned int64

	relDef, _ := e.relMgr.Get(dbName, block)
	neededAliases := filterAliasRoots(q, relDef)

	// errgroup.WithContext replaces the previous sem-channel + WaitGroup
	// pair: SetLimit(1000) gives the same "at most 1000 doc fetches in
	// flight" bound, and deriving the group's context from ctx means a
	// caller cancellation (client disconnect, timeout) now also stops any
	// fetches that haven't started yet instead of only being checked at
	// the top of this loop.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(1000)
	var mu sync.Mutex

	for _, id := range docIDs {
		if gctx.Err() != nil {
			break
		}

		docID := id
		g.Go(func() error {
			defer func() {
				if r := recover(); r != nil {
					log().Error("find_fetch_doc panic recovered", zap.Any("panic", r))
				}
			}()

			ck := e.cacheKey(dbName, block, docID)
			var doc Document
			var found bool

			if raw, ok := e.l1Cache.Get(ck); ok {
				if json.Unmarshal(raw, &doc) == nil {
					found = true
				}
			}

			if !found {
				err := txHelper.SafeView(db, func(txn *badger.Txn) error {
					item, err := txn.Get(docKey(block, docID))
					if err != nil {
						return err
					}
					return item.Value(func(v []byte) error {
						if e.config.CompressLargeDocs && len(v) > 0 {
							if decompressed, err := storage.DecompressData(v); err == nil {
								e.l1Cache.Set(ck, decompressed)
								return json.Unmarshal(decompressed, &doc)
							}
						}
						e.l1Cache.Set(ck, v)
						return json.Unmarshal(v, &doc)
					})
				})
				if err != nil {
					// badger.ErrKeyNotFound is the normal, expected case
					// (the ID list can include documents that were since
					// deleted) and not worth logging. Anything else here
					// means the key existed but its stored bytes didn't
					// decode -- decompression failure or malformed
					// JSON -- which is exactly what on-disk corruption of
					// a single document looks like. Previously this was
					// indistinguishable from "not found": the document
					// silently vanished from every query with no trace.
					// It's still excluded from results (there's no safe
					// way to serve a document that didn't decode), but
					// now at least leaves a log line pointing at which
					// document and why.
					if err != badger.ErrKeyNotFound {
						log().Warn("document unreadable, excluding from results",
							zap.String("db", dbName), zap.String("block", block),
							zap.String("doc_id", docID), zap.Error(err))
					}
					return nil
				}
			}

			filterDoc := doc
			if len(neededAliases) > 0 {
				filterDoc = e.attachRelationsForFilter(doc, relDef, neededAliases)
			}

			mu.Lock()
			scanned++
			if matchesQuery(filterDoc, q) {
				if q.ShardID == "" || doc.ShardID == q.ShardID {
					docs = append(docs, doc)
				}
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	docs = e.mergeLineDatPending(dbName, block, q, docs, relDef, neededAliases)

	total := int64(len(docs))
	docs = e.postProcessDocs(dbName, block, docs, q)

	result := &QueryResult{
		Docs:      docs,
		Total:     total,
		Scanned:   scanned,
		IndexUsed: indexUsed,
		Took:      time.Since(start),
	}

	e.intelEngine.RecordQuery(&QueryRecord{
		DB:        dbName,
		Block:     block,
		Filters:   q.Filters,
		Duration:  result.Took,
		Timestamp: time.Now(),
		IndexUsed: result.IndexUsed,
		Scanned:   result.Scanned,
		Returned:  result.Total,
	})
	e.updateDBStats(dbName, result)
	metrics.MetricQueryLatency.WithLabelValues(dbName, block, "index").Observe(result.Took.Seconds())

	return result, nil
}

func (e *Engine) scanBlock(ctx context.Context, dbName, block string, q *Query, db *badger.DB) (*QueryResult, error) {
	start := time.Now()
	prefix := docKeyPrefix(block)

	var matched []Document
	var scanned int64

	relDef, _ := e.relMgr.Get(dbName, block)
	neededAliases := filterAliasRoots(q, relDef)

	limit := q.Limit
	offset := q.Offset
	if limit <= 0 {
		limit = 10000
	}
	if offset < 0 {
		offset = 0
	}

	err := txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		skipCount := 0

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if skipCount < offset {
				skipCount++
				continue
			}

			if len(matched) >= limit && limit > 0 {
				break
			}

			var doc Document
			item := it.Item()
			if item == nil {
				continue
			}
			err := item.Value(func(v []byte) error {
				if e.config.CompressLargeDocs && len(v) > 0 {
					if decompressed, err := storage.DecompressData(v); err == nil {
						return json.Unmarshal(decompressed, &doc)
					}
				}
				return json.Unmarshal(v, &doc)
			})
			if err != nil {
				// Unlike the by-ID fetch path in Find, item came straight
				// from iterating existing keys -- there's no "not found"
				// case to filter out here, so any error decoding it is a
				// stored document that doesn't decode back: corruption,
				// not absence. Same as there, it's excluded from results
				// (nothing safe to return), but now it's logged instead
				// of just disappearing from every scan silently.
				log().Warn("document unreadable during scan, excluding from results",
					zap.String("db", dbName), zap.String("block", block),
					zap.ByteString("doc_key", item.KeyCopy(nil)), zap.Error(err))
				continue
			}

			scanned++

			if q.ShardID != "" && doc.ShardID != q.ShardID {
				continue
			}

			filterDoc := doc
			if len(neededAliases) > 0 {
				filterDoc = e.attachRelationsForFilter(doc, relDef, neededAliases)
			}

			if matchesQuery(filterDoc, q) {
				matched = append(matched, doc)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	matched = e.mergeLineDatPending(dbName, block, q, matched, relDef, neededAliases)

	total := int64(len(matched))
	docs := e.postProcessDocs(dbName, block, matched, q)

	return &QueryResult{
		Docs:      docs,
		Total:     total,
		Scanned:   scanned,
		IndexUsed: "full_scan",
		Took:      time.Since(start),
	}, nil
}

// mergeLineDatPending folds in whatever LineDat is still holding for this
// block (documents accepted via InsertFast/GENERATE/IMPORT that haven't
// reached CaimanDB's permanent storage yet) alongside docs, which already
// came from CaimanDB itself (an index lookup or a full scan). This is what
// lets a FIND/query answer "yes" for data that was accepted moments ago
// and is still in flight in the background instead of only seeing what has
// actually landed in Badger so far.
//
// Deduplicates by ID against docs (a document already found in CaimanDB
// takes priority over a same-ID LineDat entry, which can only mean it was
// already committed and LineDat just hasn't reconciled that yet) and
// applies the same matchesQuery/ShardID/relation-alias filtering the
// scan/index paths already apply to their own results, so a pending
// document only shows up when it would have matched anyway.
//
// Known limitation: the FLEX-COLUMN columnar acceleration path (see the
// top of Find) returns directly on any non-empty result without going
// through here, so a query already satisfied by at least one committed,
// columnar-indexed document won't also see newer LineDat-pending matches
// until they land in CaimanDB. Queries with nothing committed yet still
// fall through to the scan/index paths below (and this merge) as normal.
func (e *Engine) mergeLineDatPending(dbName, block string, q *Query, docs []Document, relDef *RelationDef, neededAliases map[string]bool) []Document {
	if e.lineDat == nil {
		return docs
	}
	pending := e.lineDat.PendingForBlock(dbName, block)
	if len(pending) == 0 {
		return docs
	}

	seen := make(map[string]struct{}, len(docs))
	for _, d := range docs {
		seen[d.ID] = struct{}{}
	}

	for _, doc := range pending {
		if _, dup := seen[doc.ID]; dup {
			continue
		}
		if q.ShardID != "" && doc.ShardID != q.ShardID {
			continue
		}
		filterDoc := doc
		if len(neededAliases) > 0 {
			filterDoc = e.attachRelationsForFilter(doc, relDef, neededAliases)
		}
		if !matchesQuery(filterDoc, q) {
			continue
		}
		docs = append(docs, doc)
		seen[doc.ID] = struct{}{}
	}
	return docs
}

func (e *Engine) postProcessDocs(dbName, block string, docs []Document, q *Query) []Document {
	if len(q.GroupBy) > 0 {
		docs = e.groupDocs(dbName, block, docs, q)
		if len(q.Sort) > 0 {
			sortDocs(docs, q.Sort)
		}
		if q.Limit > 0 && len(docs) > q.Limit {
			docs = docs[:q.Limit]
		}
		return docs
	}

	// Computed SELECT entries (COUNT(field) AS alias, arithmetic like
	// "duration_minutes / 60 as hours") get evaluated and written into
	// each doc's Data under their alias *before* sorting/projection, so
	// ORDER BY and the final field list can reference the alias exactly
	// like any other field.
	if len(q.SelectItems) > 0 {
		hasComputed := false
		for _, it := range q.SelectItems {
			if it.Kind != query.SelectPlain {
				hasComputed = true
				break
			}
		}
		if hasComputed {
			for i := range docs {
				docs[i].Data = query.ApplySelectExprs(docs[i].Data, q.SelectItems)
			}
			q.Select = query.SelectProjectFields(q.SelectItems)
		}
	}

	if len(q.Distinct) > 0 {
		docs = distinctByFields(docs, q.Distinct)
	}

	if len(q.Sort) > 0 {
		sortDocs(docs, q.Sort)
	}

	if len(q.Select) > 0 || len(q.Exclude) > 0 {
		docs = e.resolveRelations(dbName, block, docs, q)
	}

	// DistinctAll ("FIND <block> DISTINCT" with no field list) dedupes on
	// the fully projected row, so it has to run after Select/Exclude have
	// already trimmed Data down to what the caller actually sees --
	// otherwise two rows that look identical in the output could differ
	// only in a field the caller asked to drop.
	if q.DistinctAll {
		docs = distinctByRow(docs)
	}

	if q.Limit > 0 && len(docs) > q.Limit {
		docs = docs[:q.Limit]
	}

	return docs
}

// distinctByFields keeps the first document seen for each distinct
// combination of values at the given field paths (dot-path aware via
// getNested, so "address.city" works the same as in WHERE/SELECT).
// Order of first appearance is preserved, matching GROUP BY's own
// first-seen behavior in group_by.go.
func distinctByFields(docs []Document, fields []string) []Document {
	seen := make(map[string]struct{}, len(docs))
	out := make([]Document, 0, len(docs))
	for _, doc := range docs {
		var key strings.Builder
		for _, f := range fields {
			key.WriteString(fmt.Sprintf("%v\x1f", getNested(doc.Data, f)))
		}
		k := key.String()
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, doc)
	}
	return out
}

// distinctByRow dedupes on the entire projected Data map (JSON-encoded
// for a stable, order-independent-enough comparison key). Used for bare
// "FIND <block> DISTINCT" with no field list.
func distinctByRow(docs []Document) []Document {
	seen := make(map[string]struct{}, len(docs))
	out := make([]Document, 0, len(docs))
	for _, doc := range docs {
		b, err := json.Marshal(doc.Data)
		k := string(b)
		if err != nil {
			// Unmarshalable Data is vanishingly unlikely (Data always
			// comes from decoded JSON/BSON in this engine) but if it
			// ever happens, fall back to keeping the row rather than
			// silently dropping data over a marshal error.
			out = append(out, doc)
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, doc)
	}
	return out
}

func (e *Engine) FindByID(dbName, block, id string) (*Document, bool) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return nil, false
	}

	// LineDat is checked first: if this document was just accepted via
	// InsertFast/GENERATE/IMPORT and hasn't reached CaimanDB's permanent
	// storage yet, it lives only here for now. This is what keeps a FIND
	// run right after an accepted INSERT consistent even though the
	// actual Badger commit may still be in flight in the background.
	if e.lineDat != nil {
		if doc, ok := e.lineDat.Lookup(dbName, block, id); ok {
			return doc, true
		}
	}

	ck := e.cacheKey(dbName, block, id)
	if raw, ok := e.l1Cache.Get(ck); ok {
		var doc Document
		if json.Unmarshal(raw, &doc) == nil {
			if doc.LargeID != "" && e.externalStore != nil {
				if externalData, err := e.externalStore.Load(doc.LargeID); err == nil {
					var fullDoc Document
					if json.Unmarshal(externalData, &fullDoc) == nil {
						return &fullDoc, true
					}
				}
			}
			metrics.MetricCacheHits.Inc()
			return &doc, true
		}
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, false
	}

	var doc Document
	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		item, err := txn.Get(docKey(block, id))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if e.config.CompressLargeDocs && len(v) > 0 {
				if decompressed, err := storage.DecompressData(v); err == nil {
					e.l1Cache.Set(ck, decompressed)
					return json.Unmarshal(decompressed, &doc)
				}
			}
			e.l1Cache.Set(ck, v)
			return json.Unmarshal(v, &doc)
		})
	})
	if err != nil {
		metrics.MetricCacheMisses.Inc()
		if err != badger.ErrKeyNotFound {
			log().Warn("document unreadable, excluding from FindByID result",
				zap.String("db", dbName), zap.String("block", block),
				zap.String("doc_id", id), zap.Error(err))
		}
		return nil, false
	}

	if doc.LargeID != "" && e.externalStore != nil {
		if externalData, err := e.externalStore.Load(doc.LargeID); err == nil {
			var fullDoc Document
			if json.Unmarshal(externalData, &fullDoc) == nil {
				metrics.MetricCacheHits.Inc()
				return &fullDoc, true
			}
		}
	}

	metrics.MetricCacheHits.Inc()
	return &doc, true
}

func (e *Engine) updateDBStats(dbName string, result *QueryResult) {
	if stats, ok := e.intelEngine.stats[dbName]; ok {
		stats.QueryCount++
		stats.AvgQueryTime = (stats.AvgQueryTime*float64(stats.QueryCount-1) + float64(result.Took.Milliseconds())) / float64(stats.QueryCount)
		if result.IndexUsed != "full_scan" {
			stats.IndexHitRate = (stats.IndexHitRate*float64(stats.QueryCount-1) + 1) / float64(stats.QueryCount)
		} else {
			stats.IndexHitRate = (stats.IndexHitRate*float64(stats.QueryCount-1) + 0) / float64(stats.QueryCount)
		}
		cacheStats := e.l1Cache.Stats()
		if hitRatio, ok := cacheStats["hit_ratio"].(string); ok {
			if val, err := strconv.ParseFloat(strings.TrimSuffix(hitRatio, "%"), 64); err == nil {
				stats.CacheHitRate = val / 100
			}
		}
	}
}
