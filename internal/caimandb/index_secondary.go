package caimandb

import (
	"caimandb/internal/caimandb/cache"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cespare/xxhash/v2"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"strings"
)

// badgerWriter is satisfied by both *badger.Txn and *badger.WriteBatch (both
// expose compatible Set(key,val []byte) error / Delete(key []byte) error
// methods). buildSecondaryIndexInto/dropSecondaryIndexInto write into
// whichever one the caller is already using -- a WriteBatch for bulk
// inserts, or the already-open Txn a bulk UPDATE/DELETE is using for its
// document mutations -- instead of opening and committing one brand-new
// Badger transaction per indexed field. A document with N simple fields
// used to cost N extra commits on top of the document write itself; for a
// bulk load of a million documents with, say, eight indexable fields each,
// that's up to eight million avoidable transaction commits. Folding index
// maintenance into the write the caller was already doing removes that
// multiplier entirely.
type badgerWriter interface {
	Set(key, val []byte) error
	Delete(key []byte) error
}

func (e *Engine) buildSecondaryIndexInto(w badgerWriter, block string, doc *Document) error {
	for field, value := range doc.Data {
		if !shouldIndexField(e.config, field, value) {
			continue
		}

		switch v := value.(type) {
		case string, float64, int, int64, bool:
			strVal := fmt.Sprint(v)
			key := secondaryIndexKey(block, field, strVal, doc.ID)
			if err := w.Set(key, []byte{}); err != nil {
				log().Error("Failed to build index",
					zap.String("block", block),
					zap.String("field", field),
					zap.Error(err))
			}
		}
	}
	return nil
}

func (e *Engine) dropSecondaryIndexInto(w badgerWriter, block string, doc *Document) error {
	for field, value := range doc.Data {
		if !shouldIndexField(e.config, field, value) {
			continue
		}
		switch v := value.(type) {
		case string, float64, int, int64, bool:
			strVal := fmt.Sprint(v)
			key := secondaryIndexKey(block, field, strVal, doc.ID)
			if err := w.Delete(key); err != nil {
				log().Error("Failed to drop index",
					zap.String("block", block),
					zap.String("field", field),
					zap.Error(err))
			}
		}
	}
	return nil
}

// secondaryIndexKeysFor returns the raw index keys for doc's indexable
// fields without touching Badger at all. Used by callers (mass DELETE)
// that already collect a slice of keys to remove in one batched
// transaction and just need the index keys added to that same slice.
func (e *Engine) secondaryIndexKeysFor(block string, doc *Document) [][]byte {
	var keys [][]byte
	for field, value := range doc.Data {
		if !shouldIndexField(e.config, field, value) {
			continue
		}
		switch v := value.(type) {
		case string, float64, int, int64, bool:
			strVal := fmt.Sprint(v)
			keys = append(keys, secondaryIndexKey(block, field, strVal, doc.ID))
			// TODO(pendiente-motor): este switch nunca entra al case
			// []any (arrays) -- un documento con "etiquetas": ["a","b"]
			// no genera ninguna entrada de índice para ese campo hoy.
			// Para soportar WHERE etiquetas[0] = "a" (indexación de
			// arrays) e IN/CONTAINS ALL/ANY vía índice (en vez de full
			// scan) hace falta un case []any que itere los elementos y
			// genere una clave por (índice, valor) o al menos por
			// valor -- coordinar con el TODO de CONTAINS ALL/ANY en
			// query_filter.go, que hoy asume que solo el evaluador de
			// WHERE necesita cambiar y no toca este archivo.
		}
	}
	return keys
}

// buildSecondaryIndex indexes a single document via its own dedicated
// transaction. Kept for the single-document Insert/InsertFast path
// (insertWithIDLocal), where one document means one transaction is
// already the right cost -- unlike bulk paths (InsertBatch, UPDATE,
// DELETE), which use buildSecondaryIndexInto/dropSecondaryIndexInto/
// secondaryIndexKeysFor above to avoid a transaction per field.
func (e *Engine) buildSecondaryIndex(dbName, block string, doc *Document) error {
	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return err
	}
	return txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		return e.buildSecondaryIndexInto(txn, block, doc)
	})
}

func (e *Engine) dropSecondaryIndex(dbName, block string, doc *Document) error {
	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return err
	}
	return txHelper.SafeUpdate(db, func(txn *badger.Txn) error {
		return e.dropSecondaryIndexInto(txn, block, doc)
	})
}

func (e *Engine) lookupByIndex(dbName, block, field, value string, page int, pageSize int) (*cache.IndexResult, error) {
	if cached, ok := e.l2Cache.Get(block, field, value); ok {
		start := page * pageSize
		end := start + pageSize
		if start >= len(cached.DocIDs) {
			return &cache.IndexResult{
				DocIDs:  []string{},
				Total:   cached.Total,
				HasMore: false,
			}, nil
		}
		if end > len(cached.DocIDs) {
			end = len(cached.DocIDs)
		}
		return &cache.IndexResult{
			DocIDs:  cached.DocIDs[start:end],
			Total:   cached.Total,
			HasMore: end < len(cached.DocIDs),
		}, nil
	}

	if e.config.LazyIndexLoading {
		e.l2Cache.Set(block, field, value, []string{}, 0, false, "")
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return nil, err
	}

	var docIDs []string
	prefix := secondaryIndexPrefix(block, field, value)

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().KeyCopy(nil))
			parts := strings.Split(key, "/")
			if len(parts) >= 5 {
				docIDs = append(docIDs, parts[4])
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	total := int64(len(docIDs))
	hasMore := false
	if pageSize > 0 {
		start := page * pageSize
		end := start + pageSize
		if start < len(docIDs) {
			if end > len(docIDs) {
				end = len(docIDs)
			}
			hasMore = end < len(docIDs)
			docIDs = docIDs[start:end]
		} else {
			docIDs = []string{}
		}
	}

	result := &cache.IndexResult{
		DocIDs:  docIDs,
		Total:   total,
		HasMore: hasMore,
	}

	if total <= 5000000 {
		e.l2Cache.Set(block, field, value, docIDs, total, hasMore, "")
	}

	// Opportunistically feed this lookup's real numbers back into the
	// query optimizer's stats table -- this is the only place selectivity
	// for a real (non-"_id") field ever gets populated, which is what
	// lets AnalyzeQuery rank multiple index candidates against each other
	// on subsequent queries instead of only ever guessing from filter
	// order. dbStats.DocCount is a whole-database count (the engine only
	// tracks per-block counts in on-disk block metadata, which isn't
	// worth a disk read on every index lookup), so this is a
	// database-wide approximation of per-block selectivity, not an exact
	// figure -- good enough for ranking candidates, not for anything that
	// needs to be precise.
	if dbStats, ok := e.intelEngine.stats[dbName]; ok && dbStats.DocCount > 0 {
		selectivity := float64(total) / float64(dbStats.DocCount)
		if selectivity > 1 {
			selectivity = 1
		}
		e.queryOpt.UpdateIndexStats(field, total, selectivity)
	}

	return result, nil
}

// roaringIntersectThreshold is the candidate-set size above which
// intersectIDs switches from a plain Go map intersection to a
// RoaringBitmap one. Roaring's compressed-container representation only
// pays for itself once a set is large enough that building it (hashing
// every element) is cheaper than the cache-miss cost of probing a big Go
// map -- below this size the map is simply faster.
const roaringIntersectThreshold = 2000

// intersectIDs returns the elements common to both a and b, preserving a's
// order. Used by Find (ops_find.go) to narrow the candidate set using more
// than one secondary-index lookup before ever fetching a document, instead
// of probing only the single best-ranked field and leaving every other
// indexed equality filter to be re-checked the expensive way (fetch +
// matchesQuery) against documents a second index lookup would have ruled
// out for free.
func intersectIDs(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	if len(a) < roaringIntersectThreshold && len(b) < roaringIntersectThreshold {
		return intersectIDsMap(a, b)
	}
	return intersectIDsRoaring(a, b)
}

// intersectIDsMap is the original O(len(a)+len(b)) Go-map intersection,
// still the better choice for small candidate sets (see
// roaringIntersectThreshold).
func intersectIDsMap(a, b []string) []string {
	set := make(map[string]struct{}, len(b))
	for _, id := range b {
		set[id] = struct{}{}
	}
	out := make([]string, 0, len(a))
	for _, id := range a {
		if _, ok := set[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// intersectIDsRoaring intersects large candidate sets via a compressed
// RoaringBitmap instead of a Go map: doc IDs are opaque strings, so each
// one is hashed to a uint64 (xxhash, already used for Badger-side hashing
// elsewhere in the storage stack) and membership is tested against a
// roaring64.Bitmap built from b. This is a probabilistic pre-filter -- a
// 64-bit hash collision between two *different* doc IDs is what could
// theoretically cause a false match -- but at real dataset sizes
// (thousands to low millions of documents) that probability is
// negligible, and it's the same trade-off any hash-bucketed index makes.
// Order is preserved by iterating a rather than the bitmap.
func intersectIDsRoaring(a, b []string) []string {
	bb := roaring64.New()
	for _, id := range b {
		bb.Add(xxhash.Sum64String(id))
	}

	out := make([]string, 0, len(a))
	for _, id := range a {
		if bb.Contains(xxhash.Sum64String(id)) {
			out = append(out, id)
		}
	}
	return out
}

func shouldIndexField(config *Config, field string, value any) bool {
	if config.SkipLargeIndexes {
		switch v := value.(type) {
		case string:
			if int64(len(v)) > config.IndexSkipThreshold {
				return false
			}
		case map[string]any:
			if estimateSize(v) > config.IndexSkipThreshold {
				return false
			}
		case []any:
			if estimateArraySize(v) > config.IndexSkipThreshold {
				return false
			}
		}
	}
	return true
}

func estimateSize(v any) int64 {
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case map[string]any:
		var size int64
		for k, subv := range val {
			size += int64(len(k)) + estimateSize(subv)
		}
		return size
	case []any:
		return estimateArraySize(val)
	default:
		return int64(len(fmt.Sprint(v)))
	}
}

func estimateArraySize(arr []any) int64 {
	var size int64
	for _, v := range arr {
		size += estimateSize(v)
	}
	return size
}
