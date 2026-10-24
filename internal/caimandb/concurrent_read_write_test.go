package caimandb

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestEngine_ConcurrentFindDuringInsert covers the concurrency gap this
// package's other concurrency tests leave open: they all pit writers
// against writers (ops_insert_concurrency_test.go) or exercise
// ShardManager/FlexColumnEngine in isolation
// (shard_manager_concurrency_test.go, flexcolumn tests) -- none of them
// run Find/Count *while* InsertBatch is actively writing to the same
// block. Find takes an RLock on the same lockManager key InsertBatch
// locks (see ops_find.go's comment on lockKey), so this is specifically
// checking that RWMutex interaction holds up under real concurrent load,
// not just reasoning about it from reading the code.
//
// Run with -race; that's what would actually catch a synchronization bug
// here, not the correctness assertions alone (a torn/partial document
// read without -race might just look like "the document isn't there
// yet", which is allowed -- the race detector is what would catch two
// goroutines touching the same memory unsafely regardless of whether the
// assertions below happen to trip on this run):
//
//	go test ./internal/caimandb/ -run TestEngine_ConcurrentFindDuringInsert -race -v
func TestEngine_ConcurrentFindDuringInsert(t *testing.T) {
	e := newConcurrencyTestEngine(t)

	const dbName = "concurrent_read_write"
	const block = "block1"
	if err := e.CreateDB(dbName); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if err := e.CreateBlock(dbName, block); err != nil {
		t.Fatalf("CreateBlock failed: %v", err)
	}

	const writers = 4
	const batchesPerWriter = 20
	const docsPerBatch = 25
	const totalDocs = int64(writers * batchesPerWriter * docsPerBatch)

	var writerWG sync.WaitGroup
	var totalInserted, totalInsertErrors int64

	// stopReading is closed once every writer goroutine has finished, so
	// the reader goroutines below know to stop looping and exit. Readers
	// keep hammering Find/Count for as long as writers are active,
	// rather than firing a fixed number of times, so this actually
	// overlaps with writes instead of racing to finish first.
	stopReading := make(chan struct{})

	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			for b := 0; b < batchesPerWriter; b++ {
				docs := make([]map[string]any, docsPerBatch)
				for i := range docs {
					docs[i] = map[string]any{
						"worker": w,
						"batch":  b,
						"i":      i,
						"tag":    "concurrent_read_write",
					}
				}
				ins, errs := e.InsertBatch(dbName, block, docs)
				atomic.AddInt64(&totalInserted, ins)
				atomic.AddInt64(&totalInsertErrors, errs)
			}
		}(w)
	}

	const readers = 4
	var readerWG sync.WaitGroup
	var readErrors int64       // Find/Count calls that returned an error other than "block not found yet"
	var malformedDocs int64    // documents missing expected fields -- would indicate a torn/partial read
	var duplicateIDsSeen int64 // same document ID returned twice within one Find call

	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stopReading:
					return
				default:
				}

				res, err := e.Find(dbName, block, &Query{Limit: 100})
				if err != nil {
					atomic.AddInt64(&readErrors, 1)
					continue
				}

				seen := make(map[string]bool, len(res.Docs))
				for _, doc := range res.Docs {
					if seen[doc.ID] {
						atomic.AddInt64(&duplicateIDsSeen, 1)
					}
					seen[doc.ID] = true

					// A document InsertBatch reports as committed must be
					// whole when read back -- never a partially-written
					// Data map. tag/worker/batch/i are all set together
					// in the same map literal above, so any one missing
					// means a torn read.
					if doc.Data["tag"] != "concurrent_read_write" {
						atomic.AddInt64(&malformedDocs, 1)
						continue
					}
					for _, field := range []string{"worker", "batch", "i"} {
						if _, ok := doc.Data[field]; !ok {
							atomic.AddInt64(&malformedDocs, 1)
							break
						}
					}
				}

				if _, err := e.Count(dbName, block, nil, nil); err != nil {
					atomic.AddInt64(&readErrors, 1)
				}
			}
		}()
	}

	writerWG.Wait()
	close(stopReading)
	readerWG.Wait()

	if totalInsertErrors != 0 {
		t.Errorf("InsertBatch reported %d errors during concurrent read/write", totalInsertErrors)
	}
	if totalInserted != totalDocs {
		t.Errorf("InsertBatch reported %d documents inserted, want %d", totalInserted, totalDocs)
	}
	if readErrors != 0 {
		t.Errorf("%d Find/Count calls returned an error while writes were in flight", readErrors)
	}
	if malformedDocs != 0 {
		t.Errorf("%d documents read back during concurrent writes were missing expected fields (torn/partial read)", malformedDocs)
	}
	if duplicateIDsSeen != 0 {
		t.Errorf("%d duplicate document IDs seen within a single Find call", duplicateIDsSeen)
	}

	finalCount, err := e.Count(dbName, block, nil, nil)
	if err != nil {
		t.Fatalf("final Count failed: %v", err)
	}
	if finalCount != totalDocs {
		t.Errorf("final Count = %d, want %d (documents inserted during the concurrent read/write storm must all be durably present once writers finish)", finalCount, totalDocs)
	}
}
