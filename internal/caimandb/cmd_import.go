package caimandb

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// handleImportCmd implements:
//
//	IMPORT <block> FROM FILE '<path>' [FORMAT NDJSON|ARRAY] [BATCH <n>]
//
// It streams documents straight from disk into BulkImportReader
// (ops_bulk_import.go) instead of reading the whole file into memory and
// building one giant []map[string]any for InsertBatch -- which is exactly
// the wrong shape for the multi-GB / multi-million-document loads this
// command exists for. A .gz-suffixed path is transparently decompressed
// while streaming, the same convention RESTORE already uses for backup
// files (see ops_backup.go).
//
// Uses BULK MODE for the duration automatically (BulkImportReader's
// default), so an explicit "BULK MODE ON" beforehand is not required --
// though it's still honored if the caller already turned it on, via
// SkipBulkModeToggle semantics inside BulkImportReader.
func handleImportCmd(eng *Engine, sess *Session, tokens []string) string {
	if sess.currentDB == "" {
		return "ERROR: No database in use"
	}
	if len(tokens) < 5 || strings.ToUpper(tokens[2]) != "FROM" || strings.ToUpper(tokens[3]) != "FILE" {
		return "ERROR: usage: IMPORT <block> FROM FILE '<path>' [FORMAT NDJSON|ARRAY] [BATCH <n>]"
	}

	block := tokens[1]
	path := stripQuotes(tokens[4])

	opts := BulkImportOptions{ContinueOnError: true}

	for i := 5; i < len(tokens); i++ {
		switch strings.ToUpper(tokens[i]) {
		case "FORMAT":
			if i+1 >= len(tokens) {
				return "ERROR: FORMAT requires NDJSON or ARRAY"
			}
			i++
			opts.Format = strings.ToLower(tokens[i])
		case "BATCH":
			if i+1 >= len(tokens) {
				return "ERROR: BATCH requires a batch size"
			}
			i++
			n, err := strconv.Atoi(tokens[i])
			if err != nil || n <= 0 {
				return fmt.Sprintf("ERROR: invalid BATCH size %q", tokens[i])
			}
			opts.BatchSize = n
		default:
			return fmt.Sprintf("ERROR: unknown IMPORT option %q", tokens[i])
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		defer gz.Close()
		reader = gz
	}

	result, err := eng.BulkImportReader(sess.currentDB, block, reader, opts)
	if result == nil {
		return "ERROR: " + err.Error()
	}

	out := fmt.Sprintf("Import complete: %d decoded, %d inserted, %d errors, took %v",
		result.Decoded, result.Inserted, result.Errors, result.Took)
	if err != nil {
		out += fmt.Sprintf("\n(stopped early: %v)", err)
	}
	// IMPORT runs under BULK MODE (see this function's doc comment), and
	// FLEX-COLUMN indexing is skipped for the whole duration of bulk mode
	// to avoid its unbounded in-memory index OOMing the process on a large
	// load (see FlexColumnEngine.bulkModeActive). So unless the caller
	// opted out with SkipBulkModeToggle -- meaning some ancestor call is
	// managing bulk mode across several imports and will presumably handle
	// this itself -- point the caller at the explicit backfill step.
	if result.Inserted > 0 && !opts.SkipBulkModeToggle && eng.flexEngine != nil && eng.flexEngine.config != nil && eng.flexEngine.config.Enabled {
		out += fmt.Sprintf("\nNote: FLEX-COLUMN indexing was skipped during this import (BULK MODE). Run FLEX REINDEX %s %s to rebuild it.", sess.currentDB, block)
	}
	for _, sample := range result.ErrorSamples {
		out += "\n  - " + sample
	}
	return out
}
