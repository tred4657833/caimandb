package caimandb

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fileExists reports whether path exists and is a regular, readable file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// backupBaseName strips any directory and extension from a user-supplied
// destination, so "EXPORT users TO \"foo/bar.json\"" and "EXPORT users TO
// \"bar\"" both resolve to the same base name ("bar").
func backupBaseName(dest string) string {
	base := filepath.Base(dest)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

// docHeaders builds the CSV header row for a set of documents, guaranteeing
// both "_id" (top-level document id) and "id" (the copy of the id that also
// lives inside data, added by Insert/InsertBatch/insertWithIDLocal) are
// present, along with every other key found in data.
func docHeaders(docs []Document) []string {
	seen := map[string]bool{"_id": true, "_shard_id": true, "id": true}
	headers := []string{"_id", "_shard_id", "id"}
	for _, doc := range docs {
		for k := range doc.Data {
			if k == "id" {
				continue
			}
			if !seen[k] {
				seen[k] = true
				headers = append(headers, k)
			}
		}
	}
	sort.Strings(headers[3:])
	return headers
}

// csvCell renders one document field for CSV output. Anything that
// isn't a scalar (nested objects, arrays) is written as compact JSON
// instead of Go's default fmt.Sprint rendering, which produced
// unparseable cells like "map[insurance_covered:false]" or
// "[omnivore european]" -- those round-tripped back through IMPORT as
// meaningless strings, silently corrupting the data. A missing/nil
// value becomes an empty cell rather than the literal "<nil>".
func csvCell(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	case float64, float32, int, int32, int64, uint, uint32, uint64:
		return fmt.Sprint(val)
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprint(val)
		}
		return string(b)
	}
}

// writeDocsCSV writes an already-fetched set of documents to a CSV file,
// always including both "_id" and "id" columns.
func writeDocsCSV(docs []Document, path string) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 1<<20)
	csvW := csv.NewWriter(bw)

	headers := docHeaders(docs)
	if err := csvW.Write(headers); err != nil {
		return 0, err
	}

	var count int64
	for _, doc := range docs {
		row := make([]string, 0, len(headers))
		for _, h := range headers {
			switch h {
			case "_id":
				row = append(row, doc.ID)
			case "_shard_id":
				row = append(row, doc.ShardID)
			case "id":
				idVal := doc.ID
				if v, ok := doc.Data["id"]; ok {
					idVal = csvCell(v)
				}
				row = append(row, idVal)
			default:
				row = append(row, csvCell(doc.Data[h]))
			}
		}
		if err := csvW.Write(row); err != nil {
			return count, err
		}
		count++
	}
	csvW.Flush()
	if err := csvW.Error(); err != nil {
		return count, err
	}
	return count, bw.Flush()
}

// writeDocsJSON writes an already-fetched set of documents to a JSON file,
// one document per line (NDJSON) -- the same format the streaming Export()
// produces and Import() expects -- each including both "_id" (document id)
// and "id" (mirrored inside "data") for compatibility with tools that
// expect either convention.
func writeDocsJSON(docs []Document, path string) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 1<<20)

	var count int64
	for _, doc := range docs {
		if doc.Data == nil {
			doc.Data = map[string]any{}
		}
		if _, ok := doc.Data["id"]; !ok {
			doc.Data["id"] = doc.ID
		}
		b, err := json.Marshal(doc)
		if err != nil {
			return count, err
		}
		if _, err := bw.Write(b); err != nil {
			return count, err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return count, err
		}
		count++
	}
	return count, bw.Flush()
}

// ExportDocsBoth writes a pre-fetched slice of documents (e.g. from a
// filtered Find) to both <backups>/<name>.csv and <backups>/<name>.json.
func (e *Engine) ExportDocsBoth(docs []Document, name string) (csvPath, jsonPath string, count int64, err error) {
	dir := e.dirMgr.BackupsPath()
	base := backupBaseName(name)
	csvPath = filepath.Join(dir, base+".csv")
	jsonPath = filepath.Join(dir, base+".json")

	count, err = writeDocsCSV(docs, csvPath)
	if err != nil {
		return csvPath, jsonPath, count, err
	}
	if _, err = writeDocsJSON(docs, jsonPath); err != nil {
		return csvPath, jsonPath, count, err
	}
	return csvPath, jsonPath, count, nil
}

// ExportBoth streams a full block export (no filters) to both
// <backups>/<name>.csv and <backups>/<name>.json, using the given base name.
func (e *Engine) ExportBoth(dbName, block, name string) (csvPath, jsonPath string, count int64, err error) {
	dir := e.dirMgr.BackupsPath()
	base := backupBaseName(name)
	csvPath = filepath.Join(dir, base+".csv")
	jsonPath = filepath.Join(dir, base+".json")

	count, err = e.Export(dbName, block, csvPath)
	if err != nil {
		return csvPath, jsonPath, count, err
	}
	if _, err = e.Export(dbName, block, jsonPath); err != nil {
		return csvPath, jsonPath, count, err
	}
	return csvPath, jsonPath, count, nil
}

func (e *Engine) Export(dbName, block, dest string) (int64, error) {
	start := time.Now()

	if !e.dirMgr.BlockExists(dbName, block) {
		return 0, ErrBlockNotFound
	}

	db, err := e.pool.OpenBlock(dbName, block)
	if err != nil {
		return 0, err
	}

	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 128<<20)

	isCSV := strings.HasSuffix(strings.ToLower(dest), ".csv")
	prefix := docKeyPrefix(block)

	var csvW *csv.Writer
	var headers []string
	first := true
	var exported int64

	err = txHelper.SafeView(db, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 20000
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			var doc Document
			item.Value(func(v []byte) error { return json.Unmarshal(v, &doc) })

			if isCSV {
				if first {
					headers = append(headers, "_id", "_shard_id")
					for k := range doc.Data {
						headers = append(headers, k)
					}
					sort.Strings(headers[2:])
					csvW = csv.NewWriter(bw)
					csvW.Write(headers)
					first = false
				}
				row := []string{doc.ID, doc.ShardID}
				for _, h := range headers[2:] {
					row = append(row, csvCell(doc.Data[h]))
				}
				csvW.Write(row)
			} else {
				b, _ := json.Marshal(doc)
				bw.Write(b)
				bw.WriteByte('\n')
			}
			exported++
		}
		return nil
	})

	if csvW != nil {
		csvW.Flush()
	}
	err = bw.Flush()

	log().Info("Export completed",
		zap.String("db", dbName),
		zap.String("block", block),
		zap.String("dest", dest),
		zap.Int64("exported", exported),
		zap.Duration("duration", time.Since(start)))

	return exported, err
}

// importBatchSize es cuántas filas/líneas se acumulan antes de mandarlas
// juntas a InsertBatch. Insertar de a un documento (un e.Insert() por
// fila) significa una transacción de Badger, un lock de bloque y un
// round-trip de WAL por documento -- para archivos grandes eso es el
// cuello de botella real. Agrupar en lotes de este tamaño y llamar a
// InsertBatch reutiliza su write-batch (flush cada 1000 dentro de
// insertBatchLocal) y un solo lock por lote.
const importBatchSize = 5000

// importReadBufferSize is the starting size for the buffered reader/scanner
// used while importing. It is intentionally small (unlike the historical
// 128MB fixed buffers this replaced): bufio.NewReaderSize allocates its
// buffer up front and never grows it, and bufio.Scanner's Buffer() first
// argument is a *starting* capacity, not a pre-allocation of its max --
// Scanner only grows toward the second (maxScanTokenSize) argument if an
// actual line needs it. Pre-sizing either to 128MB regardless of the
// source file's real size wasted memory on every import and, on a host
// with a small page file, could fail the allocation outright (see the
// equivalent fix in storage.BufferPool for the WAL write path).
const importReadBufferSize = 64 << 10 // 64KB

// maxScanTokenSize is the ceiling bufio.Scanner may grow to for a single
// line/token -- not allocated up front, only reserved if a real line
// actually needs it (e.g. one huge NDJSON document on a single line).
const maxScanTokenSize = 128 << 20

func (e *Engine) Import(dbName, block, src string) (int64, int64, error) {
	start := time.Now()

	if err := e.dirMgr.CreateBlockDirectories(dbName, block); err != nil {
		return 0, 0, err
	}

	if _, statErr := os.Stat(src); statErr != nil && filepath.Dir(src) == "." {
		if candidate := filepath.Join(e.dirMgr.BackupsPath(), src); fileExists(candidate) {
			src = candidate
		}
	}

	f, err := os.Open(src)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	var inserted, errors int64
	batch := make([]map[string]any, 0, importBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ins, errs := e.InsertBatchViaLineDat(dbName, block, batch)
		inserted += ins
		errors += errs
		batch = batch[:0]
	}

	if strings.HasSuffix(strings.ToLower(src), ".csv") {
		r := csv.NewReader(bufio.NewReaderSize(f, importReadBufferSize))
		headers, err := r.Read()
		if err != nil {
			return 0, 0, err
		}
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errors++
				continue
			}
			data := make(map[string]any, len(headers))
			for i, h := range headers {
				if i < len(row) {
					if h == "_shard_id" {
						continue
					}
					if h != "_id" {
						data[h] = row[i]
					}
				}
			}
			batch = append(batch, data)
			if len(batch) >= importBatchSize {
				flush()
			}
		}
		flush()
	} else {
		br := bufio.NewReaderSize(f, importReadBufferSize)

		// Peek past leading whitespace (without consuming anything else)
		// to tell a top-level JSON array apart from NDJSON, so both of
		// the shapes the "Acepta JSON (array) o CSV" contract promises
		// actually work -- previously anything that wasn't NDJSON (a
		// real array, pretty-printed or not) silently mis-parsed as
		// mostly-invalid NDJSON: every line failed except one that
		// happened to stand alone as valid JSON, each miscounted as an
		// "error" instead of a clear failure.
		isArray := false
		for {
			b, err := br.Peek(1)
			if err != nil {
				break
			}
			if b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r' {
				br.ReadByte()
				continue
			}
			isArray = b[0] == '['
			break
		}

		if isArray {
			dec := json.NewDecoder(br)
			tok, err := dec.Token()
			if err != nil {
				return 0, 0, err
			}
			if delim, ok := tok.(json.Delim); !ok || delim != '[' {
				return 0, 0, fmt.Errorf("malformed JSON array in %s", src)
			}
			for dec.More() {
				var data map[string]any
				if err := dec.Decode(&data); err != nil {
					// A decode error mid-array can desync the decoder's
					// byte position (it may have consumed a partial,
					// invalid token), so unlike NDJSON's per-line
					// isolation there's no safe way to skip just this
					// element and keep going -- stop here rather than
					// risk silently swallowing the rest of the file.
					errors++
					break
				}
				data = unwrapExportEnvelope(data)
				delete(data, "_id")
				batch = append(batch, data)
				if len(batch) >= importBatchSize {
					flush()
				}
			}
			flush()
		} else {
			scanner := bufio.NewScanner(br)
			scanner.Buffer(make([]byte, 0, importReadBufferSize), maxScanTokenSize)
			for scanner.Scan() {
				line := bytes.TrimSpace(scanner.Bytes())
				if len(line) == 0 {
					continue
				}
				var data map[string]any
				if json.Unmarshal(line, &data) != nil {
					errors++
					continue
				}
				data = unwrapExportEnvelope(data)
				delete(data, "_id")
				batch = append(batch, data)
				if len(batch) >= importBatchSize {
					flush()
				}
			}
			flush()
		}
	}

	log().Info("Import completed",
		zap.String("db", dbName),
		zap.String("block", block),
		zap.String("src", src),
		zap.Int64("inserted", inserted),
		zap.Int64("errors", errors),
		zap.Duration("duration", time.Since(start)))

	return inserted, errors, nil
}
