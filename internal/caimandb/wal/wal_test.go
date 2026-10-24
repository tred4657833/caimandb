package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readAllWithTimeout guards against the exact failure mode this file's
// tests exist to catch: ReadAll looping forever on a truncated/corrupt
// trailing entry while holding w.mu (see the ReadAll fix in wal.go). A
// hung goroutine here is not just this test failing -- with -race and
// `go test`'s default per-test timeout, an actual regression would show
// up as the whole test binary timing out and dumping every goroutine's
// stack, not a clean t.Fatal. This wrapper turns that into a fast,
// specific failure instead.
func readAllWithTimeout(t *testing.T, w *WAL, timeout time.Duration) []WALEntry {
	t.Helper()
	type result struct {
		entries []WALEntry
		err     error
	}
	done := make(chan result, 1)
	go func() {
		entries, err := w.ReadAll()
		done <- result{entries, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("ReadAll returned an error: %v", r.err)
		}
		return r.entries
	case <-time.After(timeout):
		t.Fatalf("ReadAll did not return within %s -- likely hung retrying a truncated/corrupt entry forever (regression of the ReadAll infinite-loop bug)", timeout)
		return nil
	}
}

// lastSegmentPath returns the path of the WAL's current (highest-numbered)
// segment file, so a test can reach in and truncate/corrupt it directly to
// simulate a crash mid-write. This pokes at on-disk layout rather than any
func lastSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no WAL segment files found in %s (glob err: %v)", dir, err)
	}
	// Segment filenames are zero-padded/sortable by construction (see
	// newSegment), so the lexicographically last match is the newest.
	last := matches[0]
	for _, m := range matches[1:] {
		if m > last {
			last = m
		}
	}
	return last
}

// TestWAL_ReadAll_TruncatedTrailingEntry simulates the most common real
// crash shape: the process dies mid-write, leaving the last line in the
// segment file as a partial, unterminated JSON object. Before the ReadAll
// fix, decoding that partial object returned a non-io.EOF error forever
// (the exhausted *os.File keeps reporting io.EOF on every further read,
// but the json.Decoder never discards the partial bytes it already
// buffered), and the `continue` in ReadAll's inner loop retried the exact
// same failing Decode() call indefinitely while holding w.mu.
func TestWAL_ReadAll_TruncatedTrailingEntry(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 0, false) // streaming=false: synchronous Write(), simplest to reason about here
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	if err := w.Write("INSERT", "db1", "block1", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("Write #1 failed: %v", err)
	}
	if err := w.Write("INSERT", "db1", "block1", []byte(`{"a":2}`)); err != nil {
		t.Fatalf("Write #2 failed: %v", err)
	}

	segPath := lastSegmentPath(t, dir)

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate a crash mid-append: chop off the tail of the segment file
	// so the last complete write is left as a truncated fragment. A real
	// crash could cut anywhere; cutting well into the final entry (rather
	// than at an exact line boundary) is the more adversarial case and
	// the one that actually reproduces the original bug -- an entry that
	// simply isn't there at all (clean truncation at a line boundary)
	// hits io.EOF cleanly and was never the problem.
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("Stat(%s) failed: %v", segPath, err)
	}
	truncateAt := info.Size() - 3 // chop the last few bytes of the 2nd entry's closing `}`+newline
	if truncateAt <= 0 {
		t.Fatalf("segment file too small to truncate meaningfully: %d bytes", info.Size())
	}
	if err := os.Truncate(segPath, truncateAt); err != nil {
		t.Fatalf("Truncate(%s) failed: %v", segPath, err)
	}

	// Reopen against the now-truncated segment, the way a real restart
	// after a crash would (NewWAL calls recover() internally, which just
	// resumes the existing segment -- it does not itself replay entries;
	// replay is ReadAll(), called by the engine's own recovery routine).
	w2, err := NewWAL(dir, 0, false)
	if err != nil {
		t.Fatalf("NewWAL (reopen after truncation) failed: %v", err)
	}
	defer w2.Close()

	entries := readAllWithTimeout(t, w2, 5*time.Second)

	if len(entries) != 1 {
		t.Fatalf("ReadAll after truncation returned %d entries, want 1 (only the first, complete entry should survive; the truncated second entry must be dropped, not skipped-over)", len(entries))
	}
	if entries[0].DB != "db1" || entries[0].Block != "block1" {
		t.Fatalf("unexpected surviving entry: %+v", entries[0])
	}
}

// TestWAL_ReadAll_CorruptMiddleEntry covers the other shape of damage:
// bytes flipped/garbled in the middle of the file (bad sectors, a partial
// disk write landing in the wrong place) rather than a clean truncation at
// the tail. The fix stops replay at the first undecodable entry rather
// than skipping past it and continuing -- NDJSON has no resync marker, so
// once the byte stream is misaligned there is no safe way to tell where
// the next real entry starts. This test exists to document that tradeoff
// as intentional: entries after a corruption point are lost, not silently
// skipped-and-continued.
func TestWAL_ReadAll_CorruptMiddleEntry(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 0, false)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	if err := w.Write("INSERT", "db1", "block1", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("Write #1 failed: %v", err)
	}
	if err := w.Write("INSERT", "db1", "block1", []byte(`{"a":2}`)); err != nil {
		t.Fatalf("Write #2 failed: %v", err)
	}
	if err := w.Write("INSERT", "db1", "block1", []byte(`{"a":3}`)); err != nil {
		t.Fatalf("Write #3 failed: %v", err)
	}
	segPath := lastSegmentPath(t, dir)
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	raw, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) failed: %v", segPath, err)
	}
	// Garble a byte inside the second line (leaving the first entry, and
	// the newline boundaries, intact) so entry #1 decodes fine, entry #2
	// fails, and entry #3 -- though perfectly valid JSON on its own --
	// must NOT be returned, since the decoder cannot safely resynchronize
	// after a mid-stream parse failure.
	lines := splitLinesKeepEnds(raw)
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 WAL lines, got %d", len(lines))
	}
	corrupted := []byte(lines[1])
	for i := range corrupted {
		if corrupted[i] == ':' {
			corrupted[i] = '#' // breaks JSON syntax without changing length
			break
		}
	}
	lines[1] = string(corrupted)
	var rebuilt []byte
	for _, l := range lines {
		rebuilt = append(rebuilt, l...)
	}
	if err := os.WriteFile(segPath, rebuilt, 0640); err != nil {
		t.Fatalf("WriteFile(%s) failed: %v", segPath, err)
	}

	w2, err := NewWAL(dir, 0, false)
	if err != nil {
		t.Fatalf("NewWAL (reopen after corruption) failed: %v", err)
	}
	defer w2.Close()

	entries := readAllWithTimeout(t, w2, 5*time.Second)

	if len(entries) != 1 {
		t.Fatalf("ReadAll after mid-file corruption returned %d entries, want exactly 1 (everything at/after the corrupt entry must be dropped, including the later still-valid-looking entry #3)", len(entries))
	}
}

// splitLinesKeepEnds splits raw into lines, keeping the trailing '\n' on
// each line (except possibly the last), so the caller can mutate one
// line's bytes in place and reassemble the file without disturbing the
// newline-delimited framing.
func splitLinesKeepEnds(raw []byte) []string {
	var lines []string
	start := 0
	for i, b := range raw {
		if b == '\n' {
			lines = append(lines, string(raw[start:i+1]))
			start = i + 1
		}
	}
	if start < len(raw) {
		lines = append(lines, string(raw[start:]))
	}
	return lines
}
