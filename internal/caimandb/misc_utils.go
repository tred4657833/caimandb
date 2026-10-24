package caimandb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unicode"
)

// newID returns a new document/transaction ID. It used to be 16 raw
// crypto/rand bytes hex-encoded (32 hex chars, same 128 bits of entropy
// as a UUIDv4) -- google/uuid gives the same randomness in the standard
// RFC 4122 UUIDv4 layout instead of an unstructured hex blob, which is
// what every other modern engine's document/row IDs look like, and what
// client drivers/tools generally expect to see and be able to parse.
func newID() string {
	return uuid.New().String()
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	for _, ch := range name {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' && ch != '-' {
			return fmt.Errorf("invalid name '%s': only letters, digits, _ and - allowed", name)
		}
	}
	return nil
}

func labelEsc(s string) string {
	var b strings.Builder
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_' {
			b.WriteRune(ch)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func jsonOut(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	enc.Encode(v)
	return strings.TrimRight(buf.String(), "\n")
}

func fmtBytes(n int64) string {
	const (
		KB int64 = 1024
		MB       = 1024 * KB
		GB       = 1024 * MB
		TB       = 1024 * GB
		PB       = 1024 * TB
	)
	switch {
	case n >= PB:
		return fmt.Sprintf("%.3f PB", float64(n)/float64(PB))
	case n >= TB:
		return fmt.Sprintf("%.3f TB", float64(n)/float64(TB))
	case n >= GB:
		return fmt.Sprintf("%.3f GB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(KB))
	}
	return fmt.Sprintf("%d B", n)
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func getMemStats() map[string]any {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return map[string]any{
		"alloc":       fmtBytes(int64(m.Alloc)),
		"total_alloc": fmtBytes(int64(m.TotalAlloc)),
		"sys":         fmtBytes(int64(m.Sys)),
		"num_gc":      m.NumGC,
		"pause_total": fmtBytes(int64(m.PauseTotalNs)),
	}
}
