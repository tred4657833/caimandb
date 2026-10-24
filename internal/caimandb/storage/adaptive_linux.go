//go:build linux

package storage

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// detectSystemMemoryBytesOS parses /proc/meminfo's MemTotal line. Returns 0
// (never a guess) on any failure, so the generic caller in adaptive.go
// falls back to defaultSystemMemoryBytes -- this file's only job is "return
// a real number if possible, or 0", never to decide what the fallback is.
func detectSystemMemoryBytesOS() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		// Expected shape: "MemTotal:", "<kB>", "kB"
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || kb <= 0 {
			return 0
		}
		return kb * 1024
	}
	return 0
}
