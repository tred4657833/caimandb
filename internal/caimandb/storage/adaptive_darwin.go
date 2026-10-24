//go:build darwin

package storage

import "syscall"

// detectSystemMemoryBytesOS reads total physical RAM via the hw.memsize
// sysctl using the standard library's syscall.SysctlUint64 helper (built
// exactly for numeric sysctls like this one -- no new dependency needed).
// Returns 0 (never a guess) on failure, so the generic caller in
// adaptive.go falls back to defaultSystemMemoryBytes. Same underlying gap
// as Windows before this file existed: macOS had no implementation and
// silently used the 2GB fallback regardless of the machine's actual RAM.
func detectSystemMemoryBytesOS() int64 {
	v, err := syscall.SysctlUint64("hw.memsize")
	if err != nil || v == 0 {
		return 0
	}
	return int64(v)
}
