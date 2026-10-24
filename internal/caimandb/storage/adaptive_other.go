//go:build !linux && !windows && !darwin

package storage

// detectSystemMemoryBytesOS has no implementation for this OS. Returning 0
// tells the generic caller in adaptive.go to use defaultSystemMemoryBytes
// -- the same conservative behavior every OS had before per-OS detection
// existed, now scoped to only the OSes that actually still need it.
func detectSystemMemoryBytesOS() int64 {
	return 0
}
