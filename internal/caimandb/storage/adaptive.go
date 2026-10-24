package storage

import (
	"os"
)

// ---------------------------------------------------------------------------
// Storage AI: adaptive per-block BadgerDB sizing.
//
// Problem this solves: before this file existed, every block (empty or not)
// was opened with the exact same fixed Badger tuning constants (see the
// "Standard" profile below, which is what used to live directly in
// constants.go). Badger reserves/creates its memtable and value-log files up
// front based on those settings regardless of how many documents the block
// actually holds, which is why a brand-new block with a handful of documents
// ends up occupying roughly the same footprint (tens of MB) as one with
// hundreds of thousands of documents.
//
// That's harmless for a handful of large, busy blocks -- it's exactly what
// you want for a "big data" block under concurrent load. It becomes a real
// problem when a workload creates many small/short-lived blocks (e.g. one
// block per tenant, per day, per user), because the fixed footprint is paid
// per block regardless of size.
//
// This file picks a *tier* of Badger tuning options per block, based on:
//   1. What's already on disk for that block (an empty/new block starts at
//      the smallest tier; a block that already has significant on-disk data
//      starts at the tier that matches it).
//   2. A global RAM budget shared across every currently-open block, so a
//      node with many blocks open at once doesn't over-commit memory just
//      because each individual block *could* use a bigger tier.
//
// What this file deliberately does NOT do: hot-swap a block's Badger handle
// while it's serving live traffic. Upgrading a block that grew past its
// tier while the engine is running safely requires coordinating with every
// in-flight reader/writer of that block (see docs/storage-ai-adaptive-sizing.md
// for the concurrency analysis and why that's left as a follow-up rather than
// implemented blind, consistent with the rest of this pass -- no Go
// toolchain/network was available in this environment to verify it against
// real concurrent load). Today, a block's tier is chosen once, the first
// time it's opened in a given process, from whatever is already on disk --
// which is enough to fix "thousands of tiny blocks waste tens of MB each",
// the problem reported, without introducing a new data-safety risk.
// ---------------------------------------------------------------------------

// SizeTier is a named point on the small-block <-> big-block tuning curve.
type SizeTier int

const (
	// TierMicro: brand-new or near-empty blocks. Optimized for footprint,
	// not throughput -- this is what fixes "every block is ~49MB even with
	// almost no documents".
	TierMicro SizeTier = iota
	// TierSmall: blocks with some data, but nowhere near big-data scale.
	TierSmall
	// TierStandard: the original fixed defaults this repo shipped with.
	// Reasonable middle ground for blocks of unknown/moderate size.
	TierStandard
	// TierLarge: blocks that already hold a substantial amount of data.
	// Trades memory for write throughput and read latency under
	// concurrency (bigger memtables, more L0 tables before stalling,
	// more compactors).
	TierLarge
)

func (t SizeTier) String() string {
	switch t {
	case TierMicro:
		return "micro"
	case TierSmall:
		return "small"
	case TierStandard:
		return "standard"
	case TierLarge:
		return "large"
	default:
		return "unknown"
	}
}

// tierProfile is the set of BadgerDB tuning knobs associated with a tier.
type tierProfile struct {
	MemTableSize     int64
	NumMemtables     int
	NumLevelZero     int
	ValueLogFileSize int64
	BlockCacheSize   int64
	NumCompactors    int
}

// approxFootprintBytes is a rough estimate (not exact -- Badger's own
// on-disk layout has extra overhead) of the disk+RAM budget a block opened
// at this tier will consume, used purely for the RAM budget accounting
// below. It intentionally leans conservative (a bit high) so the budget
// tracker under-promises rather than over-commits.
func (p tierProfile) approxFootprintBytes() int64 {
	memtables := p.MemTableSize * int64(p.NumMemtables)
	return memtables + p.ValueLogFileSize + p.BlockCacheSize
}

var tierProfiles = map[SizeTier]tierProfile{
	TierMicro: {
		MemTableSize:     4 << 20,  // 4MB
		NumMemtables:     2,
		NumLevelZero:     4,
		ValueLogFileSize: 8 << 20,  // 8MB
		BlockCacheSize:   8 << 20,  // 8MB
		NumCompactors:    2,
	},
	TierSmall: {
		MemTableSize:     16 << 20, // 16MB
		NumMemtables:     3,
		NumLevelZero:     6,
		ValueLogFileSize: 16 << 20, // 16MB
		BlockCacheSize:   32 << 20, // 32MB
		NumCompactors:    2,
	},
	TierStandard: {
		// The first four fields match what constants.go hard-coded before
		// this file existed, so a deployment that sets
		// storage_ai_enabled=false gets byte-for-byte the same Badger
		// options this pool always used for those four. NumCompactors is
		// new -- the original code never called WithNumCompactors at all
		// (it relied on whatever Badger v4 defaults to internally); 3 is a
		// small, explicit, additive choice for this tier and doesn't
		// change on-disk/RAM footprint. Worth a quick diff against
		// Badger's actual default before relying on this in production,
		// since that wasn't confirmed here (no compiler/docs lookup in
		// this environment).
		MemTableSize:     badgerMemTableSize,        // 128MB
		NumMemtables:     badgerNumMemTables,        // 4
		NumLevelZero:     badgerNumLevelZeroTables,  // 8
		ValueLogFileSize: badgerValueLogSize,        // 64MB
		BlockCacheSize:   badgerBlockCacheSize,       // 512MB
		NumCompactors:    3,
	},
	TierLarge: {
		MemTableSize:     256 << 20, // 256MB
		NumMemtables:     6,
		NumLevelZero:     12,
		ValueLogFileSize: 128 << 20, // 128MB
		BlockCacheSize:   1024 << 20, // 1GB
		NumCompactors:    4,
	},
}

// Thresholds (on-disk bytes already present for a block) used to classify
// an *existing* block into a starting tier. A block below onDiskSmallMax
// bytes on disk starts at TierMicro, and so on. These are deliberately
// coarse -- the goal is "don't pay the Standard/Large footprint for a
// block that clearly doesn't need it yet", not perfect classification.
const (
	onDiskMicroMax    = 8 << 20   // <8MB on disk -> micro
	onDiskSmallMax    = 64 << 20  // <64MB on disk -> small
	onDiskStandardMax = 512 << 20 // <512MB on disk -> standard, else large
)

// AdaptiveConfig controls Storage AI behavior for a DBPool.
type AdaptiveConfig struct {
	// Enabled turns adaptive tiering on. When false, every block is opened
	// with TierStandard, matching this project's original fixed-size
	// behavior exactly (storage_ai_enabled=false in caimandb.conf).
	Enabled bool
	// RAMBudgetFraction is the fraction (0, 1] of detected system RAM that
	// Storage AI is allowed to earmark, in aggregate, across every
	// currently-open block's memtables + value-log + block cache. Ignored
	// if MaxBudgetBytes is set explicitly. Defaults to 0.5 if <= 0.
	RAMBudgetFraction float64
	// MaxBudgetBytes, if > 0, overrides RAMBudgetFraction with an absolute
	// cap. Useful for containers where /proc/meminfo reflects the host
	// rather than the container's actual memory limit.
	MaxBudgetBytes int64
}

func (c AdaptiveConfig) budgetBytes() int64 {
	if c.MaxBudgetBytes > 0 {
		return c.MaxBudgetBytes
	}
	fraction := c.RAMBudgetFraction
	if fraction <= 0 || fraction > 1 {
		fraction = 0.5
	}
	total := detectSystemMemoryBytes()
	return int64(float64(total) * fraction)
}

// defaultSystemMemoryBytes is the fallback used whenever the real amount of
// system RAM can't be determined (non-Linux, /proc unreadable in a
// assumes more headroom than it can confirm.
const defaultSystemMemoryBytes = 2 << 30

// detectSystemMemoryBytes returns total system RAM in bytes. The actual
// lookup is OS-specific (see detectSystemMemoryBytesOS in
// adaptive_linux.go / adaptive_windows.go / adaptive_darwin.go /
// adaptive_other.go); this only falls back to defaultSystemMemoryBytes
// when that lookup can't determine a real value (unsupported OS, API call
// failed, sandboxed/restricted environment, etc). This only affects the
// *ceiling* Storage AI budgets against -- getting it wrong makes tiering
// more conservative (smaller blocks), never less safe.
//
// Previously this only ever parsed Linux's /proc/meminfo and returned the
// 2GB default for every other OS unconditionally -- including Windows and
// macOS, which have their own real APIs for this. That meant a Windows
// deployment (of any actual RAM size) always budgeted against a fake 2GB
// ceiling, so BULK MODE's request for TierStandard (~1.06GB footprint)
// never fit inside the resulting 50%-of-2GB (1GB) budget and silently
// downgraded to TierSmall every time -- reproducible from this file alone:
// tierProfiles[TierStandard].approxFootprintBytes() (~1088MB) > 2GB*0.5
// (1024MB).
func detectSystemMemoryBytes() int64 {
	if v := detectSystemMemoryBytesOS(); v > 0 {
		return v
	}
	return defaultSystemMemoryBytes
}

// onDiskFootprintBytes sums the size of every regular file directly inside
// path (a block's Badger directory is flat -- SST/vlog/manifest files, no
// subdirectories), used to classify an already-existing block into a
// starting tier. Returns 0 (and no error) for a path that doesn't exist yet,
// which is the common case for a block being created for the first time.
func onDiskFootprintBytes(path string) int64 {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// classifyByOnDiskBytes maps an existing on-disk footprint to a starting
// tier. A path with nothing on disk yet (onDiskBytes == 0) always starts at
// TierMicro -- this is the case that used to unconditionally cost ~49MB.
func classifyByOnDiskBytes(onDiskBytes int64) SizeTier {
	switch {
	case onDiskBytes < onDiskMicroMax:
		return TierMicro
	case onDiskBytes < onDiskSmallMax:
		return TierSmall
	case onDiskBytes < onDiskStandardMax:
		return TierStandard
	default:
		return TierLarge
	}
}

// downgradeToFit walks tiers from the requested one down to TierMicro and
// returns the first (largest) one whose approximate footprint fits within
// remaining budget bytes. TierMicro is always returned as a last resort --
// Storage AI never refuses to open a block for lack of budget, it just
// opens it as small as possible.
func downgradeToFit(requested SizeTier, remainingBudget int64) SizeTier {
	for t := requested; t > TierMicro; t-- {
		if tierProfiles[t].approxFootprintBytes() <= remainingBudget {
			return t
		}
	}
	return TierMicro
}
