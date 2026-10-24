package storage

// Badger tuning constants used when opening a block's BadgerDB handle.
// These mirror the values in the root package's constants.go: storage is
// imported by the root caimandb package, so importing back would create a
// cycle. Keep this in sync if the root values change.
const (
	badgerValueLogSize       = 64 << 20
	badgerMemTableSize       = 128 << 20
	badgerNumMemTables       = 4
	badgerNumLevelZeroTables = 8
	badgerBlockCacheSize     = 512 << 20
	badgerValueThreshold     = 1024
)
