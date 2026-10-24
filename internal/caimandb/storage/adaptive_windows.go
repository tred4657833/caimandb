//go:build windows

package storage

import (
	"syscall"
	"unsafe"
)

// memoryStatusEx mirrors the Win32 MEMORYSTATUSEX struct. Field order and
// sizes must match the C layout exactly since a pointer to this is passed
// straight to GlobalMemoryStatusEx -- only dwLength (required by the API
// as an in-parameter so it can validate the struct version) and
// ullTotalPhys (what this file actually wants) are used, but every field
// up to ullTotalPhys must still be present so ullTotalPhys lands at the
// right offset.
type memoryStatusEx struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

// detectSystemMemoryBytesOS calls the Win32 GlobalMemoryStatusEx API to
// read total physical RAM, using only the standard library's syscall
// package (LazyDLL/LazyProc) -- no golang.org/x/sys/windows dependency
// needed just for this one call. Returns 0 (never a guess) if the DLL/proc
// can't be resolved or the call fails, so the generic caller in
// adaptive.go falls back to defaultSystemMemoryBytes.
//
// Before this file existed, Storage AI had no Windows implementation at
// all and silently used the 2GB fallback on every Windows deployment
// regardless of actual RAM -- see the comment on detectSystemMemoryBytes
// in adaptive.go for exactly how that produced the "storage_tier":"small"
// downgrade under BULK MODE even on machines with plenty of RAM.
func detectSystemMemoryBytesOS() int64 {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")
	if err := globalMemoryStatusEx.Find(); err != nil {
		return 0
	}

	var status memoryStatusEx
	status.dwLength = uint32(unsafe.Sizeof(status))

	ret, _, _ := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if ret == 0 {
		return 0
	}
	return int64(status.ullTotalPhys)
}
