// Package logging provides a shared zap logger for CaimanDB's low-level
// infrastructure packages (storage, wal, cluster, ...), which cannot
// import the root caimandb package (that would create an import cycle)
// and so cannot use its log() accessor directly.
//
// The root package installs its configured logger via SetLogger during
// startup (see caimandb.Run), so in a running server everything logs
// through one shared, consistently-configured sink. Until SetLogger is
// called, Log() lazily falls back to a sane production default so these
// packages remain independently usable (e.g. in tests).
package logging

import (
	"runtime/debug"

	"go.uber.org/zap"
)

var logger *zap.Logger

// SetLogger installs the shared logger used by infrastructure packages.
func SetLogger(l *zap.Logger) {
	logger = l
}

// Log returns the shared logger, lazily initializing a default one if
// SetLogger hasn't been called yet.
func Log() *zap.Logger {
	if logger == nil {
		l, err := zap.NewProduction()
		if err != nil {
			l = zap.NewNop()
		}
		logger = l
	}
	return logger
}

// SafeGo launches fn in its own goroutine with panic recovery. CaimanDB is
// meant to run 24/7 with dozens of long-lived background loops (cleanup,
// rate limiting, auto-tune/auto-scale, FLEX-COLUMN maintenance, caches,
// cluster health checks, async indexing, ...). In Go, an unrecovered panic
// in ANY goroutine takes down the entire process, not just that goroutine
// -- so a bug in one minor cleanup loop can crash a node that is otherwise
// healthy and serving traffic. Every background goroutine in the codebase
// should be started through SafeGo (or SafeGoLoop) instead of a bare
// `go func() { ... }()`.
//
// The panic is logged with its stack trace and swallowed: the goroutine
// exits but the process keeps running.
func SafeGo(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				Log().Error("Recovered panic in background goroutine",
					zap.String("goroutine", name),
					zap.Any("panic", r),
					zap.String("stack", string(debug.Stack())))
			}
		}()
		fn()
	}()
}

// SafeGoLoop is like SafeGo but for the common "runs forever" background
// loop pattern: if fn panics, SafeGoLoop recovers, logs, and restarts fn
// in a fresh goroutine instead of letting the loop die permanently or
// taking the whole process down with it.
func SafeGoLoop(name string, fn func()) {
	var run func()
	run = func() {
		defer func() {
			if r := recover(); r != nil {
				Log().Error("Recovered panic in background loop, restarting",
					zap.String("goroutine", name),
					zap.Any("panic", r),
					zap.String("stack", string(debug.Stack())))
				go run()
			}
		}()
		fn()
	}
	go run()
}
