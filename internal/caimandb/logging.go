package caimandb

import (
	"caimandb/internal/caimandb/logging"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync/atomic"
)

var globalLogger *zap.Logger

var logLevel atomic.Value

func initLogger(level string) {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.CallerKey = "caller"
	config.EncoderConfig.StacktraceKey = "stacktrace"

	var lvl zap.AtomicLevel
	switch level {
	case "debug":
		lvl = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		lvl = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		lvl = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		lvl = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		lvl = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	config.Level = lvl
	logLevel.Store(lvl)

	logger, err := config.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	globalLogger = logger

	// Keep the low-level infrastructure packages (storage, wal, cluster,
	// ...) logging through the same configured sink/level.
	logging.SetLogger(logger)
}

func log() *zap.Logger {
	if globalLogger == nil {
		initLogger("info")
	}
	return globalLogger
}

// safeGo/safeGoLoop are thin, package-local aliases for the shared
// logging.SafeGo/SafeGoLoop helpers (see internal/caimandb/logging for the
// full rationale). Every background goroutine started from this package
// (cleanup loops, rate limiting, auto-tune/auto-scale, FLEX-COLUMN
// maintenance, async indexing, cluster health checks, ...) must go through
// one of these instead of a bare `go func() { ... }()`, so a panic in any
// one of them logs and dies instead of taking the whole node down.
func safeGo(name string, fn func()) {
	logging.SafeGo(name, fn)
}

func safeGoLoop(name string, fn func()) {
	logging.SafeGoLoop(name, fn)
}
