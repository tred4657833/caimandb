package caimandb

import (
	"fmt"
	"strings"
)

// handleBulkCmd implements:
//
//	BULK MODE ON       -- widen turbo batch windows, relax WAL fsync policy
//	BULK MODE OFF       -- restore normal low-latency batching/fsync policy
//	BULK STATUS         -- show current turbo engine stats
//
// Intended for large loads: turn it on before a big run of INSERTs (or
// before an IMPORT), turn it back off when done. Standalone installs only
// -- see InsertFast for why cluster mode doesn't use the turbo batcher.
func handleBulkCmd(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 2 {
		return "ERROR: usage: BULK MODE ON|OFF, or BULK STATUS"
	}

	switch strings.ToUpper(tokens[1]) {
	case "STATUS":
		return jsonOut(eng.TurboStats())

	case "MODE":
		if len(tokens) < 3 {
			return "ERROR: usage: BULK MODE ON|OFF"
		}
		switch strings.ToUpper(tokens[2]) {
		case "ON":
			eng.SetBulkMode(true)
			return "Bulk mode ON: batches widened, WAL fsync relaxed until BULK MODE OFF"
		case "OFF":
			eng.SetBulkMode(false)
			return "Bulk mode OFF: normal batching/fsync policy restored"
		default:
			return fmt.Sprintf("ERROR: unknown BULK MODE argument %q -- expected ON or OFF", tokens[2])
		}

	default:
		return fmt.Sprintf("ERROR: unknown BULK subcommand %q -- expected MODE or STATUS", tokens[1])
	}
}
