// Command caimandb is the entry point for the CaimanDB server/CLI.
// All actual logic lives in the internal/caimandb package; this file
// intentionally stays thin.
package main

import "caimandb/internal/caimandb"

func main() {
	caimandb.Run()
}
