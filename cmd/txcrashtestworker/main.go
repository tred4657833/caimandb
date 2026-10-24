// Command txcrashtestworker is an external-process fixture used to test
// transaction recovery after an unconditional process kill. It intentionally
// never calls Engine.Shutdown when the configured chaos point is reached.
package main

import (
	"flag"
	"fmt"
	"os"

	"caimandb/internal/caimandb"
)

func main() {
	dataRoot := flag.String("dataroot", "", "DataRoot (required)")
	mode := flag.String("mode", "update", "transaction mode: update or delete")
	count := flag.Int("count", 100, "number of seed documents")
	flag.Parse()

	if *dataRoot == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -dataroot is required")
		os.Exit(2)
	}
	if *mode != "update" && *mode != "delete" {
		fmt.Fprintln(os.Stderr, "ERROR: -mode must be update or delete")
		os.Exit(2)
	}

	cfg := caimandb.DefaultConfig()
	cfg.DataRoot = *dataRoot
	cfg.AutoCluster = false
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.LargeDocThreshold = 0
	cfg.WALSyncPolicy = "always"
	cfg.CacheMB = 64
	cfg.L2CacheMB = 64

	e := caimandb.NewEngine(cfg)
	if err := e.CreateDB("txcrashtest"); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: CreateDB: %v\n", err)
		os.Exit(1)
	}
	if err := e.CreateBlock("txcrashtest", "block1"); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: CreateBlock: %v\n", err)
		os.Exit(1)
	}

	docs := make([]map[string]any, *count)
	for i := range docs {
		docs[i] = map[string]any{
			"id":    fmt.Sprintf("txdoc-%06d", i),
			"seq":   i,
			"state": "before",
			"tag":   "tx-chaos",
		}
	}
	inserted, errs := e.InsertBatch("txcrashtest", "block1", docs)
	if errs != 0 || inserted != int64(*count) {
		fmt.Fprintf(os.Stderr, "ERROR: seed InsertBatch inserted=%d errs=%d\n", inserted, errs)
		os.Exit(1)
	}
	fmt.Println("SEEDED", inserted)

	tx, err := e.TxManagerForTestBegin("txcrashtest", "block1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Begin: %v\n", err)
		os.Exit(1)
	}

	filters := []caimandb.Filter{{Field: "tag", Op: "=", Value: "tx-chaos"}}
	var n int64
	if *mode == "update" {
		n, err = e.TxManagerForTestUpdate(tx, filters, caimandb.UpdateOp{Set: map[string]any{"state": "after"}})
	} else {
		n, err = e.TxManagerForTestDelete(tx, filters)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: stage %s: %v\n", *mode, err)
		os.Exit(1)
	}
	fmt.Println("STAGED", n)
	fmt.Println("READY")
	if err := e.TxManagerForTestCommit(tx); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Commit: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("COMMITTED")
}
