// Command crashtestworker exists purely to be killed mid-write.
//
// It is not part of CaimanDB's public surface -- it's a test fixture for
// internal/caimandb's crash-recovery integration test (see
// crash_recovery_test.go), which needs a *separate OS process* actually
// dying to simulate a real crash. Abandoning an *Engine value from
// within the same Go test process doesn't do that: the process (and
// with it, Badger's directory lock and the WAL's open file handle) stays
// alive regardless of what the test's own variables point at, so a
// second Engine opened against the same DataRoot would just fail to
// acquire Badger's lock -- correctly, but not usefully, since that's not
// what a real crash leaves behind. A real OS process actually exiting
// (via Process.Kill(), which is a hard, unconditional termination on
// every OS Go supports -- SIGKILL-equivalent semantics on Windows too,
// not just Unix) is what releases those OS-level resources the way a
// genuine crash would, leaving whatever was actually flushed to disk at
// that instant and nothing more.
//
// It keeps inserting documents forever (or until -count is reached) and
// deliberately never calls Engine.Shutdown -- an orderly shutdown is
// plain "PROGRESS <n>" lines the parent test reads to know how far
// insertion got before it killed this process; there's no buffering to
// worry about flushing since fmt.Println writes straight to os.Stdout.
package main

import (
	"flag"
	"fmt"
	"os"

	"caimandb/internal/caimandb"
)

func main() {
	dataRoot := flag.String("dataroot", "", "DataRoot for the engine (required)")
	dbName := flag.String("db", "crashtest", "database name")
	block := flag.String("block", "block1", "block name")
	totalDocs := flag.Int("count", 1_000_000, "stop after this many documents (mainly a safety cap; the parent test is expected to kill this process long before it's reached)")
	batchSize := flag.Int("batch", 20, "documents per InsertBatch call")
	flag.Parse()

	if *dataRoot == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -dataroot is required")
		os.Exit(2)
	}

	cfg := caimandb.DefaultConfig()
	cfg.DataRoot = *dataRoot
	cfg.AutoCluster = false
	cfg.AutoSplitEnabled = false
	cfg.AutoMergeEnabled = false
	cfg.LargeDocThreshold = 0
	// Deliberately not "off": this process exists to be killed mid-write,
	// so the WAL needs to actually be durable on disk for there to be
	// anything for the parent test's recovery pass to replay. With
	// sync policy "off" every crash here would just be equivalent to
	// losing everything back to the last incidental OS flush, which
	// isn't the scenario worth testing.
	cfg.WALSyncPolicy = "always"

	e := caimandb.NewEngine(cfg)
	// meant to be killed, not to exit cleanly.

	if err := e.CreateDB(*dbName); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: CreateDB: %v\n", err)
		os.Exit(1)
	}
	if err := e.CreateBlock(*dbName, *block); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: CreateBlock: %v\n", err)
		os.Exit(1)
	}

	// Signals to the parent test that engine startup and DB/block
	// creation are done and the insert loop below is about to start --
	// so it knows a kill from this point on lands mid-workload, not
	// mid-startup.
	fmt.Println("READY")

	inserted := 0
	for inserted < *totalDocs {
		n := *batchSize
		if *totalDocs-inserted < n {
			n = *totalDocs - inserted
		}
		docs := make([]map[string]any, n)
		for i := range docs {
			docs[i] = map[string]any{"seq": inserted + i, "tag": "crashtest"}
		}
		ins, errs := e.InsertBatch(*dbName, *block, docs)
		if errs != 0 {
			fmt.Fprintf(os.Stderr, "ERROR: InsertBatch reported %d errors\n", errs)
			os.Exit(1)
		}
		inserted += int(ins)
		fmt.Printf("PROGRESS %d\n", inserted)
	}

	fmt.Println("DONE")
}
