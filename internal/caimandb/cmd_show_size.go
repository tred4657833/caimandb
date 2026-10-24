package caimandb

import (
	"fmt"
	"strings"
	"time"
)

// selectNames returns the subset of `all` that match `wanted` (exact
// match), preserving the order of `all` -- wanted is a name list the user
// typed, so its own order isn't meaningful, but a stable output order is.
// If wanted is empty, every entry in `all` is returned and nothing is
// reported missing. Anything in `wanted` not present in `all` comes back
// in `missing` so the caller can tell the user which name(s) didn't match
// instead of silently dropping them.
func selectNames(all []string, wanted []string) (selected []string, missing []string) {
	if len(wanted) == 0 {
		return all, nil
	}
	present := make(map[string]bool, len(all))
	for _, n := range all {
		present[n] = true
	}
	seen := make(map[string]bool, len(wanted))
	var missingSeen = make(map[string]bool, len(wanted))
	for _, name := range all {
		for _, w := range wanted {
			if w == name && !seen[name] {
				seen[name] = true
				selected = append(selected, name)
			}
		}
	}
	for _, w := range wanted {
		if !present[w] && !missingSeen[w] {
			missingSeen[w] = true
			missing = append(missing, w)
		}
	}
	return selected, missing
}

// handleShowDBs implements "SHOW DBS" (every database) and
// "SHOW DBS <name> [<name2> ...]" (one or more specific databases). Both
// forms render the same table, which now includes each database's total
// on-disk size and document count -- not just its block count like the
// original SHOW DBS did.
func handleShowDBs(eng *Engine, sess *Session, names []string) string {
	all, err := eng.ShowDBs()
	if err != nil {
		return "ERROR: " + err.Error()
	}
	// Aislamiento: un ADMIN/SUBADMIN/DEVELOPER/USER solo ve su propia
	// base; el Admin General (o la consola local sin autenticar) ve
	// todas. Ver hierarchy.go.
	if sess != nil && sess.authRole != "" && !IsGeneralAdmin(sess.authRole) {
		var owned []string
		for _, db := range all {
			if db == sess.authDatabase {
				owned = append(owned, db)
			}
		}
		all = owned
	}
	if len(all) == 0 {
		return "No databases found"
	}

	dbs, missing := selectNames(all, names)
	if len(dbs) == 0 {
		return "ERROR: no matching database(s): " + strings.Join(missing, ", ")
	}

	var sb strings.Builder
	sb.WriteString("Databases:\n")
	sb.WriteString(fmt.Sprintf("%-24s %-10s %-12s %-14s\n", "NAME", "BLOCKS", "DOCS", "SIZE"))
	sb.WriteString(strings.Repeat("-", 64) + "\n")

	var totalDocs int64
	var totalSize int64
	for _, db := range dbs {
		blocks, _ := eng.ShowBlocks(db)
		var docCount int64
		for _, b := range blocks {
			c, _ := eng.Count(db, b, nil, nil)
			docCount += c
		}
		size, _ := dirSize(eng.dirMgr.DBPath(db))
		totalDocs += docCount
		totalSize += size

		name := db
		if len(name) > 23 {
			name = name[:23]
		}
		sb.WriteString(fmt.Sprintf("%-24s %-10d %-12d %-14s\n",
			name, len(blocks), docCount, fmtBytes(size)))
	}
	sb.WriteString(strings.Repeat("-", 64) + "\n")
	sb.WriteString(fmt.Sprintf("Total: %d database(s), %d docs, %s\n", len(dbs), totalDocs, fmtBytes(totalSize)))
	if len(missing) > 0 {
		sb.WriteString("Not found: " + strings.Join(missing, ", ") + "\n")
	}
	return sb.String()
}

// handleShowBlocks implements "SHOW BLOCKS" (blocks of the current
// database), "SHOW BLOCKS <db>" (blocks of a specific database), and
// "SHOW BLOCKS <db> <block1> [<block2> ...]" (one or more specific blocks
// in that database). args is everything after the "BLOCKS" keyword.
func handleShowBlocks(eng *Engine, sess *Session, args []string) string {
	db := sess.currentDB
	var filterNames []string
	if len(args) > 0 && !strings.EqualFold(args[0], "WHERE") && !strings.EqualFold(args[0], "LIMIT") {
		db = args[0]
		filterNames = args[1:]
	}
	if db == "" {
		return "ERROR: No database specified and no database in use"
	}
	if err := requireDBAccess(sess, db); err != nil {
		return "ERROR: " + err.Error()
	}

	all, err := eng.ShowBlocks(db)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	if len(all) == 0 {
		return "No blocks found in database: " + db
	}

	blocks, missing := selectNames(all, filterNames)
	if len(blocks) == 0 {
		return "ERROR: no matching block(s) in " + db + ": " + strings.Join(missing, ", ")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Blocks in %s:\n", db))
	sb.WriteString(fmt.Sprintf("%-20s %-10s %-12s %-15s %-20s\n", "NAME", "DOCS", "SIZE", "SHARDS", "CREATED"))
	sb.WriteString(strings.Repeat("-", 80) + "\n")

	var totalDocs int64
	var totalSize int64
	for _, block := range blocks {
		info, err := eng.DescribeBlock(db, block)
		if err != nil {
			sb.WriteString(fmt.Sprintf("  %s (error: %v)\n", block, err))
			continue
		}
		name := block
		if len(name) > 19 {
			name = name[:19]
		}
		docCount := int64(0)
		if v, ok := info["documents"].(int64); ok {
			docCount = v
		}
		var sizeBytes int64
		size := "0 B"
		if v, ok := info["size_bytes"].(int64); ok {
			sizeBytes = v
			size = fmtBytes(v)
		}
		created := "N/A"
		if v, ok := info["created_at"].(int64); ok {
			created = time.Unix(v, 0).Format("2006-01-02 15:04")
		}
		shards := "N/A"
		if v, ok := info["shards"].(int); ok {
			shards = fmt.Sprintf("%d", v)
		} else if v, ok := info["shards"].(string); ok {
			shards = v
		}
		totalDocs += docCount
		totalSize += sizeBytes
		sb.WriteString(fmt.Sprintf("%-20s %-10d %-12s %-15s %-20s\n",
			name, docCount, size, shards, created))
	}
	sb.WriteString(strings.Repeat("-", 80) + "\n")
	sb.WriteString(fmt.Sprintf("Total blocks: %d, docs: %d, size: %s\n", len(blocks), totalDocs, fmtBytes(totalSize)))
	if len(missing) > 0 {
		sb.WriteString("Not found: " + strings.Join(missing, ", ") + "\n")
	}
	return sb.String()
}
