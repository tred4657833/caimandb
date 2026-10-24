// ops_named_index.go implements the engine side of the new
// CREATE [UNIQUE] INDEX / DROP INDEX / SHOW INDEXES ON <block> syntax
// (see cmd_index_ddl.go for the DSL command handlers and
// engine_core.go for the NamedIndexDef/DBConfig.NamedIndexes fields).
package caimandb

import (
	"fmt"
	"time"
)

// CreateNamedIndex registers a named index definition for dbName. The
// underlying Badger secondary index for the field already exists (or
// will exist on the next write) regardless -- buildSecondaryIndexInto
// indexes every simple-typed field on every document unconditionally
// (index_secondary.go) -- so this call itself never has to touch
// storage or rewrite existing documents. It:
//   - records the definition (name/block/field/unique/where) so
//     SHOW INDEXES and DROP INDEX have something to look up;
//   - makes sure the field is in cfg.Indexes too, so the query
//     optimizer's index-selection heuristic (AnalyzeQuery) considers
//     it for WHERE <field> = <value> lookups;
//   - for UNIQUE indexes, is consulted by checkUniqueConstraints on
//     every future INSERT into that block (not retroactively: existing
//     duplicate values already in the block are not checked here).
//
// TODO(pendiente-motor): the `where` argument (CREATE INDEX ... WHERE
// <condición>, a partial index) is only stored on NamedIndexDef today
// -- see engine_core.go's DBConfig.NamedIndexes comment. Nothing
// currently reads idx.Where to decide whether a given document should
// be considered indexed, so a "partial" index behaves exactly like a
// full one. Enforcing it means evaluating idx.Where (needs parsing
// into a *parse.Expr once, likely cached on NamedIndexDef, then
// matched per-document with the same matchesQuery/matchesWhere
// machinery query_filter.go already has) at the point a document is
// (a) written -- buildSecondaryIndexInto in index_secondary.go decides
// what gets indexed -- and (b) checked for uniqueness --
// checkUniqueConstraints below, which currently indexes/checks every
// document unconditionally.
func (e *Engine) CreateNamedIndex(dbName, name, block, field string, unique bool, where string) error {
	if name == "" {
		return fmt.Errorf("index name required")
	}
	if field == "" {
		return fmt.Errorf("index field required")
	}
	cfg, ok := e.intelEngine.GetConfig(dbName)
	if !ok {
		return fmt.Errorf("database not found: %s", dbName)
	}

	e.intelEngine.mu.Lock()
	defer e.intelEngine.mu.Unlock()

	if cfg.NamedIndexes == nil {
		cfg.NamedIndexes = make(map[string]NamedIndexDef)
	}
	if existing, exists := cfg.NamedIndexes[name]; exists {
		return fmt.Errorf("index already exists: %s (on %s.%s)", name, existing.Block, existing.Field)
	}
	cfg.NamedIndexes[name] = NamedIndexDef{
		Name:   name,
		Block:  block,
		Field:  field,
		Unique: unique,
		Where:  where,
	}
	if !e.intelEngine.hasIndex(cfg, field) {
		e.intelEngine.addIndex(cfg, field)
	}
	cfg.UpdatedAt = time.Now().Unix()
	return nil
}

// DropNamedIndex removes a named index definition. block is optional
// (pass "" to skip the check); when given, it must match the index's
// recorded block, mirroring "DROP INDEX <name> ON <block>" validating
// the name actually belongs to that block. This never removes the
// field from cfg.Indexes (another named index, or the automatic
// suggestIndexes pass, may still legitimately want it indexed) and
// never touches the underlying Badger secondary index, which -- as in
// CreateNamedIndex -- is not tied 1:1 to a named index at all.
func (e *Engine) DropNamedIndex(dbName, name, block string) error {
	cfg, ok := e.intelEngine.GetConfig(dbName)
	if !ok {
		return fmt.Errorf("database not found: %s", dbName)
	}

	e.intelEngine.mu.Lock()
	defer e.intelEngine.mu.Unlock()

	idx, exists := cfg.NamedIndexes[name]
	if !exists {
		return fmt.Errorf("index not found: %s", name)
	}
	if block != "" && idx.Block != block {
		return fmt.Errorf("index %s is on block %s, not %s", name, idx.Block, block)
	}
	delete(cfg.NamedIndexes, name)
	cfg.UpdatedAt = time.Now().Unix()
	return nil
}

// checkUniqueConstraints enforces every UNIQUE named index defined on
// block against data, before it's written. skipID excludes one
// document ID from the check (unused by INSERT today, where it's
// always "" since the document doesn't exist yet; kept so a future
// UPDATE-path enforcement can exclude the document being updated
// instead of comparing it against itself).
//
// TODO(pendiente-motor) -- known limitations (documented rather than
// silently partial):
//   - only wired into the plain Insert path -- InsertFast/turbo and
//     InsertBatch/bulk-import bypass it for throughput, same as they
//     already bypass other per-document checks;
//   - not enforced on UPDATE, so an update can still introduce a
//     duplicate value into a UNIQUE field;
//   - a lookupByIndex failure is treated as "no conflict found"
//     (best-effort enforcement) rather than blocking the write, so it
//     never turns a storage hiccup into a false constraint violation.
func (e *Engine) checkUniqueConstraints(dbName, block string, data map[string]any, skipID string) error {
	cfg, ok := e.intelEngine.GetConfig(dbName)
	if !ok || len(cfg.NamedIndexes) == 0 {
		return nil
	}
	for _, idx := range cfg.NamedIndexes {
		if !idx.Unique || idx.Block != block {
			continue
		}
		v, present := data[idx.Field]
		if !present || v == nil {
			continue
		}
		result, err := e.lookupByIndex(dbName, block, idx.Field, fmt.Sprint(v), 0, 2)
		if err != nil {
			continue
		}
		for _, id := range result.DocIDs {
			if id != skipID {
				return fmt.Errorf("unique constraint violated: %s.%s = %v already exists (index %s)", block, idx.Field, v, idx.Name)
			}
		}
	}
	return nil
}
