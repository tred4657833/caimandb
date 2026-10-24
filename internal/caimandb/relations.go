package caimandb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// RelationTarget describes one target block that a source block relates
// to, as registered via RELATE <block> USE <target1>,<target2>,...
//
// Targets may live in a different database than the source block, using
// "db.block" syntax in RELATE (e.g. "crm.customers"); a bare block name
// ("customers") targets a block in the same database as the source.
type RelationTarget struct {
	DB    string `json:"db"`    // target database name
	Block string `json:"block"` // target block name
	Alias string `json:"alias"` // name used in FIND ... SELECT <alias>.field (defaults to Block)
	FK    string `json:"fk"`    // conventional foreign-key field on the source doc, e.g. "director_id"
}

// RelationDef is the full set of targets registered for one source block.
type RelationDef struct {
	SourceDB    string           `json:"source_db"`
	SourceBlock string           `json:"source_block"`
	Targets     []RelationTarget `json:"targets"`
}

// RelationManager persists and caches RELATE definitions. Definitions are
// stored as one JSON file per source db+block under
// <dataRoot>/__system/relations/, so they survive restarts the same way
// block metadata does.
type RelationManager struct {
	mu       sync.RWMutex
	dataRoot string
	cache    map[string]*RelationDef // key: sourceDB + "\x00" + sourceBlock
}

func NewRelationManager(dataRoot string) *RelationManager {
	return &RelationManager{
		dataRoot: dataRoot,
		cache:    make(map[string]*RelationDef),
	}
}

func relationKey(db, block string) string { return db + "\x00" + block }

func (rm *RelationManager) filePath(db, block string) string {
	return filepath.Join(rm.dataRoot, "__system", "relations", db+"__"+block+".json")
}

// fkFieldFor derives the conventional foreign-key field name for a target
// block when there is no data available to confirm it against (empty
// source block, or sampling unavailable) -- e.g. "directors" ->
// "director_id", "genres" -> "genre_id", "customers" -> "customer_id".
// This is a best-effort heuristic (not a full inflector for English or
// Spanish), matching the "<block>_id" convention. It always returns the
// single best guess (the first candidate from singularizeCandidates);
// callers that can inspect real documents should prefer
// (*Engine).inferFKField instead, which disambiguates using the data.
func fkFieldFor(targetBlock string) string {
	return singularizeCandidates(targetBlock)[0] + "_id"
}

// singularizeCandidates returns, in preference order, the plausible
// singular stems for a plural block name, used to build candidate FK
// field names for RELATE (see Engine.Relate / inferFKField).
//
// A single deterministic stemming rule cannot cover this correctly:
// Spanish plurals ending in "...es" are genuinely ambiguous from the
// surface form alone. A consonant-final singular takes "-es"
// ("animal" -> "animales", "proveedor" -> "proveedores"), while a
// vowel-final (usually "-e") singular just takes "-s" and therefore
// *also* ends in "...es" on its own ("especie" -> "especies",
// "visitante" -> "visitantes"). "animales" minus "s" is "animale" and
// "especies" minus "s" is "especie" -- both superficially "end in a
// vowel" -- so there is no suffix-only rule that gets both right.
// Rather than silently picking one and guessing wrong (the previous
// behavior: "especies" -> "especy_id", "animales" -> "animale_id",
// "proveedores" -> "proveedore_id"), this returns every plausible stem
// so the caller can check which one's "<stem>_id" field is actually
// present in real documents.
func singularizeCandidates(name string) []string {
	lower := strings.ToLower(name)
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		for _, existing := range out {
			if existing == s {
				return
			}
		}
		out = append(out, s)
	}

	// "luces" -> "luz" (c/z alternation before Spanish "-es"), tried
	// before the generic "-es" stripping below since it needs its own
	// suffix swap rather than a plain truncation.
	if strings.HasSuffix(lower, "ces") && len(name) > 3 {
		add(name[:len(name)-3] + "z")
	}
	// Consonant-final singular + "-es" (animal -> animales, proveedor ->
	// proveedores, reloj -> relojes).
	if strings.HasSuffix(lower, "es") && len(name) > 3 {
		add(name[:len(name)-2])
	}
	// English "-y" pluralized as "-ies" (category -> categories). Kept
	// separate from the Spanish "-es" rule above since it changes the
	// last letter instead of just truncating.
	if strings.HasSuffix(lower, "ies") && len(name) > 3 {
		add(name[:len(name)-3] + "y")
	}
	// Vowel-final singular + "-s" (especie -> especies, visitante ->
	// visitantes, habitat -> habitats, zona -> zonas). This is also the
	// generic "just strip the trailing s" fallback.
	if strings.HasSuffix(lower, "s") && len(name) > 1 {
		add(name[:len(name)-1])
	}
	add(name)
	return out
}

// parseTargetSpec splits a RELATE target ("customers" or "crm.customers")
// into its database and block parts, defaulting the database to the
// source block's own database when none is given.
func parseTargetSpec(spec, defaultDB string) (db, block string, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", "", fmt.Errorf("empty target block in RELATE")
	}
	if idx := strings.Index(spec, "."); idx > 0 && idx < len(spec)-1 {
		return spec[:idx], spec[idx+1:], nil
	}
	return defaultDB, spec, nil
}

// Relate registers (replacing any previous definition) the relation
// targets for a source block. Each entry in targetSpecs is either a bare
// block name (same database as the source) or "db.block" for a
// cross-database relation.
//
// fkFor, when non-nil, is consulted for each target (sourceDB,
// sourceBlock, targetDB, targetBlock) and may return the actual
// foreign-key field name to use, e.g. resolved by sampling real
// documents (see Engine.inferFKField). An empty return, or a nil fkFor,
// falls back to the naming convention (fkFieldFor).
func (rm *RelationManager) Relate(sourceDB, sourceBlock string, targetSpecs []string, fkFor func(targetDB, targetBlock string) string) (*RelationDef, error) {
	if sourceDB == "" || sourceBlock == "" {
		return nil, fmt.Errorf("source database and block are required")
	}
	if len(targetSpecs) == 0 {
		return nil, fmt.Errorf("RELATE requires at least one target block (USE <block1>[,<block2>,...])")
	}

	def := &RelationDef{SourceDB: sourceDB, SourceBlock: sourceBlock}
	seenAlias := make(map[string]bool, len(targetSpecs))

	for _, spec := range targetSpecs {
		targetDB, targetBlock, err := parseTargetSpec(spec, sourceDB)
		if err != nil {
			return nil, err
		}

		alias := targetBlock
		if seenAlias[alias] {
			return nil, fmt.Errorf("duplicate relation target: %q (alias %q already used)", spec, alias)
		}
		seenAlias[alias] = true

		fk := fkFieldFor(targetBlock)
		if fkFor != nil {
			if custom := fkFor(targetDB, targetBlock); custom != "" {
				fk = custom
			}
		}

		def.Targets = append(def.Targets, RelationTarget{
			DB:    targetDB,
			Block: targetBlock,
			Alias: alias,
			FK:    fk,
		})
	}

	if err := rm.save(def); err != nil {
		return nil, err
	}

	rm.mu.Lock()
	rm.cache[relationKey(sourceDB, sourceBlock)] = def
	rm.mu.Unlock()

	return def, nil
}

// Get returns the relation definition registered for a source block, if
// any, loading it from disk on first access and caching it afterwards.
func (rm *RelationManager) Get(sourceDB, sourceBlock string) (*RelationDef, bool) {
	rm.mu.RLock()
	if def, ok := rm.cache[relationKey(sourceDB, sourceBlock)]; ok {
		rm.mu.RUnlock()
		return def, true
	}
	rm.mu.RUnlock()

	def, err := rm.load(sourceDB, sourceBlock)
	if err != nil || def == nil {
		return nil, false
	}

	rm.mu.Lock()
	rm.cache[relationKey(sourceDB, sourceBlock)] = def
	rm.mu.Unlock()
	return def, true
}

func (rm *RelationManager) save(def *RelationDef) error {
	dir := filepath.Join(rm.dataRoot, "__system", "relations")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	data, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(rm.filePath(def.SourceDB, def.SourceBlock), data, 0644)
}

func (rm *RelationManager) load(db, block string) (*RelationDef, error) {
	data, err := os.ReadFile(rm.filePath(db, block))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var def RelationDef
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, err
	}
	return &def, nil
}

// Relate validates the target blocks exist and registers the relation
// definition for dbName.block via the engine's RelationManager. The
// foreign-key field recorded for each target is confirmed against real
// documents already in dbName.block when possible (see inferFKField),
// rather than trusting the naming-convention guess blindly.
func (e *Engine) Relate(dbName, block string, targetSpecs []string) (*RelationDef, error) {
	if !e.dirMgr.BlockExists(dbName, block) {
		return nil, ErrBlockNotFound
	}

	for _, spec := range targetSpecs {
		targetDB, targetBlock, err := parseTargetSpec(spec, dbName)
		if err != nil {
			return nil, err
		}
		if !e.dirMgr.BlockExists(targetDB, targetBlock) {
			return nil, fmt.Errorf("related block not found: %s.%s", targetDB, targetBlock)
		}
	}

	return e.relMgr.Relate(dbName, block, targetSpecs, func(targetDB, targetBlock string) string {
		return e.inferFKField(dbName, block, targetBlock)
	})
}

// inferFKField picks the foreign-key field RELATE should record for a
// target block, by sampling real documents already stored in
// dbName.sourceBlock and checking which "<candidate>_id" field name they
// actually use. targetDB is deliberately not part of the sample (a
// cross-database relation's FK still lives on the source document, named
// after the target block, not after the target database).
//
// singularizeCandidates alone can't disambiguate Spanish plurals like
// "especies" (-> "especie") from "animales" (-> "animal") purely from the
// suffix -- both surface forms are "...es" and stripping either just "s"
// or "s"+"e" looks equally plausible in isolation. Checking which
// candidate field name real documents actually have resolves that
// ambiguity with certainty instead of guessing. When the source block is
// empty (fresh schema, RELATE issued before any INSERT) or nothing
// matches, this falls back to the first heuristic candidate exactly as
// before.
func (e *Engine) inferFKField(dbName, sourceBlock, targetBlock string) string {
	candidates := singularizeCandidates(targetBlock)
	fkCandidates := make([]string, len(candidates))
	for i, c := range candidates {
		fkCandidates[i] = c + "_id"
	}
	if len(fkCandidates) == 0 {
		return ""
	}

	res, err := e.Find(dbName, sourceBlock, &Query{Limit: 25, UseIndex: false})
	if err == nil && res != nil {
		counts := make(map[string]int, len(fkCandidates))
		for _, doc := range res.Docs {
			for _, fk := range fkCandidates {
				if _, ok := doc.Data[fk]; ok {
					counts[fk]++
				}
			}
		}
		best := ""
		bestCount := 0
		for _, fk := range fkCandidates { // candidate order breaks ties
			if counts[fk] > bestCount {
				best = fk
				bestCount = counts[fk]
			}
		}
		if best != "" {
			return best
		}
	}

	return fkCandidates[0]
}
