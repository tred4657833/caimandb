package caimandb

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// AutoRelation is a self-relation CaimanDB infers automatically from
// access patterns: when the same user reads the same document
// repeatedly within a short window, CaimanDB records an auto-relation
// between that user and the document, carrying a relevance score and
// a small sample of the document's own key metadata.
//
// Unlike RelationDef (created explicitly via RELATE and permanent),
// AutoRelations are temporal by design: every fresh access slides
// their expiry forward, and once nobody looks at the pair again they
// simply expire and get swept away by the background cleanup loop --
// they are a live relevance signal, not a durable data model.
type AutoRelation struct {
	SourceDB    string         `json:"source_db"`
	SourceBlock string         `json:"source_block"`
	DocID       string         `json:"doc_id"`
	UserID      string         `json:"user_id"`
	AccessCount int64          `json:"access_count"`
	FirstSeen   time.Time      `json:"first_seen"`
	LastSeen    time.Time      `json:"last_seen"`
	ExpiresAt   time.Time      `json:"expires_at"`
	Relevance   float64        `json:"relevance"`
	KeyMetadata map[string]any `json:"key_metadata,omitempty"`
}

func (ar *AutoRelation) expired(now time.Time) bool {
	return now.After(ar.ExpiresAt)
}

// autoRelationKey identifies one (user, document) self-relation.
func autoRelationKey(db, block, docID, userID string) string {
	return db + "\x00" + block + "\x00" + docID + "\x00" + userID
}

// accessCounter tracks raw access hits within the current sliding
// window for one (user, document) pair, purely to decide when the
// threshold is crossed. Kept separate from AutoRelation so pairs that
// never reach the threshold don't linger as full relation records.
type accessCounter struct {
	windowStart time.Time
	hits        int64
}

// AutoRelationManager tracks document-access frequency per user and
// promotes hot (user, document) pairs into temporal AutoRelations with
// a relevance score. Everything here lives in memory (plus a
// best-effort snapshot for restart continuity) -- these are working-
// set signals, not source-of-truth data, so they're allowed to be
// approximate and are always subject to TTL expiry.
type AutoRelationManager struct {
	mu       sync.RWMutex
	dataRoot string
	entries  map[string]*AutoRelation
	counts   map[string]*accessCounter

	enabled   bool
	threshold int           // accesses required within window before an auto-relation is created
	window    time.Duration // sliding window used to judge "accessed a lot"
	ttl       time.Duration // how long an auto-relation survives without further access
}

func NewAutoRelationManager(dataRoot string, cfg *Config) *AutoRelationManager {
	arm := &AutoRelationManager{
		dataRoot: dataRoot,
		entries:  make(map[string]*AutoRelation),
		counts:   make(map[string]*accessCounter),
	}
	if cfg != nil {
		arm.enabled = cfg.AutoRelationsEnabled
		arm.threshold = cfg.AutoRelationThreshold
		arm.window = cfg.AutoRelationWindow
		arm.ttl = cfg.AutoRelationTTL
	}
	if arm.threshold <= 0 {
		arm.threshold = 5
	}
	if arm.window <= 0 {
		arm.window = 10 * time.Minute
	}
	if arm.ttl <= 0 {
		arm.ttl = 24 * time.Hour
	}
	arm.load()
	return arm
}

// sampleMetadata pulls a handful of small, scalar fields out of a
// document's Data to attach to the auto-relation, so SHOW
// AUTORELATIONS gives a human something recognizable (e.g. "title:
// Inception") instead of a bare doc ID. Deliberately shallow and
// capped -- this is a relevance hint, not a copy of the document.
func sampleMetadata(data map[string]any) map[string]any {
	if len(data) == 0 {
		return nil
	}
	preferred := []string{"name", "title", "sku", "code", "email", "username", "label"}
	sample := make(map[string]any, 4)
	for _, key := range preferred {
		if v, ok := data[key]; ok && isScalarish(v) {
			sample[key] = v
			if len(sample) >= 4 {
				return sample
			}
		}
	}
	if len(sample) > 0 {
		return sample
	}
	// Fall back to the first couple of scalar fields in map iteration
	// order for docs that don't use any conventional field name above.
	for k, v := range data {
		if isScalarish(v) {
			sample[k] = v
		}
		if len(sample) >= 2 {
			break
		}
	}
	return sample
}

func isScalarish(v any) bool {
	switch v.(type) {
	case string, float64, int, int64, bool:
		return true
	default:
		return false
	}
}

// RecordAccess registers one read of dbName.block/docID by userID. Once
// the number of accesses to that exact (user, document) pair within
// the configured window reaches the threshold, an AutoRelation is
// created (or refreshed) with an updated relevance score and its TTL
// slid forward from now -- this is what keeps the relation "temporal":
// it stays alive only as long as it keeps being accessed.
func (arm *AutoRelationManager) RecordAccess(dbName, block, docID, userID string, data map[string]any) {
	if arm == nil || !arm.enabled || docID == "" {
		return
	}
	if userID == "" {
		userID = "anonymous"
	}
	now := time.Now()
	key := autoRelationKey(dbName, block, docID, userID)

	arm.mu.Lock()
	defer arm.mu.Unlock()

	c, ok := arm.counts[key]
	if !ok || now.Sub(c.windowStart) > arm.window {
		c = &accessCounter{windowStart: now}
		arm.counts[key] = c
	}
	c.hits++

	existing, hasExisting := arm.entries[key]
	if c.hits < int64(arm.threshold) && !hasExisting {
		return
	}

	relevance := relevanceScore(c.hits, now, existing)

	if hasExisting {
		existing.AccessCount++
		existing.LastSeen = now
		existing.ExpiresAt = now.Add(arm.ttl)
		existing.Relevance = relevance
		if len(existing.KeyMetadata) == 0 {
			existing.KeyMetadata = sampleMetadata(data)
		}
		return
	}

	arm.entries[key] = &AutoRelation{
		SourceDB:    dbName,
		SourceBlock: block,
		DocID:       docID,
		UserID:      userID,
		AccessCount: c.hits,
		FirstSeen:   now,
		LastSeen:    now,
		ExpiresAt:   now.Add(arm.ttl),
		Relevance:   relevance,
		KeyMetadata: sampleMetadata(data),
	}
}

// relevanceScore combines raw frequency with recency: a document hit
// many times a while ago is less relevant right now than one hit
// fewer times but very recently accessed. The result stays in an
// easy-to-read range (roughly 0-10 in normal use) rather than growing
// unbounded with access count alone.
func relevanceScore(hits int64, now time.Time, existing *AutoRelation) float64 {
	recency := 1.0
	if existing != nil {
		minutesSince := now.Sub(existing.LastSeen).Minutes()
		recency = 1.0 / (1.0 + minutesSince/30.0) // half-strength every ~30 min of silence
	}
	return math.Round(float64(hits)*recency*100) / 100
}

// List returns the still-live auto-relations for a block, most
// relevant first.
func (arm *AutoRelationManager) List(dbName, block string) []*AutoRelation {
	if arm == nil {
		return nil
	}
	now := time.Now()
	arm.mu.RLock()
	defer arm.mu.RUnlock()

	out := make([]*AutoRelation, 0, len(arm.entries))
	for _, ar := range arm.entries {
		if ar.SourceDB != dbName || ar.SourceBlock != block || ar.expired(now) {
			continue
		}
		cp := *ar
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Relevance > out[j].Relevance })
	return out
}

// CleanupExpired purges auto-relations (and their access counters)
// whose TTL has lapsed, and persists a snapshot of what's left. It
// returns the number of entries removed. This is what keeps the whole
// feature honestly temporal: nothing here survives indefinitely just
// because it was interesting once.
func (arm *AutoRelationManager) CleanupExpired() int {
	if arm == nil {
		return 0
	}
	now := time.Now()
	arm.mu.Lock()
	defer arm.mu.Unlock()

	removed := 0
	for k, ar := range arm.entries {
		if ar.expired(now) {
			delete(arm.entries, k)
			delete(arm.counts, k)
			removed++
		}
	}
	for k, c := range arm.counts {
		if _, live := arm.entries[k]; !live && now.Sub(c.windowStart) > arm.window {
			delete(arm.counts, k)
		}
	}
	arm.saveLocked()
	return removed
}

// --- persistence -----------------------------------------------------
//
// Auto-relations are working-set signals, not source-of-truth data, so
// persistence here is best-effort: a single snapshot file lets a
// restart keep "warm" relevance instead of losing everything, but
// nothing downstream depends on this file existing or being current.

func (arm *AutoRelationManager) snapshotPath() string {
	return filepath.Join(arm.dataRoot, "__system", "auto_relations", "snapshot.json")
}

func (arm *AutoRelationManager) saveLocked() {
	if arm.dataRoot == "" {
		return
	}
	path := arm.snapshotPath()
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return
	}
	list := make([]*AutoRelation, 0, len(arm.entries))
	for _, ar := range arm.entries {
		list = append(list, ar)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0640)
}

func (arm *AutoRelationManager) load() {
	if arm.dataRoot == "" {
		return
	}
	data, err := os.ReadFile(arm.snapshotPath())
	if err != nil {
		return
	}
	var list []*AutoRelation
	if err := json.Unmarshal(data, &list); err != nil {
		return
	}
	now := time.Now()
	for _, ar := range list {
		if ar.expired(now) {
			continue
		}
		key := autoRelationKey(ar.SourceDB, ar.SourceBlock, ar.DocID, ar.UserID)
		arm.entries[key] = ar
		arm.counts[key] = &accessCounter{windowStart: ar.LastSeen, hits: ar.AccessCount}
	}
}
