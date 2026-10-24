package caimandb

type Document struct {
	ID      string         `json:"_id"`
	Data    map[string]any `json:"data"`
	Meta    DocMeta        `json:"meta"`
	ShardID string         `json:"_shard_id,omitempty"`
	LargeID string         `json:"_large_id,omitempty"`
}

type DocMeta struct {
	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
	Version   int64 `json:"version"`
}

// Note: BlockMeta now lives in the storage package (storage.BlockMeta),
// alongside DirectoryManager, the only place that reads/writes it.

// unwrapExportEnvelope recognizes a decoded JSON object that is actually a
// whole exported Document (the shape writeDocsJSON/Export() produce: the
// business fields nested under "data", siblings "_id"/"meta"/"_shard_id" at
// the top level -- see the Document struct above) and returns just its
// "data" map, i.e. the original flat business fields, instead of the
// envelope itself.
//
// Both IMPORT code paths (the legacy whole-file Import() in ops_export.go
// and the streaming decodeNDJSONDocs/decodeJSONArrayDocs in
// ops_bulk_import.go, used by "IMPORT <block> FROM FILE ...") decode a raw
// JSON object per line/element and insert it as-is. Without this check,
// re-importing a file that EXPORT itself produced -- the single most
// obvious "move data between blocks/environments" workflow -- silently
// corrupts every document: instead of {"name": "leo", "species": "jaguar"}
// you get {"_shard_id": ..., "data": {...the real fields...}, "id": ...,
// "meta": {...}}, i.e. the real fields end up buried one level down inside
// a field literally called "data", and the top-level document is polluted
// with "meta"/"_shard_id" that were never real business fields to begin
// with.
//
// The detection is intentionally narrow to avoid mistaking a genuine
// document that happens to have its own top-level "data" field for an
// envelope: it requires "data" to be an object AND "meta" to be an object
// containing a numeric "created_at" -- the exact, specific shape
// DocMeta/writeDocsJSON emit and not something a hand-written NDJSON file
// would plausibly produce by coincidence. Anything else is returned
// unchanged.
func unwrapExportEnvelope(doc map[string]any) map[string]any {
	dataVal, hasData := doc["data"]
	data, dataIsMap := dataVal.(map[string]any)
	if !hasData || !dataIsMap {
		return doc
	}
	metaVal, hasMeta := doc["meta"]
	meta, metaIsMap := metaVal.(map[string]any)
	if !hasMeta || !metaIsMap {
		return doc
	}
	if _, hasCreatedAt := meta["created_at"]; !hasCreatedAt {
		return doc
	}
	return data
}
