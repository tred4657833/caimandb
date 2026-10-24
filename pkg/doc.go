// Package pkg is reserved for CaimanDB code intended for use by external
// Go programs (e.g. a future official Go client, or shared request/
// response types for the HTTP API).
//
// It's currently empty: the natural first candidates (the HTTP request/
// response shapes in internal/caimandb/http_query.go and
// http_admin.go, and the Version constant in internal/caimandb/
// constants.go) are still entangled with the main engine package and
// weren't safe to relocate mechanically -- see MIGRATION_NOTES.md at
// the repository root for why. Moving them here properly is a
// worthwhile follow-up, ideally done with a compiler on hand to verify
// each step, which this restructuring pass did not have.
package pkg
