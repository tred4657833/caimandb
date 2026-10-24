// relations_fk_infer_test.go covers the RELATE foreign-key bug reported
// against the zoo schema (animales/especies/habitats/...): RELATE used
// to guess a target block's FK field name from a pure suffix heuristic
// (singularize), which got common Spanish plurals wrong -- "especies"
// singularized to "especy" instead of "especie", "animales" to "animale"
// instead of "animal" -- so any WHERE/SELECT join through that alias
// silently matched nothing. See relations.go (singularizeCandidates,
// Engine.inferFKField) for the fix: FK candidates are now confirmed
// against real documents in the source block instead of guessed blindly.
package caimandb

import "testing"

func TestSingularizeCandidatesSpanish(t *testing.T) {
	cases := map[string]string{ // targetBlock -> expected best (first) candidate
		"habitats":  "habitat",
		"alimentos": "alimento",
	}
	for word, want := range cases {
		got := singularizeCandidates(word)[0]
		if got != want {
			t.Errorf("singularizeCandidates(%q)[0] = %q, want %q", word, got, want)
		}
	}
}

func TestSingularizeCandidatesContainsCorrectStem(t *testing.T) {
	// These are genuinely ambiguous from the suffix alone (see comment on
	// singularizeCandidates) so we only assert the correct stem shows up
	// SOMEWHERE in the candidate list -- inferFKField (data-driven) is
	// what actually disambiguates at RELATE time.
	cases := map[string]string{
		"especies":    "especie",
		"animales":    "animal",
		"proveedores": "proveedor",
		"visitantes":  "visitante",
	}
	for word, want := range cases {
		cands := singularizeCandidates(word)
		found := false
		for _, c := range cands {
			if c == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("singularizeCandidates(%q) = %v, missing expected stem %q", word, cands, want)
		}
	}
}

// TestRelateInfersFKFromRealData reproduces the reported zoo schema bug
// end-to-end: RELATE animales USE especies,habitats followed by a WHERE
// join on especies.continente, which previously returned "No documents
// found" because the FK was guessed as "especy_id" instead of the real
// stored field "especie_id".
func TestRelateInfersFKFromRealData(t *testing.T) {
	eng := newConcurrencyTestEngine(t)
	db := "zoo_test_db"
	mustOK(t, eng.CreateDB(db))
	mustOK(t, eng.CreateBlock(db, "especies"))
	mustOK(t, eng.CreateBlock(db, "animales"))
	mustOK(t, eng.CreateBlock(db, "habitats"))

	_, err := eng.Insert(db, "especies", map[string]any{"id": "esp001", "nombre": "Leon", "continente": "Africa"})
	mustOK(t, err)
	_, err = eng.Insert(db, "habitats", map[string]any{"id": "hab001", "nombre": "Sabana"})
	mustOK(t, err)
	_, err = eng.Insert(db, "animales", map[string]any{
		"id": "an001", "nombre": "Simba", "especie_id": "esp001", "habitat_id": "hab001",
	})
	mustOK(t, err)

	def, err := eng.Relate(db, "animales", []string{"especies", "habitats"})
	mustOK(t, err)

	fkByAlias := map[string]string{}
	for _, tgt := range def.Targets {
		fkByAlias[tgt.Alias] = tgt.FK
	}
	if fkByAlias["especies"] != "especie_id" {
		t.Errorf("especies FK = %q, want %q", fkByAlias["especies"], "especie_id")
	}
	if fkByAlias["habitats"] != "habitat_id" {
		t.Errorf("habitats FK = %q, want %q", fkByAlias["habitats"], "habitat_id")
	}

	// FIND animales WHERE especies.continente = "Africa" SELECT nombre,especies.nombre,habitats.nombre
	tokens := []string{
		"FIND", "animales",
		"WHERE", "especies.continente", "=", "\"Africa\"",
		"SELECT", "nombre,especies.nombre,habitats.nombre",
	}
	q, _, berr := buildFindQuery(tokens, 2)
	mustOK(t, berr)

	res, ferr := eng.Find(db, "animales", q)
	mustOK(t, ferr)
	if res.Total != 1 {
		t.Fatalf("expected 1 matching animal, got %d (docs: %+v)", res.Total, res.Docs)
	}

	// Find() already resolves SELECT-side relation aliases internally
	// (ops_find.go calls resolveRelations before returning), so res.Docs
	// is the final projected shape -- no second resolveRelations call
	// needed (and calling it again here would be wrong: by this point
	// Data no longer carries the raw especie_id/habitat_id FK fields,
	// only the already-projected nombre/especies/habitats keys).
	especiesField, ok := res.Docs[0].Data["especies"].(map[string]any)
	if !ok {
		t.Fatalf("expected especies.nombre to resolve to a map, got %#v", res.Docs[0].Data["especies"])
	}
	if especiesField["nombre"] != "Leon" {
		t.Errorf("especies.nombre = %v, want Leon", especiesField["nombre"])
	}
	habitatsField, ok := res.Docs[0].Data["habitats"].(map[string]any)
	if !ok {
		t.Fatalf("expected habitats.nombre to resolve to a map, got %#v", res.Docs[0].Data["habitats"])
	}
	if habitatsField["nombre"] != "Sabana" {
		t.Errorf("habitats.nombre = %v, want Sabana", habitatsField["nombre"])
	}
	if res.Docs[0].Data["nombre"] != "Simba" {
		t.Errorf("nombre = %v, want Simba", res.Docs[0].Data["nombre"])
	}
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}


// TestPlainFieldEqualityFilter reproduces query #26 from the bug report:
// a plain (non-relation) equality filter on a stored "_id"-suffixed
// field, with no RELATE alias involved at all.
func TestPlainFieldEqualityFilter(t *testing.T) {
	eng := newConcurrencyTestEngine(t)
	db := "zoo_test_db2"
	mustOK(t, eng.CreateDB(db))
	mustOK(t, eng.CreateBlock(db, "animales"))

	_, err := eng.Insert(db, "animales", map[string]any{
		"id": "an001", "nombre": "Simba", "especie_id": "esp001", "peso": 190, "edad": 5, "habitat_id": "hab001",
	})
	mustOK(t, err)

	tokens := []string{
		"FIND", "animales",
		"WHERE", "especie_id", "=", "\"esp001\"",
		"SELECT", "nombre,peso,edad,habitat_id",
	}
	q, _, berr := buildFindQuery(tokens, 2)
	mustOK(t, berr)

	res, ferr := eng.Find(db, "animales", q)
	mustOK(t, ferr)
	if res.Total != 1 {
		t.Fatalf("expected 1 matching animal, got %d", res.Total)
	}
}
