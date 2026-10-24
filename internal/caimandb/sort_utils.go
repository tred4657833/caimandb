package caimandb

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

// parseSortField parses one ORDER BY entry into a SortField: a plain
// field ("precio"), a descending one ("precio:DESC"), one with an
// explicit NULLS placement ("precio NULLS LAST", "precio:DESC NULLS
// FIRST"), or the special "RANDOM()" entry. Shared by both call sites
// that build an ORDER BY list (FIND and SEARCH, in cmd_find.go).
func parseSortField(f string) SortField {
	f = strings.TrimSpace(f)
	upper := strings.ToUpper(f)
	if upper == "RANDOM()" || upper == "RANDOM ( )" || upper == "RANDOM" {
		return SortField{Random: true}
	}

	nullsFirst, nullsLast := false, false
	switch {
	case strings.HasSuffix(upper, " NULLS FIRST"):
		f = strings.TrimSpace(f[:len(f)-len(" NULLS FIRST")])
		nullsFirst = true
	case strings.HasSuffix(upper, " NULLS LAST"):
		f = strings.TrimSpace(f[:len(f)-len(" NULLS LAST")])
		nullsLast = true
	}

	desc := false
	if strings.HasSuffix(strings.ToUpper(f), ":DESC") {
		f = f[:len(f)-len(":DESC")]
		desc = true
	}
	return SortField{Field: f, Desc: desc, NullsFirst: nullsFirst, NullsLast: nullsLast}
}

func sortDocs(docs []Document, sorts []SortField) {
	if len(sorts) == 1 && sorts[0].Random {
		rand.Shuffle(len(docs), func(i, j int) { docs[i], docs[j] = docs[j], docs[i] })
		return
	}

	sort.SliceStable(docs, func(i, j int) bool {
		for _, s := range sorts {
			if s.Random {
				// RANDOM() combined with other sort keys isn't a
				// coherent request (there's no stable "random key" to
				// compare two documents by within a multi-key sort);
				// skip it as a no-op key rather than erroring the
				// whole ORDER BY.
				continue
			}

			vi := getNested(docs[i].Data, s.Field)
			vj := getNested(docs[j].Data, s.Field)

			if (s.NullsFirst || s.NullsLast) && (vi == nil) != (vj == nil) {
				// Exactly one side is nil and an explicit placement
				// was requested. Per standard SQL, NULLS FIRST/LAST is
				// independent of ASC/DESC, so Desc doesn't flip this.
				iGoesFirst := (vi == nil) == s.NullsFirst
				if iGoesFirst {
					return true
				}
				return false
			}

			fi, fj := toF64(vi), toF64(vj)
			cmp := 0
			if fi != 0 || fj != 0 {
				if fi < fj {
					cmp = -1
				} else if fi > fj {
					cmp = 1
				}
			} else {
				cmp = strings.Compare(fmt.Sprint(vi), fmt.Sprint(vj))
			}
			if cmp != 0 {
				if s.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}
