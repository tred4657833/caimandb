package caimandb

import (
	"fmt"
	"sort"
	"strings"
)

type SearchResult struct {
	Doc     Document
	Score   float64
	Matches []string
}

// unquoteSearchTerm strips one layer of surrounding matching quotes
// (double or single), if present. The DSL tokenizer (parse/tokenizer.go)
// keeps a quoted token's quote characters as part of the token text --
// that's what lets a search phrase contain spaces at all -- but it means
// Search's own phrase-detection heuristic below, and every caller that
// needs the user's literal search words, must strip those quotes back
// off explicitly; nothing does that for free.
func unquoteSearchTerm(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func (e *Engine) Search(dbName, block, text string, q *Query) (*QueryResult, error) {
	if q == nil {
		q = &Query{Limit: 100}
	}

	result, err := e.Find(dbName, block, q)
	if err != nil {
		return nil, err
	}

	if len(result.Docs) == 0 {
		return result, nil
	}

	searchText := strings.ToLower(strings.TrimSpace(text))

	if strings.HasPrefix(searchText, "~") {
		return e.searchFuzzy(dbName, block, unquoteSearchTerm(strings.TrimPrefix(searchText, "~")), q)
	}

	if strings.HasPrefix(searchText, `"`) && strings.HasSuffix(searchText, `"`) {
		phrase := strings.Trim(searchText, `"`)
		return e.searchPhrase(dbName, block, phrase, q)
	}

	if strings.Contains(searchText, "+") || strings.Contains(searchText, "-") {
		return e.searchWithOperators(dbName, block, searchText, q)
	}

	return e.searchWithRelevance(dbName, block, searchText, q)
}

func (e *Engine) searchWithRelevance(dbName, block, searchText string, q *Query) (*QueryResult, error) {
	result, err := e.Find(dbName, block, q)
	if err != nil {
		return nil, err
	}

	if len(result.Docs) == 0 {
		return result, nil
	}

	type ScoredDoc struct {
		Doc     Document
		Score   float64
		Matches []string
	}

	var scored []ScoredDoc
	words := strings.Fields(searchText)

	for _, doc := range result.Docs {
		score, matches := calculateRelevance(doc, searchText, words)
		if score > 0 {
			scored = append(scored, ScoredDoc{
				Doc:     doc,
				Score:   score,
				Matches: matches,
			})
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if len(scored) > limit {
		scored = scored[:limit]
	}

	var docs []Document
	for _, s := range scored {
		docs = append(docs, s.Doc)
	}

	result.Docs = docs
	result.Total = int64(len(docs))
	result.IndexUsed = "search_relevance"
	return result, nil
}

func calculateRelevance(doc Document, searchText string, words []string) (float64, []string) {
	score := 0.0
	var matches []string

	if strings.Contains(strings.ToLower(doc.ID), searchText) {
		score += 10.0
		matches = append(matches, "_id")
	}

	for key, value := range doc.Data {
		strVal := strings.ToLower(fmt.Sprint(value))
		keyLower := strings.ToLower(key)

		weight := getFieldWeight(keyLower)

		if strings.Contains(keyLower, searchText) {
			score += weight * 2.0
			matches = append(matches, "field:"+key)
		}

		if strings.Contains(strVal, searchText) {
			score += weight
			matches = append(matches, "value:"+key)
		}

		for _, word := range words {
			if word == "" {
				continue
			}
			if strings.Contains(strVal, word) {
				score += weight * 0.5
				matches = append(matches, "word:"+word+" in "+key)
			}
		}

		if m, ok := value.(map[string]any); ok {
			s, m2 := searchMapWithScore(m, searchText, words)
			score += s * 0.5
			matches = append(matches, m2...)
		}

		if arr, ok := value.([]any); ok {
			for _, item := range arr {
				strItem := strings.ToLower(fmt.Sprint(item))
				if strings.Contains(strItem, searchText) {
					score += weight * 0.3
					matches = append(matches, "array:"+key)
				}
				if m, ok := item.(map[string]any); ok {
					s, m2 := searchMapWithScore(m, searchText, words)
					score += s * 0.3
					matches = append(matches, m2...)
				}
			}
		}
	}

	return score, matches
}

func getFieldWeight(field string) float64 {
	importantFields := map[string]float64{
		"nombre":      5.0,
		"name":        5.0,
		"titulo":      5.0,
		"title":       5.0,
		"email":       4.0,
		"usuario":     4.0,
		"username":    4.0,
		"descripcion": 3.0,
		"description": 3.0,
		"contenido":   3.0,
		"content":     3.0,
		"texto":       3.0,
		"text":        3.0,
		"ciudad":      2.0,
		"city":        2.0,
		"pais":        2.0,
		"country":     2.0,
		"tags":        2.0,
		"etiquetas":   2.0,
	}

	if weight, ok := importantFields[field]; ok {
		return weight
	}
	return 1.0
}

func searchMapWithScore(data map[string]any, searchText string, words []string) (float64, []string) {
	score := 0.0
	var matches []string

	for key, value := range data {
		strVal := strings.ToLower(fmt.Sprint(value))
		keyLower := strings.ToLower(key)

		if strings.Contains(keyLower, searchText) {
			score += 2.0
			matches = append(matches, "nested_field:"+key)
		}

		if strings.Contains(strVal, searchText) {
			score += 1.0
			matches = append(matches, "nested_value:"+key)
		}

		for _, word := range words {
			if word != "" && strings.Contains(strVal, word) {
				score += 0.5
				matches = append(matches, "nested_word:"+word+" in "+key)
			}
		}

		if m, ok := value.(map[string]any); ok {
			s, m2 := searchMapWithScore(m, searchText, words)
			score += s * 0.5
			matches = append(matches, m2...)
		}
	}

	return score, matches
}

func (e *Engine) searchPhrase(dbName, block, phrase string, q *Query) (*QueryResult, error) {
	result, err := e.Find(dbName, block, q)
	if err != nil {
		return nil, err
	}

	if len(result.Docs) == 0 {
		return result, nil
	}

	phraseLower := strings.ToLower(phrase)
	var matched []Document

	for _, doc := range result.Docs {
		if searchPhraseMatch(doc, phraseLower) {
			matched = append(matched, doc)
		}
	}

	result.Docs = matched
	result.Total = int64(len(matched))
	result.IndexUsed = "search_phrase"
	return result, nil
}

func searchPhraseMatch(doc Document, phrase string) bool {
	if strings.Contains(strings.ToLower(doc.ID), phrase) {
		return true
	}

	for _, value := range doc.Data {
		strVal := strings.ToLower(fmt.Sprint(value))
		if strings.Contains(strVal, phrase) {
			return true
		}

		if m, ok := value.(map[string]any); ok {
			if searchPhraseInMap(m, phrase) {
				return true
			}
		}

		if arr, ok := value.([]any); ok {
			for _, item := range arr {
				strItem := strings.ToLower(fmt.Sprint(item))
				if strings.Contains(strItem, phrase) {
					return true
				}
				if m, ok := item.(map[string]any); ok {
					if searchPhraseInMap(m, phrase) {
						return true
					}
				}
			}
		}
	}

	return false
}

func searchPhraseInMap(data map[string]any, phrase string) bool {
	for _, value := range data {
		strVal := strings.ToLower(fmt.Sprint(value))
		if strings.Contains(strVal, phrase) {
			return true
		}
		if m, ok := value.(map[string]any); ok {
			if searchPhraseInMap(m, phrase) {
				return true
			}
		}
		if arr, ok := value.([]any); ok {
			for _, item := range arr {
				strItem := strings.ToLower(fmt.Sprint(item))
				if strings.Contains(strItem, phrase) {
					return true
				}
				if m, ok := item.(map[string]any); ok {
					if searchPhraseInMap(m, phrase) {
						return true
					}
				}
			}
		}
	}
	return false
}

// levenshteinDistance returns the classic edit distance between a and b
// (insertions, deletions, substitutions, each cost 1).
func levenshteinDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	if len(ra) == 0 {
		return len(rb)
	}
	if len(rb) == 0 {
		return len(ra)
	}
	prev := make([]int, len(rb)+1)
	curr := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		curr[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			best := del
			if ins < best {
				best = ins
			}
			if sub < best {
				best = sub
			}
			curr[j] = best
		}
		prev, curr = curr, prev
	}
	return prev[len(rb)]
}

// fuzzyMaxDistance scales the tolerated edit distance with word length --
// a fixed distance would make short words match almost anything and long
// words barely tolerate a single typo.
func fuzzyMaxDistance(word string) int {
	n := len([]rune(word))
	switch {
	case n <= 3:
		return 1
	case n <= 7:
		return 2
	default:
		return 3
	}
}

// searchFuzzy implements the documented FUZZY/SIMILAR search mode
// (SEARCH <block> "~word" FUZZY / SEARCH <block> "~word"): each search
// word is compared against every word found in a document's own fields
// (via Levenshtein distance, tolerance scaled by fuzzyMaxDistance), and
// a document scores once per search word that comes within tolerance of
// some word in that document. Before this, FUZZY only ever prepended a
// "~" to the search text and then fell through to plain substring
// matching (searchWithRelevance) or phrase matching -- neither of which
// treats "~" specially at all, so a fuzzy search for a misspelled word
// never matched the correctly-spelled original it was meant to find.
func (e *Engine) searchFuzzy(dbName, block, searchText string, q *Query) (*QueryResult, error) {
	result, err := e.Find(dbName, block, q)
	if err != nil {
		return nil, err
	}
	if len(result.Docs) == 0 {
		return result, nil
	}

	searchWords := strings.Fields(strings.ToLower(searchText))
	if len(searchWords) == 0 {
		result.Docs = nil
		result.Total = 0
		return result, nil
	}

	type scored struct {
		doc   Document
		score float64
	}
	var matched []scored

	for _, doc := range result.Docs {
		docWords := map[string]bool{}
		collectDocWords(doc.Data, docWords)
		var score float64
		for _, sw := range searchWords {
			threshold := fuzzyMaxDistance(sw)
			best := threshold + 1
			for dw := range docWords {
				if d := levenshteinDistance(sw, dw); d < best {
					best = d
					if best == 0 {
						break
					}
				}
			}
			if best <= threshold {
				// Closer matches (smaller edit distance) score higher,
				// same "more relevant first" convention as
				// calculateRelevance's substring scoring.
				score += float64(threshold - best + 1)
			}
		}
		if score > 0 {
			matched = append(matched, scored{doc, score})
		}
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].score > matched[j].score })

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}

	docs := make([]Document, len(matched))
	for i, m := range matched {
		docs[i] = m.doc
	}
	result.Docs = docs
	result.Total = int64(len(docs))
	result.IndexUsed = "search_fuzzy"
	return result, nil
}

// collectDocWords lowercases and tokenizes every string-ish value found
// in data (recursing into nested objects and arrays) into individual
// words, for searchFuzzy's word-by-word comparison.
func collectDocWords(data map[string]any, out map[string]bool) {
	for _, value := range data {
		collectWordsFromValue(value, out)
	}
}

func collectWordsFromValue(value any, out map[string]bool) {
	switch v := value.(type) {
	case map[string]any:
		collectDocWords(v, out)
	case []any:
		for _, item := range v {
			collectWordsFromValue(item, out)
		}
	default:
		for _, w := range strings.Fields(strings.ToLower(fmt.Sprint(v))) {
			out[w] = true
		}
	}
}

func (e *Engine) searchWithOperators(dbName, block, query string, q *Query) (*QueryResult, error) {
	var must []string
	var mustNot []string
	var should []string

	for _, token := range strings.Fields(query) {
		if strings.HasPrefix(token, "+") && len(token) > 1 {
			must = append(must, strings.TrimPrefix(token, "+"))
		} else if strings.HasPrefix(token, "-") && len(token) > 1 {
			mustNot = append(mustNot, strings.TrimPrefix(token, "-"))
		} else {
			should = append(should, token)
		}
	}

	result, err := e.Find(dbName, block, q)
	if err != nil {
		return nil, err
	}

	if len(result.Docs) == 0 {
		return result, nil
	}

	var matched []Document

	for _, doc := range result.Docs {
		mustMatch := true
		for _, term := range must {
			if !searchTermInDoc(doc, term) {
				mustMatch = false
				break
			}
		}
		if !mustMatch {
			continue
		}

		mustNotMatch := true
		for _, term := range mustNot {
			if searchTermInDoc(doc, term) {
				mustNotMatch = false
				break
			}
		}
		if !mustNotMatch {
			continue
		}

		if len(should) > 0 {
			shouldMatch := false
			for _, term := range should {
				if searchTermInDoc(doc, term) {
					shouldMatch = true
					break
				}
			}
			if !shouldMatch {
				continue
			}
		}

		matched = append(matched, doc)
	}

	result.Docs = matched
	result.Total = int64(len(matched))
	result.IndexUsed = "search_operators"
	return result, nil
}

func searchTermInDoc(doc Document, term string) bool {
	termLower := strings.ToLower(term)

	if strings.Contains(strings.ToLower(doc.ID), termLower) {
		return true
	}

	for _, value := range doc.Data {
		strVal := strings.ToLower(fmt.Sprint(value))
		if strings.Contains(strVal, termLower) {
			return true
		}

		if m, ok := value.(map[string]any); ok {
			if searchTermInMap(m, termLower) {
				return true
			}
		}

		if arr, ok := value.([]any); ok {
			for _, item := range arr {
				strItem := strings.ToLower(fmt.Sprint(item))
				if strings.Contains(strItem, termLower) {
					return true
				}
				if m, ok := item.(map[string]any); ok {
					if searchTermInMap(m, termLower) {
						return true
					}
				}
			}
		}
	}

	return false
}

func searchTermInMap(data map[string]any, term string) bool {
	for _, value := range data {
		strVal := strings.ToLower(fmt.Sprint(value))
		if strings.Contains(strVal, term) {
			return true
		}
		if m, ok := value.(map[string]any); ok {
			if searchTermInMap(m, term) {
				return true
			}
		}
		if arr, ok := value.([]any); ok {
			for _, item := range arr {
				strItem := strings.ToLower(fmt.Sprint(item))
				if strings.Contains(strItem, term) {
					return true
				}
				if m, ok := item.(map[string]any); ok {
					if searchTermInMap(m, term) {
						return true
					}
				}
			}
		}
	}
	return false
}
