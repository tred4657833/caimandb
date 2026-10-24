package caimandb

import "sort"

// CaimanDB's auto-relations are inherently a bipartite, directed graph:
// every edge is "user U read document D", so there are exactly two
// kinds of node (doc / user) and edges only ever run user -> doc. The
// helpers below treat that graph literally instead of pretending it's
// a generic property graph -- DIRECTION OUT means "follow reads
// outward from a user", DIRECTION IN means "follow readers inward
// into a document", and multi-hop DEPTH only extends past one hop when
// DIRECTION BOTH lets traversal alternate user->doc->user->doc...

// autoRelGraph indexes a set of (already block-scoped, already live)
// AutoRelations by both endpoints so traversal in either direction is
// O(1) per node instead of a full scan.
type autoRelGraph struct {
	rels    []*AutoRelation
	userAdj map[string][]*AutoRelation
	docAdj  map[string][]*AutoRelation
}

func buildAutoRelGraph(rels []*AutoRelation) *autoRelGraph {
	g := &autoRelGraph{
		rels:    rels,
		userAdj: make(map[string][]*AutoRelation),
		docAdj:  make(map[string][]*AutoRelation),
	}
	for _, r := range rels {
		g.userAdj[r.UserID] = append(g.userAdj[r.UserID], r)
		g.docAdj[r.DocID] = append(g.docAdj[r.DocID], r)
	}
	return g
}

// resolveNode figures out whether an id given via FROM/TO refers to a
// document or a user in this graph, preferring a document match since
// SHOW AUTORELATIONS <block> is framed around the block's documents.
// Returns ok=false if the id isn't an endpoint of any live relation.
func (g *autoRelGraph) resolveNode(id string) (kind string, ok bool) {
	if len(g.docAdj[id]) > 0 {
		return "doc", true
	}
	if len(g.userAdj[id]) > 0 {
		return "user", true
	}
	return "", false
}

type bfsNode struct {
	kind   string // "doc" | "user"
	id     string
	depth  int
	via    *AutoRelation // edge that reached this node from its parent (nil for the root)
	parent *bfsNode
}

// neighbors returns the nodes directly reachable from (kind, id) given
// the requested traversal direction. OUT only fires from a user node
// (reads go out to documents); IN only fires from a doc node (readers
// come in from users); BOTH allows both, which is what makes chains
// longer than one hop possible.
func neighbors(g *autoRelGraph, kind, id, direction string) []*bfsNode {
	var out []*bfsNode
	if kind == "user" && (direction == "OUT" || direction == "BOTH") {
		for _, r := range g.userAdj[id] {
			out = append(out, &bfsNode{kind: "doc", id: r.DocID, via: r})
		}
	}
	if kind == "doc" && (direction == "IN" || direction == "BOTH") {
		for _, r := range g.docAdj[id] {
			out = append(out, &bfsNode{kind: "user", id: r.UserID, via: r})
		}
	}
	return out
}

// bfs walks the graph breadth-first from (startKind, startID) up to
// maxDepth hops, respecting direction, and returns every node reached
// in visit order (index 0 is always the root, depth 0).
func (g *autoRelGraph) bfs(startKind, startID, direction string, maxDepth int) []*bfsNode {
	root := &bfsNode{kind: startKind, id: startID, depth: 0}
	visited := map[string]bool{startKind + ":" + startID: true}
	queue := []*bfsNode{root}
	var order []*bfsNode

	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		if n.depth >= maxDepth {
			continue
		}
		for _, nb := range neighbors(g, n.kind, n.id, direction) {
			key := nb.kind + ":" + nb.id
			if visited[key] {
				continue
			}
			visited[key] = true
			nb.depth = n.depth + 1
			nb.parent = n
			queue = append(queue, nb)
		}
	}
	return order
}

// --- union-find, used only to detect cycles among a relation set ----

type unionFind struct{ parent map[string]string }

func newUnionFind() *unionFind { return &unionFind{parent: make(map[string]string)} }

func (u *unionFind) find(x string) string {
	if _, ok := u.parent[x]; !ok {
		u.parent[x] = x
		return x
	}
	if u.parent[x] != x {
		u.parent[x] = u.find(u.parent[x])
	}
	return u.parent[x]
}

// union merges the components containing a and b, returning true if
// they were already in the same component -- i.e. this edge closes a
// cycle rather than extending a tree.
func (u *unionFind) union(a, b string) (formedCycle bool) {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return true
	}
	u.parent[ra] = rb
	return false
}

// findCycles processes rels in a stable order (oldest first_seen
// first) and returns the subset of relations whose edge connects two
// nodes that were already reachable from one another -- i.e. the
// edges that close a cycle in the undirected access graph.
func findCycles(rels []*AutoRelation) []*AutoRelation {
	ordered := make([]*AutoRelation, len(rels))
	copy(ordered, rels)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].FirstSeen.Before(ordered[j].FirstSeen) })

	uf := newUnionFind()
	var cycles []*AutoRelation
	for _, r := range ordered {
		if uf.union("doc:"+r.DocID, "user:"+r.UserID) {
			cycles = append(cycles, r)
		}
	}
	return cycles
}

// degree returns how many live relations touch each side of r within
// the given graph -- used for ORDER BY DEGREE and orphan detection.
func (g *autoRelGraph) degree(r *AutoRelation) (docDegree, userDegree int) {
	return len(g.docAdj[r.DocID]), len(g.userAdj[r.UserID])
}

// orphans returns relations that form an isolated pair: the document
// was only ever read by this one user, and this user only ever read
// this one document -- i.e. neither side connects to anything else in
// the working set.
func findOrphans(g *autoRelGraph) []*AutoRelation {
	var out []*AutoRelation
	for _, r := range g.rels {
		docDeg, userDeg := g.degree(r)
		if docDeg == 1 && userDeg == 1 {
			out = append(out, r)
		}
	}
	return out
}

// autoRelStats summarizes a working set of relations the way STATS /
// SUMMARY report them.
type autoRelStats struct {
	Nodes        int
	Relations    int
	Docs         int
	Users        int
	AvgAccess    float64
	AvgDegree    float64
	MaxDegree    int
	Cycles       int
	Orphans      int
	Broken       int
	AvgRelevance float64
}

func computeAutoRelStats(g *autoRelGraph, brokenCount int) autoRelStats {
	stats := autoRelStats{
		Relations: len(g.rels),
		Docs:      len(g.docAdj),
		Users:     len(g.userAdj),
		Broken:    brokenCount,
	}
	stats.Nodes = stats.Docs + stats.Users
	if stats.Nodes > 0 {
		// Every relation contributes one degree to a doc node and one
		// to a user node, so the sum of all degrees is 2*Relations.
		stats.AvgDegree = 2 * float64(stats.Relations) / float64(stats.Nodes)
	}

	var totalAccess, totalRelevance float64
	for _, r := range g.rels {
		totalAccess += float64(r.AccessCount)
		totalRelevance += r.Relevance
		docDeg, userDeg := g.degree(r)
		if docDeg > stats.MaxDegree {
			stats.MaxDegree = docDeg
		}
		if userDeg > stats.MaxDegree {
			stats.MaxDegree = userDeg
		}
	}
	if stats.Relations > 0 {
		stats.AvgAccess = totalAccess / float64(stats.Relations)
		stats.AvgRelevance = totalRelevance / float64(stats.Relations)
	}
	stats.Cycles = len(findCycles(g.rels))
	stats.Orphans = len(findOrphans(g))
	return stats
}
