package caimandb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// statLine renders one "Label............. value" row, dot-leader
// style, matching the look of familiar `STATS`-style admin output.
func statLine(label, value string) string {
	pad := 20 - len(label)
	if pad < 1 {
		pad = 1
	}
	return label + strings.Repeat(".", pad) + " " + value + "\n"
}

func renderAutoRelStats(db, block string, s autoRelStats) string {
	var sb strings.Builder
	sb.WriteString(statLine("Block", db+"."+block))
	sb.WriteString(statLine("Nodes", strconv.Itoa(s.Nodes)))
	sb.WriteString(statLine("Relations", strconv.Itoa(s.Relations)))
	sb.WriteString(statLine("Documents", strconv.Itoa(s.Docs)))
	sb.WriteString(statLine("Users", strconv.Itoa(s.Users)))
	sb.WriteString(statLine("Average Degree", fmt.Sprintf("%.2f", s.AvgDegree)))
	sb.WriteString(statLine("Average Access", fmt.Sprintf("%.2f", s.AvgAccess)))
	sb.WriteString(statLine("Average Relevance", fmt.Sprintf("%.2f", s.AvgRelevance)))
	sb.WriteString(statLine("Max Degree", strconv.Itoa(s.MaxDegree)))
	sb.WriteString(statLine("Cycles", strconv.Itoa(s.Cycles)))
	sb.WriteString(statLine("Orphans", strconv.Itoa(s.Orphans)))
	sb.WriteString(statLine("Broken Links", strconv.Itoa(s.Broken)))
	return sb.String()
}

func renderAutoRelSummary(db, block string, s autoRelStats) string {
	connectedPct := 100.0
	if s.Nodes > 0 {
		isolated := s.Orphans * 2
		connectedPct = 100 * float64(s.Nodes-isolated) / float64(s.Nodes)
	}

	var sb strings.Builder
	sb.WriteString(db + "." + block + "\n")
	sb.WriteString(strings.Repeat("─", 32) + "\n")
	sb.WriteString(statLine("Nodes", strconv.Itoa(s.Nodes)))
	sb.WriteString(statLine("Relations", strconv.Itoa(s.Relations)))
	sb.WriteString(statLine("Connected", fmt.Sprintf("%.1f%%", connectedPct)))
	sb.WriteString(statLine("Average Degree", fmt.Sprintf("%.2f", s.AvgDegree)))
	sb.WriteString(statLine("Cycles", strconv.Itoa(s.Cycles)))
	sb.WriteString(statLine("Broken Links", strconv.Itoa(s.Broken)))
	sb.WriteString(statLine("Orphans", strconv.Itoa(s.Orphans)))
	return sb.String()
}

func renderAutoRelTable(db, block string, rels []*AutoRelation, broken map[*AutoRelation]bool, verbose bool) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Auto-relations for %s.%s (temporal):\n", db, block))
	for _, ar := range rels {
		line := fmt.Sprintf("  doc:%s user:%s  accesses:%d  relevance:%.2f  last_seen:%s",
			ar.DocID, ar.UserID, ar.AccessCount, ar.Relevance,
			ar.LastSeen.Format("2006-01-02 15:04:05"))
		if broken[ar] {
			line += "  [BROKEN: document deleted]"
		}
		if verbose {
			line += fmt.Sprintf("  first_seen:%s  expires:%s",
				ar.FirstSeen.Format("2006-01-02 15:04:05"), ar.ExpiresAt.Format("2006-01-02 15:04:05"))
			if len(ar.KeyMetadata) > 0 {
				line += "  meta:" + jsonOut(ar.KeyMetadata)
			}
		} else if len(ar.KeyMetadata) > 0 {
			line += "  meta:" + jsonOut(ar.KeyMetadata)
		}
		sb.WriteString(line + "\n")
	}
	return sb.String()
}

// renderAutoRelTree renders a BFS traversal (see relations_auto_graph.go)
// as an indented tree, in the same spirit as `tree`/PATHS output: the
// root is the FROM node, and each line below it is one hop reached
// via the edge shown.
func renderAutoRelTree(order []*bfsNode, verbose bool) string {
	if len(order) == 0 {
		return "(empty)\n"
	}

	children := make(map[*bfsNode][]*bfsNode)
	var root *bfsNode
	for _, n := range order {
		if n.parent == nil {
			root = n
			continue
		}
		children[n.parent] = append(children[n.parent], n)
	}
	if root == nil {
		return "(empty)\n"
	}

	var sb strings.Builder
	sb.WriteString(autoRelNodeLabel(root, verbose) + "\n")
	renderAutoRelTreeChildren(&sb, children, root, "", verbose)
	return sb.String()
}

func renderAutoRelTreeChildren(sb *strings.Builder, children map[*bfsNode][]*bfsNode, n *bfsNode, prefix string, verbose bool) {
	kids := children[n]
	for i, k := range kids {
		last := i == len(kids)-1
		branch, nextPrefix := "├── ", prefix+"│    "
		if last {
			branch, nextPrefix = "└── ", prefix+"     "
		}
		sb.WriteString(prefix + branch + autoRelNodeLabel(k, verbose) + "\n")
		renderAutoRelTreeChildren(sb, children, k, nextPrefix, verbose)
	}
}

func autoRelNodeLabel(n *bfsNode, verbose bool) string {
	label := n.kind + ":" + n.id
	if verbose && n.via != nil {
		label += fmt.Sprintf(" (relevance:%.2f, accesses:%d)", n.via.Relevance, n.via.AccessCount)
	}
	return label
}

// renderAutoRelGraph prints the working set as a plain adjacency list,
// one line per user, listing the documents they've read.
func renderAutoRelGraph(g *autoRelGraph) string {
	var sb strings.Builder
	sb.WriteString("Adjacency (user -> documents read):\n")

	users := make([]string, 0, len(g.userAdj))
	for u := range g.userAdj {
		users = append(users, u)
	}
	sort.Strings(users)

	for _, u := range users {
		docs := make([]string, 0, len(g.userAdj[u]))
		for _, r := range g.userAdj[u] {
			docs = append(docs, r.DocID)
		}
		sort.Strings(docs)
		sb.WriteString(fmt.Sprintf("  user:%s -> %s\n", u, strings.Join(docs, ", ")))
	}
	return sb.String()
}
