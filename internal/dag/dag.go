// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dag implements a language for expressing directed acyclic
// graphs.
//
// The general syntax of a rule is:
//
//	a, b < c, d;
//
// which means c and d come after a and b in the partial order
// (that is, there are edges from c and d to a and b),
// but doesn't provide a relative order between a vs b or c vs d.
//
// The rules can chain together, as in:
//
//	e < f, g < h;
//
// which is equivalent to
//
//	e < f, g;
//	f, g < h;
//
// Except for the special bottom element "NONE", each name
// must appear exactly once on the right-hand side of any rule.
// That rule serves as the definition of the allowed successor
// for that name. The definition must appear before any uses
// of the name on the left-hand side of a rule. (That is, the
// rules themselves must be ordered according to the partial
// order, for easier reading by people.)
//
// Negative assertions double-check the partial order:
//
//	i !< j
//
// means that it must NOT be the case that i < j.
// Negative assertions may appear anywhere in the rules,
// even before i and j have been defined.
//
// Comments begin with #.
package dag

import (
	"sort"
)

type Graph struct {
	Nodes   []string
	byLabel map[string]int
	edges   map[string]map[string]bool
}

func (g *Graph) AddNode(label string) bool {
	if _, ok := g.byLabel[label]; ok {
		return false
	}
	g.byLabel[label] = len(g.Nodes)
	g.Nodes = append(g.Nodes, label)
	g.edges[label] = map[string]bool{}
	return true
}

func (g *Graph) HasNode(label string) bool {
	_, ok := g.byLabel[label]
	return ok
}

func (g *Graph) AddEdge(from, to string) {
	g.edges[from][to] = true
}

func (g *Graph) DelEdge(from, to string) {
	delete(g.edges[from], to)
}

func (g *Graph) HasEdge(from, to string) bool {
	return g.edges[from] != nil && g.edges[from][to]
}

func (g *Graph) Edges(from string) []string {
	edges := make([]string, 0, 16)
	for k := range g.edges[from] {
		edges = append(edges, k)
	}
	sort.Slice(edges, func(i, j int) bool { return g.byLabel[edges[i]] < g.byLabel[edges[j]] })
	return edges
}
