// Copyright 2024 rg0now. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dag

// New creates an empty grapg.
func New() *Graph {
	return &Graph{byLabel: map[string]int{}, edges: map[string]map[string]bool{}}
}

// Roots returns a roots of the DAG, i.e., the nodes without an incoming edge.
func (g *Graph) Roots() []string {
	roots := make([]string, 0, len(g.Nodes))

	for _, j := range g.Nodes {
		isRoot := true
		for _, i := range g.Nodes {
			if g.HasEdge(i, j) {
				isRoot = false
				break
			}
		}
		if isRoot {
			roots = append(roots, j)
		}
	}
	return roots
}
