package visualize

// DotGenerator generates Graphviz DOT diagrams.
type DotGenerator struct{}

// Generate creates a Graphviz DOT diagram from the graph.
func (d *DotGenerator) Generate(g *Graph) string {
	dotGraph := BuildDotGraph(g)
	return dotGraph.String()
}
