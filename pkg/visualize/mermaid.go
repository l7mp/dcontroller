package visualize

import (
	"fmt"

	"github.com/emicklei/dot"
)

// MermaidGenerator generates Mermaid flowchart diagrams.
type MermaidGenerator struct{}

// Generate creates a Mermaid flowchart from the graph using the dot library.
func (m *MermaidGenerator) Generate(g *Graph) string {
	dotGraph := BuildDotGraph(g)

	// Generate Mermaid flowchart with left-to-right orientation.
	mermaid := dot.MermaidFlowchart(dotGraph, dot.MermaidLeftToRight)

	// Wrap in markdown code block.
	return fmt.Sprintf("```mermaid\n%s\n```\n", mermaid)
}
