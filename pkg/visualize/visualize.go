// Package visualize provides functionality for visualizing Δ-controller operators as diagrams.
package visualize

import (
	"fmt"
	"strings"

	"github.com/emicklei/dot"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
)

// Graph represents the visualization graph of an operator.
type Graph struct {
	OperatorName string
	Controllers  []ControllerNode
	Connections  []Connection
}

// ControllerNode represents a single controller in the graph.
type ControllerNode struct {
	Name     string
	Sources  []ResourceRef
	Pipeline []string
	Target   ResourceRef
}

// ResourceRef represents a reference to a Kubernetes resource (GVK).
type ResourceRef struct {
	Group   string
	Version string
	Kind    string
	Type    string // Source type (Watcher/Periodic/OneShot) or Target type (Updater/Patcher)
}

// Connection represents an edge between two controllers (via a view).
type Connection struct {
	FromController string
	ToController   string
	ViewKind       string
	TargetType     string // Type when written (Updater/Patcher)
	SourceType     string // Type when read (Watcher/Periodic/OneShot)
}

// BuildGraph constructs a visualization graph from an Operator spec.
func BuildGraph(op *opv1a1.Operator) *Graph {
	g := &Graph{
		OperatorName: op.Name,
		Controllers:  make([]ControllerNode, 0, len(op.Spec.Controllers)),
		Connections:  make([]Connection, 0),
	}

	// Build controller nodes.
	for _, ctrl := range op.Spec.Controllers {
		node := ControllerNode{
			Name:     ctrl.Name,
			Sources:  make([]ResourceRef, len(ctrl.Sources)),
			Pipeline: extractPipelineStages(ctrl.Pipeline),
			Target:   buildResourceRef(ctrl.Target.Resource, string(ctrl.Target.Type)),
		}

		for i, src := range ctrl.Sources {
			srcType := string(src.Type)
			if srcType == "" {
				srcType = "Watcher"
			}
			node.Sources[i] = buildResourceRef(src.Resource, srcType)
		}

		g.Controllers = append(g.Controllers, node)
	}

	// Build connections between controllers (via views).
	g.buildConnections()

	return g
}

// buildResourceRef creates a ResourceRef from a Resource and type.
func buildResourceRef(res opv1a1.Resource, resType string) ResourceRef {
	group := ""
	if res.Group != nil {
		group = *res.Group
	}
	version := ""
	if res.Version != nil {
		version = *res.Version
	}

	return ResourceRef{
		Group:   group,
		Version: version,
		Kind:    res.Kind,
		Type:    resType,
	}
}

// extractPipelineStages extracts top-level pipeline stage names.
func extractPipelineStages(pipeline opv1a1.Pipeline) []string {
	stages := make([]string, 0, len(pipeline.Ops))
	for _, op := range pipeline.Ops {
		opType := op.OpType()
		// Add placeholder for the action.
		stages = append(stages, fmt.Sprintf("%s: ...", opType))
	}
	return stages
}

// buildConnections identifies connections between controllers via views.
func (g *Graph) buildConnections() {
	// Map of view kinds to the controllers that produce them and their target type.
	viewProducers := make(map[string]struct {
		controller string
		targetType string
	})

	// First pass: identify which controllers produce which views.
	for _, ctrl := range g.Controllers {
		if isView(ctrl.Target) {
			viewKey := resourceKey(ctrl.Target)
			viewProducers[viewKey] = struct {
				controller string
				targetType string
			}{ctrl.Name, ctrl.Target.Type}
		}
	}

	// Second pass: find controllers that consume views and create connections.
	for _, ctrl := range g.Controllers {
		for _, src := range ctrl.Sources {
			if isView(src) {
				viewKey := resourceKey(src)
				if producer, exists := viewProducers[viewKey]; exists {
					g.Connections = append(g.Connections, Connection{
						FromController: producer.controller,
						ToController:   ctrl.Name,
						ViewKind:       src.Kind,
						TargetType:     producer.targetType,
						SourceType:     src.Type,
					})
				}
			}
		}
	}
}

// isView checks if a resource is a view (either has "View" in kind or view.dcontroller.io in group).
func isView(ref ResourceRef) bool {
	return strings.Contains(ref.Kind, "View") || strings.Contains(ref.Group, "view.dcontroller.io")
}

// resourceKey creates a unique key for a resource.
func resourceKey(ref ResourceRef) string {
	return fmt.Sprintf("%s/%s/%s", ref.Group, ref.Version, ref.Kind)
}

// IsTerminalView checks if a view is terminal (produced but not consumed by any controller).
func (g *Graph) IsTerminalView(ctrl ControllerNode) bool {
	if !isView(ctrl.Target) {
		return false
	}

	targetKey := resourceKey(ctrl.Target)
	// Check if any controller consumes this view.
	for _, c := range g.Controllers {
		for _, src := range c.Sources {
			if isView(src) && resourceKey(src) == targetKey {
				return false
			}
		}
	}
	return true
}

// FormatGVK formats a resource reference as a GVK string for display (without type).
func FormatGVK(ref ResourceRef) string {
	parts := []string{}
	if ref.Group != "" {
		parts = append(parts, ref.Group)
	}
	if ref.Version != "" {
		if ref.Group == "" {
			parts = append(parts, "v1") // Default for core resources.
		}
		parts = append(parts, ref.Version)
	}
	parts = append(parts, ref.Kind)

	return strings.Join(parts, "/")
}

// BuildDotGraph creates a dot.Graph from the visualization graph.
// This unified graph can then be rendered in different formats (DOT, Mermaid, etc.).
func BuildDotGraph(g *Graph) *dot.Graph {
	graph := dot.NewGraph(dot.Directed)
	graph.Attr("rankdir", "LR")    // Left to right layout.
	graph.Attr("compound", "true") // Allow edges between clusters.
	graph.Attr("newrank", "true")  // Better ranking algorithm.
	graph.Attr("label", g.OperatorName)
	graph.Attr("labelloc", "t") // Label at top.
	graph.Attr("fontsize", "16")

	// Track nodes for connections.
	controllerNodes := make(map[string]dot.Node)
	sourceNodes := make(map[string]dot.Node)
	targetNodes := make(map[string]dot.Node)

	// Create controller nodes.
	for _, ctrl := range g.Controllers {
		// Build label: controller-name: @stage1 -> @stage2 -> ...
		label := ctrl.Name
		if len(ctrl.Pipeline) > 0 {
			label += ": "
			for i, stage := range ctrl.Pipeline {
				if i > 0 {
					label += " -> "
				}
				label += strings.TrimSuffix(stage, ": ...")
			}
		}

		node := graph.Node(ctrl.Name).
			Attr("label", label).
			Attr("shape", "box").
			Attr("style", "filled,rounded").
			Attr("fillcolor", "lightblue").
			Attr("color", "darkblue").
			Attr("penwidth", "2").
			Attr("fontname", "helvetica")

		controllerNodes[ctrl.Name] = node
	}

	// Create source nodes and edges.
	for _, ctrl := range g.Controllers {
		for _, src := range ctrl.Sources {
			if !isView(src) {
				key := resourceKey(src)
				srcNode, exists := sourceNodes[key]
				if !exists {
					nodeLabel := FormatGVK(src)
					srcNode = graph.Node(key).
						Attr("label", nodeLabel).
						Attr("shape", "ellipse").
						Attr("style", "filled").
						Attr("fillcolor", "lightgreen")
					sourceNodes[key] = srcNode
				}
				// Connect source to controller with type label.
				sourceType := src.Type
				if sourceType == "" {
					sourceType = "Watcher"
				}
				graph.Edge(srcNode, controllerNodes[ctrl.Name]).
					Attr("label", sourceType).
					Attr("fontname", "helvetica").
					Attr("fontsize", "10")
			}
		}
	}

	// Create target nodes and edges (external resources and terminal views).
	for _, ctrl := range g.Controllers {
		// Show non-view targets or terminal views.
		if !isView(ctrl.Target) || g.IsTerminalView(ctrl) {
			key := resourceKey(ctrl.Target)
			targetNode, exists := targetNodes[key]
			if !exists {
				nodeLabel := FormatGVK(ctrl.Target)
				// Use different style for terminal views.
				if g.IsTerminalView(ctrl) {
					targetNode = graph.Node(key).
						Attr("label", nodeLabel).
						Attr("shape", "box").
						Attr("style", "filled,rounded").
						Attr("fillcolor", "lightcyan")
				} else {
					targetNode = graph.Node(key).
						Attr("label", nodeLabel).
						Attr("shape", "ellipse").
						Attr("style", "filled").
						Attr("fillcolor", "lightyellow")
				}
				targetNodes[key] = targetNode
			}
			// Connect controller to target with type label.
			targetType := ctrl.Target.Type
			if targetType == "" {
				targetType = "Updater"
			}
			graph.Edge(controllerNodes[ctrl.Name], targetNode).
				Attr("label", targetType).
				Attr("fontname", "helvetica").
				Attr("fontsize", "10")
		}
	}

	// Connect controllers via views.
	for _, conn := range g.Connections {
		fromNode, fromExists := controllerNodes[conn.FromController]
		toNode, toExists := controllerNodes[conn.ToController]
		if fromExists && toExists {
			// Build label: ViewKind (write:TargetType, read:SourceType)
			targetType := conn.TargetType
			if targetType == "" {
				targetType = "Updater"
			}
			sourceType := conn.SourceType
			if sourceType == "" {
				sourceType = "Watcher"
			}
			label := fmt.Sprintf("%s (write:%s, read:%s)", conn.ViewKind, targetType, sourceType)

			graph.Edge(fromNode, toNode).
				Attr("label", label).
				Attr("style", "dashed").
				Attr("color", "blue").
				Attr("fontname", "helvetica").
				Attr("fontsize", "10")
		}
	}

	return graph
}
