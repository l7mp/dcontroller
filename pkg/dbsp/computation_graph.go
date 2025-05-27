package dbsp

import (
	"fmt"
)

// ComputationGraph represents a tree of computation nodes.
type ComputationGraph struct {
	nodes  map[string]*GraphNode
	inputs []string // Input node IDs
	output string   // Output node ID (always exactly one)
	nextID int      // For generating unique node IDs
}

type GraphNode struct {
	ID     string
	Op     Operator
	Inputs []*GraphNode
	Output *GraphNode
}

func NewComputationGraph() *ComputationGraph {
	return &ComputationGraph{
		nodes:  make(map[string]*GraphNode),
		inputs: make([]string, 0),
		nextID: 0,
	}
}

// AddNode adds a node to the graph and returns its ID
func (cg *ComputationGraph) AddNode(node Operator) string {
	id := fmt.Sprintf("node_%d_%s", cg.nextID, node.Name())
	cg.nextID++

	graphNode := &GraphNode{
		ID:     id,
		Op:     node,
		Inputs: make([]*GraphNode, 0),
	}

	cg.nodes[id] = graphNode
	return id
}

// Connect two nodes with an edge
func (cg *ComputationGraph) Connect(fromID, toID string) error {
	from, fromExists := cg.nodes[fromID]
	to, toExists := cg.nodes[toID]

	if !fromExists {
		return fmt.Errorf("source node %s not found", fromID)
	}
	if !toExists {
		return fmt.Errorf("target node %s not found", toID)
	}

	// Check arity constraints
	expectedInputs := to.Op.Arity()
	currentInputs := len(to.Inputs)

	if currentInputs >= expectedInputs {
		return fmt.Errorf("node %s already has %d inputs (max %d)",
			toID, currentInputs, expectedInputs)
	}

	from.Output = to
	to.Inputs = append(to.Inputs, from)

	return nil
}

// SetInputs marks nodes as inputs to the computation
func (cg *ComputationGraph) SetInputs(nodeIDs ...string) error {
	for _, nodeID := range nodeIDs {
		if _, exists := cg.nodes[nodeID]; !exists {
			return fmt.Errorf("input node %s not found", nodeID)
		}
	}
	cg.inputs = nodeIDs
	return nil
}

// SetOutput marks a single node as the output
func (cg *ComputationGraph) SetOutput(nodeID string) error {
	if _, exists := cg.nodes[nodeID]; !exists {
		return fmt.Errorf("output node %s not found", nodeID)
	}
	cg.output = nodeID
	return nil
}

// Validate checks graph structure
func (cg *ComputationGraph) Validate() error {
	if len(cg.inputs) == 0 {
		return fmt.Errorf("no input nodes specified")
	}
	if cg.output == "" {
		return fmt.Errorf("must have exactly one output node")
	}

	// Check that all nodes have correct number of inputs
	for id, node := range cg.nodes {
		expectedInputs := node.Op.Arity()
		actualInputs := len(node.Inputs)

		// Input nodes can have 0 inputs regardless of arity
		isInputNode := false
		for _, inputID := range cg.inputs {
			if inputID == id {
				isInputNode = true
				break
			}
		}

		if !isInputNode && actualInputs != expectedInputs {
			return fmt.Errorf("node %s has %d inputs, expected %d",
				id, actualInputs, expectedInputs)
		}
	}

	return nil
}

// GetNode returns a node by ID
func (cg *ComputationGraph) GetNode(id string) (*GraphNode, bool) {
	node, exists := cg.nodes[id]
	return node, exists
}

// GetAllNodes returns all nodes
func (cg *ComputationGraph) GetAllNodes() map[string]*GraphNode {
	return cg.nodes
}

// DeepCopy creates a copy of the graph
func (cg *ComputationGraph) DeepCopy() *ComputationGraph {
	// Simplified - just return the same graph for now
	// Real implementation would need to copy all nodes and rewire connections
	return cg
}

// String representation for debugging
func (cg *ComputationGraph) String() string {
	result := fmt.Sprintf("Graph with %d nodes:\n", len(cg.nodes))
	result += fmt.Sprintf("Inputs: %v\n", cg.inputs)
	result += fmt.Sprintf("Output: %v\n", cg.output)

	for id, node := range cg.nodes {
		inputIDs := make([]string, len(node.Inputs))
		for i, input := range node.Inputs {
			inputIDs[i] = input.ID
		}
		result += fmt.Sprintf("  %s (%s): inputs=%v, output=%v\n",
			id, node.Op.Name(), inputIDs, cg.output)
	}

	return result
}
