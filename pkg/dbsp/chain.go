package dbsp

import (
	"fmt"
	"strings"
)

// ChainGraph represents your specialized graph structure directly
// Always: [Inputs] -> [Optional N-ary Join] -> [Chain of Linear Ops] -> [Output]
type ChainGraph struct {
	inputs   []string              // Input node IDs
	inputIdx map[string]string     // Map internal id to external input name
	joinNode string                // Optional N-ary join node ID (empty if no join)
	chain    []string              // Ordered chain of operation node IDs
	output   string                // Output node ID
	nodes    map[string]*GraphNode // All nodes by ID
	nextID   int                   // For generating unique node IDs
}

type GraphNode struct {
	ID     string
	Op     Operator
	Inputs []*GraphNode
	Output *GraphNode
}

func NewChainGraph() *ChainGraph {
	return &ChainGraph{
		inputs:   make([]string, 0),
		inputIdx: map[string]string{},
		chain:    make([]string, 0),
		nodes:    make(map[string]*GraphNode),
		nextID:   0,
	}
}

// AddInput adds an input node
func (g *ChainGraph) AddInput(op *InputOp) string {
	// if input already exists, silently return
	for _, v := range g.inputIdx {
		if v == op.Name() { // external name equals internal id
			return ""
		}
	}
	id := fmt.Sprintf("input_%d", g.nextID)
	g.nextID++
	g.inputs = append(g.inputs, id)
	g.inputIdx[id] = op.Name()
	g.nodes[id] = &GraphNode{ID: id, Op: op}
	return id
}

// Arity returns the number of inputs of the chain.
func (g *ChainGraph) Arity() int {
	return len(g.inputs)
}

// GetInput returns a named input of the chain.
func (g *ChainGraph) GetInput(name string) *InputOp {
	for _, inputID := range g.inputs {
		inputName, ok := g.inputIdx[inputID]
		if ok && name == inputName {
			return g.nodes[inputID].Op.(*InputOp)
		}
	}
	return nil
}

// SetJoin sets the join node (if multiple inputs)
func (g *ChainGraph) SetJoin(op Operator) string {
	id := fmt.Sprintf("join_%d", g.nextID)
	g.nextID++

	g.nodes[id] = &GraphNode{ID: id, Op: op}
	g.joinNode = id

	// If this is the only operation, it's also the output
	if len(g.chain) == 0 {
		g.output = id
	}

	return id
}

// GetChain returns the operations along the linear chain.
func (g *ChainGraph) GetChain() []Operator {
	ret := make([]Operator, len(g.chain))
	for i, id := range g.chain {
		ret[i] = g.nodes[id].Op
	}
	return ret
}

// AddToChain adds an operation to the linear chain
func (g *ChainGraph) AddToChain(op Operator) string {
	id := fmt.Sprintf("op_%d", g.nextID)
	g.nextID++

	g.nodes[id] = &GraphNode{ID: id, Op: op}
	g.chain = append(g.chain, id)

	// Update output to be the last node in chain
	g.output = id
	return id
}

// GetStartNode returns the node where the linear chain begins
func (g *ChainGraph) GetStartNode() string {
	if g.joinNode != "" {
		return g.joinNode
	}
	if len(g.inputs) > 0 {
		return g.inputs[0]
	}
	return ""
}

// Validate checks the graph structure
func (g *ChainGraph) Validate() error {
	if len(g.inputs) == 0 {
		return fmt.Errorf("no input nodes")
	}
	if len(g.inputs) > 1 && g.joinNode == "" {
		return fmt.Errorf("multiple inputs require a join node")
	}
	if g.output == "" {
		// Try to infer output
		switch {
		case len(g.chain) > 0:
			g.output = g.chain[len(g.chain)-1]
		case g.joinNode != "":
			g.output = g.joinNode
		case len(g.inputs) == 1:
			g.output = g.inputs[0]
		default:
			return fmt.Errorf("no output node and cannot infer one")
		}
	}
	return nil
}

// String representation for debugging (horizontal layout)
func (g *ChainGraph) String() string {
	parts := []string{}

	// Build the flow description
	if len(g.inputs) == 1 {
		// Single input: Input -> [Join] -> Chain -> Output
		inputName := g.nodes[g.inputs[0]].Op.id()
		parts = append(parts, inputName)
	} else {
		// Multiple inputs: (Input1, Input2, ...) -> Join -> Chain -> Output
		inputNames := make([]string, len(g.inputs))
		for i, inputID := range g.inputs {
			inputNames[i] = g.nodes[inputID].Op.id()
		}
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(inputNames, ", ")))
	}

	// Add join if present
	if g.joinNode != "" {
		parts = append(parts, g.nodes[g.joinNode].Op.id())
	}

	// Add chain operations
	for _, nodeID := range g.chain {
		parts = append(parts, g.nodes[nodeID].Op.id())
	}

	return strings.Join(parts, " → ")
}

// getNode returns a node from its id. Names are internal to the graph, only inputs have proper
// names.
func (g *ChainGraph) getNode(id string) *GraphNode {
	return g.nodes[id]
}
