package dbsp

import (
	"fmt"
)

// LinearChainGraph represents your specialized graph structure directly
// Always: [Inputs] -> [Optional N-ary Join] -> [Chain of Linear Ops] -> [Output]
type LinearChainGraph struct {
	inputs   []string              // Input node IDs
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

func NewLinearChainGraph() *LinearChainGraph {
	return &LinearChainGraph{
		inputs: make([]string, 0),
		chain:  make([]string, 0),
		nodes:  make(map[string]*GraphNode),
		nextID: 0,
	}
}

// AddInput adds an input node
func (g *LinearChainGraph) AddInput(op Operator) string {
	id := fmt.Sprintf("input_%d", g.nextID)
	g.nextID++

	g.nodes[id] = &GraphNode{ID: id, Op: op}
	g.inputs = append(g.inputs, id)
	return id
}

// SetJoin sets the join node (if multiple inputs)
func (g *LinearChainGraph) SetJoin(op Operator) string {
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

// AddToChain adds an operation to the linear chain
func (g *LinearChainGraph) AddToChain(op Operator) string {
	id := fmt.Sprintf("op_%d", g.nextID)
	g.nextID++

	g.nodes[id] = &GraphNode{ID: id, Op: op}
	g.chain = append(g.chain, id)

	// Update output to be the last node in chain
	g.output = id
	return id
}

// GetStartNode returns the node where the linear chain begins
func (g *LinearChainGraph) GetStartNode() string {
	if g.joinNode != "" {
		return g.joinNode
	}
	if len(g.inputs) > 0 {
		return g.inputs[0]
	}
	return ""
}

// Validate checks the graph structure
func (g *LinearChainGraph) Validate() error {
	if len(g.inputs) == 0 {
		return fmt.Errorf("no input nodes")
	}
	if len(g.inputs) > 1 && g.joinNode == "" {
		return fmt.Errorf("multiple inputs require a join node")
	}
	if g.output == "" {
		// Try to infer output
		if len(g.chain) > 0 {
			g.output = g.chain[len(g.chain)-1]
		} else if g.joinNode != "" {
			g.output = g.joinNode
		} else if len(g.inputs) == 1 {
			g.output = g.inputs[0]
		} else {
			return fmt.Errorf("no output node and cannot infer one")
		}
	}
	return nil
}

// String representation for debugging
func (g *LinearChainGraph) String() string {
	result := fmt.Sprintf("LinearChainGraph:\n")
	result += fmt.Sprintf("  Inputs (%d): ", len(g.inputs))
	for i, inputID := range g.inputs {
		if i > 0 {
			result += ", "
		}
		result += g.nodes[inputID].Op.Name()
	}
	result += "\n"

	if g.joinNode != "" {
		result += fmt.Sprintf("  Join: %s\n", g.nodes[g.joinNode].Op.Name())
	}

	result += "  Chain: "
	for i, nodeID := range g.chain {
		if i > 0 {
			result += " -> "
		}
		result += g.nodes[nodeID].Op.Name()
	}
	result += "\n"

	return result
}

// LinearChainRewriteEngine works directly on LinearChainGraph
type LinearChainRewriteEngine struct {
	rules []LinearChainRule
}

func NewLinearChainRewriteEngine() *LinearChainRewriteEngine {
	re := &LinearChainRewriteEngine{
		rules: make([]LinearChainRule, 0),
	}

	// Add rules in order of application priority
	re.AddRule(&JoinIncrementalizationRule{})
	re.AddRule(&LinearChainIncrementalizationRule{})
	re.AddRule(&IntegrationDifferentiationEliminationRule{})
	re.AddRule(&DistinctOptimizationRule{})
	re.AddRule(&LinearOperatorFusionRule{})

	return re
}

type LinearChainRule interface {
	Name() string
	CanApply(graph *LinearChainGraph) bool
	Apply(graph *LinearChainGraph) error
}

func (re *LinearChainRewriteEngine) AddRule(rule LinearChainRule) {
	re.rules = append(re.rules, rule)
}

// Optimize applies all rules until fixpoint
func (re *LinearChainRewriteEngine) Optimize(graph *LinearChainGraph) error {
	if err := graph.Validate(); err != nil {
		return fmt.Errorf("invalid graph: %w", err)
	}

	changed := true
	iterations := 0
	maxIterations := 20

	for changed && iterations < maxIterations {
		changed = false
		iterations++

		for _, rule := range re.rules {
			if rule.CanApply(graph) {
				fmt.Printf("Applying rule: %s\n", rule.Name())

				if err := rule.Apply(graph); err != nil {
					return fmt.Errorf("rule %s failed: %w", rule.Name(), err)
				}

				changed = true
				break // Apply one rule at a time
			}
		}
	}

	if iterations >= maxIterations {
		return fmt.Errorf("rewrite engine did not converge after %d iterations", maxIterations)
	}

	fmt.Printf("Linear chain optimization converged after %d iterations\n", iterations)
	return nil
}

// Rule 1: Convert N-ary join to incremental version
type JoinIncrementalizationRule struct{}

func (r *JoinIncrementalizationRule) Name() string {
	return "JoinIncrementalization"
}

func (r *JoinIncrementalizationRule) CanApply(graph *LinearChainGraph) bool {
	if graph.joinNode == "" {
		return false
	}

	joinOp := graph.nodes[graph.joinNode].Op
	_, canIncrement := IncrementalizeOp(joinOp)
	return canIncrement && joinOp.OpType() == OpTypeBilinear
}

func (r *JoinIncrementalizationRule) Apply(graph *LinearChainGraph) error {
	joinOp := graph.nodes[graph.joinNode].Op
	incrementalOp, _ := IncrementalizeOp(joinOp)

	// Replace the join operator directly
	graph.nodes[graph.joinNode].Op = incrementalOp

	return nil
}

// Rule 2: Incrementalize the entire linear chain
type LinearChainIncrementalizationRule struct{}

func (r *LinearChainIncrementalizationRule) Name() string {
	return "LinearChainIncrementalization"
}

func (r *LinearChainIncrementalizationRule) CanApply(graph *LinearChainGraph) bool {
	// Check if we have any non-incremental operations that can be incrementalized
	for _, nodeID := range graph.chain {
		node := graph.nodes[nodeID]
		if _, canInc := IncrementalizeOp(node.Op); canInc {
			return true
		}
	}
	return false
}

func (r *LinearChainIncrementalizationRule) Apply(graph *LinearChainGraph) error {
	// Process each operation in the chain
	for _, nodeID := range graph.chain {
		node := graph.nodes[nodeID]

		if incOp, canInc := IncrementalizeOp(node.Op); canInc {
			// Replace with incremental version
			node.Op = incOp
		}
		// Linear operators remain unchanged (they are their own incremental version)
	}

	return nil
}

// Rule 3: Remove I->D pairs (they cancel out)
type IntegrationDifferentiationEliminationRule struct{}

func (r *IntegrationDifferentiationEliminationRule) Name() string {
	return "IntegrationDifferentiationElimination"
}

func (r *IntegrationDifferentiationEliminationRule) CanApply(graph *LinearChainGraph) bool {
	// Look for adjacent I->D pairs in the chain
	for i := 0; i < len(graph.chain)-1; i++ {
		op1 := graph.nodes[graph.chain[i]].Op
		op2 := graph.nodes[graph.chain[i+1]].Op

		_, isI := op1.(*IntegratorOp)
		_, isD := op2.(*DifferentiatorOp)
		if isI && isD {
			return true
		}
	}
	return false
}

func (r *IntegrationDifferentiationEliminationRule) Apply(graph *LinearChainGraph) error {
	newChain := make([]string, 0, len(graph.chain))

	i := 0
	for i < len(graph.chain) {
		if i < len(graph.chain)-1 {
			op1 := graph.nodes[graph.chain[i]].Op
			op2 := graph.nodes[graph.chain[i+1]].Op

			_, isI := op1.(*IntegratorOp)
			_, isD := op2.(*DifferentiatorOp)

			if isI && isD {
				// Skip both I and D - they cancel out
				// Remove the nodes from the graph
				delete(graph.nodes, graph.chain[i])
				delete(graph.nodes, graph.chain[i+1])
				i += 2
				continue
			}
		}

		// Keep this operation
		newChain = append(newChain, graph.chain[i])
		i++
	}

	graph.chain = newChain

	// Update output
	if len(graph.chain) > 0 {
		graph.output = graph.chain[len(graph.chain)-1]
	} else if graph.joinNode != "" {
		graph.output = graph.joinNode
	} else if len(graph.inputs) > 0 {
		graph.output = graph.inputs[0]
	}

	return nil
}

// Rule 4: Optimize distinct operations
type DistinctOptimizationRule struct{}

func (r *DistinctOptimizationRule) Name() string {
	return "DistinctOptimization"
}

func (r *DistinctOptimizationRule) CanApply(graph *LinearChainGraph) bool {
	// Look for distinct->distinct patterns
	for i := 0; i < len(graph.chain)-1; i++ {
		op1 := graph.nodes[graph.chain[i]].Op
		op2 := graph.nodes[graph.chain[i+1]].Op

		_, isDistinct1 := op1.(*DistinctOp)
		_, isDistinct2 := op2.(*DistinctOp)

		if isDistinct1 && isDistinct2 {
			return true
		}
	}
	return false
}

func (r *DistinctOptimizationRule) Apply(graph *LinearChainGraph) error {
	newChain := make([]string, 0, len(graph.chain))

	i := 0
	for i < len(graph.chain) {
		if i < len(graph.chain)-1 {
			op1 := graph.nodes[graph.chain[i]].Op
			op2 := graph.nodes[graph.chain[i+1]].Op

			_, isDistinct1 := op1.(*DistinctOp)
			_, isDistinct2 := op2.(*DistinctOp)

			if isDistinct1 && isDistinct2 {
				// Keep only the second distinct (idempotent)
				delete(graph.nodes, graph.chain[i])
				newChain = append(newChain, graph.chain[i+1])
				i += 2
				continue
			}
		}

		// Keep this operation
		newChain = append(newChain, graph.chain[i])
		i++
	}

	graph.chain = newChain

	// Update output
	if len(graph.chain) > 0 {
		graph.output = graph.chain[len(graph.chain)-1]
	}

	return nil
}

// Rule 5: Fuse adjacent linear operations for efficiency
type LinearOperatorFusionRule struct{}

func (r *LinearOperatorFusionRule) Name() string {
	return "LinearOperatorFusion"
}

func (r *LinearOperatorFusionRule) CanApply(graph *LinearChainGraph) bool {
	// Look for adjacent fuseable operations
	for i := 0; i < len(graph.chain)-1; i++ {
		op1 := graph.nodes[graph.chain[i]].Op
		op2 := graph.nodes[graph.chain[i+1]].Op

		_, isSel := op1.(*SelectionOp)
		_, isProj := op2.(*ProjectionOp)
		if isSel && isProj {
			return true
		}
	}
	return false
}

func (r *LinearOperatorFusionRule) Apply(graph *LinearChainGraph) error {
	newChain := make([]string, 0, len(graph.chain))

	i := 0
	for i < len(graph.chain) {
		if i < len(graph.chain)-1 {
			op1 := graph.nodes[graph.chain[i]].Op
			op2 := graph.nodes[graph.chain[i+1]].Op

			sel, isSel := op1.(*SelectionOp)
			proj, isProj := op2.(*ProjectionOp)

			if isSel && isProj {
				// Create fused operation
				fusedOp, err := FuseFilterProject(sel, proj)
				if err != nil {
					return err
				}

				// Replace second node with fused operation
				graph.nodes[graph.chain[i+1]].Op = fusedOp

				// Remove first node
				delete(graph.nodes, graph.chain[i])

				// Keep only the fused node
				newChain = append(newChain, graph.chain[i+1])
				i += 2
				continue
			}
		}

		// Keep this operation unfused
		newChain = append(newChain, graph.chain[i])
		i++
	}

	graph.chain = newChain

	// Update output
	if len(graph.chain) > 0 {
		graph.output = graph.chain[len(graph.chain)-1]
	}

	return nil
}
