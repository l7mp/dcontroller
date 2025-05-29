package dbsp

import (
	"fmt"
	"strings"
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

// GetNode returns a node from its id.
func (g *LinearChainGraph) GetNode(id string) *GraphNode {
	return g.nodes[id]
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
// String representation for debugging (horizontal layout)
func (g *LinearChainGraph) String() string {
	var parts []string

	// Build the flow description
	if len(g.inputs) == 1 {
		// Single input: Input -> [Join] -> Chain -> Output
		inputName := g.nodes[g.inputs[0]].Op.Name()
		parts = append(parts, inputName)
	} else {
		// Multiple inputs: (Input1, Input2, ...) -> Join -> Chain -> Output
		inputNames := make([]string, len(g.inputs))
		for i, inputID := range g.inputs {
			inputNames[i] = g.nodes[inputID].Op.Name()
		}
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(inputNames, ", ")))
	}

	// Add join if present
	if g.joinNode != "" {
		parts = append(parts, g.nodes[g.joinNode].Op.Name())
	}

	// Add chain operations
	for _, nodeID := range g.chain {
		parts = append(parts, g.nodes[nodeID].Op.Name())
	}

	return strings.Join(parts, " → ")
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

	// fmt.Printf("Linear chain optimization converged after %d iterations\n", iterations)
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
	return r.hasSelectionThenProjections(graph) || // Priority 1: Selection followed by projections (σ → π^n)
		r.hasConsecutiveProjections(graph) || // Priority 2: Consecutive projections (π^n)
		r.hasProjectThenSelect(graph) // Priority 3: Project then select (π → σ)
}
func (r *LinearOperatorFusionRule) Apply(graph *LinearChainGraph) error {
	// Apply in priority order
	if r.hasSelectionThenProjections(graph) || r.hasConsecutiveProjections(graph) {
		return r.fuseSelectionThenProjections(graph) // Now handles both!
	}

	if r.hasProjectThenSelect(graph) {
		return r.fuseProjectThenSelect(graph)
	}

	return nil
}

// Pattern detection methods
func (r *LinearOperatorFusionRule) hasSelectionThenProjections(graph *LinearChainGraph) bool {
	for i := 0; i < len(graph.chain)-1; i++ {
		if _, isSel := graph.nodes[graph.chain[i]].Op.(*SelectionOp); isSel {
			// Check if followed by one or more projections
			j := i + 1
			projCount := 0
			for j < len(graph.chain) {
				if _, isProj := graph.nodes[graph.chain[j]].Op.(*ProjectionOp); isProj {
					projCount++
					j++
				} else {
					break
				}
			}
			if projCount > 0 {
				return true
			}
		}
	}
	return false
}

func (r *LinearOperatorFusionRule) hasConsecutiveProjections(graph *LinearChainGraph) bool {
	consecutiveCount := 0
	for _, nodeID := range graph.chain {
		if _, isProj := graph.nodes[nodeID].Op.(*ProjectionOp); isProj {
			consecutiveCount++
			if consecutiveCount >= 2 {
				return true
			}
		} else {
			consecutiveCount = 0
		}
	}
	return false
}

func (r *LinearOperatorFusionRule) hasProjectThenSelect(graph *LinearChainGraph) bool {
	for i := 0; i < len(graph.chain)-1; i++ {
		op1 := graph.nodes[graph.chain[i]].Op
		op2 := graph.nodes[graph.chain[i+1]].Op

		_, isProj := op1.(*ProjectionOp)
		_, isSel := op2.(*SelectionOp)
		if isProj && isSel {
			return true
		}
	}
	return false
}

// Fusion implementation methods
func (r *LinearOperatorFusionRule) fuseSelectionThenProjections(graph *LinearChainGraph) error {
	newChain := make([]string, 0, len(graph.chain))

	i := 0
	for i < len(graph.chain) {
		op := graph.nodes[graph.chain[i]].Op

		// Check for selection OR projection at start of fuseable sequence
		if selOp, isSel := op.(*SelectionOp); isSel {
			// Handle σ → π^n case
			j := i + 1
			var projEvals []Evaluator

			for j < len(graph.chain) {
				if projOp, isProj := graph.nodes[graph.chain[j]].Op.(*ProjectionOp); isProj {
					projEvals = append(projEvals, projOp.eval)
					j++
				} else {
					break
				}
			}

			if len(projEvals) > 0 {
				// Create fused select-then-projections
				fusedOp := NewSelectThenProjections(selOp.eval, projEvals)

				// Replace selection with fused operation
				graph.nodes[graph.chain[i]].Op = fusedOp
				newChain = append(newChain, graph.chain[i])

				// Remove the projections
				for k := i + 1; k < j; k++ {
					delete(graph.nodes, graph.chain[k])
				}

				i = j
			} else {
				// Selection with no following projections
				newChain = append(newChain, graph.chain[i])
				i++
			}
		} else if projOp, isProj := op.(*ProjectionOp); isProj {
			// Handle π^n case (no preceding selection)
			evals := []Evaluator{projOp.eval}
			j := i + 1

			for j < len(graph.chain) {
				if nextProjOp, isNextProj := graph.nodes[graph.chain[j]].Op.(*ProjectionOp); isNextProj {
					evals = append(evals, nextProjOp.eval)
					j++
				} else {
					break
				}
			}

			if len(evals) > 1 {
				// Create N-ary projection (no selection)
				fusedOp := NewSelectThenProjections(nil, evals)
				graph.nodes[graph.chain[i]].Op = fusedOp
				newChain = append(newChain, graph.chain[i])

				// Remove other projections
				for k := i + 1; k < j; k++ {
					delete(graph.nodes, graph.chain[k])
				}
				i = j
			} else {
				newChain = append(newChain, graph.chain[i])
				i++
			}
		} else {
			// Neither selection nor projection
			newChain = append(newChain, graph.chain[i])
			i++
		}
	}

	graph.chain = newChain
	r.updateOutput(graph)
	return nil
}

func (r *LinearOperatorFusionRule) fuseProjectThenSelect(graph *LinearChainGraph) error {
	newChain := make([]string, 0, len(graph.chain))

	i := 0
	for i < len(graph.chain) {
		if i < len(graph.chain)-1 {
			op1 := graph.nodes[graph.chain[i]].Op
			op2 := graph.nodes[graph.chain[i+1]].Op

			proj, isProj := op1.(*ProjectionOp)
			sel, isSel := op2.(*SelectionOp)

			if isProj && isSel {
				// π → σ: Create project-then-select fusion
				fusedOp := NewProjectThenSelect(proj.eval, sel.eval)
				graph.nodes[graph.chain[i+1]].Op = fusedOp
				delete(graph.nodes, graph.chain[i])
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
	r.updateOutput(graph)
	return nil
}

// Helper to update output after chain modifications
func (r *LinearOperatorFusionRule) updateOutput(graph *LinearChainGraph) {
	if len(graph.chain) > 0 {
		graph.output = graph.chain[len(graph.chain)-1]
	} else if graph.joinNode != "" {
		graph.output = graph.joinNode
	}
}
