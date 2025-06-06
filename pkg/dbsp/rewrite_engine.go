package dbsp

import (
	"fmt"
)

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
	CanApply(graph *ChainGraph) bool
	Apply(graph *ChainGraph) error
}

func (re *LinearChainRewriteEngine) AddRule(rule LinearChainRule) {
	re.rules = append(re.rules, rule)
}

// Optimize applies all rules until fixpoint
func (re *LinearChainRewriteEngine) Optimize(graph *ChainGraph) error {
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

func (r *JoinIncrementalizationRule) CanApply(graph *ChainGraph) bool {
	if graph.joinNode == "" {
		return false
	}

	joinOp := graph.nodes[graph.joinNode].Op
	_, canIncrement := IncrementalizeOp(joinOp)
	return canIncrement && joinOp.OpType() == OpTypeBilinear
}

func (r *JoinIncrementalizationRule) Apply(graph *ChainGraph) error {
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

func (r *LinearChainIncrementalizationRule) CanApply(graph *ChainGraph) bool {
	// Check if we have any non-incremental operations that can be incrementalized
	for _, nodeID := range graph.chain {
		node := graph.nodes[nodeID]
		if _, canInc := IncrementalizeOp(node.Op); canInc {
			return true
		}
	}
	return false
}

func (r *LinearChainIncrementalizationRule) Apply(graph *ChainGraph) error {
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

func (r *IntegrationDifferentiationEliminationRule) CanApply(graph *ChainGraph) bool {
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

func (r *IntegrationDifferentiationEliminationRule) Apply(graph *ChainGraph) error {
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

func (r *DistinctOptimizationRule) CanApply(graph *ChainGraph) bool {
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

func (r *DistinctOptimizationRule) Apply(graph *ChainGraph) error {
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

func (r *LinearOperatorFusionRule) CanApply(graph *ChainGraph) bool {
	return r.hasSelectionThenProjections(graph) || // Priority 1: Selection followed by projections (σ → π^n)
		r.hasConsecutiveProjections(graph) || // Priority 2: Consecutive projections (π^n)
		r.hasProjectThenSelect(graph) // Priority 3: Project then select (π → σ)
}
func (r *LinearOperatorFusionRule) Apply(graph *ChainGraph) error {
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
func (r *LinearOperatorFusionRule) hasSelectionThenProjections(graph *ChainGraph) bool {
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

func (r *LinearOperatorFusionRule) hasConsecutiveProjections(graph *ChainGraph) bool {
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

func (r *LinearOperatorFusionRule) hasProjectThenSelect(graph *ChainGraph) bool {
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
func (r *LinearOperatorFusionRule) fuseSelectionThenProjections(graph *ChainGraph) error {
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

func (r *LinearOperatorFusionRule) fuseProjectThenSelect(graph *ChainGraph) error {
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
func (r *LinearOperatorFusionRule) updateOutput(graph *ChainGraph) {
	if len(graph.chain) > 0 {
		graph.output = graph.chain[len(graph.chain)-1]
	} else if graph.joinNode != "" {
		graph.output = graph.joinNode
	}
}
