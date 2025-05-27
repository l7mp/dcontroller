package dbsp

// RewriteEngine applies DBSP algebraic transformation rules
type RewriteEngine struct {
	rules []DBSPRule
}

func NewRewriteEngine() *RewriteEngine {
	re := &RewriteEngine{
		rules: make([]DBSPRule, 0),
	}

	// Add core DBSP rewrite rules in order of application
	re.AddRule(&LinearOperatorIncrementalizationRule{})
	re.AddRule(&BilinearOperatorIncrementalizationRule{})
	re.AddRule(&IntegrationDifferentiationCancellationRule{})
	re.AddRule(&DistinctIdempotenceRule{})
	re.AddRule(&DistinctCommutesWithLinearRule{})
	re.AddRule(&ChainRuleDecompositionRule{})

	return re
}

// DBSPRule represents a DSP transformation rule.
type DBSPRule interface {
	Name() string
	Matches(pattern []*GraphNode) bool
	Apply(pattern []*GraphNode) ([]*GraphNode, error)
	PatternSize() int // How many nodes this rule looks at
}

func (re *RewriteEngine) AddRule(rule DBSPRule) {
	re.rules = append(re.rules, rule)
}

// // Apply DBSP rules until fixpoint
// func (re *RewriteEngine) Optimize(graph *ComputationGraph) (*ComputationGraph, error) {
// 	changed := true
// 	iterations := 0
// 	maxIterations := 50 // Safety limit

// 	for changed && iterations < maxIterations {
// 		changed = false
// 		iterations++

// 		// Try each rule
// 		for _, rule := range re.rules {
// 			patterns := re.findPatterns(graph, rule)

// 			for _, pattern := range patterns {
// 				if rule.Matches(pattern) {
// 					newNodes, err := rule.Apply(pattern)
// 					if err != nil {
// 						return nil, fmt.Errorf("rule %s failed: %w", rule.Name(), err)
// 					}

// 					err = re.replacePattern(graph, pattern, newNodes)
// 					if err != nil {
// 						return nil, fmt.Errorf("failed to replace pattern: %w", err)
// 					}

// 					changed = true
// 					fmt.Printf("Applied DBSP rule: %s\n", rule.Name())
// 					break // Apply one rule at a time
// 				}
// 			}

// 			if changed {
// 				break
// 			}
// 		}
// 	}

// 	if iterations >= maxIterations {
// 		return nil, fmt.Errorf("rewrite engine did not converge after %d iterations", maxIterations)
// 	}

// 	fmt.Printf("DBSP optimization converged after %d iterations\n", iterations)
// 	return graph, nil
// }

// // Simplified pattern finder for linear pipeline structure
// func (re *RewriteEngine) findPatterns(graph *ComputationGraph, rule DBSPRule) [][]*GraphNode {
// 	patterns := make([][]*GraphNode, 0)
// 	patternSize := rule.PatternSize()

// 	// Get the linear sequence of nodes (inputs -> join? -> chain -> output)
// 	sequence := re.getLinearSequence(graph)

// 	// Generate all subsequences of the required size
// 	for i := 0; i <= len(sequence)-patternSize; i++ {
// 		pattern := sequence[i : i+patternSize]
// 		patterns = append(patterns, pattern)
// 	}

// 	return patterns
// }

// // Extract linear sequence from our guaranteed structure
// func (re *RewriteEngine) getLinearSequence(graph *ComputationGraph) []*GraphNode {
// 	sequence := make([]*GraphNode, 0)

// 	// Start from input(s)
// 	for _, inputID := range graph.inputs {
// 		sequence = append(sequence, graph.nodes[inputID])
// 	}

// 	// If multiple inputs, find the join node
// 	if len(graph.inputs) > 1 {
// 		for _, node := range graph.nodes {
// 			if len(node.Inputs) > 1 { // This is the join
// 				sequence = append(sequence, node)
// 				break
// 			}
// 		}
// 	}

// 	// Follow the linear chain to output
// 	var current *GraphNode
// 	if len(graph.inputs) > 1 {
// 		// Start from join
// 		for _, node := range graph.nodes {
// 			if len(node.Inputs) > 1 {
// 				current = node
// 				break
// 			}
// 		}
// 	} else {
// 		// Start from single input
// 		current = graph.nodes[graph.inputs[0]]
// 	}

// 	// Walk the chain (skip input and join as they're already added)
// 	for len(current.Outputs) > 0 {
// 		current = current.Outputs[0]
// 		sequence = append(sequence, current)
// 	}

// 	return sequence
// }

// // Simplified pattern replacement for linear structure
// func (re *RewriteEngine) replacePattern(graph *ComputationGraph, oldPattern, newNodes []*GraphNode) error {
// 	// For now, just log what we would do
// 	// Real implementation would need to rewire the linear chain properly
// 	fmt.Printf("Would replace pattern of %d nodes with %d nodes\n", len(oldPattern), len(newNodes))
// 	return nil
// }
