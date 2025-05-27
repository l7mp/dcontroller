package dbsp

import "fmt"

// Rule 1: Linear operators are their own incremental version
// I -> LinearOp -> D ≡ LinearOp
type LinearOperatorIncrementalizationRule struct{}

func (r *LinearOperatorIncrementalizationRule) Name() string {
	return "LinearOperatorIncrementalization"
}

func (r *LinearOperatorIncrementalizationRule) PatternSize() int { return 3 }

func (r *LinearOperatorIncrementalizationRule) Matches(pattern []*GraphNode) bool {
	if len(pattern) != 3 {
		return false
	}

	i, op, d := pattern[0], pattern[1], pattern[2]

	// Check for I -> LinearOp -> D pattern
	_, isI := i.Op.(*IntegratorOp)
	_, isD := d.Op.(*DifferentiatorOp)
	isLinear := op.Op.OpType() == OpTypeLinear

	return isI && isLinear && isD
}

func (r *LinearOperatorIncrementalizationRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
	// I -> LinearOp -> D becomes just LinearOp
	// This is the fundamental DBSP theorem: LinearOp^Δ = LinearOp
	fmt.Printf("Incrementalizing linear operator: %s\n", pattern[1].Op.Name())
	return []*GraphNode{pattern[1]}, nil
}

// Rule 2: Bilinear operators need expansion formula
// I -> I -> BilinearOp -> D ≡ BilinearOp + I(z^(-1)(left))×right + left×I(z^(-1)(right))
// Updated rule that works for any bilinear operator
// type BilinearOperatorIncrementalizationRule struct {
// 	registry *BilinearOperatorRegistry
// }

// func NewBilinearOperatorIncrementalizationRule(registry *BilinearOperatorRegistry) *BilinearOperatorIncrementalizationRule {
// 	return &BilinearOperatorIncrementalizationRule{
// 		registry: registry,
// 	}
// }

// func (r *BilinearOperatorIncrementalizationRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
// 	bilinearNode := pattern[2] // The bilinear operator (join, cartesian product, etc.)

// 	// Create incremental version using registry
// 	incrementalOp, err := r.registry.CreateIncrementalFromOperator(bilinearNode.Node)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create incremental operator: %w", err)
// 	}

// 	newNode := &GraphNode{
// 		ID:   "incremental_" + bilinearNode.ID,
// 		Node: incrementalOp,
// 	}

// 	fmt.Printf("Incrementalized bilinear operator: %s\n", bilinearNode.Node.Name())
// 	return []*GraphNode{newNode}, nil
// }

type BilinearOperatorIncrementalizationRule struct{}

func (r *BilinearOperatorIncrementalizationRule) Name() string {
	return "BilinearOperatorIncrementalization"
}

func (r *BilinearOperatorIncrementalizationRule) PatternSize() int { return 4 }

func (r *BilinearOperatorIncrementalizationRule) Matches(pattern []*GraphNode) bool {
	if len(pattern) != 4 {
		return false
	}

	i1, i2, op, d := pattern[0], pattern[1], pattern[2], pattern[3]

	// Check for I -> I -> BilinearOp -> D pattern (for joins)
	_, isI1 := i1.Op.(*IntegratorOp)
	_, isI2 := i2.Op.(*IntegratorOp)
	_, isD := d.Op.(*DifferentiatorOp)
	_, ok := IncrementalizeOp(op.Op) // can we incrementalize at all?
	isBilinear := ok && op.Op.OpType() == OpTypeBilinear

	return isI1 && isI2 && isBilinear && isD
}

func (r *BilinearOperatorIncrementalizationRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
	// Apply DBSP bilinear expansion formula
	// (a × b)^Δ = a×b + I(z^(-1)(a))×b + a×I(z^(-1)(b))
	op, _ := IncrementalizeOp(pattern[2].Op) // match already checked this

	newNode := &GraphNode{
		ID: "inc" + op.Name(),
		Op: op,
	}

	return []*GraphNode{newNode}, nil
}

// Rule 3: Integration and differentiation cancel out
// D(I(x)) = x and I(D(x)) = x
type IntegrationDifferentiationCancellationRule struct{}

func (r *IntegrationDifferentiationCancellationRule) Name() string {
	return "IntegrationDifferentiationCancellation"
}

func (r *IntegrationDifferentiationCancellationRule) PatternSize() int { return 2 }

func (r *IntegrationDifferentiationCancellationRule) Matches(pattern []*GraphNode) bool {
	if len(pattern) != 2 {
		return false
	}

	first, second := pattern[0], pattern[1]

	// Check for D -> I or I -> D
	_, firstIsD := first.Op.(*DifferentiatorOp)
	_, secondIsI := second.Op.(*IntegratorOp)
	_, firstIsI := first.Op.(*IntegratorOp)
	_, secondIsD := second.Op.(*DifferentiatorOp)

	return (firstIsD && secondIsI) || (firstIsI && secondIsD)
}

func (r *IntegrationDifferentiationCancellationRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
	// D(I(x)) = x or I(D(x)) = x - eliminate both operators
	fmt.Printf("Cancelling I/D pair\n")
	return []*GraphNode{}, nil // Remove both nodes
}

// Rule 4: Distinct is idempotent
// distinct(distinct(x)) = distinct(x)
type DistinctIdempotenceRule struct{}

func (r *DistinctIdempotenceRule) Name() string {
	return "DistinctIdempotence"
}

func (r *DistinctIdempotenceRule) PatternSize() int { return 2 }

func (r *DistinctIdempotenceRule) Matches(pattern []*GraphNode) bool {
	if len(pattern) != 2 {
		return false
	}

	first, second := pattern[0], pattern[1]
	_, firstDistinct := first.Op.(*DistinctOp)
	_, secondDistinct := second.Op.(*DistinctOp)

	return firstDistinct && secondDistinct
}

func (r *DistinctIdempotenceRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
	// distinct(distinct(x)) = distinct(x)
	fmt.Printf("Removing redundant distinct\n")
	return []*GraphNode{pattern[1]}, nil
}

// Rule 5: Distinct commutes with linear operators when input is positive
// LinearOp(distinct(x)) = distinct(LinearOp(x)) for positive x
type DistinctCommutesWithLinearRule struct{}

func (r *DistinctCommutesWithLinearRule) Name() string {
	return "DistinctCommutesWithLinear"
}

func (r *DistinctCommutesWithLinearRule) PatternSize() int { return 2 }

func (r *DistinctCommutesWithLinearRule) Matches(pattern []*GraphNode) bool {
	if len(pattern) != 2 {
		return false
	}

	first, second := pattern[0], pattern[1]

	// Check for LinearOp -> distinct pattern
	isLinear := first.Op.OpType() == OpTypeLinear
	_, isDistinct := second.Op.(*DistinctOp)

	return isLinear && isDistinct
}

func (r *DistinctCommutesWithLinearRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
	// LinearOp -> distinct becomes distinct -> LinearOp (potentially more efficient)
	fmt.Printf("Commuting %s with distinct\n", pattern[0].Op.Name())
	return []*GraphNode{pattern[1], pattern[0]}, nil // Swap order
}

// Rule 6: Chain rule decomposition
// (Q1 ∘ Q2)^Δ = Q1^Δ ∘ Q2^Δ
type ChainRuleDecompositionRule struct{}

func (r *ChainRuleDecompositionRule) Name() string {
	return "ChainRuleDecomposition"
}

func (r *ChainRuleDecompositionRule) PatternSize() int { return 5 }

func (r *ChainRuleDecompositionRule) Matches(pattern []*GraphNode) bool {
	if len(pattern) != 4 {
		return false
	}

	// Check for I -> Q1 -> Q2 -> D pattern
	i, q1, q2, d := pattern[0], pattern[1], pattern[2], pattern[3]

	_, isI := i.Op.(*IntegratorOp)
	_, isD := d.Op.(*DifferentiatorOp)

	// Q1 and Q2 should be query operators (not I/D)
	q1IsQuery := r.isQueryOperator(q1)
	q2IsQuery := r.isQueryOperator(q2)

	return isI && q1IsQuery && q2IsQuery && isD
}

func (r *ChainRuleDecompositionRule) isQueryOperator(node *GraphNode) bool {
	switch node.Op.(type) {
	case *ProjectionOp, *SelectionOp, *BinaryJoinOp:
		return true
	default:
		return false
	}
}

func (r *ChainRuleDecompositionRule) Apply(pattern []*GraphNode) ([]*GraphNode, error) {
	// I -> Q1 -> Q2 -> D becomes Q1^Δ -> Q2^Δ
	// Apply incrementalization to each query operator separately
	q1, q2 := pattern[1], pattern[2]

	fmt.Printf("Applying chain rule to %s ∘ %s\n", q1.Op.Name(), q2.Op.Name())

	// For linear operators, Q^Δ = Q
	if q1.Op.OpType() == OpTypeLinear && q2.Op.OpType() == OpTypeLinear {
		return []*GraphNode{q1, q2}, nil
	}

	// TODO: Handle bilinear and non-linear cases
	return []*GraphNode{q1, q2}, nil
}
