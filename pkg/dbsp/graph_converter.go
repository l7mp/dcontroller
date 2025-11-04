package dbsp

import "fmt"

// opConverter is a function that converts an operator to a different mode.
type opConverter func(Operator) (Operator, error)

// ToSnapshotGraph converts an incremental ChainGraph to a snapshot ChainGraph.
// This replaces incremental operators (IncrementalJoinOp, IncrementalGatherOp) with their
// snapshot equivalents (JoinOp, GatherOp). Linear operators remain unchanged.
func ToSnapshotGraph(incrementalGraph *ChainGraph) (*ChainGraph, error) {
	return convertGraph(incrementalGraph, "incremental", convertToSnapshotJoin, convertToSnapshotOperator)
}

// ToIncrementalGraph converts a snapshot ChainGraph to an incremental ChainGraph.
// This replaces snapshot operators (JoinOp, GatherOp) with their incremental equivalents
// (IncrementalJoinOp, IncrementalGatherOp). Linear operators remain unchanged.
func ToIncrementalGraph(snapshotGraph *ChainGraph) (*ChainGraph, error) {
	return convertGraph(snapshotGraph, "snapshot", convertToIncrementalJoin, convertToIncrementalOperator)
}

// convertGraph is a helper that converts a ChainGraph using the provided converter functions.
func convertGraph(sourceGraph *ChainGraph, sourceType string, joinConverter, opConverter opConverter) (*ChainGraph, error) {
	if err := sourceGraph.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s graph: %w", sourceType, err)
	}

	targetGraph := NewChainGraph()

	// Copy inputs (InputOp is the same for both).
	for _, inputID := range sourceGraph.inputs {
		inputNode := sourceGraph.nodes[inputID]
		inputOp := inputNode.Op.(*InputOp)
		targetGraph.AddInput(inputOp)
	}

	// Convert join node if present.
	if sourceGraph.joinNode != "" {
		joinNode := sourceGraph.nodes[sourceGraph.joinNode]
		convertedJoinOp, err := joinConverter(joinNode.Op)
		if err != nil {
			return nil, fmt.Errorf("failed to convert join: %w", err)
		}
		targetGraph.SetJoin(convertedJoinOp)
	}

	// Convert chain operators.
	for _, nodeID := range sourceGraph.chain {
		node := sourceGraph.nodes[nodeID]
		convertedOp, err := opConverter(node.Op)
		if err != nil {
			return nil, fmt.Errorf("failed to convert operator %s: %w", node.Op.id(), err)
		}
		targetGraph.AddToChain(convertedOp)
	}

	return targetGraph, nil
}

// convertToSnapshotJoin converts an incremental join to snapshot join.
func convertToSnapshotJoin(op Operator) (Operator, error) {
	switch o := op.(type) {
	case *IncrementalJoinOp:
		// Convert IncrementalJoinOp -> JoinOp.
		return NewJoin(o.eval, o.inputs), nil
	case *IncrementalBinaryJoinOp:
		// Convert IncrementalBinaryJoinOp -> BinaryJoinOp.
		return NewBinaryJoin(o.eval, o.inputs), nil
	case *JoinOp, *BinaryJoinOp:
		// Already snapshot, return as-is.
		return op, nil
	default:
		return nil, fmt.Errorf("unsupported join operator type: %T", op)
	}
}

// convertToIncrementalJoin converts a snapshot join to incremental join.
func convertToIncrementalJoin(op Operator) (Operator, error) {
	switch o := op.(type) {
	case *JoinOp:
		// Convert JoinOp -> IncrementalJoinOp.
		return NewIncrementalJoin(o.eval, o.inputs), nil
	case *BinaryJoinOp:
		// Convert BinaryJoinOp -> IncrementalBinaryJoinOp.
		return NewIncrementalBinaryJoin(o.eval, o.inputs), nil
	case *IncrementalJoinOp, *IncrementalBinaryJoinOp:
		// Already incremental, return as-is.
		return op, nil
	default:
		return nil, fmt.Errorf("unsupported join operator type: %T", op)
	}
}

// convertToSnapshotOperator converts an incremental operator to snapshot operator.
func convertToSnapshotOperator(op Operator) (Operator, error) {
	switch o := op.(type) {
	case *IncrementalGatherOp:
		// Convert IncrementalGatherOp -> GatherOp.
		return NewGather(o.keyExtractor, o.valueExtractor, o.aggregator), nil

	// Linear operators are the same for snapshot and incremental.
	case *ProjectionOp, *SelectionOp, *DistinctOp:
		return op, nil

	// Fused operators are also the same.
	case *ProjectThenSelectOp, *SelectThenProjectionsOp:
		return op, nil

	// Structural operators (integrator, differentiator, delay) don't make sense in snapshot mode.
	case *IntegratorOp:
		return nil, fmt.Errorf("IntegratorOp cannot be converted to snapshot (only valid in incremental mode)")
	case *DifferentiatorOp:
		return nil, fmt.Errorf("DifferentiatorOp cannot be converted to snapshot (only valid in incremental mode)")
	case *DelayOp:
		return nil, fmt.Errorf("DelayOp cannot be converted to snapshot (only valid in incremental mode)")

	// Already snapshot operator.
	case *GatherOp:
		return op, nil

	default:
		return nil, fmt.Errorf("unsupported operator type for snapshot conversion: %T", op)
	}
}

// convertToIncrementalOperator converts a snapshot operator to incremental operator.
func convertToIncrementalOperator(op Operator) (Operator, error) {
	switch o := op.(type) {
	case *GatherOp:
		// Convert GatherOp -> IncrementalGatherOp.
		return NewIncrementalGather(o.keyExtractor, o.valueExtractor, o.aggregator), nil

	// Linear operators are the same for snapshot and incremental.
	case *ProjectionOp, *SelectionOp, *DistinctOp:
		return op, nil

	// Fused operators are also the same.
	case *ProjectThenSelectOp, *SelectThenProjectionsOp:
		return op, nil

	// Already incremental operator.
	case *IncrementalGatherOp:
		return op, nil

	// Structural operators remain unchanged (only used in incremental mode).
	case *IntegratorOp, *DifferentiatorOp, *DelayOp:
		return op, nil

	default:
		return nil, fmt.Errorf("unsupported operator type for incremental conversion: %T", op)
	}
}
