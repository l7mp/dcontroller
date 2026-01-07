package dbsp

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
)

type DeltaZSet = map[string]*DocumentZSet

// Executor executes incremental queries on the specialized linear chain graph.
type Executor struct {
	graph *ChainGraph
	log   logr.Logger
}

// NewExecutor returns a new executor for evaluating incremental or snapshot graphs.
func NewExecutor(graph *ChainGraph, log logr.Logger) (*Executor, error) {
	if err := graph.Validate(); err != nil {
		return nil, fmt.Errorf("invalid graph: %w", err)
	}

	return &Executor{
		graph: graph,
		log:   log,
	}, nil
}

// Process processes inputs through the executor.
// For incremental executors, inputs are deltas. For snapshot executors, inputs are complete states.
func (e *Executor) Process(inputs DeltaZSet) (*DocumentZSet, error) {
	// Step 1: Validate inputs
	if len(inputs) != len(e.graph.inputs) {
		return nil, fmt.Errorf("expected %d inputs, got %d", len(e.graph.inputs), len(inputs))
	}

	inputStrs := []string{}
	for _, inputID := range e.graph.inputs {
		inputName, ok := e.graph.inputIdx[inputID]
		if !ok {
			return nil, fmt.Errorf("internal error: no input for ID %s", inputID)
		}
		zset, exists := inputs[inputName]
		if !exists {
			return nil, fmt.Errorf("missing input for node %s", inputName)
		}
		inputStrs = append(inputStrs, fmt.Sprintf("%s->%s", inputName, zset.String()))
	}

	// Step 2: Execute join (if exists) on inputs
	var currentResult *DocumentZSet
	var err error

	if e.graph.joinNode != "" {
		e.log.V(2).Info("processing join", "input-delta", strings.Join(inputStrs, ","))

		// Execute N-ary join
		joinNode := e.graph.nodes[e.graph.joinNode]
		joinInputs := make([]*DocumentZSet, len(e.graph.inputs))
		for i, inputID := range e.graph.inputs {
			joinInputs[i] = inputs[e.graph.inputIdx[inputID]]
		}

		currentResult, err = joinNode.Op.Process(joinInputs...)
		if err != nil {
			return nil, fmt.Errorf("join operation %s failed: %w", joinNode.Op.id(), err)
		}

		e.log.V(4).Info("join ready", "result", currentResult.String())
	} else {
		// Single input, no join needed
		currentResult = inputs[e.graph.inputIdx[e.graph.inputs[0]]]
	}

	e.log.V(2).Info("processing aggregations", "input-delta", strings.Join(inputStrs, ","))

	// Step 3: Execute linear chain (all operations are incremental-friendly)
	for i, nodeID := range e.graph.chain {
		node := e.graph.nodes[nodeID]

		// previousSize := currentResult.Size()
		currentResult, err = node.Op.Process(currentResult)
		if err != nil {
			return nil, fmt.Errorf("operation %s (step %d) failed: %w", node.Op.id(), i, err)
		}

		// fmt.Printf("Step %d - %s: %d -> %d documents\n",
		// 	i, node.Op.Name(), previousSize, currentResult.Size())
	}

	e.log.V(4).Info("executor ready", "result", currentResult.String())

	return currentResult, nil
}

// Reset resets all stateful nodes (for incremental computation).
func (e *Executor) Reset() {
	// Reset join node if it's stateful
	if e.graph.joinNode != "" {
		e.resetOperator(e.graph.nodes[e.graph.joinNode].Op)
	}

	// Reset all operations in the chain
	for _, nodeID := range e.graph.chain {
		e.resetOperator(e.graph.nodes[nodeID].Op)
	}
}

func (e *Executor) resetOperator(op Operator) {
	switch o := op.(type) {
	case *IntegratorOp:
		o.Reset()
	case *DifferentiatorOp:
		o.Reset()
	case *DelayOp:
		o.Reset()
	case *IncrementalBinaryJoinOp:
		o.Reset()
		// Note: Non-linear operators (GatherOp) don't need Reset() - they are lifted with
		// I→Op→D and the Integrator/Differentiator handle state management.
	}
}

// GetExecutionPlan returns a human-readable execution plan.
func (e *Executor) GetExecutionPlan() string {
	plan := "Execution Plan:\n"

	// Show inputs
	plan += fmt.Sprintf("1. Inputs (%d): ", len(e.graph.inputs))
	for i, inputID := range e.graph.inputs {
		if i > 0 {
			plan += ", "
		}
		plan += e.graph.nodes[inputID].Op.id()
	}
	plan += "\n"

	// Show join
	step := 2
	if e.graph.joinNode != "" {
		joinOp := e.graph.nodes[e.graph.joinNode].Op
		plan += fmt.Sprintf("%d. Join: %s (%s)\n", step, joinOp.id(), getOpTypeString(joinOp))
		step++
	}

	// Show chain
	for i, nodeID := range e.graph.chain {
		op := e.graph.nodes[nodeID].Op
		plan += fmt.Sprintf("%d. %s (%s)\n", step+i, op.id(), getOpTypeString(op))
	}

	return plan
}

// getOpTypeString returns the op type as a string.
func getOpTypeString(op Operator) string {
	switch op.OpType() {
	case OpTypeLinear:
		return "Linear"
	case OpTypeBilinear:
		return "Bilinear"
	case OpTypeNonLinear:
		return "NonLinear"
	case OpTypeStructural:
		return "Structural"
	default:
		return "Unknown"
	}
}

// GetNodeResult returns intermediate results for debugging.
func (e *Executor) GetNodeResult(nodeID string, deltaInputs map[string]*DocumentZSet) (*DocumentZSet, error) {
	node, exists := e.graph.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// For debugging: execute up to this specific node
	// This is inefficient but useful for testing

	if nodeID == e.graph.joinNode {
		// Execute join
		joinInputs := make([]*DocumentZSet, len(e.graph.inputs))
		for i, inputID := range e.graph.inputs {
			joinInputs[i] = deltaInputs[inputID]
		}
		return node.Op.Process(joinInputs...)
	}

	// Find position in chain
	chainPos := -1
	for i, chainNodeID := range e.graph.chain {
		if chainNodeID == nodeID {
			chainPos = i
			break
		}
	}

	if chainPos == -1 {
		return nil, fmt.Errorf("node %s not found in chain", nodeID)
	}

	// Execute up to this position
	var currentResult *DocumentZSet
	var err error

	// Start with join result or first input
	if e.graph.joinNode != "" {
		joinNode := e.graph.nodes[e.graph.joinNode]
		joinInputs := make([]*DocumentZSet, len(e.graph.inputs))
		for i, inputID := range e.graph.inputs {
			joinInputs[i] = deltaInputs[inputID]
		}
		currentResult, err = joinNode.Op.Process(joinInputs...)
		if err != nil {
			return nil, err
		}
	} else {
		currentResult = deltaInputs[e.graph.inputs[0]]
	}

	// Execute chain up to target position
	for i := 0; i <= chainPos; i++ {
		chainNode := e.graph.nodes[e.graph.chain[i]]
		currentResult, err = chainNode.Op.Process(currentResult)
		if err != nil {
			return nil, err
		}
	}

	return currentResult, nil
}

// SnapshotExecutor executes snapshot (state-of-the-world) queries on the linear chain graph.
// Unlike the incremental Executor, SnapshotExecutor is stateless and processes complete input
// states rather than deltas. It embeds an Executor and reuses its Process() method.
type SnapshotExecutor struct {
	*Executor
}

// NewSnapshotExecutor returns a new snapshot executor for state-of-the-world reconciliation.
func NewSnapshotExecutor(graph *ChainGraph, log logr.Logger) (*SnapshotExecutor, error) {
	// Create the underlying executor
	executor, err := NewExecutor(graph, log)
	if err != nil {
		return nil, err
	}

	return &SnapshotExecutor{
		Executor: executor,
	}, nil
}

// Reset is a no-op for snapshot executor (stateless).
func (e *SnapshotExecutor) Reset() {
	// Snapshot executor is stateless, nothing to reset.
}

// Note: GetExecutionPlan() is inherited from the embedded Executor.
