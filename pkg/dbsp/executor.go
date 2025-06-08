package dbsp

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
)

type DeltaZSet = map[string]*DocumentZSet

// Executor executes incremental queries on the specialized linear chain graph
type Executor struct {
	graph *ChainGraph
	log   logr.Logger
}

func NewExecutor(graph *ChainGraph, log logr.Logger) (*Executor, error) {
	if err := graph.Validate(); err != nil {
		return nil, fmt.Errorf("invalid graph: %w", err)
	}

	if !isIncrementalGraph(graph) {
		return nil, fmt.Errorf("graph is not optimized for incremental execution")
	}

	return &Executor{
		graph: graph,
		log:   log,
	}, nil
}

// ProcessDelta processes one delta input and produces delta output
// This is the core incremental execution method
func (e *Executor) ProcessDelta(deltaInputs DeltaZSet) (*DocumentZSet, error) {
	// Step 1: Validate inputs
	if len(deltaInputs) != len(e.graph.inputs) {
		return nil, fmt.Errorf("expected %d inputs, got %d", len(e.graph.inputs), len(deltaInputs))
	}

	inputs := []string{}
	for _, inputID := range e.graph.inputs {
		inputName, ok := e.graph.inputIdx[inputID]
		if !ok {
			return nil, fmt.Errorf("internal error: no input for ID %s", inputID)
		}
		zset, exists := deltaInputs[inputName]
		if !exists {
			return nil, fmt.Errorf("missing input for node %s", inputName)
		}
		inputs = append(inputs, fmt.Sprintf("%s->%s", inputName, zset.String()))
	}

	// Step 2: Execute join (if exists) on delta inputs
	var currentResult *DocumentZSet
	var err error

	if e.graph.joinNode != "" {
		e.log.V(2).Info("processing join", "delta", strings.Join(inputs, ","))

		// Execute incremental N-ary join
		joinNode := e.graph.nodes[e.graph.joinNode]
		joinInputs := make([]*DocumentZSet, len(e.graph.inputs))
		for i, inputID := range e.graph.inputs {
			joinInputs[i] = deltaInputs[e.graph.inputIdx[inputID]]
		}

		currentResult, err = joinNode.Op.Process(joinInputs...)
		if err != nil {
			return nil, fmt.Errorf("join operation %s failed: %w", joinNode.Op.id(), err)
		}

		e.log.V(4).Info("join ready", "result", currentResult.String())
	} else {
		// Single input, no join needed
		currentResult = deltaInputs[e.graph.inputIdx[e.graph.inputs[0]]]
	}

	e.log.V(2).Info("processing aggregations", "delta", strings.Join(inputs, ","))

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

	e.log.V(4).Info("aggregations ready", "result", currentResult.String())

	return currentResult, nil
}

// isIncrementalGraph checks if the graph has been optimized for incremental execution
func isIncrementalGraph(graph *ChainGraph) bool {
	// Check if join is incremental (if it exists)
	if graph.joinNode != "" {
		joinOp := graph.nodes[graph.joinNode].Op
		switch joinOp.(type) {
		case *IncrementalBinaryJoinOp, *IncrementalJoinOp:
			// Good - incremental join
		case *BinaryJoinOp, *JoinOp:
			// Bad - non-incremental join
			return false
		}
	}

	// Check if there are any I->D pairs that should have been eliminated
	for i := 0; i < len(graph.chain)-1; i++ {
		op1 := graph.nodes[graph.chain[i]].Op
		op2 := graph.nodes[graph.chain[i+1]].Op

		_, isI := op1.(*IntegratorOp)
		_, isD := op2.(*DifferentiatorOp)
		if isI && isD {
			// Should have been eliminated by rewrite engine
			return false
		}
	}

	return true
}

// Reset all stateful nodes (for incremental computation)
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
	case *IncrementalGatherOp:
		o.Reset()
		// Add other stateful operators as needed
	}
}

// GetExecutionPlan returns a human-readable execution plan
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
		plan += fmt.Sprintf("%d. Join: %s (%s)\n", step, joinOp.id(), e.getOpTypeString(joinOp))
		step++
	}

	// Show chain
	for i, nodeID := range e.graph.chain {
		op := e.graph.nodes[nodeID].Op
		plan += fmt.Sprintf("%d. %s (%s)\n", step+i, op.id(), e.getOpTypeString(op))
	}

	return plan
}

func (e *Executor) getOpTypeString(op Operator) string {
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

// GetNodeResult returns intermediate results for debugging (optional caching)
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

// IncrementalExecutionContext helps track incremental execution state
type IncrementalExecutionContext struct {
	executor         *Executor
	cumulativeInputs map[string]*DocumentZSet // Running total of all inputs
	cumulativeOutput *DocumentZSet            // Running total of output
	timestep         int
}

func NewIncrementalExecutionContext(executor *Executor) *IncrementalExecutionContext {
	return &IncrementalExecutionContext{
		executor:         executor,
		cumulativeInputs: make(map[string]*DocumentZSet),
		cumulativeOutput: NewDocumentZSet(),
		timestep:         0,
	}
}

// ProcessDelta processes one delta and updates cumulative state
func (ctx *IncrementalExecutionContext) ProcessDelta(deltaInputs map[string]*DocumentZSet) (*DocumentZSet, error) {
	// fmt.Printf("\n=== Incremental Context: Timestep %d ===\n", ctx.timestep)

	// Update cumulative inputs
	for inputID, delta := range deltaInputs {
		if ctx.cumulativeInputs[inputID] == nil {
			ctx.cumulativeInputs[inputID] = NewDocumentZSet()
		}

		var err error
		ctx.cumulativeInputs[inputID], err = ctx.cumulativeInputs[inputID].Add(delta)
		if err != nil {
			return nil, fmt.Errorf("failed to update cumulative input %s: %w", inputID, err)
		}
	}

	// Execute on delta
	deltaOutput, err := ctx.executor.ProcessDelta(deltaInputs)
	if err != nil {
		return nil, err
	}

	// Update cumulative output
	ctx.cumulativeOutput, err = ctx.cumulativeOutput.Add(deltaOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to update cumulative output: %w", err)
	}

	// fmt.Printf("Timestep %d: processed delta (%d docs) -> cumulative (%d docs)\n",
	// 	ctx.timestep, deltaOutput.Size(), ctx.cumulativeOutput.Size())

	ctx.timestep++
	return deltaOutput, nil
}

// GetCumulativeOutput returns the current cumulative output
func (ctx *IncrementalExecutionContext) GetCumulativeOutput() *DocumentZSet {
	result := ctx.cumulativeOutput.DeepCopy()
	return result
}

// Reset the context for a fresh start
func (ctx *IncrementalExecutionContext) Reset() {
	ctx.cumulativeInputs = make(map[string]*DocumentZSet)
	ctx.cumulativeOutput = NewDocumentZSet()
	ctx.timestep = 0
	ctx.executor.Reset()
}
