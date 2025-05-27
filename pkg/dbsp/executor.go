package dbsp

// import (
// 	"fmt"
// 	"sort"
// )

// // Executor runs computation graphs
// type Executor struct {
// 	graph         *ComputationGraph
// 	executionPlan []string                 // Topologically sorted node execution order
// 	nodeResults   map[string]*DocumentZSet // Cache of node outputs
// }

// func NewExecutor(graph *ComputationGraph) (*Executor, error) {
// 	// Validate graph structure
// 	if err := graph.Validate(); err != nil {
// 		return nil, fmt.Errorf("invalid graph: %w", err)
// 	}

// 	executor := &Executor{
// 		graph:       graph,
// 		nodeResults: make(map[string]*DocumentZSet),
// 	}

// 	// Build execution plan
// 	plan, err := executor.buildExecutionPlan()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to build execution plan: %w", err)
// 	}

// 	executor.executionPlan = plan
// 	return executor, nil
// }

// // Build topologically sorted execution plan
// func (e *Executor) buildExecutionPlan() ([]string, error) {
// 	// Kahn's algorithm for topological sorting
// 	inDegree := make(map[string]int)

// 	// Calculate in-degrees
// 	for id := range e.graph.nodes {
// 		inDegree[id] = 0
// 	}
// 	for _, node := range e.graph.nodes {
// 		for _, output := range node.Outputs {
// 			inDegree[output.ID]++
// 		}
// 	}

// 	// Find nodes with no incoming edges
// 	queue := make([]string, 0)
// 	for id, degree := range inDegree {
// 		if degree == 0 {
// 			queue = append(queue, id)
// 		}
// 	}

// 	// Sort queue for deterministic execution order
// 	sort.Strings(queue)

// 	plan := make([]string, 0)

// 	for len(queue) > 0 {
// 		// Remove node with no incoming edges
// 		current := queue[0]
// 		queue = queue[1:]
// 		plan = append(plan, current)

// 		// Remove edges from current node
// 		currentNode := e.graph.nodes[current]
// 		newNodes := make([]string, 0)

// 		for _, output := range currentNode.Outputs {
// 			inDegree[output.ID]--
// 			if inDegree[output.ID] == 0 {
// 				newNodes = append(newNodes, output.ID)
// 			}
// 		}

// 		// Add newly available nodes to queue (sorted)
// 		sort.Strings(newNodes)
// 		queue = append(queue, newNodes...)
// 		sort.Strings(queue)
// 	}

// 	// Check for cycles
// 	if len(plan) != len(e.graph.nodes) {
// 		return nil, fmt.Errorf("graph contains cycles")
// 	}

// 	return plan, nil
// }

// // Execute one timestep of computation
// func (e *Executor) Execute(inputs map[string]*DocumentZSet) (*DocumentZSet, error) {
// 	// Clear previous results
// 	e.nodeResults = make(map[string]*DocumentZSet)

// 	// Set input values
// 	for _, inputID := range e.graph.inputs {
// 		if inputData, exists := inputs[inputID]; exists {
// 			e.nodeResults[inputID] = inputData
// 		} else {
// 			return nil, fmt.Errorf("missing input data for node %s", inputID)
// 		}
// 	}

// 	// Execute nodes in topological order
// 	for _, nodeID := range e.executionPlan {
// 		// Skip if this node already has a result (input node)
// 		if _, hasResult := e.nodeResults[nodeID]; hasResult {
// 			continue
// 		}

// 		node := e.graph.nodes[nodeID]

// 		// Gather inputs for this node
// 		nodeInputs := make([]*DocumentZSet, len(node.Inputs))
// 		for i, inputNode := range node.Inputs {
// 			inputData, exists := e.nodeResults[inputNode.ID]
// 			if !exists {
// 				return nil, fmt.Errorf("missing input result for node %s (needed by %s)",
// 					inputNode.ID, nodeID)
// 			}
// 			nodeInputs[i] = inputData
// 		}

// 		// Execute the node
// 		result, err := node.Node.Process(nodeInputs...)
// 		if err != nil {
// 			return nil, fmt.Errorf("node %s execution failed: %w", nodeID, err)
// 		}

// 		e.nodeResults[nodeID] = result

// 		fmt.Printf("Executed %s -> %d documents\n", nodeID, result.Size())
// 	}

// 	// Return result from output node
// 	outputID := e.graph.outputs[0]
// 	result, exists := e.nodeResults[outputID]
// 	if !exists {
// 		return nil, fmt.Errorf("no result from output node %s", outputID)
// 	}

// 	return result, nil
// }

// // Reset all stateful nodes (for incremental computation)
// func (e *Executor) Reset() {
// 	for _, node := range e.graph.nodes {
// 		switch n := node.Node.(type) {
// 		case *IntegratorOp:
// 			n.Reset()
// 		case *DifferentiatorOp:
// 			n.Reset()
// 		case *DelayNode:
// 			n.Reset()
// 		}
// 	}
// }

// // GetExecutionPlan returns the topologically sorted execution order
// func (e *Executor) GetExecutionPlan() []string {
// 	return e.executionPlan
// }

// // GetNodeResult returns the cached result for a node (for debugging)
// func (e *Executor) GetNodeResult(nodeID string) (*DocumentZSet, bool) {
// 	result, exists := e.nodeResults[nodeID]
// 	return result, exists
// }

// // ExecuteMultipleTimesteps runs computation for multiple timesteps
// func (e *Executor) ExecuteMultipleTimesteps(timesteps []map[string]*DocumentZSet) ([]*DocumentZSet, error) {
// 	results := make([]*DocumentZSet, len(timesteps))

// 	for i, inputs := range timesteps {
// 		fmt.Printf("\n=== Timestep %d ===\n", i)

// 		result, err := e.Execute(inputs)
// 		if err != nil {
// 			return nil, fmt.Errorf("timestep %d failed: %w", i, err)
// 		}

// 		results[i] = result
// 		fmt.Printf("Timestep %d result: %d documents\n", i, result.Size())
// 	}

// 	return results, nil
// }
