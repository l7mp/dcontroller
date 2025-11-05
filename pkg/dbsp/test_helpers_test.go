package dbsp

import "fmt"

// IncrementalExecutionContext is a test utility that tracks cumulative execution state across
// multiple timesteps. This is used exclusively for testing to verify that incremental DBSP
// operators maintain correctness over time.
//
// NOTE: This file has a _test.go suffix, so it's automatically excluded from production builds
// and only compiled during testing.
type IncrementalExecutionContext struct {
	executor         *Executor
	cumulativeInputs map[string]*DocumentZSet // Running total of all inputs
	cumulativeOutput *DocumentZSet            // Running total of output
	timestep         int
}

// NewIncrementalExecutionContext returns a new test execution context for validating incremental
// semantics across multiple deltas.
func NewIncrementalExecutionContext(executor *Executor) *IncrementalExecutionContext {
	return &IncrementalExecutionContext{
		executor:         executor,
		cumulativeInputs: make(map[string]*DocumentZSet),
		cumulativeOutput: NewDocumentZSet(),
		timestep:         0,
	}
}

// Process processes one delta and updates cumulative state for test validation.
func (ctx *IncrementalExecutionContext) Process(deltaInputs map[string]*DocumentZSet) (*DocumentZSet, error) {
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
	deltaOutput, err := ctx.executor.Process(deltaInputs)
	if err != nil {
		return nil, err
	}

	// Update cumulative output
	ctx.cumulativeOutput, err = ctx.cumulativeOutput.Add(deltaOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to update cumulative output: %w", err)
	}

	ctx.timestep++
	return deltaOutput, nil
}

// GetCumulativeOutput returns the current cumulative output for test assertions.
func (ctx *IncrementalExecutionContext) GetCumulativeOutput() *DocumentZSet {
	result := ctx.cumulativeOutput.DeepCopy()
	return result
}

// Reset resets the context for a fresh test run.
func (ctx *IncrementalExecutionContext) Reset() {
	ctx.cumulativeInputs = make(map[string]*DocumentZSet)
	ctx.cumulativeOutput = NewDocumentZSet()
	ctx.timestep = 0
	ctx.executor.Reset()
}
