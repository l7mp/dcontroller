package dbsp

import (
	"fmt"
)

// Snapshot N-ary join (non-incremental).
type JoinOp struct {
	BaseOp
	eval   Evaluator
	inputs []string
	n      int
}

// NewJoin returns a new snapshop join op.
func NewJoin(eval Evaluator, inputs []string) *JoinOp {
	return &JoinOp{
		BaseOp: NewBaseOp(fmt.Sprintf("snapshot_⋈_%d", len(inputs)), len(inputs)),
		eval:   eval,
		inputs: inputs,
		n:      len(inputs),
	}
}

func (op *JoinOp) OpType() OperatorType              { return OpTypeBilinear }
func (op *JoinOp) IsTimeInvariant() bool             { return true }
func (op *JoinOp) HasZeroPreservationProperty() bool { return true }

// Process evaluates the op.
func (op *JoinOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	return cartesianJoin(op.eval, op.inputs, inputs, 0, make([]Document, op.n), make([]int, op.n))
}

// IncrementalJoinOp implements an incremental n-ary join.
type IncrementalJoinOp struct {
	BaseOp
	eval   Evaluator
	n      int // number of inputs
	inputs []string
	// State for each input (previous snapshots)
	prevStates []*DocumentZSet
}

// NewIncrementalJoinOp creates a new incremental n-ary join.
func NewIncrementalJoin(eval Evaluator, inputs []string) *IncrementalJoinOp {
	return &IncrementalJoinOp{
		BaseOp:     NewBaseOp(fmt.Sprintf("⋈_%d", len(inputs)), len(inputs)),
		eval:       eval,
		inputs:     inputs,
		n:          len(inputs),
		prevStates: make([]*DocumentZSet, len(inputs)),
	}
}

func (op *IncrementalJoinOp) OpType() OperatorType              { return OpTypeBilinear }
func (op *IncrementalJoinOp) IsTimeInvariant() bool             { return true }
func (op *IncrementalJoinOp) HasZeroPreservationProperty() bool { return true }

// Process evaluates the op.
func (op *IncrementalJoinOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	// Initialize previous states on first call
	for i, prevState := range op.prevStates {
		if prevState == nil {
			op.prevStates[i] = NewDocumentZSet()
		}
	}

	// Generate all 2^n - 1 terms where exactly k inputs are deltas
	result := NewDocumentZSet()
	for mask := 1; mask < (1 << op.n); mask++ {
		term, err := op.computeTerm(inputs, mask)
		if err != nil {
			return nil, err
		}

		result, err = result.Add(term)
		if err != nil {
			return nil, err
		}
	}

	// Update state for next iteration
	for i, delta := range inputs {
		var err error
		op.prevStates[i], err = op.prevStates[i].Add(delta)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (op *IncrementalJoinOp) computeTerm(inputs []*DocumentZSet, mask int) (*DocumentZSet, error) {
	// Create the input combination for this term
	termInputs := make([]*DocumentZSet, op.n)

	for i := 0; i < op.n; i++ {
		if (mask & (1 << i)) != 0 {
			// Use delta (current input)
			termInputs[i] = inputs[i]
		} else {
			// Use previous state
			termInputs[i] = op.prevStates[i]
		}
	}

	// Generate all combinations of documents from all inputs
	return cartesianJoin(op.eval, op.inputs, termInputs, 0, make([]Document, op.n), make([]int, op.n))
}

func cartesianJoin(eval Evaluator, inputNames []string, inputs []*DocumentZSet, inputIndex int, currentDocs []Document, currentMults []int) (*DocumentZSet, error) {
	if inputIndex == len(inputNames) {
		// We have a complete tuple - evaluate the join condition
		joinInput := make(Document)
		totalMult := 1

		for i, doc := range currentDocs {
			joinInput[inputNames[i]] = doc
			totalMult *= currentMults[i]
		}

		// Apply the evaluator
		joinedDocs, err := eval.Evaluate(joinInput)
		if err != nil {
			return nil, err
		}

		result := NewDocumentZSet()
		for _, joinedDoc := range joinedDocs {
			if err := result.AddDocumentMutate(DeepCopyDocument(joinedDoc), totalMult); err != nil {
				return nil, err
			}
		}

		return result, nil
	}

	// Recurse through all documents in current input
	result := NewDocumentZSet()
	input := inputs[inputIndex]

	for key, mult := range input.counts {
		doc := input.docs[key]
		currentDocs[inputIndex] = doc
		currentMults[inputIndex] = mult

		termResult, err := cartesianJoin(eval, inputNames, inputs, inputIndex+1, currentDocs, currentMults)
		if err != nil {
			return nil, err
		}

		result, err = result.Add(termResult)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// BinaryJoinOp is a snapshot binary join op.
type BinaryJoinOp struct {
	BaseOp
	inputs []string
	eval   Evaluator
}

// NewBinaryJoinOp creates a new snapshot binary join op.
func NewBinaryJoin(eval Evaluator, inputs []string) *BinaryJoinOp {
	return &BinaryJoinOp{
		BaseOp: NewBaseOp("⋈", 2),
		eval:   eval,
		inputs: inputs,
	}
}

func (n *BinaryJoinOp) OpType() OperatorType              { return OpTypeBilinear }
func (n *BinaryJoinOp) IsTimeInvariant() bool             { return true }
func (n *BinaryJoinOp) HasZeroPreservationProperty() bool { return true }

// Process evaluates the op.
func (n *BinaryJoinOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	left, right := inputs[0], inputs[1]
	result := NewDocumentZSet()

	// Process unique documents with their multiplicities
	for leftKey, leftMult := range left.counts {
		leftDoc := left.docs[leftKey]
		for rightKey, rightMult := range right.counts {
			rightDoc := right.docs[rightKey]

			// Create join input for evaluator
			joinInput := Document{
				n.inputs[0]: leftDoc,
				n.inputs[1]: rightDoc,
			}

			// Apply join condition
			joinedDocs, err := n.eval.Evaluate(joinInput)
			if err != nil {
				return nil, err
			}

			// BILINEAR: multiply multiplicities
			resultMult := leftMult * rightMult

			for _, joinedDoc := range joinedDocs {
				result, err = result.AddDocument(DeepCopyDocument(joinedDoc), resultMult)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return result, nil
}

// IncrementalBinaryJoinOp implements an incremental binary join.
type IncrementalBinaryJoinOp struct {
	BaseOp
	eval   Evaluator
	inputs []string

	// Internal state (previous snapshots)
	prevLeft  *DocumentZSet
	prevRight *DocumentZSet

	// Three snapshot joins for the bilinear expansion
	join1 *BinaryJoinOp // ΔL ⋈ ΔR
	join2 *BinaryJoinOp // prev_L ⋈ ΔR
	join3 *BinaryJoinOp // ΔL ⋈ prev_R
}

// NewIncrementalBinaryJoinOp creates a new incremental binary join.
func NewIncrementalBinaryJoin(eval Evaluator, inputs []string) *IncrementalBinaryJoinOp {
	return &IncrementalBinaryJoinOp{
		BaseOp:    NewBaseOp("incremental_join", 2),
		eval:      eval,
		inputs:    inputs,
		prevLeft:  NewDocumentZSet(),
		prevRight: NewDocumentZSet(),
		join1:     NewBinaryJoin(eval, inputs),
		join2:     NewBinaryJoin(eval, inputs),
		join3:     NewBinaryJoin(eval, inputs),
	}
}

// Process evaluates the op.
func (op *IncrementalBinaryJoinOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	deltaL, deltaR := inputs[0], inputs[1]

	// Compute the three terms of bilinear expansion
	// Term 1: ΔL ⋈ ΔR
	term1, err := op.join1.Process(deltaL, deltaR)
	if err != nil {
		return nil, fmt.Errorf("ΔL ⋈ ΔR failed: %w", err)
	}

	// Term 2: prev_L ⋈ ΔR
	term2, err := op.join2.Process(op.prevLeft, deltaR)
	if err != nil {
		return nil, fmt.Errorf("prev_L ⋈ ΔR failed: %w", err)
	}

	// Term 3: ΔL ⋈ prev_R
	term3, err := op.join3.Process(deltaL, op.prevRight)
	if err != nil {
		return nil, fmt.Errorf("ΔL ⋈ prev_R failed: %w", err)
	}

	// Combine all terms: result = term1 + term2 + term3
	result, err := term1.Add(term2)
	if err != nil {
		return nil, err
	}
	result, err = result.Add(term3)
	if err != nil {
		return nil, err
	}

	// Update internal state for next iteration
	op.prevLeft, err = op.prevLeft.Add(deltaL)
	if err != nil {
		return nil, err
	}
	op.prevRight, err = op.prevRight.Add(deltaR)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (op *IncrementalBinaryJoinOp) OpType() OperatorType              { return OpTypeBilinear }
func (op *IncrementalBinaryJoinOp) IsTimeInvariant() bool             { return true }
func (op *IncrementalBinaryJoinOp) HasZeroPreservationProperty() bool { return true }

// Reset method for testing.
func (op *IncrementalBinaryJoinOp) Reset() {
	op.prevLeft = NewDocumentZSet()
	op.prevRight = NewDocumentZSet()
}
