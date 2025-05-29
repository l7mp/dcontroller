package dbsp

import "fmt"

// ProjectThenSelectOp - True fusion: project then select in single pass
type ProjectThenSelectOp struct {
	BaseOp
	projEval Evaluator
	selEval  Evaluator
}

func NewProjectThenSelect(projEval, selEval Evaluator) *ProjectThenSelectOp {
	return &ProjectThenSelectOp{
		BaseOp:   NewBaseOp("[π→σ]", 1),
		projEval: projEval,
		selEval:  selEval,
	}
}

func (op *ProjectThenSelectOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]
	result := NewDocumentZSet()

	// Single pass: project AND select each document
	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Apply projection
		projectedDocs, err := op.projEval.Evaluate(doc)
		if err != nil {
			return nil, err
		}

		for _, projDoc := range projectedDocs {
			// Apply selection to projected doc
			selectedDocs, err := op.selEval.Evaluate(projDoc)
			if err != nil {
				return nil, err
			}

			// Add final results
			for _, finalDoc := range selectedDocs {
				if err := result.AddDocumentMutate(finalDoc, multiplicity); err != nil {
					return nil, err
				}
			}
		}
	}
	return result, nil
}

func (op *ProjectThenSelectOp) OpType() OperatorType              { return OpTypeLinear }
func (op *ProjectThenSelectOp) IsTimeInvariant() bool             { return true }
func (op *ProjectThenSelectOp) HasZeroPreservationProperty() bool { return true }

// SelectThenProjectionsOp - optional selection + 1+ projections
type SelectThenProjectionsOp struct {
	BaseOp
	selEval   Evaluator   // Selection evaluator (can be nil)
	projEvals []Evaluator // Chain of projection evaluators (1 or more)
}

func NewSelectThenProjections(selEval Evaluator, projEvals []Evaluator) *SelectThenProjectionsOp {
	if len(projEvals) == 0 {
		panic("requires at least one projection evaluator")
	}

	var name string
	if selEval != nil {
		name = "σ→π"
		if len(projEvals) > 1 {
			name = fmt.Sprintf("[σ→π^%d]", len(projEvals))
		}
	} else {
		name = "π"
		if len(projEvals) > 1 {
			name = fmt.Sprintf("[π^%d]", len(projEvals))
		}
	}

	return &SelectThenProjectionsOp{
		BaseOp:    NewBaseOp(name, 1),
		selEval:   selEval,
		projEvals: projEvals,
	}
}

func (op *SelectThenProjectionsOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]
	result := NewDocumentZSet()

	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Apply optional selection first
		var docsToProcess []Document
		if op.selEval != nil {
			selectedDocs, err := op.selEval.Evaluate(doc)
			if err != nil {
				return nil, err
			}
			docsToProcess = selectedDocs
		} else {
			// No selection, process original document
			docsToProcess = []Document{doc}
		}

		// Apply projection chain to each document that passed selection
		for _, docToProcess := range docsToProcess {
			currentDoc := docToProcess

			// Apply chain of projections sequentially
			for _, projEval := range op.projEvals {
				projectedDocs, err := projEval.Evaluate(currentDoc)
				if err != nil {
					return nil, err
				}

				if len(projectedDocs) != 1 {
					return nil, fmt.Errorf("projection evaluator produced %d documents, expected 1", len(projectedDocs))
				}

				currentDoc = projectedDocs[0]
			}

			// Add final transformed document
			if err := result.AddDocumentMutate(currentDoc, multiplicity); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (op *SelectThenProjectionsOp) OpType() OperatorType              { return OpTypeLinear }
func (op *SelectThenProjectionsOp) IsTimeInvariant() bool             { return true }
func (op *SelectThenProjectionsOp) HasZeroPreservationProperty() bool { return true }

// FusedOp is a naive fused op that just calls the subsequent nodes process function along the chain. Currently unused.
type FusedOp struct {
	BaseOp
	nodes []Operator
}

func NewFusedOp(nodes []Operator, name string) (*FusedOp, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("cannot create empty fused node")
	}

	return &FusedOp{
		BaseOp: NewBaseOp("fused:"+name, nodes[0].Arity()),
		nodes:  nodes,
	}, nil
}

func (n *FusedOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	// Chain execution through all nodes, propagating errors
	result := inputs[0]
	for i, node := range n.nodes {
		var err error
		result, err = node.Process(result)
		if err != nil {
			return NewDocumentZSet(), fmt.Errorf("error in fused node %s at step %d (%s): %w",
				n.Name(), i, node.Name(), err)
		}
	}

	return result, nil
}

func (n *FusedOp) OpType() OperatorType              { return OpTypeLinear }
func (n *FusedOp) IsTimeInvariant() bool             { return true }
func (n *FusedOp) HasZeroPreservationProperty() bool { return true }

// Helper function for safe fusion
func FuseFilterProject(filter *SelectionOp, project *ProjectionOp) (Operator, error) {
	return NewFusedOp(
		[]Operator{filter, project},
		fmt.Sprintf("%s→%s", filter.Name(), project.Name()),
	)
}
