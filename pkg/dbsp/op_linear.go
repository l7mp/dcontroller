package dbsp

import (
	"fmt"
)

// Projection node
type ProjectionOp struct {
	BaseOp
	eval Evaluator
}

func NewProjection(eval Evaluator) *ProjectionOp {
	return &ProjectionOp{
		BaseOp: NewBaseOp("π", 1),
		eval:   eval,
	}
}

func (n *ProjectionOp) OpType() OperatorType              { return OpTypeLinear }
func (n *ProjectionOp) IsTimeInvariant() bool             { return true }
func (n *ProjectionOp) HasZeroPreservationProperty() bool { return true }

func (n *ProjectionOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := n.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]
	result := NewDocumentZSet()

	for key, multiplicity := range input.counts {
		// evaluator modifies doc
		doc, err := deepCopyDocument(input.docs[key])
		if err != nil {
			return nil, fmt.Errorf("failed to deep copy document for modification: %w", err)
		}
		projectedDocs, err := n.eval.Evaluate(doc)
		if err != nil {
			return nil, err
		}

		for _, projectedDoc := range projectedDocs {
			if err = result.AddDocumentMutate(projectedDoc, multiplicity); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// Selection node
type SelectionOp struct {
	BaseOp
	eval Evaluator
}

func NewSelection(name string, eval Evaluator) *SelectionOp {
	return &SelectionOp{
		BaseOp: NewBaseOp("σ", 1),
		eval:   eval,
	}
}

func (n *SelectionOp) OpType() OperatorType              { return OpTypeLinear }
func (n *SelectionOp) IsTimeInvariant() bool             { return true }
func (n *SelectionOp) HasZeroPreservationProperty() bool { return true }

func (n *SelectionOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	err := n.validateInputs(inputs)
	if err != nil {
		return nil, err
	}

	input := inputs[0]
	result := NewDocumentZSet()

	for key, multiplicity := range input.counts {
		doc := input.docs[key]
		selectedDocs := []Document{}
		selectedDocs, err = n.eval.Evaluate(doc)
		if err != nil {
			return nil, err
		}

		for _, selectedDoc := range selectedDocs {
			if err = result.AddDocumentMutate(selectedDoc, multiplicity); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// UnwindOp flattens arrays within documents.
type UnwindOp struct {
	BaseOp
	// Extracts the array to unwind from the document
	arrayExtractor Extractor
	// Transforms the document by replacing array field with single element
	transformer Transformer
}

func NewUnwind(arrayExtractor Extractor, transformer Transformer) *UnwindOp {
	return &UnwindOp{
		BaseOp:         NewBaseOp("unwind", 1),
		arrayExtractor: arrayExtractor,
		transformer:    transformer,
	}
}

func (op *UnwindOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]
	result := NewDocumentZSet()

	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Extract the array to unwind
		arrayValue, err := op.arrayExtractor.Extract(doc)
		if err != nil {
			return nil, fmt.Errorf("array extraction failed: %w", err)
		}

		if arrayValue == nil {
			continue // No array field found, skip document
		}

		arraySlice, ok := arrayValue.([]any)
		if !ok {
			// Not an array - skip gracefully
			continue
		}

		// Create one document for each array element
		for _, element := range arraySlice {
			// Transform document with current element: modifies document
			transformed, err := deepCopyDocument(input.docs[key])
			if err != nil {
				return nil, fmt.Errorf("failed to deep copy document for modification: %w", err)
			}

			transformedDoc, err := op.transformer.Transform(transformed, element)
			if err != nil {
				return nil, fmt.Errorf("document transformation failed: %w", err)
			}

			// Add transformed document with original multiplicity
			if err = result.AddDocumentMutate(transformedDoc, multiplicity); err != nil {
				return nil, fmt.Errorf("failed to add unwound document: %w", err)
			}
		}
	}

	return result, nil
}

func (op *UnwindOp) OpType() OperatorType              { return OpTypeLinear }
func (op *UnwindOp) IsTimeInvariant() bool             { return true }
func (op *UnwindOp) HasZeroPreservationProperty() bool { return true }
