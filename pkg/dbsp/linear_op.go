package dbsp

import (
	"errors"
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
		doc := input.docs[key]
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

// Snapshot Gather Operation (stateless)
type GatherOp struct {
	BaseOp
	extractEval Evaluator // Returns keyValueDoc with "key" and "value" fields
	setEval     Evaluator // Sets aggregated values list in a document that is passed in as the key
}

func NewGather(extractEval, setEval Evaluator) *GatherOp {
	return &GatherOp{
		BaseOp:      NewBaseOp("gather", 1),
		extractEval: extractEval,
		setEval:     setEval,
	}
}

func (op *GatherOp) OpType() OperatorType              { return OpTypeLinear }
func (op *GatherOp) IsTimeInvariant() bool             { return true }
func (op *GatherOp) HasZeroPreservationProperty() bool { return true }

func (op *GatherOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]

	// Step 1: Group documents by key
	groups := make(map[string][]*GroupedDocument)

	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Extract key and value using extractEval
		keyValueDocs, err := op.extractEval.Evaluate(doc)
		if err != nil {
			return nil, fmt.Errorf("extract evaluation failed: %w", err)
		}

		// Process each keyValueDoc (usually just one)
		for _, kvDoc := range keyValueDocs {
			_, keyOk := kvDoc["key"]
			_, valOk := kvDoc["value"]
			if !keyOk || !valOk {
				return nil, errors.New("expected extract evaluation to return a key-value doc")
			}
			keyV, err := computeJSONAny(kvDoc["key"])
			if err != nil {
				return nil, err
			}
			valV, err := computeJSONAny(kvDoc["value"])
			if err != nil {
				return nil, err
			}

			groups[keyV] = append(groups[keyV], &GroupedDocument{
				Original:     doc,
				Value:        valV,
				Multiplicity: multiplicity,
			})
		}
	}

	// Step 2: Aggregate each group
	result := NewDocumentZSet()

	for groupKey, groupDocs := range groups {
		// Collect all values from this group
		var allValues []any
		totalMultiplicity := 0

		for _, groupedDoc := range groupDocs {
			// Add values (weighted by multiplicity)
			for i := 0; i < groupedDoc.Multiplicity; i++ {
				allValues = append(allValues, groupedDoc.Value)
			}
			totalMultiplicity += groupedDoc.Multiplicity
		}

		// Create keyValueDoc for setter
		setterInput := Document{
			"key":   groupKey,
			"value": allValues, // List of all values for this group
		}

		// Use setter evaluator to create result documents
		resultDocs, err := op.setEval.Evaluate(setterInput)
		if err != nil {
			return nil, fmt.Errorf("setter evaluation failed: %w", err)
		}

		// Add result documents with total multiplicity
		for _, resultDoc := range resultDocs {
			if err = result.AddDocumentMutate(resultDoc, totalMultiplicity); err != nil {
				return nil, fmt.Errorf("failed to add result document: %w", err)
			}
		}
	}

	return result, nil
}

// Helper struct for grouping
type GroupedDocument struct {
	Original     Document
	Value        any // The extracted value for this document
	Multiplicity int
}

// Incremental Gather Operation (stateful)
// Implements optimized gather^Δ with O(|delta|) complexity
type IncrementalGatherOp struct {
	BaseOp
	extractEval Evaluator
	setEval     Evaluator

	// Optimized state: track current groups efficiently
	currentGroups map[string][]any // groupKey -> list of values in this group
}

func NewIncrementalGather(extractEval, setEval Evaluator) *IncrementalGatherOp {
	return &IncrementalGatherOp{
		BaseOp:        NewBaseOp("gather^Δ", 1),
		extractEval:   extractEval,
		setEval:       setEval,
		currentGroups: make(map[string][]any),
	}
}

func (op *IncrementalGatherOp) OpType() OperatorType              { return OpTypeLinear }
func (op *IncrementalGatherOp) IsTimeInvariant() bool             { return true }
func (op *IncrementalGatherOp) HasZeroPreservationProperty() bool { return true }

func (op *IncrementalGatherOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]

	// OPTIMIZED: Only process the delta documents, not the entire snapshot
	result := NewDocumentZSet()

	// Step 1: Process each document in the delta
	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Extract key and value using extractEval
		keyValueDocs, err := op.extractEval.Evaluate(doc)
		if err != nil {
			return nil, fmt.Errorf("extract evaluation failed: %w", err)
		}

		for _, kvDoc := range keyValueDocs {
			groupKey := fmt.Sprintf("%v", kvDoc["key"])
			value := kvDoc["value"]

			// Get current values for this group
			currentValues, groupExists := op.currentGroups[groupKey]
			if !groupExists {
				currentValues = make([]any, 0)
			}

			// Calculate old result for this group (for delta calculation)
			var oldResultDocs []Document
			if groupExists && len(currentValues) > 0 {
				oldSetterInput := Document{
					"key":   groupKey,
					"value": currentValues,
				}
				oldResultDocs, err = op.setEval.Evaluate(oldSetterInput)
				if err != nil {
					return nil, fmt.Errorf("old setter evaluation failed: %w", err)
				}
			}

			// Update the group's values
			newValues := make([]any, len(currentValues))
			copy(newValues, currentValues)

			for i := 0; i < abs(multiplicity); i++ {
				if multiplicity > 0 {
					newValues = append(newValues, value)
				} else {
					// Removal: find and remove matching value
					newValues = removeFirstMatch(newValues, value)
				}
			}

			// Calculate new result for this group
			var newResultDocs []Document
			if len(newValues) > 0 {
				newSetterInput := Document{
					"key":   groupKey,
					"value": newValues,
				}
				newResultDocs, err = op.setEval.Evaluate(newSetterInput)
				if err != nil {
					return nil, fmt.Errorf("new setter evaluation failed: %w", err)
				}
			}

			// Generate delta output: remove old, add new
			for _, oldDoc := range oldResultDocs {
				if err = result.AddDocumentMutate(oldDoc, -1); err != nil {
					return nil, err
				}
			}

			for _, newDoc := range newResultDocs {
				if err = result.AddDocumentMutate(newDoc, 1); err != nil {
					return nil, err
				}
			}

			// Update internal state
			if len(newValues) > 0 {
				op.currentGroups[groupKey] = newValues
			} else {
				delete(op.currentGroups, groupKey)
			}
		}
	}

	return result, nil
}

// Reset method for testing
func (op *IncrementalGatherOp) Reset() {
	op.currentGroups = make(map[string][]any)
}

// Helper functions
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func removeFirstMatch(slice []any, item any) []any {
	for i, v := range slice {
		vKey, _ := computeJSONAny(v)
		iKey, _ := computeJSONAny(item)
		if vKey == iKey {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
