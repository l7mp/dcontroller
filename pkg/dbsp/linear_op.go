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
			// Transform document with current element
			transformedDoc, err := op.transformer.Transform(doc, element)
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

// Snapshot Gather Operation (stateless)
type GatherOp struct {
	BaseOp
	keyExtractor   Extractor   // Extracts grouping key from document
	valueExtractor Extractor   // Extracts value to aggregate from document
	aggregator     Transformer // Creates result document from key and aggregated values
}

func NewGather(keyExtractor, valueExtractor Extractor, aggregator Transformer) *GatherOp {
	return &GatherOp{
		BaseOp:         NewBaseOp("gather", 1),
		keyExtractor:   keyExtractor,
		valueExtractor: valueExtractor,
		aggregator:     aggregator,
	}
}

func (op *GatherOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]

	// Step 1: Group documents by key
	groups := make(map[string]*GroupData)

	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Extract key and value
		groupKey, err := op.keyExtractor.Extract(doc)
		if err != nil {
			return nil, fmt.Errorf("key extraction failed: %w", err)
		}
		if groupKey == nil {
			continue // Skip documents without key
		}

		value, err := op.valueExtractor.Extract(doc)
		if err != nil {
			return nil, fmt.Errorf("value extraction failed: %w", err)
		}
		if value == nil {
			continue // Skip documents without value
		}

		// Create JSON string for map key (allows any type as key)
		keyForMap, err := computeJSONAny(groupKey)
		if err != nil {
			return nil, err
		}

		// Initialize or update group
		if groups[keyForMap] == nil {
			groups[keyForMap] = &GroupData{
				Key:    groupKey,
				Values: make([]any, 0),
			}
		}

		// Add values (weighted by multiplicity)
		for i := 0; i < multiplicity; i++ {
			groups[keyForMap].Values = append(groups[keyForMap].Values, value)
		}
	}

	// Step 2: Aggregate each group
	result := NewDocumentZSet()

	for _, groupData := range groups {
		// Use aggregator to create result document
		resultDoc, err := op.aggregator.Transform(Document{}, &AggregateInput{
			Key:    groupData.Key,
			Values: groupData.Values,
		})
		if err != nil {
			return nil, fmt.Errorf("aggregation failed: %w", err)
		}

		// Add result document (multiplicity is already incorporated in values)
		if err = result.AddDocumentMutate(resultDoc, 1); err != nil {
			return nil, fmt.Errorf("failed to add result document: %w", err)
		}
	}

	return result, nil
}

// Helper structs
type GroupData struct {
	Key    any
	Values []any
}

type AggregateInput struct {
	Key    any
	Values []any
}

func (op *GatherOp) OpType() OperatorType              { return OpTypeNonLinear }
func (op *GatherOp) IsTimeInvariant() bool             { return true }
func (op *GatherOp) HasZeroPreservationProperty() bool { return true }

// Incremental Gather Operation (stateful)
// Implements optimized gather^Δ with O(|delta|) complexity
// Incremental Gather Operation (stateful)
type IncrementalGatherOp struct {
	BaseOp
	keyExtractor   Extractor
	valueExtractor Extractor
	aggregator     Transformer

	// Optimized state: track current groups efficiently
	currentGroups map[string]*GroupData // groupKey -> current group data
}

func NewIncrementalGather(keyExtractor, valueExtractor Extractor, aggregator Transformer) *IncrementalGatherOp {
	return &IncrementalGatherOp{
		BaseOp:         NewBaseOp("gather^Δ", 1),
		keyExtractor:   keyExtractor,
		valueExtractor: valueExtractor,
		aggregator:     aggregator,
		currentGroups:  make(map[string]*GroupData),
	}
}

func (op *IncrementalGatherOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]
	result := NewDocumentZSet()

	// Process each document in the delta
	for key, multiplicity := range input.counts {
		doc := input.docs[key]

		// Extract key and value
		groupKey, err := op.keyExtractor.Extract(doc)
		if err != nil {
			return nil, fmt.Errorf("key extraction failed: %w", err)
		}
		if groupKey == nil {
			continue
		}

		value, err := op.valueExtractor.Extract(doc)
		if err != nil {
			return nil, fmt.Errorf("value extraction failed: %w", err)
		}
		if value == nil {
			continue
		}

		// Create string key for map grouping
		groupKeyStr, err := computeJSONAny(groupKey)
		if err != nil {
			return nil, fmt.Errorf("failed to compute group key: %w", err)
		}

		// Get current group data
		currentGroup := op.currentGroups[groupKeyStr]
		var oldValues []any
		if currentGroup != nil {
			oldValues = make([]any, len(currentGroup.Values))
			copy(oldValues, currentGroup.Values)
		}

		// Calculate old result for this group (for delta calculation)
		var oldResultDoc Document
		if len(oldValues) > 0 {
			oldResultDoc, err = op.aggregator.Transform(Document{}, &AggregateInput{
				Key:    groupKey,
				Values: oldValues,
			})
			if err != nil {
				return nil, fmt.Errorf("old aggregation failed: %w", err)
			}
		}

		// Update the group's values
		newValues := make([]any, len(oldValues))
		copy(newValues, oldValues)

		for i := 0; i < abs(multiplicity); i++ {
			if multiplicity > 0 {
				newValues = append(newValues, value)
			} else {
				// Removal: find and remove matching value
				newValues = removeFirstMatch(newValues, value)
			}
		}

		// Calculate new result for this group
		var newResultDoc Document
		if len(newValues) > 0 {
			newResultDoc, err = op.aggregator.Transform(Document{}, &AggregateInput{
				Key:    groupKey,
				Values: newValues,
			})
			if err != nil {
				return nil, fmt.Errorf("new aggregation failed: %w", err)
			}
		}

		// Generate delta output: remove old, add new
		if len(oldValues) > 0 {
			if err = result.AddDocumentMutate(oldResultDoc, -1); err != nil {
				return nil, err
			}
		}

		if len(newValues) > 0 {
			if err = result.AddDocumentMutate(newResultDoc, 1); err != nil {
				return nil, err
			}
		}

		// Update internal state
		if len(newValues) > 0 {
			op.currentGroups[groupKeyStr] = &GroupData{
				Key:    groupKey,
				Values: newValues,
			}
		} else {
			delete(op.currentGroups, groupKeyStr)
		}
	}

	return result, nil
}

func (op *IncrementalGatherOp) OpType() OperatorType              { return OpTypeNonLinear }
func (op *IncrementalGatherOp) IsTimeInvariant() bool             { return true }
func (op *IncrementalGatherOp) HasZeroPreservationProperty() bool { return true }

// Reset method for testing
func (op *IncrementalGatherOp) Reset() {
	op.currentGroups = map[string]*GroupData{}
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
