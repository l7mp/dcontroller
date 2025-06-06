package dbsp

import (
	"fmt"
)

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

// GatherOp processes each group and preserves input document content.
func (op *GatherOp) Process(inputs ...*DocumentZSet) (*DocumentZSet, error) {
	if err := op.validateInputs(inputs); err != nil {
		return nil, err
	}

	input := inputs[0]

	// Step 1: Group documents by key, preserving one representative document per group
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
				Key:      groupKey,
				Values:   make([]any, 0),
				Document: doc, // Preserve one document from this group
			}
		}

		// Add/remove values based on multiplicity sign
		if multiplicity > 0 {
			// Additions
			for i := 0; i < multiplicity; i++ {
				groups[keyForMap].Values = append(groups[keyForMap].Values, value)
			}
		} else if multiplicity < 0 {
			// Deletions - remove matching values
			for i := 0; i < -multiplicity; i++ {
				groups[keyForMap].Values = removeFirstMatch(groups[keyForMap].Values, value)
			}
		}
	}

	// Step 2: Aggregate each group, preserving document content
	result := NewDocumentZSet()

	for _, groupData := range groups {
		// Pass representative document to aggregator to preserve content
		resultDoc, err := op.aggregator.Transform(groupData.Document, &AggregateInput{
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
	Key      any
	Values   []any
	Document Document
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
		currentGroups:  make(map[string]*GroupData), // groupKey -> current group data with doc
	}
}

// IncrementalGatherOp processes deltas and preserves input document content.
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
		var representativeDoc Document

		if currentGroup != nil {
			oldValues = make([]any, len(currentGroup.Values))
			copy(oldValues, currentGroup.Values)
			representativeDoc = currentGroup.Document
		} else {
			// First document in this group becomes the representative
			representativeDoc = doc
		}

		// Calculate old result for this group (for delta calculation)
		var oldResultDoc Document
		if len(oldValues) > 0 {
			oldResultDoc, err = op.aggregator.Transform(representativeDoc, &AggregateInput{
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
			newResultDoc, err = op.aggregator.Transform(representativeDoc, &AggregateInput{
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
				Key:      groupKey,
				Values:   newValues,
				Document: representativeDoc,
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
