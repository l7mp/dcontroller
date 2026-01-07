package dbsp

import (
	"cmp"
	"fmt"
	"slices"
)

// Snapshot Gather Operation.
//
// NOTE: Non-linear operators (like GatherOp) do NOT have dedicated incremental versions.
// Instead, they are lifted using the DBSP formula F^Δ = D ∘ F ∘ I by the NonLinearLiftingRule
// in the rewrite engine. This wraps the snapshot operator with Integrator before and
// Differentiator after, achieving incremental semantics without custom stateful code.
type GatherOp struct {
	BaseOp
	keyExtractor   Extractor   // Extracts grouping key from document
	valueExtractor Extractor   // Extracts value to aggregate from document
	aggregator     Transformer // Creates result document from key and aggregated values
}

// NewGather creates a new snapshot gather op.
func NewGather(keyExtractor, valueExtractor Extractor, aggregator Transformer) *GatherOp {
	return &GatherOp{
		BaseOp:         NewBaseOp("gather", 1),
		keyExtractor:   keyExtractor,
		valueExtractor: valueExtractor,
		aggregator:     aggregator,
	}
}

// Process evaluates the op.
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
		// Optionally sort values for deterministic output.
		if SortGatherValues && len(groupData.Values) > 1 {
			sortValues(groupData.Values)
		}

		// Pass a deepcopy of the representative document to aggregator to preserve content
		resultDoc, err := op.aggregator.Transform(DeepCopyDocument(groupData.Document), &AggregateInput{
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

// GroupData is helper struct for gather.
type GroupData struct {
	Key      any
	Values   []any
	Document Document
}

// AggregateInput is helper struct for gather.
type AggregateInput struct {
	Key    any
	Values []any
}

func (op *GatherOp) OpType() OperatorType              { return OpTypeNonLinear }
func (op *GatherOp) IsTimeInvariant() bool             { return true }
func (op *GatherOp) HasZeroPreservationProperty() bool { return true }

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

// sortValues sorts a slice of any values for deterministic output.
// Values are sorted by their JSON representation to handle heterogeneous types.
func sortValues(values []any) {
	slices.SortFunc(values, func(a, b any) int {
		// Try to compare as ordered types first for efficiency.
		switch av := a.(type) {
		case int:
			if bv, ok := b.(int); ok {
				return cmp.Compare(av, bv)
			}
		case int64:
			if bv, ok := b.(int64); ok {
				return cmp.Compare(av, bv)
			}
		case float64:
			if bv, ok := b.(float64); ok {
				return cmp.Compare(av, bv)
			}
		case string:
			if bv, ok := b.(string); ok {
				return cmp.Compare(av, bv)
			}
		}

		// Fall back to JSON representation for mixed types.
		aKey, _ := computeJSONAny(a)
		bKey, _ := computeJSONAny(b)
		return cmp.Compare(aKey, bKey)
	})
}
