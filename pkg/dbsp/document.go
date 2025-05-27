package dbsp

import (
	"encoding/json"
	"fmt"
)

// Document represents an unstructured document as map[string]any
// Can contain embedded maps, slices, and primitives (int64, float64, string, bool)
type Document = map[string]any

// DocumentZSet implements Z-sets for atomic documents
// Documents are treated as opaque units - no internal structure operations
type DocumentZSet struct {
	// Use JSON representation as key since documents aren't directly comparable
	docs   map[string]Document // JSON key -> original document
	counts map[string]int      // JSON key -> multiplicity
}

// Error type for better error handling
type ZSetError struct {
	Message string
	Cause   error
}

func (e *ZSetError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func newZSetError(message string, cause error) error {
	return &ZSetError{Message: message, Cause: cause}
}

// NewDocumentZSet creates an empty DocumentZSet
func NewDocumentZSet() *DocumentZSet {
	return &DocumentZSet{
		docs:   make(map[string]Document),
		counts: make(map[string]int),
	}
}

// computeJSONKey creates a deterministic JSON representation for document identity
// This is the key function that defines document equality
func computeJSONKey(doc Document) (string, error) {
	// Convert to canonical form for deterministic JSON
	canonical, err := toCanonicalForm(doc)
	if err != nil {
		return "", newZSetError("failed to convert document to canonical form", err)
	}

	bytes, err := json.Marshal(canonical)
	if err != nil {
		return "", newZSetError("failed to marshal document to JSON", err)
	}

	return string(bytes), nil
}

// toCanonicalForm ensures deterministic JSON representation
// Recursively processes nested structures while preserving semantics
func toCanonicalForm(val any) (any, error) {
	switch v := val.(type) {
	case map[string]any:
		// Process nested maps recursively
		result := make(map[string]any)
		for k, subVal := range v {
			canonical, err := toCanonicalForm(subVal)
			if err != nil {
				return nil, newZSetError(fmt.Sprintf("failed to canonicalize map field '%s'", k), err)
			}
			result[k] = canonical
		}
		return result, nil

	case []any:
		// Process arrays recursively, preserving order
		result := make([]any, len(v))
		for i, subVal := range v {
			canonical, err := toCanonicalForm(subVal)
			if err != nil {
				return nil, newZSetError(fmt.Sprintf("failed to canonicalize array element at index %d", i), err)
			}
			result[i] = canonical
		}
		return result, nil

	case int64, float64, string, bool, nil:
		// Primitives are already canonical
		return v, nil

	default:
		// Handle any other types that might sneak in
		return v, nil
	}
}

// DeepEqual checks if two documents are equal using JSON comparison
func DeepEqual(a, b Document) (bool, error) {
	keyA, err := computeJSONKey(a)
	if err != nil {
		return false, newZSetError("failed to compute key for first document", err)
	}

	keyB, err := computeJSONKey(b)
	if err != nil {
		return false, newZSetError("failed to compute key for second document", err)
	}

	return keyA == keyB, nil
}

// deepCopy creates a deep copy of a document or any nested structure
func deepCopy(val any) (any, error) {
	switch v := val.(type) {
	case map[string]any:
		result := make(map[string]any)
		for k, subVal := range v {
			copied, err := deepCopy(subVal)
			if err != nil {
				return nil, newZSetError(fmt.Sprintf("failed to deep copy map field '%s'", k), err)
			}
			result[k] = copied
		}
		return result, nil

	case []any:
		result := make([]any, len(v))
		for i, subVal := range v {
			copied, err := deepCopy(subVal)
			if err != nil {
				return nil, newZSetError(fmt.Sprintf("failed to deep copy array element at index %d", i), err)
			}
			result[i] = copied
		}
		return result, nil

	case int64, float64, string, bool, nil:
		// Primitives can be copied directly
		return v, nil

	default:
		// For unknown types, try to copy directly
		return v, nil
	}
}

// computeJSONAny creates a deterministic JSON representation for an arbitrary any value
func computeJSONAny(doc any) (string, error) {
	bytes, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("failed to marshal value to JSON: %w", err)
	}

	return string(bytes), nil
}

// AddDocument adds a document to the ZSet with given multiplicity by modifying the Zset in
// place. This is the core operation for building Z-sets.
func (dz *DocumentZSet) AddDocumentMutate(doc Document, count int) error {
	if count == 0 {
		return nil
	}

	key, err := computeJSONKey(doc)
	if err != nil {
		return err
	}

	if _, exists := dz.counts[key]; exists {
		dz.counts[key] += count
	} else {
		copied, err := deepCopy(doc)
		if err != nil {
			return err
		}
		dz.docs[key] = copied.(Document)
		dz.counts[key] = count
	}

	if dz.counts[key] == 0 {
		delete(dz.counts, key)
		delete(dz.docs, key)
	}

	return nil
}

// AddDocument adds a document to the ZSet with given multiplicity by creating a new ZSet. This is
// the core operation for building Z-sets.
func (dz *DocumentZSet) AddDocument(doc Document, count int) (*DocumentZSet, error) {
	result, err := dz.DeepCopy()
	if err != nil {
		return nil, err
	}
	err = result.AddDocumentMutate(doc, count)
	return result, err
}

// Add performs Z-set addition (union with multiplicity)
func (dz *DocumentZSet) Add(other *DocumentZSet) (*DocumentZSet, error) {
	if other == nil {
		return dz.DeepCopy()
	}

	result, err := dz.DeepCopy()
	if err != nil {
		return nil, newZSetError("failed to copy Z-set for addition", err)
	}

	// Add each document from other Z-set
	for key, count := range other.counts {
		doc := other.docs[key]
		if err := result.AddDocumentMutate(doc, count); err != nil {
			return nil, newZSetError("failed to add document during Z-set addition", err)
		}
	}

	return result, nil
}

// Subtract performs Z-set subtraction
func (dz *DocumentZSet) Subtract(other *DocumentZSet) (*DocumentZSet, error) {
	if other == nil {
		return dz.DeepCopy()
	}

	result, err := dz.DeepCopy()
	if err != nil {
		return nil, newZSetError("failed to copy Z-set for subtraction", err)
	}

	// Subtract each document from other Z-set (add with negative count)
	for key, count := range other.counts {
		doc := other.docs[key]
		if err := result.AddDocumentMutate(doc, -count); err != nil {
			return nil, newZSetError("failed to subtract document during Z-set subtraction", err)
		}
	}

	return result, nil
}

// Distinct converts Z-set to set semantics (all multiplicities become 1)
// This is crucial for converting from multiset to set semantics
func (dz *DocumentZSet) Distinct() (*DocumentZSet, error) {
	result := NewDocumentZSet()

	for key, count := range dz.counts {
		if count > 0 {
			doc := dz.docs[key]
			var err error
			result, err = result.AddDocument(doc, 1) // Force count to 1
			if err != nil {
				return nil, newZSetError("failed to add document during distinct operation", err)
			}
		}
		// Note: negative counts are filtered out (don't appear in result)
	}

	return result, nil
}

// DeepCopy creates a deep copy of the DocumentZSet
func (dz *DocumentZSet) DeepCopy() (*DocumentZSet, error) {
	result := &DocumentZSet{
		docs:   make(map[string]Document),
		counts: make(map[string]int),
	}

	for key, doc := range dz.docs {
		copied, err := deepCopy(doc)
		if err != nil {
			return nil, newZSetError("failed to deep copy document in Z-set", err)
		}
		result.docs[key] = copied.(Document)
		result.counts[key] = dz.counts[key]
	}

	return result, nil
}

// GetDocuments returns all documents as a slice (with multiplicities)
// Documents with multiplicity n appear n times in the result
func (dz *DocumentZSet) GetDocuments() ([]Document, error) {
	var result []Document

	for key, count := range dz.counts {
		if count <= 0 {
			continue // Skip non-positive counts
		}

		doc := dz.docs[key]
		for i := 0; i < count; i++ {
			copied, err := deepCopy(doc)
			if err != nil {
				return nil, newZSetError("failed to copy document in GetDocuments", err)
			}
			result = append(result, copied.(Document))
		}
	}

	return result, nil
}

// GetUniqueDocuments returns all unique documents (ignoring multiplicities)
func (dz *DocumentZSet) GetUniqueDocuments() ([]Document, error) {
	var result []Document

	for key, count := range dz.counts {
		if count > 0 {
			doc := dz.docs[key]
			copied, err := deepCopy(doc)
			if err != nil {
				return nil, newZSetError("failed to copy document in GetUniqueDocuments", err)
			}
			result = append(result, copied.(Document))
		}
	}

	return result, nil
}

// IsZero checks if the Z-set is empty (no documents with positive multiplicity)
func (dz *DocumentZSet) IsZero() bool {
	return len(dz.counts) == 0
}

// Size returns total number of documents (counting multiplicities)
func (dz *DocumentZSet) Size() int {
	total := 0
	for _, count := range dz.counts {
		if count > 0 {
			total += count
		}
	}
	return total
}

// UniqueCount returns number of unique documents (ignoring multiplicities)
func (dz *DocumentZSet) UniqueCount() int {
	count := 0
	for _, multiplicity := range dz.counts {
		if multiplicity > 0 {
			count++
		}
	}
	return count
}

// GetMultiplicity returns the multiplicity of a specific document
func (dz *DocumentZSet) GetMultiplicity(doc Document) (int, error) {
	key, err := computeJSONKey(doc)
	if err != nil {
		return 0, newZSetError("failed to compute document key", err)
	}

	if count, exists := dz.counts[key]; exists {
		return count, nil
	}

	return 0, nil // Document not in Z-set
}

// Contains checks if a document exists in the Z-set (with positive multiplicity)
func (dz *DocumentZSet) Contains(doc Document) (bool, error) {
	multiplicity, err := dz.GetMultiplicity(doc)
	if err != nil {
		return false, err
	}
	return multiplicity > 0, nil
}

// Helper functions for creating documents and Z-sets
func NewDocument() Document { return make(Document) }

// newDocumentFromPairs creates a new document from key-value pairs
func newDocumentFromPairs(pairs ...any) (Document, error) {
	if len(pairs)%2 != 0 {
		return nil, newZSetError("NewDocument requires even number of arguments (key-value pairs)", nil)
	}

	doc := make(Document)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			return nil, newZSetError(fmt.Sprintf("key at position %d must be a string", i), nil)
		}
		value := pairs[i+1]
		doc[key] = value
	}

	return doc, nil
}

// SingletonZSet creates a Z-set containing a single document with multiplicity 1
func SingletonZSet(doc Document) (*DocumentZSet, error) {
	zset := NewDocumentZSet()
	return zset.AddDocument(doc, 1)
}

// FromDocuments creates a Z-set from a slice of documents (each with multiplicity 1)
func FromDocuments(docs []Document) (*DocumentZSet, error) {
	result := NewDocumentZSet()

	for i, doc := range docs {
		var err error
		result, err = result.AddDocument(doc, 1)
		if err != nil {
			return nil, newZSetError(fmt.Sprintf("failed to add document at index %d", i), err)
		}
	}

	return result, nil
}

// String returns a string representation of the Z-set for debugging
func (dz *DocumentZSet) String() string {
	if dz.IsZero() {
		return "∅"
	}

	result := "{"
	first := true

	for key, count := range dz.counts {
		if !first {
			result += ", "
		}

		doc := dz.docs[key]
		if count == 1 {
			result += fmt.Sprintf("%v", doc)
		} else {
			result += fmt.Sprintf("%v×%d", doc, count)
		}
		first = false
	}

	result += "}"
	return result
}
