package dbsp

import (
	"fmt"
)

// DocumentZSet implements Z-sets for atomic documents.  Documents are treated as opaque units - no
// internal structure operations are considered.
type DocumentZSet struct {
	// Use JSON representation as key since documents aren't directly comparable
	docs   map[string]Document // JSON key -> original document
	counts map[string]int      // JSON key -> multiplicity
}

// Error type for better error handling.
type ZSetError struct {
	Message string
	Cause   error
}

// Error implements the error interface.
func (e *ZSetError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func newZSetError(message string, cause error) error {
	return &ZSetError{Message: message, Cause: cause}
}

// NewDocumentZSet creates an empty DocumentZSet.
func NewDocumentZSet() *DocumentZSet {
	return &DocumentZSet{
		docs:   make(map[string]Document),
		counts: make(map[string]int),
	}
}

// AddDocument adds a document to the ZSet with given multiplicity and creates a new ZSet. This is
// the core operation for building Z-sets. It copies the added doc.
func (dz *DocumentZSet) AddDocument(doc Document, count int) (*DocumentZSet, error) {
	result := dz.ShallowCopy()
	err := result.AddDocumentMutate(doc, count)
	return result, err
}

// AddDocumentMutate adds a document to the ZSet with given multiplicity by modifying the Zset in
// place.
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
		dz.docs[key] = doc
		dz.counts[key] = count
	}

	if dz.counts[key] == 0 {
		delete(dz.counts, key)
		delete(dz.docs, key)
	}

	return nil
}

// Add performs Z-set addition (union with multiplicity).
func (dz *DocumentZSet) Add(other *DocumentZSet) (*DocumentZSet, error) {
	if other == nil {
		return dz.DeepCopy(), nil
	}

	result := dz.DeepCopy()

	// Add each document from other Z-set
	for key, count := range other.counts {
		doc := other.docs[key]
		if err := result.AddDocumentMutate(doc, count); err != nil {
			return nil, newZSetError("failed to add document during Z-set addition", err)
		}
	}

	return result, nil
}

// Subtract performs Z-set subtraction.
func (dz *DocumentZSet) Subtract(other *DocumentZSet) (*DocumentZSet, error) {
	if other == nil {
		return dz.DeepCopy(), nil
	}

	result := dz.DeepCopy()

	// Subtract each document from other Z-set (add with negative count)
	for key, count := range other.counts {
		doc := other.docs[key]
		if err := result.AddDocumentMutate(doc, -count); err != nil {
			return nil, newZSetError("failed to subtract document during Z-set subtraction", err)
		}
	}

	return result, nil
}

// Distinct converts Z-set to set semantics (all multiplicities become 1). This is crucial for
// converting from multiset to set semantics
func (dz *DocumentZSet) Distinct() (*DocumentZSet, error) {
	result := NewDocumentZSet()

	for key, count := range dz.counts {
		if count > 0 {
			doc := dz.docs[key]
			if err := result.AddDocumentMutate(doc, 1); err != nil {
				return nil, newZSetError("failed to add document during 'distinct' operation", err)
			}
		}
		// Note: negative counts are filtered out (don't appear in result)
	}

	return result, nil
}

// Unique converts a Z-set to set semantics preserving multiplicity sign (all multiplicities become +/-1).
func (dz *DocumentZSet) Unique() (*DocumentZSet, error) {
	result := NewDocumentZSet()

	for key, count := range dz.counts {
		doc := dz.docs[key]
		mul := 1
		if count < 0 {
			mul = -1
		}
		if err := result.AddDocumentMutate(doc, mul); err != nil {
			return nil, newZSetError("failed to add document during 'unique' operation", err)
		}
	}

	return result, nil
}

// ShallowCopy creates a shallow copy of the DocumentZSet.
func (dz *DocumentZSet) ShallowCopy() *DocumentZSet {
	result := &DocumentZSet{
		docs:   make(map[string]Document, len(dz.docs)),
		counts: make(map[string]int, len(dz.counts)),
	}

	// Copy map references - documents themselves are not copied
	for key, doc := range dz.docs {
		result.docs[key] = doc // Shallow: same document reference
	}

	for key, count := range dz.counts {
		result.counts[key] = count
	}

	return result
}

// DeepCopy creates a deep copy of the DocumentZSet.
func (dz *DocumentZSet) DeepCopy() *DocumentZSet {
	result := &DocumentZSet{
		docs:   make(map[string]Document),
		counts: make(map[string]int),
	}

	for key, doc := range dz.docs {
		result.docs[key] = DeepCopyDocument(doc)
		result.counts[key] = dz.counts[key]
	}

	return result
}

// DocumentEntry represents a document with its multiplicity in a Z-set.
type DocumentEntry struct {
	Document     Document
	Multiplicity int
}

// List returns all documents with their multiplicities (including negative ones).
func (dz *DocumentZSet) List() ([]DocumentEntry, error) {
	result := make([]DocumentEntry, 0, len(dz.counts))

	for key, mult := range dz.counts {
		result = append(result, DocumentEntry{
			Document:     DeepCopyDocument(dz.docs[key]),
			Multiplicity: mult,
		})
	}

	return result, nil
}

// GetDocuments returns all documents as a slice (with multiplicities). Documents with multiplicity
// n appear n times in the result.
func (dz *DocumentZSet) GetDocuments() ([]Document, error) {
	var result []Document

	for key, count := range dz.counts {
		if count <= 0 {
			continue // Skip non-positive counts
		}

		doc := dz.docs[key]
		for i := 0; i < count; i++ {
			result = append(result, DeepCopyDocument(doc))
		}
	}

	return result, nil
}

// GetUniqueDocuments returns all unique documents (ignoring multiplicities).
func (dz *DocumentZSet) GetUniqueDocuments() ([]Document, error) {
	var result []Document

	for key, count := range dz.counts {
		if count > 0 {
			doc := dz.docs[key]
			result = append(result, DeepCopyDocument(doc))
		}
	}

	return result, nil
}

// IsZero checks if the Z-set is empty (no documents with positive multiplicity).
func (dz *DocumentZSet) IsZero() bool {
	return len(dz.counts) == 0
}

// Size returns the number of documents counting only positive multiplicities.
func (dz *DocumentZSet) Size() int {
	total := 0
	for _, count := range dz.counts {
		if count > 0 {
			total += count
		}
	}
	return total
}

// TotalSize returns the total number of documents, counting both positive and negative multiplicities.
func (dz *DocumentZSet) TotalSize() int {
	total := 0
	for _, count := range dz.counts {
		if count > 0 {
			total += count
		} else {
			total += -count
		}
	}
	return total
}

// UniqueCount returns number of unique documents (ignoring multiplicities).
func (dz *DocumentZSet) UniqueCount() int {
	count := 0
	for _, multiplicity := range dz.counts {
		if multiplicity > 0 {
			count++
		}
	}
	return count
}

// GetMultiplicity returns the multiplicity of a specific document.
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

// Contains checks if a document exists in the Z-set with positive multiplicity.
func (dz *DocumentZSet) Contains(doc Document) (bool, error) {
	multiplicity, err := dz.GetMultiplicity(doc)
	if err != nil {
		return false, err
	}
	return multiplicity > 0, nil
}

// NewDocument creates a new document and wraps it to be added to a Z-sets.
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

// SingletonZSet creates a Z-set containing a single document with multiplicity 1.
func SingletonZSet(doc Document) (*DocumentZSet, error) {
	zset := NewDocumentZSet()
	return zset.AddDocument(doc, 1)
}

// FromDocuments creates a Z-set from a slice of documents (each with multiplicity 1).
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

// String returns a string representation of the Z-set for debugging.
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
		result += fmt.Sprintf("%v×%d", doc, count)
		first = false
	}

	result += "}"
	return result
}
