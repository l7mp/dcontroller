package dbsp

import (
	"encoding/json"
	"fmt"
)

// Document represents an unstructured document as map[string]any
// Can contain embedded maps, slices, and primitives (int64, float64, string, bool)
type Document = map[string]any

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

// DeepCopyAny creates a deep copy of a document or any nested structure
func DeepCopyAny(val any) any {
	switch v := val.(type) {
	case map[string]any:
		result := make(map[string]any)
		for k, subVal := range v {
			result[k] = DeepCopyAny(subVal)
		}
		return result

	case []any:
		result := make([]any, len(v))
		for i, subVal := range v {
			result[i] = DeepCopyAny(subVal)
		}
		return result

	case int64, float64, string, bool, nil:
		// Primitives can be copied directly
		return v

	default:
		// For unknown types, try to copy directly
		return v
	}
}

// DeepCopyDocument creates a deep copy of a document.
func DeepCopyDocument(val any) Document {
	c := DeepCopyAny(val)
	return c.(Document)
}

// computeJSONAny creates a deterministic JSON representation for an arbitrary any value
func computeJSONAny(doc any) (string, error) {
	bytes, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("failed to marshal value to JSON: %w", err)
	}

	return string(bytes), nil
}
