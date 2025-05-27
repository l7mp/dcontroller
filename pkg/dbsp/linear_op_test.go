package dbsp

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Simple projection evaluator - extracts specific fields
type FieldProjectionEvaluator struct {
	fields []string
}

func NewFieldProjection(fields ...string) *FieldProjectionEvaluator {
	return &FieldProjectionEvaluator{fields: fields}
}

func (e *FieldProjectionEvaluator) Evaluate(doc Document) ([]Document, error) {
	result := Document{}
	for _, field := range e.fields {
		if value, exists := doc[field]; exists {
			result[field] = value
		}
	}
	return []Document{result}, nil
}

func (e *FieldProjectionEvaluator) String() string {
	return "FieldProjection(" + fmt.Sprintf("%v", e.fields) + ")"
}

// Simple selection evaluator - filters based on field conditions
type FieldFilterEvaluator struct {
	field string
	value any
}

func NewFieldFilter(field string, value any) *FieldFilterEvaluator {
	return &FieldFilterEvaluator{field: field, value: value}
}

func (e *FieldFilterEvaluator) Evaluate(doc Document) ([]Document, error) {
	if docValue, exists := doc[e.field]; exists {
		// Use JSON comparison for consistency with your document equality
		docKey, err := computeJSONAny(docValue)
		if err != nil {
			return []Document{}, nil
		}
		expectedKey, err := computeJSONAny(e.value)
		if err != nil {
			return []Document{}, nil
		}

		if docKey == expectedKey {
			return []Document{doc}, nil
		}
	}
	return []Document{}, nil
}

func (e *FieldFilterEvaluator) String() string {
	return fmt.Sprintf("FieldFilter(%s = %v)", e.field, e.value)
}

// Range filter for numeric values
type RangeFilterEvaluator struct {
	field string
	min   int64
	max   int64
}

func NewRangeFilter(field string, min, max int64) *RangeFilterEvaluator {
	return &RangeFilterEvaluator{field: field, min: min, max: max}
}

func (e *RangeFilterEvaluator) Evaluate(doc Document) ([]Document, error) {
	if docValue, exists := doc[e.field]; exists {
		if intVal, ok := docValue.(int64); ok && intVal >= e.min && intVal <= e.max {
			return []Document{doc}, nil
		}
	}
	return []Document{}, nil
}

func (e *RangeFilterEvaluator) String() string {
	return fmt.Sprintf("RangeFilter(%s ∈ [%d, %d])", e.field, e.min, e.max)
}

var _ = Describe("Linear Operators", func() {
	Context("Projection Operator", func() {
		var projectionOp *ProjectionOp
		var inputZSet *DocumentZSet

		BeforeEach(func() {
			// Create test documents with various structures
			doc1, err := newDocumentFromPairs(
				"name", "Alice",
				"age", int64(30),
				"city", "NYC",
				"metadata", map[string]any{
					"created": "2024-01-01",
					"tags":    []any{"user", "active"},
				},
			)
			Expect(err).NotTo(HaveOccurred())

			doc2, err := newDocumentFromPairs(
				"name", "Bob",
				"age", int64(25),
				"department", "Engineering",
			)
			Expect(err).NotTo(HaveOccurred())

			inputZSet = NewDocumentZSet()
			inputZSet, err = inputZSet.AddDocument(doc1, 2) // Multiplicity 2
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(doc2, 1)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should project single field", func() {
			projectionOp = NewProjection(NewFieldProjection("name"))

			result, err := projectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should have 3 documents total (2 Alice + 1 Bob)
			Expect(result.Size()).To(Equal(3))
			Expect(result.UniqueCount()).To(Equal(2)) // Two unique names

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Verify projected documents only have name field
			for _, doc := range docs {
				Expect(doc).To(HaveKey("name"))
				Expect(doc).NotTo(HaveKey("age"))
				Expect(doc).NotTo(HaveKey("city"))
			}
		})

		It("should project multiple fields", func() {
			projectionOp = NewProjection(NewFieldProjection("name", "age"))

			result, err := projectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			for _, doc := range docs {
				Expect(doc).To(HaveKey("name"))
				Expect(doc).To(HaveKey("age"))
				Expect(doc).NotTo(HaveKey("city"))
				Expect(doc).NotTo(HaveKey("department"))
			}
		})

		It("should handle missing fields gracefully", func() {
			projectionOp = NewProjection(NewFieldProjection("name", "nonexistent"))

			result, err := projectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			for _, doc := range docs {
				Expect(doc).To(HaveKey("name"))
				Expect(doc).NotTo(HaveKey("nonexistent"))
			}
		})

		It("should preserve multiplicities", func() {
			projectionOp = NewProjection(NewFieldProjection("age"))

			result, err := projectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Check multiplicity for age 30 (should be 2)
			ageDoc := Document{"age": int64(30)}
			mult, err := result.GetMultiplicity(ageDoc)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(2))
		})

		It("should be linear (OpType)", func() {
			projectionOp = NewProjection(NewFieldProjection("name"))
			Expect(projectionOp.OpType()).To(Equal(OpTypeLinear))
			Expect(projectionOp.IsTimeInvariant()).To(BeTrue())
			Expect(projectionOp.HasZeroPreservationProperty()).To(BeTrue())
		})
	})

	Context("Selection Operator", func() {
		var selectionOp *SelectionOp
		var inputZSet *DocumentZSet

		BeforeEach(func() {
			doc1, err := newDocumentFromPairs("name", "Alice", "age", int64(30), "active", true)
			Expect(err).NotTo(HaveOccurred())

			doc2, err := newDocumentFromPairs("name", "Bob", "age", int64(25), "active", false)
			Expect(err).NotTo(HaveOccurred())

			doc3, err := newDocumentFromPairs("name", "Charlie", "age", int64(35), "active", true)
			Expect(err).NotTo(HaveOccurred())

			inputZSet = NewDocumentZSet()
			inputZSet, err = inputZSet.AddDocument(doc1, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(doc2, 2) // Higher multiplicity
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(doc3, 1)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should filter by exact field match", func() {
			selectionOp = NewSelection("active_filter", NewFieldFilter("active", true))

			result, err := selectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should have Alice (mult 1) + Charlie (mult 1) = 2 docs
			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			names := make([]string, 0)
			for _, doc := range docs {
				names = append(names, doc["name"].(string))
			}
			Expect(names).To(ConsistOf("Alice", "Charlie"))
		})

		It("should filter by numeric range", func() {
			selectionOp = NewSelection("age_filter", NewRangeFilter("age", 25, 30))

			result, err := selectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should have Alice (mult 1) + Bob (mult 2) = 3 docs
			Expect(result.Size()).To(Equal(3))
			Expect(result.UniqueCount()).To(Equal(2))
		})

		It("should preserve multiplicities during filtering", func() {
			selectionOp = NewSelection("name_filter", NewFieldFilter("name", "Bob"))

			result, err := selectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Bob had multiplicity 2 in input
			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(1))

			bobDoc, err := newDocumentFromPairs("name", "Bob", "age", int64(25), "active", false)
			Expect(err).NotTo(HaveOccurred())
			mult, err := result.GetMultiplicity(bobDoc)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(2))
		})

		It("should return empty for no matches", func() {
			selectionOp = NewSelection("nonexistent_filter", NewFieldFilter("name", "Nonexistent"))

			result, err := selectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle missing fields", func() {
			selectionOp = NewSelection("missing_field_filter", NewFieldFilter("nonexistent", "value"))

			result, err := selectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should be linear (OpType)", func() {
			selectionOp = NewSelection("test", NewFieldFilter("name", "Alice"))
			Expect(selectionOp.OpType()).To(Equal(OpTypeLinear))
			Expect(selectionOp.IsTimeInvariant()).To(BeTrue())
			Expect(selectionOp.HasZeroPreservationProperty()).To(BeTrue())
		})
	})

	Context("Linear Operator Composition", func() {
		It("should compose selection and projection", func() {
			// Create input with diverse documents
			doc1, err := newDocumentFromPairs("name", "Alice", "age", int64(30), "active", true, "city", "NYC")
			Expect(err).NotTo(HaveOccurred())
			doc2, err := newDocumentFromPairs("name", "Bob", "age", int64(25), "active", false, "city", "LA")
			Expect(err).NotTo(HaveOccurred())
			doc3, err := newDocumentFromPairs("name", "Charlie", "age", int64(35), "active", true, "city", "SF")
			Expect(err).NotTo(HaveOccurred())

			inputZSet := NewDocumentZSet()
			inputZSet, err = inputZSet.AddDocument(doc1, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(doc2, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(doc3, 1)
			Expect(err).NotTo(HaveOccurred())

			// First filter active users
			selectionOp := NewSelection("active_filter", NewFieldFilter("active", true))
			filtered, err := selectionOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Then project name and city
			projectionOp := NewProjection(NewFieldProjection("name", "city"))
			result, err := projectionOp.Process(filtered)
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 documents (Alice and Charlie)
			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			for _, doc := range docs {
				Expect(doc).To(HaveKey("name"))
				Expect(doc).To(HaveKey("city"))
				Expect(doc).NotTo(HaveKey("age"))
				Expect(doc).NotTo(HaveKey("active"))
			}
		})
	})

	Context("Linear Operator Properties", func() {
		var doc1, doc2 Document
		var zset1, zset2 *DocumentZSet

		BeforeEach(func() {
			var err error
			doc1, err = newDocumentFromPairs("x", int64(1), "y", "a")
			Expect(err).NotTo(HaveOccurred())
			doc2, err = newDocumentFromPairs("x", int64(2), "y", "b")
			Expect(err).NotTo(HaveOccurred())

			zset1, err = SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			zset2, err = SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should satisfy linearity: Op(a + b) = Op(a) + Op(b)", func() {
			projOp := NewProjection(NewFieldProjection("x"))

			// Op(a + b)
			combined, err := zset1.Add(zset2)
			Expect(err).NotTo(HaveOccurred())
			result1, err := projOp.Process(combined)
			Expect(err).NotTo(HaveOccurred())

			// Op(a) + Op(b)
			projA, err := projOp.Process(zset1)
			Expect(err).NotTo(HaveOccurred())
			projB, err := projOp.Process(zset2)
			Expect(err).NotTo(HaveOccurred())
			result2, err := projA.Add(projB)
			Expect(err).NotTo(HaveOccurred())

			// Results should be equal
			Expect(result1.Size()).To(Equal(result2.Size()))
			Expect(result1.UniqueCount()).To(Equal(result2.UniqueCount()))
		})

		It("should preserve zero: Op(∅) = ∅", func() {
			projOp := NewProjection(NewFieldProjection("x"))
			emptyZSet := NewDocumentZSet()

			result, err := projOp.Process(emptyZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle negative multiplicities correctly", func() {
			// Create Z-set with negative multiplicity
			negativeZSet := NewDocumentZSet()
			negativeZSet, err := negativeZSet.AddDocument(doc1, -1)
			Expect(err).NotTo(HaveOccurred())

			projOp := NewProjection(NewFieldProjection("x"))
			result, err := projOp.Process(negativeZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have negative multiplicity too
			expectedProjected := Document{"x": int64(1)}
			mult, err := result.GetMultiplicity(expectedProjected)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-1))
		})
	})
})
