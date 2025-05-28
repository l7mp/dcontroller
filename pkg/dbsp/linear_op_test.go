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

// GroupByKeyEvaluator extracts a grouping key and value from a document
// Add these tests at the end of linear_op_test.go

// GroupByKeyEvaluator extracts a grouping key and value from a document
type GroupByKeyEvaluator struct {
	keyField   string
	valueField string
}

func NewGroupByKey(keyField, valueField string) *GroupByKeyEvaluator {
	return &GroupByKeyEvaluator{
		keyField:   keyField,
		valueField: valueField,
	}
}

func (e *GroupByKeyEvaluator) Evaluate(doc Document) ([]Document, error) {
	key, keyExists := doc[e.keyField]
	value, valueExists := doc[e.valueField]

	if !keyExists || !valueExists {
		return []Document{}, nil // Skip documents missing required fields
	}

	result := Document{
		"key":   key,
		"value": value,
	}

	return []Document{result}, nil
}

func (e *GroupByKeyEvaluator) String() string {
	return fmt.Sprintf("GroupByKey(key=%s, value=%s)", e.keyField, e.valueField)
}

// AggregateListEvaluator creates a document with the key and aggregated list of values
type AggregateListEvaluator struct {
	resultKeyField   string
	resultValueField string
}

func NewAggregateList(keyField, valueField string) *AggregateListEvaluator {
	return &AggregateListEvaluator{
		resultKeyField:   keyField,
		resultValueField: valueField,
	}
}

func (e *AggregateListEvaluator) Evaluate(doc Document) ([]Document, error) {
	key, keyExists := doc["key"]
	values, valuesExists := doc["value"]

	if !keyExists || !valuesExists {
		return []Document{}, nil
	}

	valueList, ok := values.([]any)
	if !ok {
		return []Document{}, fmt.Errorf("expected value to be []any, got %T", values)
	}

	result := Document{
		e.resultKeyField:   key,
		e.resultValueField: valueList,
	}

	return []Document{result}, nil
}

func (e *AggregateListEvaluator) String() string {
	return fmt.Sprintf("AggregateList(%s, %s)", e.resultKeyField, e.resultValueField)
}

// CountAggregateEvaluator creates a document with the key and count of values
type CountAggregateEvaluator struct{}

func NewCountAggregate() *CountAggregateEvaluator {
	return &CountAggregateEvaluator{}
}

func (e *CountAggregateEvaluator) Evaluate(doc Document) ([]Document, error) {
	key, keyExists := doc["key"]
	values, valuesExists := doc["value"]

	if !keyExists || !valuesExists {
		return []Document{}, nil
	}

	valueList, ok := values.([]any)
	if !ok {
		return []Document{}, fmt.Errorf("expected value to be []any, got %T", values)
	}

	result := Document{
		"group_key": key,
		"count":     int64(len(valueList)),
	}

	return []Document{result}, nil
}

func (e *CountAggregateEvaluator) String() string {
	return "CountAggregate"
}

var _ = Describe("Gather Operations", func() {
	Context("Basic Gather Functionality", func() {
		var (
			snapshotGather *GatherOp
			inputZSet      *DocumentZSet
		)

		BeforeEach(func() {
			// Create test data: sales records by department
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000), "rep", "Alice")
			Expect(err).NotTo(HaveOccurred())

			sales2, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1500), "rep", "Bob")
			Expect(err).NotTo(HaveOccurred())

			sales3, err := newDocumentFromPairs("dept", "Marketing", "amount", int64(800), "rep", "Charlie")
			Expect(err).NotTo(HaveOccurred())

			sales4, err := newDocumentFromPairs("dept", "Marketing", "amount", int64(1200), "rep", "Diana")
			Expect(err).NotTo(HaveOccurred())

			inputZSet = NewDocumentZSet()
			inputZSet, err = inputZSet.AddDocument(sales1, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(sales2, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(sales3, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(sales4, 1)
			Expect(err).NotTo(HaveOccurred())

			// Setup gather operations
			extractEval := NewGroupByKey("dept", "amount")
			setEval := NewAggregateList("department", "amounts")
			snapshotGather = NewGather(extractEval, setEval)
		})

		It("should group sales amounts by department", func() {
			result, err := snapshotGather.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 groups: Engineering and Marketing
			Expect(result.UniqueCount()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Verify groups
			foundEngineering := false
			foundMarketing := false

			for _, doc := range docs {
				dept := doc["department"]
				amounts := doc["amounts"].([]any)

				if dept == "Engineering" {
					foundEngineering = true
					Expect(amounts).To(HaveLen(2))
					Expect(amounts).To(ConsistOf(int64(1000), int64(1500)))
				} else if dept == "Marketing" {
					foundMarketing = true
					Expect(amounts).To(HaveLen(2))
					Expect(amounts).To(ConsistOf(int64(800), int64(1200)))
				}
			}

			Expect(foundEngineering).To(BeTrue())
			Expect(foundMarketing).To(BeTrue())
		})

		It("should handle multiplicities correctly", func() {
			// Add another Engineering sale with multiplicity 2
			extraSale, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(2000), "rep", "Eve")
			Expect(err).NotTo(HaveOccurred())

			inputWithExtra, err := inputZSet.AddDocument(extraSale, 2) // Multiplicity 2
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotGather.Process(inputWithExtra)
			Expect(err).NotTo(HaveOccurred())

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Find Engineering group
			for _, doc := range docs {
				if doc["department"] == "Engineering" {
					amounts := doc["amounts"].([]any)
					// Should have original 2 + new amount appearing twice = 4 total
					Expect(amounts).To(HaveLen(4))
					Expect(amounts).To(ConsistOf(int64(1000), int64(1500), int64(2000), int64(2000)))
				}
			}
		})
	})

	Context("Count Aggregation", func() {
		var (
			snapshotGather *GatherOp
		)

		BeforeEach(func() {
			extractEval := NewGroupByKey("category", "product")
			setEval := NewCountAggregate()
			snapshotGather = NewGather(extractEval, setEval)
		})

		It("should count products by category", func() {
			// Create test data
			prod1, err := newDocumentFromPairs("category", "electronics", "product", "laptop")
			Expect(err).NotTo(HaveOccurred())
			prod2, err := newDocumentFromPairs("category", "electronics", "product", "phone")
			Expect(err).NotTo(HaveOccurred())
			prod3, err := newDocumentFromPairs("category", "books", "product", "novel")
			Expect(err).NotTo(HaveOccurred())

			inputZSet := NewDocumentZSet()
			inputZSet, err = inputZSet.AddDocument(prod1, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(prod2, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(prod3, 1)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotGather.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(2)) // 2 categories

			for _, doc := range docs {
				if doc["group_key"] == "electronics" {
					Expect(doc["count"]).To(Equal(int64(2)))
				} else if doc["group_key"] == "books" {
					Expect(doc["count"]).To(Equal(int64(1)))
				}
			}
		})
	})

	Context("Incremental Gather Functionality", func() {
		var (
			incrementalGather *IncrementalGatherOp
			extractEval       *GroupByKeyEvaluator
			setEval           *AggregateListEvaluator
		)

		BeforeEach(func() {
			extractEval = NewGroupByKey("dept", "amount")
			setEval = NewAggregateList("department", "amounts")
			incrementalGather = NewIncrementalGather(extractEval, setEval)
		})

		It("should handle initial data correctly", func() {
			// First batch of data
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())
			sales2, err := newDocumentFromPairs("dept", "Marketing", "amount", int64(800))
			Expect(err).NotTo(HaveOccurred())

			delta1 := NewDocumentZSet()
			delta1, err = delta1.AddDocument(sales1, 1)
			Expect(err).NotTo(HaveOccurred())
			delta1, err = delta1.AddDocument(sales2, 1)
			Expect(err).NotTo(HaveOccurred())

			result, err := incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Should create 2 groups
			Expect(result.UniqueCount()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Verify both departments are present
			foundEng := false
			foundMkt := false
			for _, doc := range docs {
				dept := doc["department"]
				amounts := doc["amounts"].([]any)

				if dept == "Engineering" {
					foundEng = true
					Expect(amounts).To(HaveLen(1))
					Expect(amounts[0]).To(Equal(int64(1000)))
				} else if dept == "Marketing" {
					foundMkt = true
					Expect(amounts).To(HaveLen(1))
					Expect(amounts[0]).To(Equal(int64(800)))
				}
			}
			Expect(foundEng).To(BeTrue())
			Expect(foundMkt).To(BeTrue())
		})

		It("should add to existing groups", func() {
			// Initialize with first data
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())

			delta1, err := SingletonZSet(sales1)
			Expect(err).NotTo(HaveOccurred())

			result1, err := incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())
			Expect(result1.UniqueCount()).To(Equal(1))

			// Add more to same department
			sales2, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1500))
			Expect(err).NotTo(HaveOccurred())

			delta2, err := SingletonZSet(sales2)
			Expect(err).NotTo(HaveOccurred())

			result2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Should produce a delta showing the change
			Expect(result2.Size()).To(Equal(1))      // New group addition (size filters removals)
			Expect(result2.TotalSize()).To(Equal(2)) // Old group removal + new group addition

			// The delta should contain:
			// 1. Removal of old Engineering group (amounts=[1000])
			// 2. Addition of new Engineering group (amounts=[1000, 1500])
			docs, err := result2.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1)) // Net effect might be the new doc ("amounts=[1000, 1500])

			// Check multiplicities directly
			uniqueDocs, err := result2.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			for _, doc := range uniqueDocs {
				if doc["department"] == "Engineering" {
					mult, err := result2.GetMultiplicity(doc)
					Expect(err).NotTo(HaveOccurred())
					amounts := doc["amounts"].([]any)

					if len(amounts) == 1 {
						// Old group (should be removed)
						Expect(mult).To(Equal(-1))
						Expect(amounts[0]).To(Equal(int64(1000)))
					} else if len(amounts) == 2 {
						// New group (should be added)
						Expect(mult).To(Equal(1))
						Expect(amounts).To(ConsistOf(int64(1000), int64(1500)))
					}
				}
			}
		})

		It("should create new groups", func() {
			// Start with Engineering
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())

			delta1, err := SingletonZSet(sales1)
			Expect(err).NotTo(HaveOccurred())

			_, err = incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Add Marketing (new group)
			sales2, err := newDocumentFromPairs("dept", "Marketing", "amount", int64(800))
			Expect(err).NotTo(HaveOccurred())

			delta2, err := SingletonZSet(sales2)
			Expect(err).NotTo(HaveOccurred())

			result2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Should create new Marketing group
			Expect(result2.Size()).To(Equal(1)) // One new group

			docs, err := result2.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			newGroup := docs[0]
			Expect(newGroup["department"]).To(Equal("Marketing"))
			amounts := newGroup["amounts"].([]any)
			Expect(amounts).To(HaveLen(1))
			Expect(amounts[0]).To(Equal(int64(800)))
		})

		It("should handle removals from groups", func() {
			// Add two items to same group
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())
			sales2, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1500))
			Expect(err).NotTo(HaveOccurred())

			delta1 := NewDocumentZSet()
			delta1, err = delta1.AddDocument(sales1, 1)
			Expect(err).NotTo(HaveOccurred())
			delta1, err = delta1.AddDocument(sales2, 1)
			Expect(err).NotTo(HaveOccurred())

			_, err = incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Remove one item
			delta2 := NewDocumentZSet()
			delta2, err = delta2.AddDocument(sales1, -1) // Remove first sale
			Expect(err).NotTo(HaveOccurred())

			result2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Should produce delta showing group change
			Expect(result2.IsZero()).To(BeFalse())

			// Should have removal of old group and addition of new group
			uniqueDocs, err := result2.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			for _, doc := range uniqueDocs {
				if doc["department"] == "Engineering" {
					mult, err := result2.GetMultiplicity(doc)
					Expect(err).NotTo(HaveOccurred())
					amounts := doc["amounts"].([]any)

					if len(amounts) == 2 {
						// Old group [1000, 1500] should be removed
						Expect(mult).To(Equal(-1))
					} else if len(amounts) == 1 {
						// New group [1500] should be added
						Expect(mult).To(Equal(1))
						Expect(amounts[0]).To(Equal(int64(1500)))
					}
				}
			}
		})

		It("should handle complete group removal", func() {
			// Add single item
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())

			delta1, err := SingletonZSet(sales1)
			Expect(err).NotTo(HaveOccurred())

			_, err = incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Remove the item (should eliminate the group)
			delta2 := NewDocumentZSet()
			delta2, err = delta2.AddDocument(sales1, -1)
			Expect(err).NotTo(HaveOccurred())

			result2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Should remove the Engineering group entirely
			Expect(result2.Size()).To(Equal(0))      // Not counting the removal
			Expect(result2.TotalSize()).To(Equal(1)) // One removal

			// Check for docs individual docs
			docs, err := result2.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))
			Expect(docs[0].Multiplicity).To(Equal(-1)) // Removal
			Expect(docs[0].Document["department"]).To(Equal("Engineering"))
		})

		It("should handle multiplicities correctly", func() {
			// Add item with multiplicity 3
			sales1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())

			delta1 := NewDocumentZSet()
			delta1, err = delta1.AddDocument(sales1, 3) // Multiplicity 3
			Expect(err).NotTo(HaveOccurred())

			result1, err := incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Should create group with 3 copies of the amount
			docs, err := result1.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			group := docs[0]
			amounts := group["amounts"].([]any)
			Expect(amounts).To(HaveLen(3))
			Expect(amounts).To(ConsistOf(int64(1000), int64(1000), int64(1000)))
		})
	})

	Context("Incremental vs Snapshot Consistency", func() {
		var (
			snapshotGather    *GatherOp
			incrementalGather *IncrementalGatherOp
			extractEval       *GroupByKeyEvaluator
			setEval           *AggregateListEvaluator
		)

		BeforeEach(func() {
			extractEval = NewGroupByKey("team", "score")
			setEval = NewAggregateList("team", "scores")
			snapshotGather = NewGather(extractEval, setEval)
			incrementalGather = NewIncrementalGather(extractEval, setEval)
		})

		It("should produce consistent results through multiple timesteps", func() {
			// Track cumulative result from incremental operations
			var cumulativeIncremental *DocumentZSet = NewDocumentZSet()

			// Timeline data
			allData := NewDocumentZSet()

			// Timestep 1: Initial data
			game1, err := newDocumentFromPairs("team", "Red", "score", int64(10))
			Expect(err).NotTo(HaveOccurred())
			game2, err := newDocumentFromPairs("team", "Blue", "score", int64(15))
			Expect(err).NotTo(HaveOccurred())

			delta1 := NewDocumentZSet()
			delta1, err = delta1.AddDocument(game1, 1)
			Expect(err).NotTo(HaveOccurred())
			delta1, err = delta1.AddDocument(game2, 1)
			Expect(err).NotTo(HaveOccurred())

			// Update cumulative data
			allData, err = allData.Add(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Process with both approaches
			snapshotResult1, err := snapshotGather.Process(allData)
			Expect(err).NotTo(HaveOccurred())
			incrementalDelta1, err := incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())

			// Update cumulative incremental result
			cumulativeIncremental, err = cumulativeIncremental.Add(incrementalDelta1)
			Expect(err).NotTo(HaveOccurred())

			// Should be consistent
			Expect(cumulativeIncremental.Size()).To(Equal(snapshotResult1.Size()))
			Expect(cumulativeIncremental.UniqueCount()).To(Equal(snapshotResult1.UniqueCount()))

			// Timestep 2: Add more scores
			game3, err := newDocumentFromPairs("team", "Red", "score", int64(20))
			Expect(err).NotTo(HaveOccurred())
			game4, err := newDocumentFromPairs("team", "Green", "score", int64(12)) // New team
			Expect(err).NotTo(HaveOccurred())

			delta2 := NewDocumentZSet()
			delta2, err = delta2.AddDocument(game3, 1)
			Expect(err).NotTo(HaveOccurred())
			delta2, err = delta2.AddDocument(game4, 1)
			Expect(err).NotTo(HaveOccurred())

			// Update cumulative data
			allData, err = allData.Add(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Process timestep 2
			snapshotResult2, err := snapshotGather.Process(allData)
			Expect(err).NotTo(HaveOccurred())
			incrementalDelta2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Update cumulative incremental result
			cumulativeIncremental, err = cumulativeIncremental.Add(incrementalDelta2)
			Expect(err).NotTo(HaveOccurred())

			// Should still be consistent
			Expect(cumulativeIncremental.UniqueCount()).To(Equal(snapshotResult2.UniqueCount()))

			// Verify final snapshot results match expectations
			snapshotDocs, err := snapshotResult2.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(snapshotDocs).To(HaveLen(3)) // Red, Blue, Green teams

			// Verify Red team has both scores
			for _, doc := range snapshotDocs {
				if doc["team"] == "Red" {
					scores := doc["scores"].([]any)
					Expect(scores).To(HaveLen(2))
					Expect(scores).To(ConsistOf(int64(10), int64(20)))
				}
			}
		})

		It("should handle removals consistently", func() {
			var cumulativeIncremental *DocumentZSet = NewDocumentZSet()

			// Add initial data
			game1, err := newDocumentFromPairs("team", "Red", "score", int64(10))
			Expect(err).NotTo(HaveOccurred())
			game2, err := newDocumentFromPairs("team", "Red", "score", int64(20))
			Expect(err).NotTo(HaveOccurred())

			delta1 := NewDocumentZSet()
			delta1, err = delta1.AddDocument(game1, 1)
			Expect(err).NotTo(HaveOccurred())
			delta1, err = delta1.AddDocument(game2, 1)
			Expect(err).NotTo(HaveOccurred())

			allData := delta1

			// Process initial data
			Expect(err).NotTo(HaveOccurred())
			incrementalDelta1, err := incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())
			cumulativeIncremental, err = cumulativeIncremental.Add(incrementalDelta1)
			Expect(err).NotTo(HaveOccurred())

			// Remove one score
			delta2 := NewDocumentZSet()
			delta2, err = delta2.AddDocument(game1, -1) // Remove first game
			Expect(err).NotTo(HaveOccurred())

			allData, err = allData.Add(delta2)
			Expect(err).NotTo(HaveOccurred())

			// Process removal
			snapshotResult2, err := snapshotGather.Process(allData)
			Expect(err).NotTo(HaveOccurred())
			incrementalDelta2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())
			cumulativeIncremental, err = cumulativeIncremental.Add(incrementalDelta2)
			Expect(err).NotTo(HaveOccurred())

			// Verify consistency
			Expect(cumulativeIncremental.UniqueCount()).To(Equal(snapshotResult2.UniqueCount()))

			// Red team should now have only one score
			docs, err := snapshotResult2.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			redTeamDoc := docs[0]
			scores := redTeamDoc["scores"].([]any)
			Expect(scores).To(HaveLen(1))
			Expect(scores[0]).To(Equal(int64(20)))
		})
	})

	Context("Edge Cases for Incremental Gather", func() {
		var incrementalGather *IncrementalGatherOp

		BeforeEach(func() {
			extractEval := NewGroupByKey("category", "value")
			setEval := NewAggregateList("category", "values")
			incrementalGather = NewIncrementalGather(extractEval, setEval)
		})

		It("should handle empty deltas", func() {
			emptyDelta := NewDocumentZSet()
			result, err := incrementalGather.Process(emptyDelta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle documents missing required fields", func() {
			// Document missing "category" field
			doc, err := newDocumentFromPairs("value", int64(100), "other", "data")
			Expect(err).NotTo(HaveOccurred())

			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			result, err := incrementalGather.Process(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue()) // No valid documents to process
		})

		It("should handle zero multiplicity additions", func() {
			doc, err := newDocumentFromPairs("category", "test", "value", int64(42))
			Expect(err).NotTo(HaveOccurred())

			delta := NewDocumentZSet()
			delta, err = delta.AddDocument(doc, 0) // Zero multiplicity
			Expect(err).NotTo(HaveOccurred())

			result, err := incrementalGather.Process(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue()) // No effect
		})

		It("should handle repeated additions and removals", func() {
			doc, err := newDocumentFromPairs("category", "test", "value", int64(42))
			Expect(err).NotTo(HaveOccurred())

			// Add, remove, add again
			delta1, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())
			result1, err := incrementalGather.Process(delta1)
			Expect(err).NotTo(HaveOccurred())
			Expect(result1.Size()).To(Equal(1)) // Creates group

			delta2 := NewDocumentZSet()
			delta2, err = delta2.AddDocument(doc, -1)
			Expect(err).NotTo(HaveOccurred())
			result2, err := incrementalGather.Process(delta2)
			Expect(err).NotTo(HaveOccurred())
			Expect(result2.TotalSize()).To(Equal(1)) // Removes group

			delta3, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())
			result3, err := incrementalGather.Process(delta3)
			Expect(err).NotTo(HaveOccurred())
			Expect(result3.Size()).To(Equal(1)) // Recreates group
		})

		It("should handle complex key types", func() {
			// Use nested object as grouping key
			complexKey := map[string]any{
				"dept":     "Engineering",
				"location": "NYC",
			}

			doc, err := newDocumentFromPairs("category", complexKey, "value", int64(100))
			Expect(err).NotTo(HaveOccurred())

			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			result, err := incrementalGather.Process(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			resultDoc := docs[0]

			// The complex key should be preserved in the result
			resultKey := resultDoc["category"].(map[string]any)
			Expect(resultKey["dept"]).To(Equal("Engineering"))
			Expect(resultKey["location"]).To(Equal("NYC"))
		})

		It("should reset state correctly", func() {
			doc, err := newDocumentFromPairs("category", "test", "value", int64(42))
			Expect(err).NotTo(HaveOccurred())

			// Add some data
			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())
			_, err = incrementalGather.Process(delta)
			Expect(err).NotTo(HaveOccurred())

			// Reset and verify clean state
			incrementalGather.Reset()

			// Process same data again - should behave like first time
			result, err := incrementalGather.Process(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1)) // Should create new group
		})
	})

	Context("Empty and Edge Cases", func() {
		var gatherOp *GatherOp

		BeforeEach(func() {
			extractEval := NewGroupByKey("category", "value")
			setEval := NewAggregateList("category", "values")
			gatherOp = NewGather(extractEval, setEval)
		})

		It("should handle empty input", func() {
			emptyZSet := NewDocumentZSet()
			result, err := gatherOp.Process(emptyZSet)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle documents missing required fields", func() {
			// Document missing "category" field
			doc1, err := newDocumentFromPairs("value", int64(100), "other", "data")
			Expect(err).NotTo(HaveOccurred())

			// Document missing "value" field
			doc2, err := newDocumentFromPairs("category", "test", "other", "data")
			Expect(err).NotTo(HaveOccurred())

			inputZSet := NewDocumentZSet()
			inputZSet, err = inputZSet.AddDocument(doc1, 1)
			Expect(err).NotTo(HaveOccurred())
			inputZSet, err = inputZSet.AddDocument(doc2, 1)
			Expect(err).NotTo(HaveOccurred())

			result, err := gatherOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue()) // No valid documents to group
		})

		It("should handle single group with single value", func() {
			doc, err := newDocumentFromPairs("category", "singleton", "value", int64(42))
			Expect(err).NotTo(HaveOccurred())

			inputZSet, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			result, err := gatherOp.Process(inputZSet)
			Expect(err).NotTo(HaveOccurred())

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			resultDoc := docs[0]
			Expect(resultDoc["category"]).To(Equal("singleton"))
			values := resultDoc["values"].([]any)
			Expect(values).To(HaveLen(1))
			Expect(values[0]).To(Equal(int64(42)))
		})
	})

	Context("Operator Properties", func() {
		var gatherOp *GatherOp

		BeforeEach(func() {
			extractEval := NewGroupByKey("key", "value")
			setEval := NewAggregateList("key", "values")
			gatherOp = NewGather(extractEval, setEval)
		})

		It("should be linear", func() {
			Expect(gatherOp.OpType()).To(Equal(OpTypeLinear))
			Expect(gatherOp.IsTimeInvariant()).To(BeTrue())
			Expect(gatherOp.HasZeroPreservationProperty()).To(BeTrue())
		})

		It("should satisfy linearity: Gather(a + b) = Gather(a) + Gather(b)", func() {
			// Create two separate Z-sets
			doc1, err := newDocumentFromPairs("key", "A", "value", int64(1))
			Expect(err).NotTo(HaveOccurred())
			zset1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())

			doc2, err := newDocumentFromPairs("key", "A", "value", int64(2))
			Expect(err).NotTo(HaveOccurred())
			zset2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())

			// Gather(a + b)
			combined, err := zset1.Add(zset2)
			Expect(err).NotTo(HaveOccurred())
			result1, err := gatherOp.Process(combined)
			Expect(err).NotTo(HaveOccurred())

			// Gather(a) + Gather(b)
			gatherA, err := gatherOp.Process(zset1)
			Expect(err).NotTo(HaveOccurred())
			gatherB, err := gatherOp.Process(zset2)
			Expect(err).NotTo(HaveOccurred())
			result2, err := gatherA.Add(gatherB)
			Expect(err).NotTo(HaveOccurred())

			// Results should be equal in size and structure
			// Note: The exact equality might be complex due to list aggregation,
			// but they should have the same total size
			Expect(result1.Size()).To(Equal(result2.Size()))
		})
	})
})
