package dbsp

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// For GatherOp tests - replace the old evaluators
func createGatherEvaluators(keyField, valueField, resultKeyField, resultValueField string) (*FieldExtractor, *FieldExtractor, *ListAggregateTransformer) {
	keyExtractor := NewFieldExtractor(keyField)
	valueExtractor := NewFieldExtractor(valueField)
	aggregator := NewListAggregateTransformer(resultKeyField, resultValueField)
	return keyExtractor, valueExtractor, aggregator
}

func createCountGatherEvaluators(keyField, valueField string) (*FieldExtractor, *FieldExtractor, *CountAggregateTransformer) {
	keyExtractor := NewFieldExtractor(keyField)
	valueExtractor := NewFieldExtractor(valueField)
	aggregator := NewCountAggregateTransformer()
	return keyExtractor, valueExtractor, aggregator
}

// Gather tests
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
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			snapshotGather = NewGather(keyExt, valueExt, aggregator)
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

		It("should handle deletions correctly", func() {
			input := NewDocumentZSet()

			// Add documents with positive multiplicities
			doc1 := Document{"dept": "A", "amount": int64(10)}
			doc2 := Document{"dept": "A", "amount": int64(20)}
			doc3 := Document{"dept": "B", "amount": int64(30)}

			Expect(input.AddDocumentMutate(doc1, 2)).To(Succeed()) // amount 10 appears twice
			Expect(input.AddDocumentMutate(doc2, 1)).To(Succeed()) // amount 20 appears once
			Expect(input.AddDocumentMutate(doc3, 1)).To(Succeed()) // amount 30 appears once

			// Add negative multiplicities (deletions)
			Expect(input.AddDocumentMutate(doc1, -1)).To(Succeed()) // remove one occurrence of amount 10
			Expect(input.AddDocumentMutate(doc2, -1)).To(Succeed()) // remove the occurrence of amount 20

			result, err := snapshotGather.Process(input)
			Expect(err).NotTo(HaveOccurred())

			entries, err := result.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(entries).To(HaveLen(2)) // Groups A and B

			// Convert to map for easier verification
			resultMap := make(map[string]Document)
			for _, entry := range entries {
				Expect(entry.Document).To(HaveKey("department"))
				group := entry.Document["department"].(string)
				resultMap[group] = entry.Document
			}

			// Verify group A: should have one value remaining (2-1=1)
			Expect(resultMap).To(HaveKey("A"))
			Expect(resultMap["A"]).To(HaveKey("amounts"))
			Expect(resultMap["A"]["amounts"]).To(HaveLen(1)) // Only one value remaining
			Expect(resultMap["A"]["amounts"].([]any)[0]).To(Equal(int64(10)))

			// Verify group B: unchanged
			Expect(resultMap).To(HaveKey("B"))
			Expect(resultMap["B"]).To(HaveKey("amounts"))
			Expect(resultMap["B"]["amounts"]).To(HaveLen(1))                  // Only one value remaining
			Expect(resultMap["B"]["amounts"].([]any)[0]).To(Equal(int64(30))) // One value
		})

		It("should remove empty groups from result", func() {
			input := NewDocumentZSet()

			doc1 := Document{"dept": "A", "amount": int64(10)}
			doc2 := Document{"dept": "A", "amount": int64(20)}

			// Add then completely remove
			Expect(input.AddDocumentMutate(doc1, 1)).To(Succeed())
			Expect(input.AddDocumentMutate(doc2, 1)).To(Succeed())
			Expect(input.AddDocumentMutate(doc1, -1)).To(Succeed()) // Remove amount 10
			Expect(input.AddDocumentMutate(doc2, -1)).To(Succeed()) // Remove amount 20

			result, err := snapshotGather.Process(input)
			Expect(err).NotTo(HaveOccurred())

			// Group A should not appear in result (no values left)
			entries, err := result.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(entries).To(BeEmpty()) // No groups should remain
		})

		It("should gracefully handle removal of values that don't exist", func() {
			input := NewDocumentZSet()

			doc1 := Document{"dept": "A", "amount": int64(10)}
			doc2 := Document{"dept": "A", "amount": int64(99)} // Different value

			// Add one value, try to remove a different one
			Expect(input.AddDocumentMutate(doc1, 1)).To(Succeed())
			Expect(input.AddDocumentMutate(doc2, -1)).To(Succeed()) // Try to remove non-existent value

			result, err := snapshotGather.Process(input)
			Expect(err).NotTo(HaveOccurred())

			entries, err := result.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(entries).To(HaveLen(1)) // Group A should still exist

			doc := entries[0].Document
			Expect(doc["department"]).To(Equal("A"))
			Expect(doc["amounts"]).To(HaveLen(1))                  // Original value still there
			Expect(doc["amounts"].([]any)[0]).To(Equal(int64(10))) // Original value still there
		})
	})

	Context("Count Aggregation", func() {
		var (
			snapshotGather *GatherOp
		)

		BeforeEach(func() {
			keyExt, valueExt, countAgg := createCountGatherEvaluators("category", "product")
			snapshotGather = NewGather(keyExt, valueExt, countAgg)
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
		)

		BeforeEach(func() {
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			incrementalGather = NewIncrementalGather(keyExt, valueExt, aggregator)
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
		)

		BeforeEach(func() {
			keyExt, valueExt, aggregator := createGatherEvaluators("team", "score", "team", "scores")
			snapshotGather = NewGather(keyExt, valueExt, aggregator)
			incrementalGather = NewIncrementalGather(keyExt, valueExt, aggregator)
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
			keyExt, valueExt, aggregator := createGatherEvaluators("category", "value", "category", "values")
			incrementalGather = NewIncrementalGather(keyExt, valueExt, aggregator)
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
			keyExt, valueExt, aggregator := createGatherEvaluators("category", "value", "category", "values")
			gatherOp = NewGather(keyExt, valueExt, aggregator)
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
			keyExt, valueExt, aggregator := createGatherEvaluators("key", "value", "key", "values")
			gatherOp = NewGather(keyExt, valueExt, aggregator)
		})

		It("should be be nonlinear", func() {
			Expect(gatherOp.OpType()).To(Equal(OpTypeNonLinear))
			Expect(gatherOp.IsTimeInvariant()).To(BeTrue())
			Expect(gatherOp.HasZeroPreservationProperty()).To(BeTrue())
		})

		It("should satisfy input associativity: order of processing doesn't matter", func() {
			// Create documents from two different groups
			docA1, err := newDocumentFromPairs("key", "A", "value", int64(1))
			Expect(err).NotTo(HaveOccurred())
			docA2, err := newDocumentFromPairs("key", "A", "value", int64(2))
			Expect(err).NotTo(HaveOccurred())
			docB1, err := newDocumentFromPairs("key", "B", "value", int64(10))
			Expect(err).NotTo(HaveOccurred())

			// Process in different orders
			order1 := NewDocumentZSet()
			order1, err = order1.AddDocument(docA1, 1)
			Expect(err).NotTo(HaveOccurred())
			order1, err = order1.AddDocument(docB1, 1)
			Expect(err).NotTo(HaveOccurred())
			order1, err = order1.AddDocument(docA2, 1)
			Expect(err).NotTo(HaveOccurred())

			order2 := NewDocumentZSet()
			order2, err = order2.AddDocument(docA2, 1)
			Expect(err).NotTo(HaveOccurred())
			order2, err = order2.AddDocument(docA1, 1)
			Expect(err).NotTo(HaveOccurred())
			order2, err = order2.AddDocument(docB1, 1)
			Expect(err).NotTo(HaveOccurred())

			result1, err := gatherOp.Process(order1)
			Expect(err).NotTo(HaveOccurred())
			result2, err := gatherOp.Process(order2)
			Expect(err).NotTo(HaveOccurred())

			// Results should be equivalent (same groups, same contents)
			Expect(result1.Size()).To(Equal(result2.Size()))
			Expect(result1.UniqueCount()).To(Equal(result2.UniqueCount()))

			docs1, err := result1.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			docs2, err := result2.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 groups in both cases
			Expect(docs1).To(HaveLen(2))
			Expect(docs2).To(HaveLen(2))

			// Verify groups have same content (regardless of order)
			for _, doc := range docs1 {
				key := doc["key"]
				values := doc["values"].([]any)

				if key == "A" {
					Expect(values).To(ConsistOf(int64(1), int64(2)))
				} else if key == "B" {
					Expect(values).To(ConsistOf(int64(10)))
				}
			}
		})
	})
})

type ReplaceFieldTransformer struct {
	field string
}

func (t *ReplaceFieldTransformer) Transform(doc Document, value any) (Document, error) {
	result := make(Document)
	for k, v := range doc {
		if k != "items" { // Remove the original array field
			result[k] = v
		}
	}
	result[t.field] = value // Add the single item
	return result, nil
}

func (t *ReplaceFieldTransformer) String() string {
	return fmt.Sprintf("replace_%s", t.field)
}

type ArrayAggregateTransformer struct {
	keyField   string
	valueField string
}

func (t *ArrayAggregateTransformer) Transform(doc Document, value any) (Document, error) {
	aggregateInput := value.(*AggregateInput)

	result := Document{
		t.keyField:   aggregateInput.Key,
		t.valueField: aggregateInput.Values, // This is the aggregated array
	}

	return result, nil
}

func (t *ArrayAggregateTransformer) String() string {
	return fmt.Sprintf("array_agg_%s_%s", t.keyField, t.valueField)
}

var _ = Describe("ComplexChainEval", func() {
	// Does gather produce one delete or multiple deletes?
	// Hypothesis: gather should produce both:
	// - {id: "X", items: [1]} -> -1  (from processing item: 1 with mult -1)
	// - {id: "X", items: [2]} -> -1  (from processing item: 2 with mult -1)
	//
	// We might also get:
	// - {id: "X", items: [1,2]} -> -1 (aggregated result)

	It("should process an unwind followed by a gather - snapshot implementation", func() {
		// Setup: Original document with array [1,2]
		originalDoc := Document{
			"id":    "X",
			"items": []any{1, 2},
		}

		// Create input delta: add the original document
		inputDelta := NewDocumentZSet()
		err := inputDelta.AddDocumentMutate(originalDoc, 1)
		Expect(err).NotTo(HaveOccurred())

		// Step 1: Unwind operation
		unwindOp := NewUnwind(
			&FieldExtractor{fieldName: "items"},     // Extract items array
			&ReplaceFieldTransformer{field: "item"}, // Replace with single item
		)

		unwoundResult, err := unwindOp.Process(inputDelta)
		Expect(err).NotTo(HaveOccurred())

		// unwind blows up the list into 2 docs
		Expect(unwoundResult.TotalSize()).To(Equal(2))
		docs, err := unwoundResult.List()
		Expect(err).NotTo(HaveOccurred())
		for _, doc := range docs {
			Expect(doc.Document).To(HaveKey("id"))
			Expect(doc.Document).To(HaveKey("item"))
			Expect([]any{1, 2}).To(ContainElement(doc.Document["item"]))
			Expect(doc.Multiplicity).To(Equal(1))
		}

		// Step 2: Gather operation
		gatherOp := NewGather(
			&FieldExtractor{fieldName: "id"},                                // Group by id
			&FieldExtractor{fieldName: "item"},                              // Extract item values
			&ArrayAggregateTransformer{keyField: "id", valueField: "items"}, // Rebuild array
		)

		gatheredResult, err := gatherOp.Process(unwoundResult)
		Expect(err).NotTo(HaveOccurred())

		Expect(gatheredResult.TotalSize()).To(Equal(1))
		docs, err = gatheredResult.List()
		Expect(err).NotTo(HaveOccurred())
		for _, doc := range docs {
			Expect(doc.Document).To(HaveKey("id"))
			Expect(doc.Document).To(HaveKey("items"))
			Expect([]any{[]any{2, 1}, []any{1, 2}}).To(ContainElement(doc.Document["items"]))
			Expect(doc.Multiplicity).To(Equal(1))
		}

		// Create another input delta: delete the original document
		err = inputDelta.AddDocumentMutate(originalDoc, -1)
		Expect(err).NotTo(HaveOccurred())

		unwoundResult, err = unwindOp.Process(inputDelta)
		Expect(err).NotTo(HaveOccurred())

		Expect(unwoundResult.TotalSize()).To(Equal(0))

		// Step 2: Gather operation
		gatheredResult, err = gatherOp.Process(unwoundResult)
		Expect(err).NotTo(HaveOccurred())
		Expect(gatheredResult.TotalSize()).To(Equal(0))
	})

	It("should process an unwind followed by a gather - incremental implementation", func() {
		// Setup: Original document with array [1,2]
		originalDoc := Document{
			"id":    "X",
			"items": []any{1, 2},
		}

		// Create input delta: add the original document
		inputDelta := NewDocumentZSet()
		err := inputDelta.AddDocumentMutate(originalDoc, 1)
		Expect(err).NotTo(HaveOccurred())

		// Step 1: Unwind operation
		unwindOp := NewUnwind(
			&FieldExtractor{fieldName: "items"},     // Extract items array
			&ReplaceFieldTransformer{field: "item"}, // Replace with single item
		)

		unwoundResult, err := unwindOp.Process(inputDelta)
		Expect(err).NotTo(HaveOccurred())

		Expect(unwoundResult.TotalSize()).To(Equal(2))
		docs, err := unwoundResult.List()
		Expect(err).NotTo(HaveOccurred())
		for _, doc := range docs {
			Expect(doc.Document).To(HaveKey("id"))
			Expect(doc.Document).To(HaveKey("item"))
			Expect([]any{1, 2}).To(ContainElement(doc.Document["item"]))
			Expect(doc.Multiplicity).To(Equal(1))
		}

		// Step 2: Gather operation
		gatherOp := NewIncrementalGather(
			&FieldExtractor{fieldName: "id"},                                // Group by id
			&FieldExtractor{fieldName: "item"},                              // Extract item values
			&ArrayAggregateTransformer{keyField: "id", valueField: "items"}, // Rebuild array
		)

		gatheredResult, err := gatherOp.Process(unwoundResult)
		Expect(err).NotTo(HaveOccurred())

		Expect(gatheredResult.TotalSize()).To(Equal(1))
		docs, err = gatheredResult.List()
		Expect(err).NotTo(HaveOccurred())
		doc := docs[0]
		Expect(doc.Document).To(HaveKey("id"))
		Expect(doc.Document).To(HaveKey("items"))
		Expect([]any{[]any{2, 1}, []any{1, 2}}).To(ContainElement(doc.Document["items"]))
		Expect(doc.Multiplicity).To(Equal(1))

		// Reset the input delta and delete the original document
		inputDelta = NewDocumentZSet()
		err = inputDelta.AddDocumentMutate(originalDoc, -1)
		Expect(err).NotTo(HaveOccurred())

		unwoundResult, err = unwindOp.Process(inputDelta)
		Expect(err).NotTo(HaveOccurred())

		Expect(unwoundResult.TotalSize()).To(Equal(2))
		docs, err = unwoundResult.List()
		Expect(err).NotTo(HaveOccurred())
		for _, doc := range docs {
			Expect(doc.Document).To(HaveKey("id"))
			Expect(doc.Document).To(HaveKey("item"))
			Expect([]any{1, 2}).To(ContainElement(doc.Document["item"]))
			Expect(doc.Multiplicity).To(Equal(-1))
		}

		// Step 2: Gather operation
		gatheredResult, err = gatherOp.Process(unwoundResult)
		Expect(err).NotTo(HaveOccurred())

		Expect(gatheredResult.TotalSize()).To(Equal(1))
		docs, err = gatheredResult.List()
		Expect(err).NotTo(HaveOccurred())
		doc = docs[0]
		Expect(doc.Document).To(HaveKey("id"))
		Expect(doc.Document).To(HaveKey("items"))
		Expect([]any{[]any{2, 1}, []any{1, 2}}).To(ContainElement(doc.Document["items"]))
		Expect(doc.Multiplicity).To(Equal(-1))
	})
})
