package dbsp

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDocumentZSet(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DBSP Suite")
}

var _ = Describe("DocumentZSet", func() {
	var (
		emptyZSet *DocumentZSet
		doc1      Document
		doc2      Document
		doc3      Document
		doc4      Document
		zset1     *DocumentZSet
		zset2     *DocumentZSet
	)

	BeforeEach(func() {
		var err error
		emptyZSet = NewDocumentZSet()

		// Create test documents with different structures
		doc1, err = newDocumentFromPairs("name", "Alice", "age", int64(30))
		Expect(err).NotTo(HaveOccurred())

		doc2, err = newDocumentFromPairs("name", "Bob", "age", int64(25), "city", "NYC")
		Expect(err).NotTo(HaveOccurred())

		doc3, err = newDocumentFromPairs("name", "Alice", "age", int64(30)) // Same as doc1
		Expect(err).NotTo(HaveOccurred())

		// Document with completely different structure
		doc4, err = newDocumentFromPairs(
			"product", "laptop",
			"specs", map[string]any{
				"cpu":     "Intel i7",
				"memory":  int64(16),
				"storage": []any{"SSD", int64(512)},
			},
			"tags", []any{"electronics", "computer", "portable"},
		)
		Expect(err).NotTo(HaveOccurred())

		zset1, err = SingletonZSet(doc1)
		Expect(err).NotTo(HaveOccurred())

		zset2, err = SingletonZSet(doc2)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Document Creation and Equality", func() {
		It("should create documents correctly", func() {
			Expect(doc1).To(HaveKeyWithValue("name", "Alice"))
			Expect(doc1).To(HaveKeyWithValue("age", int64(30)))
			Expect(doc2).To(HaveKeyWithValue("city", "NYC"))
		})

		It("should detect equal documents", func() {
			equal, err := DeepEqual(doc1, doc3)
			Expect(err).NotTo(HaveOccurred())
			Expect(equal).To(BeTrue())
		})

		It("should detect different documents", func() {
			equal, err := DeepEqual(doc1, doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(equal).To(BeFalse())
		})

		It("should handle documents with different structures", func() {
			equal, err := DeepEqual(doc1, doc4)
			Expect(err).NotTo(HaveOccurred())
			Expect(equal).To(BeFalse())
		})

		It("should handle complex nested structures", func() {
			complexDoc1, err := newDocumentFromPairs(
				"user", map[string]any{
					"profile": map[string]any{
						"name":    "Charlie",
						"details": []any{"admin", int64(42), true},
					},
				},
			)
			Expect(err).NotTo(HaveOccurred())

			complexDoc2, err := newDocumentFromPairs(
				"user", map[string]any{
					"profile": map[string]any{
						"name":    "Charlie",
						"details": []any{"admin", int64(42), true},
					},
				},
			)
			Expect(err).NotTo(HaveOccurred())

			equal, err := DeepEqual(complexDoc1, complexDoc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(equal).To(BeTrue())
		})
	})

	Describe("Basic ZSet Operations", func() {
		It("should create empty ZSet", func() {
			Expect(emptyZSet.IsZero()).To(BeTrue())
			Expect(emptyZSet.Size()).To(Equal(0))
			Expect(emptyZSet.UniqueCount()).To(Equal(0))
		})

		It("should create singleton ZSet", func() {
			Expect(zset1.IsZero()).To(BeFalse())
			Expect(zset1.Size()).To(Equal(1))
			Expect(zset1.UniqueCount()).To(Equal(1))

			contains, err := zset1.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains).To(BeTrue())
		})

		It("should handle document multiplicity", func() {
			multiplicity, err := zset1.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(multiplicity).To(Equal(1))

			multiplicity, err = zset1.GetMultiplicity(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(multiplicity).To(Equal(0))
		})

		It("should add documents with custom multiplicity", func() {
			zsetMultiple, err := emptyZSet.AddDocument(doc1, 3)
			Expect(err).NotTo(HaveOccurred())

			Expect(zsetMultiple.Size()).To(Equal(3))
			Expect(zsetMultiple.UniqueCount()).To(Equal(1))

			multiplicity, err := zsetMultiple.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(multiplicity).To(Equal(3))
		})
	})

	Describe("ZSet Addition", func() {
		It("should add two disjoint ZSets", func() {
			result, err := zset1.Add(zset2)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(2))

			contains1, err := result.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains1).To(BeTrue())

			contains2, err := result.Contains(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains2).To(BeTrue())
		})

		It("should add ZSets with overlapping documents", func() {
			zset3, err := SingletonZSet(doc3) // Same as doc1
			Expect(err).NotTo(HaveOccurred())

			result, err := zset1.Add(zset3)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(2))        // Two instances of same document
			Expect(result.UniqueCount()).To(Equal(1)) // But only one unique document

			multiplicity, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(multiplicity).To(Equal(2))
		})

		It("should add ZSets with completely different document structures", func() {
			zset4, err := SingletonZSet(doc4)
			Expect(err).NotTo(HaveOccurred())

			result, err := zset1.Add(zset4)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(2))

			contains1, err := result.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains1).To(BeTrue())

			contains4, err := result.Contains(doc4)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains4).To(BeTrue())
		})

		It("should handle addition with empty ZSet", func() {
			result, err := zset1.Add(emptyZSet)
			Expect(err).NotTo(HaveOccurred())

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			equal, err := DeepEqual(docs[0], doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(equal).To(BeTrue())
			Expect(result.Size()).To(Equal(1))
		})

		It("should be commutative", func() {
			result1, err := zset1.Add(zset2)
			Expect(err).NotTo(HaveOccurred())

			result2, err := zset2.Add(zset1)
			Expect(err).NotTo(HaveOccurred())

			Expect(result1.Size()).To(Equal(result2.Size()))
			Expect(result1.UniqueCount()).To(Equal(result2.UniqueCount()))

			// Both should contain the same documents
			contains1_1, err := result1.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			contains1_2, err := result2.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains1_1).To(Equal(contains1_2))

			contains2_1, err := result1.Contains(doc2)
			Expect(err).NotTo(HaveOccurred())
			contains2_2, err := result2.Contains(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains2_1).To(Equal(contains2_2))
		})
	})

	Describe("ZSet Subtraction", func() {
		It("should subtract disjoint ZSets", func() {
			result, err := zset1.Subtract(zset2)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))

			contains1, err := result.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains1).To(BeTrue())

			contains2, err := result.Contains(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains2).To(BeFalse())
		})

		It("should subtract identical ZSets to get empty", func() {
			result, err := zset1.Subtract(zset1)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should subtract with overlapping documents", func() {
			// Create ZSet with doc1 having multiplicity 3
			zsetMultiple, err := emptyZSet.AddDocument(doc1, 3)
			Expect(err).NotTo(HaveOccurred())

			// Subtract ZSet with doc1 having multiplicity 1
			result, err := zsetMultiple.Subtract(zset1)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(1))

			multiplicity, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(multiplicity).To(Equal(2))
		})

		It("should handle subtraction resulting in negative multiplicities", func() {
			// Create ZSet with doc1 multiplicity 1
			// Subtract ZSet with doc1 multiplicity 3
			zsetMultiple, err := emptyZSet.AddDocument(doc1, 3)
			Expect(err).NotTo(HaveOccurred())

			result, err := zset1.Subtract(zsetMultiple)
			Expect(err).NotTo(HaveOccurred())

			// Result should have doc1 with multiplicity 1 - 3 = -2
			Expect(result.IsZero()).To(BeFalse()) // Not zero - contains doc with negative multiplicity

			mult, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-2)) // Negative multiplicity

			contains, err := result.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains).To(BeFalse()) // Contains() checks for positive multiplicity
		})

		It("should subtract from empty ZSet", func() {
			result, err := emptyZSet.Subtract(zset1)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeFalse())
			mult, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-1)) // Negative multiplicity
		})

		It("should handle subtraction with different document structures", func() {
			zset4, err := SingletonZSet(doc4)
			Expect(err).NotTo(HaveOccurred())

			// Add both different document types
			combined, err := zset1.Add(zset4)
			Expect(err).NotTo(HaveOccurred())

			// Subtract one type
			result, err := combined.Subtract(zset4)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))

			contains1, err := result.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains1).To(BeTrue())

			contains4, err := result.Contains(doc4)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains4).To(BeFalse())
		})
	})

	Describe("Distinct Operation", func() {
		It("should convert multiset to set", func() {
			// Create ZSet with multiple copies of same document
			zsetMultiple, err := emptyZSet.AddDocument(doc1, 5)
			Expect(err).NotTo(HaveOccurred())

			result, err := zsetMultiple.Distinct()
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))

			multiplicity, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(multiplicity).To(Equal(1))
		})

		It("should handle mixed multiplicities", func() {
			zsetMixed, err := emptyZSet.AddDocument(doc1, 3)
			Expect(err).NotTo(HaveOccurred())
			zsetMixed, err = zsetMixed.AddDocument(doc2, 7)
			Expect(err).NotTo(HaveOccurred())

			result, err := zsetMixed.Distinct()
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(2))

			mult1, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult1).To(Equal(1))

			mult2, err := result.GetMultiplicity(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult2).To(Equal(1))
		})

		It("should filter out negative multiplicities", func() {
			// Create a scenario with negative multiplicity
			zsetPos, err := emptyZSet.AddDocument(doc1, 2)
			Expect(err).NotTo(HaveOccurred())
			zsetNeg, err := emptyZSet.AddDocument(doc1, 3)
			Expect(err).NotTo(HaveOccurred())

			// This should result in doc1 having multiplicity -1
			result, err := zsetPos.Subtract(zsetNeg)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeFalse())

			mult1, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult1).To(Equal(-1))

			// Distinct of empty should be empty
			distinct, err := result.Distinct()
			Expect(err).NotTo(HaveOccurred())
			Expect(distinct.IsZero()).To(BeTrue())
		})

		It("should presetve negative multiplicities in Unique", func() {
			zsetMixed, err := emptyZSet.AddDocument(doc1, 3)
			Expect(err).NotTo(HaveOccurred())
			zsetMixed, err = zsetMixed.AddDocument(doc2, -7)
			Expect(err).NotTo(HaveOccurred())

			result, err := zsetMixed.Unique()
			Expect(err).NotTo(HaveOccurred())

			fmt.Println(result.String())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))
			Expect(result.TotalSize()).To(Equal(2))

			mult1, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult1).To(Equal(1))

			mult2, err := result.GetMultiplicity(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult2).To(Equal(-1))
		})
	})

	Describe("Algebraic Properties", func() {
		var zset3, zset4 *DocumentZSet

		BeforeEach(func() {
			var err error
			zset3, err = SingletonZSet(doc3)
			Expect(err).NotTo(HaveOccurred())

			zset4, err = SingletonZSet(doc4)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should be associative for addition", func() {
			// (zset1 + zset2) + zset3
			temp1, err := zset1.Add(zset2)
			Expect(err).NotTo(HaveOccurred())
			result1, err := temp1.Add(zset3)
			Expect(err).NotTo(HaveOccurred())

			// zset1 + (zset2 + zset3)
			temp2, err := zset2.Add(zset3)
			Expect(err).NotTo(HaveOccurred())
			result2, err := zset1.Add(temp2)
			Expect(err).NotTo(HaveOccurred())

			// Results should be equal
			Expect(result1.Size()).To(Equal(result2.Size()))
			Expect(result1.UniqueCount()).To(Equal(result2.UniqueCount()))

			// Check multiplicities
			mult1_1, err := result1.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			mult1_2, err := result2.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult1_1).To(Equal(mult1_2))
		})

		It("should have zero element (empty ZSet)", func() {
			// zset + empty = zset
			result, err := zset1.Add(emptyZSet)
			Expect(err).NotTo(HaveOccurred())

			mult1, err := result.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			origMult1, err := zset1.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult1).To(Equal(origMult1))

			// empty + zset = zset
			result2, err := emptyZSet.Add(zset1)
			Expect(err).NotTo(HaveOccurred())

			mult2, err := result2.GetMultiplicity(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult2).To(Equal(origMult1))
		})

		It("should have inverse element (subtraction)", func() {
			// zset - zset = empty
			result, err := zset1.Subtract(zset1)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle addition and subtraction with heterogeneous documents", func() {
			// Mix documents of completely different structures
			complexZSet, err := zset1.Add(zset4) // Person + Product
			Expect(err).NotTo(HaveOccurred())

			Expect(complexZSet.Size()).To(Equal(2))
			Expect(complexZSet.UniqueCount()).To(Equal(2))

			// Subtract one type
			result, err := complexZSet.Subtract(zset1)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))

			contains4, err := result.Contains(doc4)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains4).To(BeTrue())

			contains1, err := result.Contains(doc1)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains1).To(BeFalse())
		})
	})

	Describe("Document Retrieval", func() {
		It("should list documents", func() {
			result, err := emptyZSet.Subtract(zset1)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeFalse())

			docs, err := result.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))
			Expect(docs[0].Multiplicity).To(Equal(-1)) // Removal
			Expect(docs[0].Document).To(Equal(map[string]any{"name": "Alice", "age": int64(30)}))
		})

		It("should get all documents with multiplicities", func() {
			zsetMultiple, err := emptyZSet.AddDocument(doc1, 2)
			Expect(err).NotTo(HaveOccurred())
			zsetMultiple, err = zsetMultiple.AddDocument(doc2, 1)
			Expect(err).NotTo(HaveOccurred())

			docs, err := zsetMultiple.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(3)) // 2 copies of doc1 + 1 copy of doc2

			// Count occurrences
			doc1Count := 0
			doc2Count := 0
			for _, doc := range docs {
				equal1, _ := DeepEqual(doc, doc1)
				equal2, _ := DeepEqual(doc, doc2)
				if equal1 {
					doc1Count++
				} else if equal2 {
					doc2Count++
				}
			}

			Expect(doc1Count).To(Equal(2))
			Expect(doc2Count).To(Equal(1))
		})

		It("should get unique documents ignoring multiplicities", func() {
			zsetMultiple, err := emptyZSet.AddDocument(doc1, 5)
			Expect(err).NotTo(HaveOccurred())
			zsetMultiple, err = zsetMultiple.AddDocument(doc2, 3)
			Expect(err).NotTo(HaveOccurred())

			docs, err := zsetMultiple.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(2))

			// Should contain both documents exactly once
			foundDoc1 := false
			foundDoc2 := false
			for _, doc := range docs {
				equal1, _ := DeepEqual(doc, doc1)
				equal2, _ := DeepEqual(doc, doc2)
				if equal1 {
					foundDoc1 = true
				} else if equal2 {
					foundDoc2 = true
				}
			}

			Expect(foundDoc1).To(BeTrue())
			Expect(foundDoc2).To(BeTrue())
		})
	})

	Describe("DeepCopy and Immutability", func() {
		It("should create independent copies", func() {
			original, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())

			copied, err := original.DeepCopy()
			Expect(err).NotTo(HaveOccurred())

			// Modify the copy
			modified, err := copied.AddDocument(doc2, 1)
			Expect(err).NotTo(HaveOccurred())

			// Original should be unchanged
			Expect(original.Size()).To(Equal(1))
			Expect(modified.Size()).To(Equal(2))

			contains, err := original.Contains(doc2)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains).To(BeFalse())
		})

		It("should handle deep copying of complex nested documents", func() {
			complexDoc, err := newDocumentFromPairs(
				"level1", map[string]any{
					"level2": map[string]any{
						"level3": []any{
							map[string]any{"deep": "value"},
							int64(42),
						},
					},
				},
			)
			Expect(err).NotTo(HaveOccurred())

			zset, err := SingletonZSet(complexDoc)
			Expect(err).NotTo(HaveOccurred())

			copied, err := zset.DeepCopy()
			Expect(err).NotTo(HaveOccurred())

			// Verify the copy is independent
			docs1, err := zset.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			docs2, err := copied.GetDocuments()
			Expect(err).NotTo(HaveOccurred())

			equal, err := DeepEqual(docs1[0], docs2[0])
			Expect(err).NotTo(HaveOccurred())
			Expect(equal).To(BeTrue())

			// Modifying nested structure in original shouldn't affect copy
			// (This is more of a design verification than a mutation test)
		})
	})

	Describe("Error Handling", func() {
		It("should handle invalid document creation", func() {
			_, err := newDocumentFromPairs("key1", "value1", "key2") // Odd number of args
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("even number of arguments"))
		})

		It("should handle non-string keys", func() {
			_, err := newDocumentFromPairs(42, "value") // Non-string key
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must be a string"))
		})

		It("should handle nil ZSet operations gracefully", func() {
			var nilZSet *DocumentZSet

			result, err := zset1.Add(nilZSet)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(zset1.Size()))

			result, err = zset1.Subtract(nilZSet)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(zset1.Size()))
		})
	})

	Describe("Edge Cases", func() {
		It("should handle documents with nil values", func() {
			docWithNil, err := newDocumentFromPairs("name", "Alice", "optional", nil)
			Expect(err).NotTo(HaveOccurred())

			zset, err := SingletonZSet(docWithNil)
			Expect(err).NotTo(HaveOccurred())

			Expect(zset.Size()).To(Equal(1))
			contains, err := zset.Contains(docWithNil)
			Expect(err).NotTo(HaveOccurred())
			Expect(contains).To(BeTrue())
		})

		It("should handle empty nested structures", func() {
			emptyNested, err := newDocumentFromPairs(
				"emptyMap", map[string]any{},
				"emptyArray", []any{},
				"name", "test",
			)
			Expect(err).NotTo(HaveOccurred())

			zset, err := SingletonZSet(emptyNested)
			Expect(err).NotTo(HaveOccurred())

			Expect(zset.Size()).To(Equal(1))
		})

		It("should handle zero multiplicity additions", func() {
			result, err := zset1.AddDocument(doc2, 0)
			Expect(err).NotTo(HaveOccurred())

			// Adding with zero multiplicity should be no-op
			Expect(result.Size()).To(Equal(zset1.Size()))
			Expect(result.UniqueCount()).To(Equal(zset1.UniqueCount()))
		})
	})
})
