package dbsp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// Replace with your actual package path
)

// SimpleJoinEvaluator implements the simplest possible join condition
// Joins documents if they have matching "id" fields
type SimpleJoinEvaluator struct{}

func (e *SimpleJoinEvaluator) Evaluate(doc Document) ([]Document, error) {
	// Extract left and right documents
	left, leftOk := doc["left"].(Document)
	right, rightOk := doc["right"].(Document)

	if !leftOk || !rightOk {
		return []Document{}, nil // No join if malformed input
	}

	// Extract ID fields (assuming they're int64)
	leftID, leftIDOk := left["id"].(int64)
	rightID, rightIDOk := right["id"].(int64)

	if !leftIDOk || !rightIDOk {
		return []Document{}, nil // No join if no ID fields
	}

	// Join condition: IDs must match
	if leftID == rightID {
		// Create joined document
		result := Document{
			"left_name":  left["name"],
			"right_name": right["name"],
			"id":         leftID,
		}
		return []Document{result}, nil
	}

	return []Document{}, nil // No match
}

func (e *SimpleJoinEvaluator) String() string {
	return "SimpleJoinEvaluator(left.id = right.id)"
}

var _ = Describe("Join Operator", func() {
	var joinOp *BinaryJoinOp
	var evaluator *SimpleJoinEvaluator

	BeforeEach(func() {
		evaluator = &SimpleJoinEvaluator{}
		joinOp = NewBinaryJoin(evaluator)
	})

	Context("Basic join functionality", func() {
		It("should join matching documents", func() {
			// Create left Z-set: {person with id=1}
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet, err := SingletonZSet(leftDoc)
			Expect(err).NotTo(HaveOccurred())

			// Create right Z-set: {project with id=1}
			rightDoc := Document{"id": int64(1), "name": "ProjectX"}
			rightZSet, err := SingletonZSet(rightDoc)
			Expect(err).NotTo(HaveOccurred())

			// Execute join
			result, err := joinOp.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Verify result
			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			joinedDoc := docs[0]
			Expect(joinedDoc["id"]).To(Equal(int64(1)))
			Expect(joinedDoc["left_name"]).To(Equal("Alice"))
			Expect(joinedDoc["right_name"]).To(Equal("ProjectX"))
		})

		It("should not join non-matching documents", func() {
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet, err := SingletonZSet(leftDoc)
			Expect(err).NotTo(HaveOccurred())

			rightDoc := Document{"id": int64(2), "name": "ProjectY"}
			rightZSet, err := SingletonZSet(rightDoc)
			Expect(err).NotTo(HaveOccurred())

			result, err := joinOp.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})
	})

	Context("Multiplicity handling", func() {
		It("should multiply multiplicities correctly", func() {
			// Left: document with multiplicity 2
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc, 2)).NotTo(HaveOccurred())

			// Right: document with multiplicity 3
			rightDoc := Document{"id": int64(1), "name": "ProjectX"}
			rightZSet := NewDocumentZSet()
			Expect(rightZSet.AddDocumentMutate(rightDoc, 3)).NotTo(HaveOccurred())

			// Execute join
			result, err := joinOp.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity 2 × 3 = 6
			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(6)) // 6 copies of the joined document
		})

		It("should handle negative multiplicities", func() {
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc, -1)).NotTo(HaveOccurred()) // Deletion

			rightDoc := Document{"id": int64(1), "name": "ProjectX"}
			rightZSet := NewDocumentZSet()
			Expect(rightZSet.AddDocumentMutate(rightDoc, 2)).NotTo(HaveOccurred())

			result, err := joinOp.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should have multiplicity (-1) × 2 = -2
			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(0)) // No positive documents

			// Check multiplicity directly
			expectedJoin := Document{
				"left_name":  "Alice",
				"right_name": "ProjectX",
				"id":         int64(1),
			}
			mult, err := result.GetMultiplicity(expectedJoin)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-2))
		})
	})
})

// var _ = Describe("Join Operator", func() {
// var joinOp *IncrementalBinaryJoinOp
// var evaluator *SimpleJoinEvaluator

// BeforeEach(func() {
// 	evaluator = &SimpleJoinEvaluator{}
// 	joinOp = NewIncrementalBinaryJoin(evaluator)
// })

// FContext("Incremental join test", func() {
// 	It("should not expose quadratic scaling", func() {
// 		// Create large left Z-set with unique documents
// 		leftZSet := NewDocumentZSet()
// 		for i := 0; i < 1000; i++ {
// 			doc := Document{"id": int64(i), "name": fmt.Sprintf("Person%d", i)}
// 			var err error
// 			leftZSet, err = leftZSet.AddDocument(doc, 1)
// 			Expect(err).NotTo(HaveOccurred())
// 		}

// 		// Create large right Z-set with unique documents
// 		rightZSet := NewDocumentZSet()
// 		for i := 0; i < 1000; i++ {
// 			doc := Document{"id": int64(i), "name": fmt.Sprintf("Project%d", i)}
// 			var err error
// 			rightZSet, err = rightZSet.AddDocument(doc, 1)
// 			Expect(err).NotTo(HaveOccurred())
// 		}

// 		// This should be fast if implemented correctly,
// 		// but will be slow with current GetDocuments() approach
// 		result, err := joinOp.Process(leftZSet, rightZSet)
// 		Expect(err).NotTo(HaveOccurred())

// 		// Should have 1000 joined documents (one per matching ID)
// 		Expect(result.Size()).To(Equal(1000))
// 	})

// 	It("should show the GetDocuments() expansion problem", func() {
// 		// Document with high multiplicity
// 		leftDoc := Document{"id": int64(1), "name": "Alice"}
// 		leftZSet := NewDocumentZSet()
// 		leftZSet, err := leftZSet.AddDocument(leftDoc, 100) // High multiplicity
// 		Expect(err).NotTo(HaveOccurred())

// 		rightDoc := Document{"id": int64(1), "name": "ProjectX"}
// 		rightZSet := NewDocumentZSet()
// 		rightZSet, err = rightZSet.AddDocument(rightDoc, 200) // High multiplicity
// 		Expect(err).NotTo(HaveOccurred())

// 		// GetDocuments() will create 100 + 200 = 300 document instances
// 		// Join will do 100 × 200 = 20,000 iterations
// 		// But there's really only 1 unique document on each side!

// 		result, err := joinOp.Process(leftZSet, rightZSet)
// 		Expect(err).NotTo(HaveOccurred())

// 		// Result should have multiplicity 100 × 200 = 20,000
// 		// But achieved through massive redundant computation
// 		Expect(result.Size()).To(Equal(20000))
// 	})
// })
// })
