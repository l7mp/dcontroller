package dbsp

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Enhanced join evaluator with more flexible conditions
type FlexibleJoinEvaluator struct {
	inputs    []string
	condition string // "id", "category", etc.
}

func NewFlexibleJoin(condition string, inputs []string) *FlexibleJoinEvaluator {
	return &FlexibleJoinEvaluator{condition: condition, inputs: inputs}
}

func (e *FlexibleJoinEvaluator) Evaluate(doc Document) ([]Document, error) {
	left, leftOk := doc[e.inputs[0]].(Document)
	right, rightOk := doc[e.inputs[1]].(Document)

	if !leftOk || !rightOk {
		return []Document{}, nil
	}

	// Extract join key fields
	leftKey, leftKeyOk := left[e.condition]
	rightKey, rightKeyOk := right[e.condition]

	if !leftKeyOk || !rightKeyOk {
		return []Document{}, nil
	}

	// Check equality using JSON comparison for consistency
	leftKeyJSON, err := computeJSONAny(leftKey)
	if err != nil {
		return []Document{}, err
	}
	rightKeyJSON, err := computeJSONAny(rightKey)
	if err != nil {
		return []Document{}, err
	}

	if leftKeyJSON == rightKeyJSON {
		// Create joined document with both sides
		result := Document{
			"join_key": leftKey,
		}
		// Add all fields from left with prefix
		for k, v := range left {
			result["left_"+k] = v
		}
		// Add all fields from right with prefix
		for k, v := range right {
			result["right_"+k] = v
		}
		return []Document{result}, nil
	}

	return []Document{}, nil
}

func (e *FlexibleJoinEvaluator) String() string {
	return fmt.Sprintf("FlexibleJoin(%s.%s = %s.%s)", e.inputs[0], e.condition, e.inputs[1], e.condition)
}

var _ = Describe("Binary Join Operators", func() {
	var (
		snapshotJoin    *BinaryJoinOp
		incrementalJoin *IncrementalBinaryJoinOp
		evaluator       *FlexibleJoinEvaluator
	)

	BeforeEach(func() {
		inputs := []string{"users", "projects"}
		evaluator = NewFlexibleJoin("id", inputs)
		snapshotJoin = NewBinaryJoin(evaluator, inputs)
		incrementalJoin = NewIncrementalBinaryJoin(evaluator, inputs)
	})

	Context("Basic Join Functionality", func() {
		It("should join matching documents correctly", func() {
			// Left: users
			leftDoc1 := Document{"id": int64(1), "name": "Alice", "dept": "Engineering"}
			leftDoc2 := Document{"id": int64(2), "name": "Bob", "dept": "Sales"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc1, 1)).NotTo(HaveOccurred())
			Expect(leftZSet.AddDocumentMutate(leftDoc2, 1)).NotTo(HaveOccurred())

			// Right: projects
			rightDoc1 := Document{"id": int64(1), "project": "WebApp", "budget": int64(50000)}
			rightDoc3 := Document{"id": int64(3), "project": "MobileApp", "budget": int64(30000)}
			rightZSet := NewDocumentZSet()
			Expect(rightZSet.AddDocumentMutate(rightDoc1, 1)).NotTo(HaveOccurred())
			Expect(rightZSet.AddDocumentMutate(rightDoc3, 1)).NotTo(HaveOccurred())

			// Execute join
			result, err := snapshotJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should only join Alice (id=1) with WebApp (id=1)
			Expect(result.Size()).To(Equal(1))

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			joinedDoc := docs[0]

			Expect(joinedDoc["join_key"]).To(Equal(int64(1)))
			Expect(joinedDoc["left_name"]).To(Equal("Alice"))
			Expect(joinedDoc["left_dept"]).To(Equal("Engineering"))
			Expect(joinedDoc["right_project"]).To(Equal("WebApp"))
			Expect(joinedDoc["right_budget"]).To(Equal(int64(50000)))
		})

		It("should handle no matches gracefully", func() {
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet, err := SingletonZSet(leftDoc)
			Expect(err).NotTo(HaveOccurred())

			rightDoc := Document{"id": int64(2), "name": "Bob"}
			rightZSet, err := SingletonZSet(rightDoc)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle multiplicities correctly (bilinear property)", func() {
			// Left doc with multiplicity 3
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc, 3)).NotTo(HaveOccurred())

			// Right doc with multiplicity 2
			rightDoc := Document{"id": int64(1), "project": "WebApp"}
			rightZSet := NewDocumentZSet()
			Expect(rightZSet.AddDocumentMutate(rightDoc, 2)).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity 3 × 2 = 6
			Expect(result.Size()).To(Equal(6))
			Expect(result.UniqueCount()).To(Equal(1))
		})
	})

	Context("Incremental vs Snapshot Consistency", func() {
		var (
			leftSnapshots  []*DocumentZSet
			rightSnapshots []*DocumentZSet
			leftDeltas     []*DocumentZSet
			rightDeltas    []*DocumentZSet
		)

		BeforeEach(func() {
			// Reset incremental join state
			incrementalJoin.Reset()

			// Define our test data sequence
			// Users (left side)
			alice := Document{"id": int64(1), "name": "Alice", "role": "Engineer"}
			bob := Document{"id": int64(2), "name": "Bob", "role": "Manager"}
			charlie := Document{"id": int64(3), "name": "Charlie", "role": "Designer"}

			// Projects (right side)
			proj1 := Document{"id": int64(1), "title": "WebApp", "status": "active"}
			proj2 := Document{"id": int64(2), "title": "MobileApp", "status": "planning"}

			// Timeline of changes
			// T0: Empty
			leftSnapshots = append(leftSnapshots, NewDocumentZSet())
			rightSnapshots = append(rightSnapshots, NewDocumentZSet())
			leftDeltas = append(leftDeltas, NewDocumentZSet())
			rightDeltas = append(rightDeltas, NewDocumentZSet())

			// T1: Add Alice and WebApp
			left1 := NewDocumentZSet()
			Expect(left1.AddDocumentMutate(alice, 1)).NotTo(HaveOccurred())
			leftSnapshots = append(leftSnapshots, left1)

			right1 := NewDocumentZSet()
			Expect(right1.AddDocumentMutate(proj1, 1)).NotTo(HaveOccurred())
			rightSnapshots = append(rightSnapshots, right1)

			// Deltas for T1
			deltaLeft1 := NewDocumentZSet()
			Expect(deltaLeft1.AddDocumentMutate(alice, 1)).NotTo(HaveOccurred())
			leftDeltas = append(leftDeltas, deltaLeft1)

			deltaRight1 := NewDocumentZSet()
			Expect(deltaRight1.AddDocumentMutate(proj1, 1)).NotTo(HaveOccurred())
			rightDeltas = append(rightDeltas, deltaRight1)

			// T2: Add Bob and MobileApp
			left2, err := left1.AddDocument(bob, 1)
			Expect(err).NotTo(HaveOccurred())
			leftSnapshots = append(leftSnapshots, left2)

			right2, err := right1.AddDocument(proj2, 1)
			Expect(err).NotTo(HaveOccurred())
			rightSnapshots = append(rightSnapshots, right2)

			// Deltas for T2
			deltaLeft2 := NewDocumentZSet()
			Expect(deltaLeft2.AddDocumentMutate(bob, 1)).NotTo(HaveOccurred())
			leftDeltas = append(leftDeltas, deltaLeft2)

			deltaRight2 := NewDocumentZSet()
			Expect(deltaRight2.AddDocumentMutate(proj2, 1)).NotTo(HaveOccurred())
			rightDeltas = append(rightDeltas, deltaRight2)

			// T3: Add Charlie (no matching project)
			left3, err := left2.AddDocument(charlie, 1)
			Expect(err).NotTo(HaveOccurred())
			leftSnapshots = append(leftSnapshots, left3)

			rightSnapshots = append(rightSnapshots, right2) // No change

			deltaLeft3 := NewDocumentZSet()
			Expect(deltaLeft3.AddDocumentMutate(charlie, 1)).NotTo(HaveOccurred())
			leftDeltas = append(leftDeltas, deltaLeft3)

			rightDeltas = append(rightDeltas, NewDocumentZSet()) // Empty delta

			// T4: Remove Alice (should remove Alice-WebApp join)
			left4, err := left3.AddDocument(alice, -1) // Remove Alice
			Expect(err).NotTo(HaveOccurred())
			leftSnapshots = append(leftSnapshots, left4)

			rightSnapshots = append(rightSnapshots, right2) // No change

			deltaLeft4 := NewDocumentZSet()
			Expect(deltaLeft4.AddDocumentMutate(alice, -1)).NotTo(HaveOccurred())
			leftDeltas = append(leftDeltas, deltaLeft4)

			rightDeltas = append(rightDeltas, NewDocumentZSet()) // Empty delta
		})

		It("should produce identical results for snapshot vs incremental approaches", func() {
			cumulativeSnapshot := NewDocumentZSet() // Running snapshot

			// Track both approaches through the timeline
			for t := 1; t < len(leftSnapshots); t++ {
				// fmt.Printf("\n=== Timestep %d ===\n", t)

				// Snapshot approach: join current snapshots
				snapshotResult, err := snapshotJoin.Process(leftSnapshots[t], rightSnapshots[t])
				Expect(err).NotTo(HaveOccurred())

				// Incremental approach: process deltas
				incrementalResult, err := incrementalJoin.Process(leftDeltas[t], rightDeltas[t])
				Expect(err).NotTo(HaveOccurred())

				// fmt.Printf("Snapshot result size: %d, Incremental result size: %d\n",
				// 	snapshotResult.Size(), incrementalResult.Size())

				// Add incremental delta to cumulative snapshot
				cumulativeSnapshot, err = cumulativeSnapshot.Add(incrementalResult)
				Expect(err).NotTo(HaveOccurred())

				// Should be equal
				Expect(cumulativeSnapshot.Size()).To(Equal(snapshotResult.Size()),
					fmt.Sprintf("Size mismatch at timestep %d", t))
				Expect(cumulativeSnapshot.UniqueCount()).To(Equal(snapshotResult.UniqueCount()),
					fmt.Sprintf("UniqueCount mismatch at timestep %d", t))

				// Verify document-by-document equality
				if !snapshotResult.IsZero() {
					snapshotDocs, err := snapshotResult.GetUniqueDocuments()
					Expect(err).NotTo(HaveOccurred())
					cumulativeDocs, err := cumulativeSnapshot.GetUniqueDocuments()
					Expect(err).NotTo(HaveOccurred())
					Expect(cumulativeDocs).To(HaveLen(len(snapshotDocs)))

					// Check multiplicities match
					for _, snapDoc := range snapshotDocs {
						snapMult, err := snapshotResult.GetMultiplicity(snapDoc)
						Expect(err).NotTo(HaveOccurred())
						cumMult, err := cumulativeSnapshot.GetMultiplicity(snapDoc)
						Expect(err).NotTo(HaveOccurred())

						Expect(cumMult).To(Equal(snapMult),
							fmt.Sprintf("Multiplicity mismatch for doc %v at timestep %d", snapDoc, t))
					}
				}

				// fmt.Printf("✓ Timestep %d: Snapshot=%d docs, Incremental delta=%d docs, Cumulative=%d docs\n",
				// 	t, snapshotResult.Size(), incrementalResult.Size(), cumulativeSnapshot.Size())
			}
		})

		It("should handle specific timeline events correctly", func() {
			// T1: Should have Alice-WebApp join
			incrementalResult, err := incrementalJoin.Process(leftDeltas[1], rightDeltas[1])
			Expect(err).NotTo(HaveOccurred())
			// fmt.Printf("Incremental result in timestep 1: %s\n", incrementalResult.String())
			Expect(incrementalResult.Size()).To(Equal(1))

			// T2: Should add Bob-MobileApp join
			incrementalResult, err = incrementalJoin.Process(leftDeltas[2], rightDeltas[2])
			Expect(err).NotTo(HaveOccurred())
			// fmt.Printf("Incremental result in timestep 2: %s\n", incrementalResult.String())
			Expect(incrementalResult.Size()).To(Equal(1)) // Only the new join

			// T3: Should have no new joins (Charlie has no matching project)
			incrementalResult, err = incrementalJoin.Process(leftDeltas[3], rightDeltas[3])
			Expect(err).NotTo(HaveOccurred())
			// fmt.Printf("Incremental result in timestep 3: %s\n", incrementalResult.String())
			Expect(incrementalResult.IsZero()).To(BeTrue())

			// T4: Should remove Alice-WebApp join
			incrementalResult, err = incrementalJoin.Process(leftDeltas[4], rightDeltas[4])
			Expect(err).NotTo(HaveOccurred())
			// fmt.Printf("Incremental result in timestep 4: %s\n", incrementalResult.String())
			Expect(incrementalResult.TotalSize()).To(Equal(1)) // need negative multiplicities

			// The result should be a negative (removal) of the Alice-WebApp join
			docs, err := incrementalResult.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(BeEmpty()) // No positive documents

			// Check for negative multiplicity directly
			expectedJoin := Document{
				"join_key":     int64(1),
				"left_id":      int64(1),
				"left_name":    "Alice",
				"left_role":    "Engineer",
				"right_id":     int64(1),
				"right_title":  "WebApp",
				"right_status": "active",
			}
			mult, err := incrementalResult.GetMultiplicity(expectedJoin)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-1)) // Negative indicates removal
		})
	})

	Context("Complex Scenarios", func() {
		It("should handle multiple matches per key", func() {
			// Two users with same ID (weird but valid for Z-sets)
			leftDoc1 := Document{"id": int64(1), "name": "Alice", "type": "Employee"}
			leftDoc2 := Document{"id": int64(1), "name": "Alice", "type": "Contractor"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc1, 1)).NotTo(HaveOccurred())
			Expect(leftZSet.AddDocumentMutate(leftDoc2, 1)).NotTo(HaveOccurred())

			// One project with matching ID
			rightDoc := Document{"id": int64(1), "project": "WebApp"}
			rightZSet, err := SingletonZSet(rightDoc)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 joins (each user with the project)
			Expect(result.Size()).To(Equal(2))
			Expect(result.UniqueCount()).To(Equal(2)) // Different join results
		})

		It("should handle high multiplicities efficiently", func() {
			// Document with high multiplicity
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc, 100)).NotTo(HaveOccurred())

			rightDoc := Document{"id": int64(1), "project": "WebApp"}
			rightZSet := NewDocumentZSet()
			Expect(rightZSet.AddDocumentMutate(rightDoc, 50)).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity 100 × 50 = 5000
			Expect(result.Size()).To(Equal(5000))
			Expect(result.UniqueCount()).To(Equal(1)) // Only one unique join
		})

		It("should handle negative multiplicities in joins", func() {
			// Positive and negative documents
			leftDoc := Document{"id": int64(1), "name": "Alice"}
			leftZSet := NewDocumentZSet()
			Expect(leftZSet.AddDocumentMutate(leftDoc, 2)).NotTo(HaveOccurred())  // +2
			Expect(leftZSet.AddDocumentMutate(leftDoc, -1)).NotTo(HaveOccurred()) // Net +1

			rightDoc := Document{"id": int64(1), "project": "WebApp"}
			rightZSet := NewDocumentZSet()
			Expect(rightZSet.AddDocumentMutate(rightDoc, -3)).NotTo(HaveOccurred()) // -3

			result, err := snapshotJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity 1 × (-3) = -3
			expectedJoin := Document{
				"join_key":      int64(1),
				"left_id":       int64(1),
				"left_name":     "Alice",
				"right_id":      int64(1),
				"right_project": "WebApp",
			}
			mult, err := result.GetMultiplicity(expectedJoin)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-3))
		})
	})

	Context("Join on Different Fields", func() {
		It("should join on non-id fields", func() {
			inputs := []string{"left", "right"}
			categoryJoin := NewBinaryJoin(NewFlexibleJoin("category", inputs), inputs)

			leftDoc := Document{"id": int64(1), "name": "Laptop", "category": "Electronics"}
			leftZSet, err := SingletonZSet(leftDoc)
			Expect(err).NotTo(HaveOccurred())

			rightDoc := Document{"id": int64(2), "description": "High-tech gadgets", "category": "Electronics"}
			rightZSet, err := SingletonZSet(rightDoc)
			Expect(err).NotTo(HaveOccurred())

			result, err := categoryJoin.Process(leftZSet, rightZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			joinedDoc := docs[0]

			Expect(joinedDoc["join_key"]).To(Equal("Electronics"))
			Expect(joinedDoc["left_name"]).To(Equal("Laptop"))
			Expect(joinedDoc["right_description"]).To(Equal("High-tech gadgets"))
		})
	})

	Context("Operator Properties", func() {
		It("should be bilinear", func() {
			Expect(snapshotJoin.OpType()).To(Equal(OpTypeBilinear))
			Expect(incrementalJoin.OpType()).To(Equal(OpTypeBilinear))
		})

		It("should be time invariant", func() {
			Expect(snapshotJoin.IsTimeInvariant()).To(BeTrue())
			Expect(incrementalJoin.IsTimeInvariant()).To(BeTrue())
		})

		It("should have zero preservation property", func() {
			Expect(snapshotJoin.HasZeroPreservationProperty()).To(BeTrue())
			Expect(incrementalJoin.HasZeroPreservationProperty()).To(BeTrue())
		})
	})
})

// N-ary join evaluator that joins documents based on matching "id" fields across all inputs
type NaryJoinEvaluator struct {
	inputs    []string
	n         int
	condition string // field to join on (e.g., "id")
}

func NewNaryJoin(condition string, inputs []string) *NaryJoinEvaluator {
	return &NaryJoinEvaluator{condition: condition, inputs: inputs, n: len(inputs)}
}

func (e *NaryJoinEvaluator) Evaluate(doc Document) ([]Document, error) {
	// Extract all input documents
	var inputDocs []Document
	var joinKey any
	joinKeySet := false

	// Collect all input documents and verify they have the same join key
	var joinKeyJSON string
	for i := 0; i < e.n; i++ {
		inputKey := e.inputs[i]
		inputDoc, exists := doc[inputKey].(Document)
		if !exists {
			return nil, fmt.Errorf("no input for %q (index %d)", inputKey, i)
		}

		// Extract join key
		if keyValue, keyExists := inputDoc[e.condition]; keyExists {
			inputDocs = append(inputDocs, inputDoc)
			if !joinKeySet {
				joinKey = keyValue
				joinKeySet = true
				joinKeyString, err := computeJSONAny(joinKey)
				if err != nil {
					return []Document{}, err
				}
				joinKeyJSON = joinKeyString
			} else {
				// Check if this key matches the first one
				keyJSON, err := computeJSONAny(keyValue)
				if err != nil {
					return []Document{}, err
				}
				if keyJSON != joinKeyJSON {
					return []Document{}, nil // Keys don't match, no join
				}
			}
		}
	}

	if len(inputDocs) != e.n || !joinKeySet {
		return []Document{}, nil
	}

	// Create joined document with all fields from all inputs
	result := Document{
		"join_key": joinKey,
	}

	// Add all fields from each input with prefix
	for i, inputDoc := range inputDocs {
		for k, v := range inputDoc {
			result[fmt.Sprintf("input_%d_%s", i, k)] = v
		}
	}

	return []Document{result}, nil
}

func (e *NaryJoinEvaluator) String() string {
	return fmt.Sprintf("NaryJoin(%s)", e.condition)
}

var _ = Describe("N-ary Join Operators", func() {
	Context("3-way Join", func() {
		var (
			snapshotJoin *JoinOp
			// incrementalJoin *IncrementalJoinOp
			evaluator *NaryJoinEvaluator
		)

		BeforeEach(func() {
			inputs := []string{"users", "projects", "departments"}
			evaluator = NewNaryJoin("id", inputs)
			snapshotJoin = NewJoin(evaluator, inputs)
			// incrementalJoin = NewIncrementalJoin(evaluator, 3)
		})

		It("should join three matching documents correctly", func() {
			// Input 1: users
			user := Document{"id": int64(1), "name": "Alice", "role": "Engineer"}
			userZSet, err := SingletonZSet(user)
			Expect(err).NotTo(HaveOccurred())

			// Input 2: projects
			project := Document{"id": int64(1), "title": "WebApp", "status": "active"}
			projectZSet, err := SingletonZSet(project)
			Expect(err).NotTo(HaveOccurred())

			// Input 3: departments
			dept := Document{"id": int64(1), "name": "Engineering", "budget": int64(100000)}
			deptZSet, err := SingletonZSet(dept)
			Expect(err).NotTo(HaveOccurred())

			// Execute 3-way join
			result, err := snapshotJoin.Process(userZSet, projectZSet, deptZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			joinedDoc := docs[0]

			// Verify joined document contains all fields
			Expect(joinedDoc["join_key"]).To(Equal(int64(1)))
			Expect(joinedDoc["input_0_name"]).To(Equal("Alice"))
			Expect(joinedDoc["input_1_title"]).To(Equal("WebApp"))
			Expect(joinedDoc["input_2_name"]).To(Equal("Engineering"))
			Expect(joinedDoc["input_2_budget"]).To(Equal(int64(100000)))
		})

		It("should handle partial matches (no join when not all inputs match)", func() {
			// User with id=1
			user := Document{"id": int64(1), "name": "Alice"}
			userZSet, err := SingletonZSet(user)
			Expect(err).NotTo(HaveOccurred())

			// Project with id=1
			project := Document{"id": int64(1), "title": "WebApp"}
			projectZSet, err := SingletonZSet(project)
			Expect(err).NotTo(HaveOccurred())

			// Department with id=2 (different!)
			dept := Document{"id": int64(2), "name": "Marketing"}
			deptZSet, err := SingletonZSet(dept)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(userZSet, projectZSet, deptZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle multiplicities correctly (N-ary bilinear property)", func() {
			// User with multiplicity 2
			user := Document{"id": int64(1), "name": "Alice"}
			userZSet := NewDocumentZSet()
			Expect(userZSet.AddDocumentMutate(user, 2)).NotTo(HaveOccurred())

			// Project with multiplicity 3
			project := Document{"id": int64(1), "title": "WebApp"}
			projectZSet := NewDocumentZSet()
			Expect(projectZSet.AddDocumentMutate(project, 3)).NotTo(HaveOccurred())

			// Department with multiplicity 4
			dept := Document{"id": int64(1), "name": "Engineering"}
			deptZSet := NewDocumentZSet()
			Expect(deptZSet.AddDocumentMutate(dept, 4)).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(userZSet, projectZSet, deptZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity 2 × 3 × 4 = 24
			Expect(result.Size()).To(Equal(24))
			Expect(result.UniqueCount()).To(Equal(1))
		})
	})

	Context("4-way Join", func() {
		var (
			snapshotJoin *JoinOp
			// incrementalJoin *IncrementalJoinOp
			evaluator *NaryJoinEvaluator
		)

		BeforeEach(func() {
			inputs := []string{"products", "reviews", "suppliers", "inventories"}
			evaluator = NewNaryJoin("category", inputs)
			snapshotJoin = NewJoin(evaluator, inputs)
			// incrementalJoin = NewIncrementalJoin(evaluator, 4)
		})

		It("should join four collections on category field", func() {
			// Products
			product := Document{"category": "electronics", "name": "Laptop", "price": int64(1000)}
			productZSet, err := SingletonZSet(product)
			Expect(err).NotTo(HaveOccurred())

			// Reviews
			review := Document{"category": "electronics", "rating": int64(5), "comment": "Great!"}
			reviewZSet, err := SingletonZSet(review)
			Expect(err).NotTo(HaveOccurred())

			// Suppliers
			supplier := Document{"category": "electronics", "company": "TechCorp", "location": "USA"}
			supplierZSet, err := SingletonZSet(supplier)
			Expect(err).NotTo(HaveOccurred())

			// Inventory
			inventory := Document{"category": "electronics", "stock": int64(50), "warehouse": "A1"}
			inventoryZSet, err := SingletonZSet(inventory)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(productZSet, reviewZSet, supplierZSet, inventoryZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			joinedDoc := docs[0]

			Expect(joinedDoc["join_key"]).To(Equal("electronics"))
			Expect(joinedDoc["input_0_name"]).To(Equal("Laptop"))
			Expect(joinedDoc["input_1_rating"]).To(Equal(int64(5)))
			Expect(joinedDoc["input_2_company"]).To(Equal("TechCorp"))
			Expect(joinedDoc["input_3_stock"]).To(Equal(int64(50)))
		})

		It("should handle empty inputs gracefully", func() {
			// One empty input should result in empty output
			product := Document{"category": "electronics", "name": "Laptop"}
			productZSet, err := SingletonZSet(product)
			Expect(err).NotTo(HaveOccurred())

			emptyZSet := NewDocumentZSet()

			result, err := snapshotJoin.Process(productZSet, emptyZSet, emptyZSet, emptyZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})
	})

	Context("Minimal 3-way Join Cases", func() {
		var (
			snapshotJoin    *JoinOp
			incrementalJoin *IncrementalJoinOp
			evaluator       *NaryJoinEvaluator
			inputs          []string
		)

		BeforeEach(func() {
			inputs = []string{"input_0", "input_1", "input_2"}
			evaluator = NewNaryJoin("id", inputs)
			snapshotJoin = NewJoin(evaluator, inputs)
			incrementalJoin = NewIncrementalJoin(evaluator, inputs)
		})

		It("should handle the simplest possible 3-way join", func() {
			// Single document in each input, all with same ID
			doc1 := Document{"id": int64(1), "name": "A"}
			doc2 := Document{"id": int64(1), "name": "B"}
			doc3 := Document{"id": int64(1), "name": "C"}

			zset1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			zset2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())
			zset3, err := SingletonZSet(doc3)
			Expect(err).NotTo(HaveOccurred())

			// Test snapshot version first
			result, err := snapshotJoin.Process(zset1, zset2, zset3)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("Snapshot 3-way join result:\n")
			// fmt.Printf("  Size: %d\n", result.Size())
			// fmt.Printf("  UniqueCount: %d\n", result.UniqueCount())

			// Should be exactly 1 join with multiplicity 1×1×1 = 1
			Expect(result.Size()).To(Equal(1), "Expected exactly 1 joined document")
			Expect(result.UniqueCount()).To(Equal(1), "Expected exactly 1 unique document")

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			joinedDoc := docs[0]
			Expect(joinedDoc["join_key"]).To(Equal(int64(1)))
			// fmt.Printf("  Joined doc: %+v\n", joinedDoc)
		})

		It("should handle 3-way join with no matches", func() {
			// Documents with different IDs
			doc1 := Document{"id": int64(1), "name": "A"}
			doc2 := Document{"id": int64(2), "name": "B"} // Different ID
			doc3 := Document{"id": int64(1), "name": "C"}

			zset1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			zset2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())
			zset3, err := SingletonZSet(doc3)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(zset1, zset2, zset3)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("No-match 3-way join result:\n")
			// fmt.Printf("  Size: %d\n", result.Size())
			// fmt.Printf("  IsZero: %v\n", result.IsZero())

			// Should be empty
			Expect(result.IsZero()).To(BeTrue(), "Expected no joins when IDs don't match")
		})

		It("should test incremental 3-way step by step", func() {
			// fmt.Printf("\n=== INCREMENTAL 3-WAY JOIN DEBUG ===\n")

			// Reset to clean state
			incrementalJoin = NewIncrementalJoin(evaluator, inputs)

			// Same simple data
			doc1 := Document{"id": int64(1), "name": "A"}
			doc2 := Document{"id": int64(1), "name": "B"}
			doc3 := Document{"id": int64(1), "name": "C"}

			delta1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			delta2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())
			delta3, err := SingletonZSet(doc3)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("Input deltas:\n")
			// fmt.Printf("  Delta1 size: %d\n", delta1.Size())
			// fmt.Printf("  Delta2 size: %d\n", delta2.Size())
			// fmt.Printf("  Delta3 size: %d\n", delta3.Size())

			// Process with incremental join
			result, err := incrementalJoin.Process(delta1, delta2, delta3)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("Incremental result:\n")
			// fmt.Printf("  Size: %d\n", result.Size())
			// fmt.Printf("  UniqueCount: %d\n", result.UniqueCount())

			// THIS IS WHERE THE BUG SHOWS UP
			// Expected: 1 (same as snapshot)
			// Actual: 7 (according to your observation)

			if result.Size() != 1 {
				fmt.Printf("BUG DETECTED: Expected size 1, got %d\n", result.Size())

				// Let's examine what we actually got
				docs, err := result.GetDocuments()
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("  All documents (%d total):\n", len(docs))
				for i, doc := range docs {
					fmt.Printf("    [%d]: %+v\n", i, doc)
				}

				// Check unique documents and their multiplicities
				uniqueDocs, err := result.GetUniqueDocuments()
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("  Unique documents (%d total):\n", len(uniqueDocs))
				for i, doc := range uniqueDocs {
					mult, err := result.GetMultiplicity(doc)
					Expect(err).NotTo(HaveOccurred())
					fmt.Printf("    [%d]: multiplicity=%d, doc=%+v\n", i, mult, doc)
				}
			}

			// This will fail if the bug exists, helping us identify the issue
			Expect(result.Size()).To(Equal(1), "Incremental should match snapshot result")
		})
	})

	Context("Binary vs 3-ary Comparison", func() {
		It("should compare binary join result with equivalent manual 3-way", func() {
			// Let's see if the issue is specific to N-ary or affects binary too
			inputs := []string{"left", "right"}
			binaryEval := NewFlexibleJoin("id", inputs)
			binaryJoin := NewBinaryJoin(binaryEval, inputs)

			doc1 := Document{"id": int64(1), "name": "A"}
			doc2 := Document{"id": int64(1), "name": "B"}

			zset1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			zset2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())

			binaryResult, err := binaryJoin.Process(zset1, zset2)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("Binary join (A ⋈ B):\n")
			// fmt.Printf("  Size: %d\n", binaryResult.Size())
			// fmt.Printf("  UniqueCount: %d\n", binaryResult.UniqueCount())

			// This should definitely be 1
			Expect(binaryResult.Size()).To(Equal(1))
		})
	})

	Context("Manual Cartesian Product Debug", func() {
		It("should manually trace the cartesian product logic", func() {
			// Let's debug the cartesianJoin function step by step
			inputNames := []string{"input_0", "input_1", "input_2"}
			evaluator := NewNaryJoin("id", inputNames)

			doc1 := Document{"id": int64(1), "value": "A"}
			doc2 := Document{"id": int64(1), "value": "B"}
			doc3 := Document{"id": int64(1), "value": "C"}

			input1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			input2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())
			input3, err := SingletonZSet(doc3)
			Expect(err).NotTo(HaveOccurred())

			inputs := []*DocumentZSet{input1, input2, input3}

			// fmt.Printf("\nCartesian Product Debug:\n")
			// fmt.Printf("Input 0: %d documents\n", input1.Size())
			// fmt.Printf("Input 1: %d documents\n", input2.Size())
			// fmt.Printf("Input 2: %d documents\n", input3.Size())

			// Let's manually call the cartesian join function
			result, err := cartesianJoin(evaluator, inputNames, inputs, 0, make([]Document, 3), make([]int, 3))
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("Cartesian result: size=%d, unique=%d\n", result.Size(), result.UniqueCount())

			if result.Size() > 1 {
				docs, err := result.GetDocuments()
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("All result documents:\n")
				for i, doc := range docs {
					fmt.Printf("  [%d]: %+v\n", i, doc)
				}
			}

			// Should be exactly 1: one combination of (doc1, doc2, doc3)
			Expect(result.Size()).To(Equal(1), "Cartesian of 1×1×1 should be 1")
		})
	})

	Context("Incremental State Inspection", func() {
		It("should inspect the incremental join's internal state", func() {
			inputs := []string{"input_0", "input_1", "input_2"}
			evaluator := NewNaryJoin("id", inputs)
			incrementalJoin := NewIncrementalJoin(evaluator, inputs)

			doc := Document{"id": int64(1), "name": "test"}
			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("\nIncremental state inspection:\n")
			// fmt.Printf("Initial state - prevStates length: %d\n", len(incrementalJoin.prevStates))

			// // Check initial state
			// for i, state := range incrementalJoin.prevStates {
			// 	if state == nil {
			// 		fmt.Printf("  prevStates[%d]: nil\n", i)
			// 	} else {
			// 		fmt.Printf("  prevStates[%d]: size=%d\n", i, state.Size())
			// 	}
			// }

			// Process first call
			// result, err := incrementalJoin.Process(delta, NewDocumentZSet(), NewDocumentZSet())
			_, err = incrementalJoin.Process(delta, NewDocumentZSet(), NewDocumentZSet())
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("After first call:\n")
			// fmt.Printf("  Result size: %d\n", result.Size())
			// for i, state := range incrementalJoin.prevStates {
			// 	fmt.Printf("  prevStates[%d]: size=%d\n", i, state.Size())
			// }
		})
	})

	Context("Incremental vs Snapshot Consistency (N-ary)", func() {
		var (
			snapshotJoin    *JoinOp
			incrementalJoin *IncrementalJoinOp
			evaluator       *NaryJoinEvaluator
			inputs          []string
		)

		BeforeEach(func() {
			inputs = []string{"input_0", "input_1", "input_2"}
			evaluator = NewNaryJoin("id", inputs)
			snapshotJoin = NewJoin(evaluator, inputs)
			incrementalJoin = NewIncrementalJoin(evaluator, inputs)
		})

		It("should produce consistent results for 3-way incremental join", func() {
			cumulativeSnapshot := NewDocumentZSet() // Running snapshot

			// Test data
			user1 := Document{"id": int64(1), "name": "Alice"}
			user2 := Document{"id": int64(2), "name": "Bob"}

			proj1 := Document{"id": int64(1), "title": "WebApp"}
			proj2 := Document{"id": int64(2), "title": "MobileApp"}

			dept1 := Document{"id": int64(1), "name": "Engineering"}
			dept2 := Document{"id": int64(2), "name": "Design"}

			// Timeline of changes
			// T1: Add user1, proj1, dept1 (should create 1 join)
			userDelta1, err := SingletonZSet(user1)
			Expect(err).NotTo(HaveOccurred())
			projDelta1, err := SingletonZSet(proj1)
			Expect(err).NotTo(HaveOccurred())
			deptDelta1, err := SingletonZSet(dept1)
			Expect(err).NotTo(HaveOccurred())

			// Snapshot approach
			snapshotResult, err := snapshotJoin.Process(userDelta1, projDelta1, deptDelta1)
			Expect(err).NotTo(HaveOccurred())

			// Incremental approach
			incrementalResult, err := incrementalJoin.Process(userDelta1, projDelta1, deptDelta1)
			Expect(err).NotTo(HaveOccurred())

			// fmt.Printf("Snapshot result in timestep 1: %s\n", snapshotResult.String())
			// fmt.Printf("Incremental result in timestep 1: %s\n", incrementalResult.String())

			// Add incremental delta to cumulative snapshot
			cumulativeSnapshot, err = cumulativeSnapshot.Add(incrementalResult)
			Expect(err).NotTo(HaveOccurred())

			// Should be identical
			Expect(cumulativeSnapshot.Size()).To(Equal(snapshotResult.Size()))
			Expect(cumulativeSnapshot.UniqueCount()).To(Equal(snapshotResult.UniqueCount()))
			Expect(snapshotResult.Size()).To(Equal(1)) // One 3-way join

			// T2: Add user2, proj2, dept2 (should create another join)
			userDelta2, err := SingletonZSet(user2)
			Expect(err).NotTo(HaveOccurred())
			projDelta2, err := SingletonZSet(proj2)
			Expect(err).NotTo(HaveOccurred())
			deptDelta2, err := SingletonZSet(dept2)
			Expect(err).NotTo(HaveOccurred())

			// Snapshot: join all accumulated data
			allUsers, err := userDelta1.Add(userDelta2)
			Expect(err).NotTo(HaveOccurred())
			allProjs, err := projDelta1.Add(projDelta2)
			Expect(err).NotTo(HaveOccurred())
			allDepts, err := deptDelta1.Add(deptDelta2)
			Expect(err).NotTo(HaveOccurred())

			snapshotResult2, err := snapshotJoin.Process(allUsers, allProjs, allDepts)
			Expect(err).NotTo(HaveOccurred())

			// Incremental: process delta
			incrementalResult2, err := incrementalJoin.Process(userDelta2, projDelta2, deptDelta2)
			Expect(err).NotTo(HaveOccurred())

			// Add incremental delta to cumulative snapshot
			_, err = cumulativeSnapshot.Add(incrementalResult)
			Expect(err).NotTo(HaveOccurred())

			// Snapshot should have 2 joins total
			Expect(snapshotResult2.Size()).To(Equal(2))

			// Incremental should produce 1 new join (the delta)
			Expect(incrementalResult2.Size()).To(Equal(1))
		})
	})

	Context("Edge Cases", func() {
		var (
			snapshotJoin *JoinOp
			evaluator    *NaryJoinEvaluator
			inputs       []string
		)

		BeforeEach(func() {
			inputs = []string{"users", "projects", "departments"}
			evaluator = NewNaryJoin("id", inputs)
			snapshotJoin = NewJoin(evaluator, inputs)
		})

		It("should handle documents missing join keys", func() {
			user := Document{"id": int64(1), "name": "Alice"}
			userZSet, err := SingletonZSet(user)
			Expect(err).NotTo(HaveOccurred())

			// Project missing "id" field
			project := Document{"title": "WebApp", "status": "active"}
			projectZSet, err := SingletonZSet(project)
			Expect(err).NotTo(HaveOccurred())

			dept := Document{"id": int64(1), "name": "Engineering"}
			deptZSet, err := SingletonZSet(dept)
			Expect(err).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(userZSet, projectZSet, deptZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle negative multiplicities in N-way join", func() {
			user := Document{"id": int64(1), "name": "Alice"}
			userZSet := NewDocumentZSet()
			Expect(userZSet.AddDocumentMutate(user, -1)).NotTo(HaveOccurred()) // Deletion

			project := Document{"id": int64(1), "title": "WebApp"}
			projectZSet := NewDocumentZSet()
			Expect(projectZSet.AddDocumentMutate(project, 2)).NotTo(HaveOccurred())

			dept := Document{"id": int64(1), "name": "Engineering"}
			deptZSet := NewDocumentZSet()
			Expect(deptZSet.AddDocumentMutate(dept, 3)).NotTo(HaveOccurred())

			result, err := snapshotJoin.Process(userZSet, projectZSet, deptZSet)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity (-1) × 2 × 3 = -6
			expectedJoin := Document{
				"join_key":      int64(1),
				"input_0_id":    int64(1),
				"input_0_name":  "Alice",
				"input_1_id":    int64(1),
				"input_1_title": "WebApp",
				"input_2_id":    int64(1),
				"input_2_name":  "Engineering",
			}
			mult, err := result.GetMultiplicity(expectedJoin)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-6))
		})

		It("should validate arity correctly", func() {
			// Try to call 3-way join with wrong number of inputs
			user := Document{"id": int64(1), "name": "Alice"}
			userZSet, err := SingletonZSet(user)
			Expect(err).NotTo(HaveOccurred())

			project := Document{"id": int64(1), "title": "WebApp"}
			projectZSet, err := SingletonZSet(project)
			Expect(err).NotTo(HaveOccurred())

			// Only 2 inputs instead of 3
			_, err = snapshotJoin.Process(userZSet, projectZSet)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("expects 3 inputs, got 2"))
		})
	})

	Context("Operator Properties (N-ary)", func() {
		var joinOp *JoinOp

		BeforeEach(func() {
			inputs := []string{"input_0", "input_1", "input_2"}
			evaluator := NewNaryJoin("id", inputs)
			joinOp = NewJoin(evaluator, inputs)
		})

		It("should have correct operator properties", func() {
			Expect(joinOp.OpType()).To(Equal(OpTypeBilinear))
			Expect(joinOp.IsTimeInvariant()).To(BeTrue())
			Expect(joinOp.HasZeroPreservationProperty()).To(BeTrue())
		})

		It("should preserve associativity property conceptually", func() {
			// While we can't easily test mathematical associativity with our current
			// implementation, we can verify that different groupings of the same
			// data produce equivalent results through multiple binary joins

			user := Document{"id": int64(1), "name": "Alice"}
			project := Document{"id": int64(1), "title": "WebApp"}
			dept := Document{"id": int64(1), "name": "Engineering"}

			userZSet, err := SingletonZSet(user)
			Expect(err).NotTo(HaveOccurred())
			projectZSet, err := SingletonZSet(project)
			Expect(err).NotTo(HaveOccurred())
			deptZSet, err := SingletonZSet(dept)
			Expect(err).NotTo(HaveOccurred())

			// 3-way join should produce exactly one result
			result, err := joinOp.Process(userZSet, projectZSet, deptZSet)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))
			Expect(result.UniqueCount()).To(Equal(1))
		})
	})
})
