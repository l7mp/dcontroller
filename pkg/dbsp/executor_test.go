package dbsp

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LinearChainExecutor", func() {
	var (
		executor *LinearChainExecutor
		graph    *LinearChainGraph
		rewriter *LinearChainRewriteEngine
	)

	BeforeEach(func() {
		graph = NewLinearChainGraph()
		rewriter = NewLinearChainRewriteEngine()
	})

	Context("Single Input Pipeline", func() {
		BeforeEach(func() {
			// Build: Input -> Project -> Select -> Output
			graph.AddInput(NewInput("users"))
			graph.AddToChain(NewProjection(NewFieldProjection("name", "age")))
			graph.AddToChain(NewSelection("adults", NewRangeFilter("age", 18, 100)))

			// Optimize and create executor
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should process single delta correctly", func() {
			// Create test data
			alice, err := newDocumentFromPairs("name", "Alice", "age", int64(25), "city", "NYC")
			Expect(err).NotTo(HaveOccurred())

			bob, err := newDocumentFromPairs("name", "Bob", "age", int64(17), "city", "LA")
			Expect(err).NotTo(HaveOccurred())

			charlie, err := newDocumentFromPairs("name", "Charlie", "age", int64(30), "city", "SF")
			Expect(err).NotTo(HaveOccurred())

			// Create delta input
			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaUsers.AddDocumentMutate(bob, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaUsers.AddDocumentMutate(charlie, 1)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
			}

			// Execute
			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 adults (Alice, Charlie) after projection and selection
			Expect(result.Size()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Verify projected fields only (name, age)
			for _, doc := range docs {
				Expect(doc).To(HaveKey("name"))
				Expect(doc).To(HaveKey("age"))
				Expect(doc).NotTo(HaveKey("city")) // Should be projected out

				// Verify age filter (adults only)
				age := doc["age"].(int64)
				Expect(age).To(BeNumerically(">=", 18))
			}

			// Verify specific people
			names := make([]string, 0)
			for _, doc := range docs {
				names = append(names, doc["name"].(string))
			}
			Expect(names).To(ConsistOf("Alice", "Charlie"))
		})

		It("should handle empty deltas", func() {
			emptyDelta := NewDocumentZSet()
			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: emptyDelta,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle negative multiplicities (deletions)", func() {
			alice, err := newDocumentFromPairs("name", "Alice", "age", int64(25))
			Expect(err).NotTo(HaveOccurred())

			// Delta with deletion
			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, -1) // Remove Alice
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should have removal result
			Expect(result.IsZero()).To(BeFalse())

			// Check that it's a removal (negative multiplicity)
			expectedDoc := Document{"name": "Alice", "age": int64(25)}
			mult, err := result.GetMultiplicity(expectedDoc)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(-1))
		})

		It("should handle high multiplicities", func() {
			alice, err := newDocumentFromPairs("name", "Alice", "age", int64(25))
			Expect(err).NotTo(HaveOccurred())

			// Delta with high multiplicity
			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, 100)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should preserve multiplicity through pipeline
			Expect(result.Size()).To(Equal(100))
			Expect(result.UniqueCount()).To(Equal(1))
		})
	})

	Context("Binary Join Pipeline", func() {
		BeforeEach(func() {
			// Build: Users + Projects -> Join -> Project -> Output
			graph.AddInput(NewInput("users"))
			graph.AddInput(NewInput("projects"))
			graph.SetJoin(NewBinaryJoin(NewFlexibleJoin("id")))
			graph.AddToChain(NewProjection(NewFieldProjection("left_name", "right_title")))

			// Optimize and create executor
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle matching join", func() {
			// Users
			alice, err := newDocumentFromPairs("id", int64(1), "name", "Alice")
			Expect(err).NotTo(HaveOccurred())
			bob, err := newDocumentFromPairs("id", int64(2), "name", "Bob")
			Expect(err).NotTo(HaveOccurred())

			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaUsers.AddDocumentMutate(bob, 1)
			Expect(err).NotTo(HaveOccurred())

			// Projects
			proj1, err := newDocumentFromPairs("id", int64(1), "title", "WebApp")
			Expect(err).NotTo(HaveOccurred())
			proj3, err := newDocumentFromPairs("id", int64(3), "title", "MobileApp")
			Expect(err).NotTo(HaveOccurred())

			deltaProjects := NewDocumentZSet()
			err = deltaProjects.AddDocumentMutate(proj1, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaProjects.AddDocumentMutate(proj3, 1)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,    // users
				graph.inputs[1]: deltaProjects, // projects
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should have 1 join (Alice-WebApp, both have id=1)
			Expect(result.Size()).To(Equal(1))

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			joinedDoc := docs[0]

			Expect(joinedDoc["left_name"]).To(Equal("Alice"))
			Expect(joinedDoc["right_title"]).To(Equal("WebApp"))
		})

		It("should handle no matches in join", func() {
			// Users with id=1
			alice, err := newDocumentFromPairs("id", int64(1), "name", "Alice")
			Expect(err).NotTo(HaveOccurred())
			deltaUsers, err := SingletonZSet(alice)
			Expect(err).NotTo(HaveOccurred())

			// Projects with id=2 (no match)
			proj2, err := newDocumentFromPairs("id", int64(2), "title", "MobileApp")
			Expect(err).NotTo(HaveOccurred())
			deltaProjects, err := SingletonZSet(proj2)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
				graph.inputs[1]: deltaProjects,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// No matches = empty result
			Expect(result.IsZero()).To(BeTrue())
		})

		It("should handle multiplicity correctly in joins (bilinear property)", func() {
			// User with multiplicity 3
			alice, err := newDocumentFromPairs("id", int64(1), "name", "Alice")
			Expect(err).NotTo(HaveOccurred())
			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, 3)
			Expect(err).NotTo(HaveOccurred())

			// Project with multiplicity 2
			proj1, err := newDocumentFromPairs("id", int64(1), "title", "WebApp")
			Expect(err).NotTo(HaveOccurred())
			deltaProjects := NewDocumentZSet()
			err = deltaProjects.AddDocumentMutate(proj1, 2)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
				graph.inputs[1]: deltaProjects,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Result should have multiplicity 3 × 2 = 6
			Expect(result.Size()).To(Equal(6))
			Expect(result.UniqueCount()).To(Equal(1))
		})
	})

	Context("Incremental Join Behavior", func() {
		var incrementalContext *IncrementalExecutionContext

		BeforeEach(func() {
			// Build join pipeline
			graph.AddInput(NewInput("users"))
			graph.AddInput(NewInput("projects"))
			graph.SetJoin(NewBinaryJoin(NewFlexibleJoin("id")))

			// Optimize for incremental execution
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			incrementalContext = NewIncrementalExecutionContext(executor)
		})

		It("should demonstrate incremental join behavior over multiple timesteps", func() {
			// Timestep 1: Add Alice and WebApp
			alice, err := newDocumentFromPairs("id", int64(1), "name", "Alice")
			Expect(err).NotTo(HaveOccurred())
			webApp, err := newDocumentFromPairs("id", int64(1), "title", "WebApp")
			Expect(err).NotTo(HaveOccurred())

			delta1Users, err := SingletonZSet(alice)
			Expect(err).NotTo(HaveOccurred())
			delta1Projects, err := SingletonZSet(webApp)
			Expect(err).NotTo(HaveOccurred())

			delta1Inputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta1Users,
				graph.inputs[1]: delta1Projects,
			}

			result1, err := incrementalContext.ProcessDelta(delta1Inputs)
			Expect(err).NotTo(HaveOccurred())

			// Should create Alice-WebApp join
			Expect(result1.Size()).To(Equal(1))

			// Timestep 2: Add Bob and MobileApp
			bob, err := newDocumentFromPairs("id", int64(2), "name", "Bob")
			Expect(err).NotTo(HaveOccurred())
			mobileApp, err := newDocumentFromPairs("id", int64(2), "title", "MobileApp")
			Expect(err).NotTo(HaveOccurred())

			delta2Users, err := SingletonZSet(bob)
			Expect(err).NotTo(HaveOccurred())
			delta2Projects, err := SingletonZSet(mobileApp)
			Expect(err).NotTo(HaveOccurred())

			delta2Inputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta2Users,
				graph.inputs[1]: delta2Projects,
			}

			result2, err := incrementalContext.ProcessDelta(delta2Inputs)
			Expect(err).NotTo(HaveOccurred())

			// Should create Bob-MobileApp join
			Expect(result2.Size()).To(Equal(1))

			// Cumulative should have both joins
			cumulative := incrementalContext.GetCumulativeOutput()
			Expect(cumulative.Size()).To(Equal(2))

			// Timestep 3: Add Charlie (user only, no matching project)
			charlie, err := newDocumentFromPairs("id", int64(3), "name", "Charlie")
			Expect(err).NotTo(HaveOccurred())

			delta3Users, err := SingletonZSet(charlie)
			Expect(err).NotTo(HaveOccurred())
			delta3Projects := NewDocumentZSet() // Empty

			delta3Inputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta3Users,
				graph.inputs[1]: delta3Projects,
			}

			result3, err := incrementalContext.ProcessDelta(delta3Inputs)
			Expect(err).NotTo(HaveOccurred())

			// Should not create any new joins
			Expect(result3.IsZero()).To(BeTrue())

			// Cumulative should still have 2 joins
			cumulative = incrementalContext.GetCumulativeOutput()
			Expect(cumulative.Size()).To(Equal(2))

			// Timestep 4: Remove Alice
			delta4Users := NewDocumentZSet()
			err = delta4Users.AddDocumentMutate(alice, -1) // Remove
			Expect(err).NotTo(HaveOccurred())
			delta4Projects := NewDocumentZSet() // Empty

			delta4Inputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta4Users,
				graph.inputs[1]: delta4Projects,
			}

			result4, err := incrementalContext.ProcessDelta(delta4Inputs)
			Expect(err).NotTo(HaveOccurred())

			// Should remove Alice-WebApp join
			Expect(result4.IsZero()).To(BeFalse())

			// Check for negative result (removal)
			docs, err := result4.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(docs)).To(BeNumerically(">", 0))

			// At least one document should have negative multiplicity
			hasNegative := false
			for _, doc := range docs {
				if doc.Multiplicity < 0 {
					hasNegative = true
					break
				}
			}
			Expect(hasNegative).To(BeTrue(), "Expected at least one document with negative multiplicity")
		})

		It("should handle complex incremental scenarios", func() {
			// Add matching pair
			alice, err := newDocumentFromPairs("id", int64(1), "name", "Alice")
			Expect(err).NotTo(HaveOccurred())
			webApp, err := newDocumentFromPairs("id", int64(1), "title", "WebApp")
			Expect(err).NotTo(HaveOccurred())

			deltaUsers, err := SingletonZSet(alice)
			Expect(err).NotTo(HaveOccurred())
			deltaProjects, err := SingletonZSet(webApp)
			Expect(err).NotTo(HaveOccurred())

			// Step 1: Add both
			result1, err := incrementalContext.ProcessDelta(map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
				graph.inputs[1]: deltaProjects,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result1.Size()).To(Equal(1))

			// Step 2: Add another project with same ID
			webApp2, err := newDocumentFromPairs("id", int64(1), "title", "WebApp2")
			Expect(err).NotTo(HaveOccurred())
			deltaProjects2, err := SingletonZSet(webApp2)
			Expect(err).NotTo(HaveOccurred())

			result2, err := incrementalContext.ProcessDelta(map[string]*DocumentZSet{
				graph.inputs[0]: NewDocumentZSet(), // No new users
				graph.inputs[1]: deltaProjects2,
			})
			Expect(err).NotTo(HaveOccurred())

			// Should create Alice-WebApp2 join (Alice was already in state)
			Expect(result2.Size()).To(Equal(1))

			// Cumulative should have 2 joins now
			cumulative := incrementalContext.GetCumulativeOutput()
			Expect(cumulative.Size()).To(Equal(2))
		})
	})

	Context("Gather Operations", func() {
		BeforeEach(func() {
			// Build: Input -> Gather -> Output
			graph.AddInput(NewInput("sales"))
			extractEval := NewGroupByKey("dept", "amount")
			setEval := NewAggregateList("department", "amounts")
			graph.AddToChain(NewGather(extractEval, setEval))

			// Optimize and create executor
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle basic gather operation", func() {
			// Sales data
			sale1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000), "rep", "Alice")
			Expect(err).NotTo(HaveOccurred())
			sale2, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1500), "rep", "Bob")
			Expect(err).NotTo(HaveOccurred())
			sale3, err := newDocumentFromPairs("dept", "Marketing", "amount", int64(800), "rep", "Charlie")
			Expect(err).NotTo(HaveOccurred())

			deltaSales := NewDocumentZSet()
			err = deltaSales.AddDocumentMutate(sale1, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaSales.AddDocumentMutate(sale2, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaSales.AddDocumentMutate(sale3, 1)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaSales,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 groups
			Expect(result.UniqueCount()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Verify groups
			foundEngineering := false
			foundMarketing := false

			for _, doc := range docs {
				dept := doc["department"]
				amounts := doc["amounts"].([]any)

				switch dept {
				case "Engineering":
					foundEngineering = true
					Expect(amounts).To(HaveLen(2))
					Expect(amounts).To(ConsistOf(int64(1000), int64(1500)))
				case "Marketing":
					foundMarketing = true
					Expect(amounts).To(HaveLen(1))
					Expect(amounts).To(ContainElement(int64(800)))
				}
			}

			Expect(foundEngineering).To(BeTrue())
			Expect(foundMarketing).To(BeTrue())
		})
	})

	Context("Complex Pipelines", func() {
		It("should handle join + project + select + gather pipeline", func() {
			// Build complex pipeline: Join -> Project -> Select -> Gather
			graph.AddInput(NewInput("users"))
			graph.AddInput(NewInput("sales"))
			graph.SetJoin(NewBinaryJoin(NewFlexibleJoin("user_id")))
			graph.AddToChain(NewProjection(NewFieldProjection("left_name", "right_amount", "right_dept")))
			graph.AddToChain(NewSelection("big_sales", NewRangeFilter("right_amount", 1000, 10000)))

			extractEval := NewGroupByKey("right_dept", "right_amount")
			setEval := NewAggregateList("department", "amounts")
			graph.AddToChain(NewGather(extractEval, setEval))

			// Optimize and create executor
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			// Test data
			alice, err := newDocumentFromPairs("user_id", int64(1), "name", "Alice")
			Expect(err).NotTo(HaveOccurred())
			bob, err := newDocumentFromPairs("user_id", int64(2), "name", "Bob")
			Expect(err).NotTo(HaveOccurred())

			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaUsers.AddDocumentMutate(bob, 1)
			Expect(err).NotTo(HaveOccurred())

			sale1, err := newDocumentFromPairs("user_id", int64(1), "amount", int64(1500), "dept", "Engineering")
			Expect(err).NotTo(HaveOccurred())
			sale2, err := newDocumentFromPairs("user_id", int64(1), "amount", int64(500), "dept", "Engineering") // Too small
			Expect(err).NotTo(HaveOccurred())
			sale3, err := newDocumentFromPairs("user_id", int64(2), "amount", int64(2000), "dept", "Marketing")
			Expect(err).NotTo(HaveOccurred())

			deltaSales := NewDocumentZSet()
			err = deltaSales.AddDocumentMutate(sale1, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaSales.AddDocumentMutate(sale2, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaSales.AddDocumentMutate(sale3, 1)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
				graph.inputs[1]: deltaSales,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should have 2 groups (Engineering: 1500, Marketing: 2000)
			// sale2 should be filtered out by selection (amount < 1000)
			Expect(result.UniqueCount()).To(Equal(2))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			for _, doc := range docs {
				dept := doc["department"]
				amounts := doc["amounts"].([]any)

				switch dept {
				case "Engineering":
					Expect(amounts).To(HaveLen(1))
					Expect(amounts[0]).To(Equal(int64(1500)))
				case "Marketing":
					Expect(amounts).To(HaveLen(1))
					Expect(amounts[0]).To(Equal(int64(2000)))
				default:
					Fail("Unexpected department: " + fmt.Sprintf("%v", dept))
				}
			}
		})
	})

	Context("Error Handling", func() {
		BeforeEach(func() {
			graph.AddInput(NewInput("users"))
			graph.AddToChain(NewProjection(NewFieldProjection("name")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle missing inputs", func() {
			// Don't provide required input
			deltaInputs := map[string]*DocumentZSet{}

			_, err := executor.ProcessDelta(deltaInputs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("expected 1 inputs, got 0"))
		})

		It("should handle wrong number of inputs", func() {
			// Provide wrong input ID
			deltaInputs := map[string]*DocumentZSet{
				"wrong_id": NewDocumentZSet(),
			}

			_, err := executor.ProcessDelta(deltaInputs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("missing input"))
		})

		It("should reject non-incremental graphs", func() {
			// Create non-optimized graph
			badGraph := NewLinearChainGraph()
			badGraph.AddInput(NewInput("users"))
			badGraph.AddInput(NewInput("projects"))
			badGraph.SetJoin(NewBinaryJoin(NewFlexibleJoin("id"))) // Non-incremental join

			// Should reject at executor creation
			_, err := NewLinearChainExecutor(badGraph)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not optimized for incremental execution"))
		})
	})

	Context("State Management", func() {
		var incrementalContext *IncrementalExecutionContext

		BeforeEach(func() {
			graph.AddInput(NewInput("data"))
			extractEval := NewGroupByKey("category", "value")
			setEval := NewAggregateList("category", "values")
			graph.AddToChain(NewGather(extractEval, setEval))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			incrementalContext = NewIncrementalExecutionContext(executor)
		})

		It("should reset state correctly", func() {
			// Add some data
			doc, err := newDocumentFromPairs("category", "A", "value", int64(100))
			Expect(err).NotTo(HaveOccurred())
			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			_, err = incrementalContext.ProcessDelta(map[string]*DocumentZSet{
				graph.inputs[0]: delta,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify state exists
			cumulative := incrementalContext.GetCumulativeOutput()
			Expect(cumulative.Size()).To(Equal(1))

			// Reset
			incrementalContext.Reset()

			// Verify state cleared
			cumulative = incrementalContext.GetCumulativeOutput()
			Expect(cumulative.IsZero()).To(BeTrue())
			Expect(incrementalContext.timestep).To(Equal(0))
		})

		It("should track cumulative state correctly", func() {
			doc1, err := newDocumentFromPairs("category", "A", "value", int64(100))
			Expect(err).NotTo(HaveOccurred())
			doc2, err := newDocumentFromPairs("category", "A", "value", int64(200))
			Expect(err).NotTo(HaveOccurred())

			// Step 1
			delta1, err := SingletonZSet(doc1)
			Expect(err).NotTo(HaveOccurred())
			_, err = incrementalContext.ProcessDelta(map[string]*DocumentZSet{
				graph.inputs[0]: delta1,
			})
			Expect(err).NotTo(HaveOccurred())

			cumulative1 := incrementalContext.GetCumulativeOutput()
			Expect(cumulative1.Size()).To(Equal(1))

			// Step 2
			delta2, err := SingletonZSet(doc2)
			Expect(err).NotTo(HaveOccurred())
			_, err = incrementalContext.ProcessDelta(map[string]*DocumentZSet{
				graph.inputs[0]: delta2,
			})
			Expect(err).NotTo(HaveOccurred())

			// Cumulative should reflect both additions
			cumulative2 := incrementalContext.GetCumulativeOutput()
			Expect(cumulative2.Size()).To(Equal(1)) // Same group, larger list

			// Verify the group contains both values
			docs, err := cumulative2.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			values := docs[0]["values"].([]any)
			Expect(values).To(HaveLen(2))
			Expect(values).To(ConsistOf(int64(100), int64(200)))
		})
	})

	Context("Debugging Support", func() {
		BeforeEach(func() {
			graph.AddInput(NewInput("users"))
			graph.AddToChain(NewProjection(NewFieldProjection("name")))
			graph.AddToChain(NewSelection("filter", NewFieldFilter("name", "Alice")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should provide execution plan", func() {
			plan := executor.GetExecutionPlan()
			Expect(plan).To(ContainSubstring("Execution Plan:"))
			Expect(plan).To(ContainSubstring("Inputs"))
			Expect(plan).To(ContainSubstring("users"))
		})

		It("should apply operator fusion", func() {
			alice, err := newDocumentFromPairs("name", "Alice", "age", int64(25))
			Expect(err).NotTo(HaveOccurred())
			bob, err := newDocumentFromPairs("name", "Bob", "age", int64(30))
			Expect(err).NotTo(HaveOccurred())

			deltaUsers := NewDocumentZSet()
			err = deltaUsers.AddDocumentMutate(alice, 1)
			Expect(err).NotTo(HaveOccurred())
			err = deltaUsers.AddDocumentMutate(bob, 1)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: deltaUsers,
			}

			Expect(len(graph.chain)).To(Equal(1)) // Fused operation

			// Get final result
			finalResult, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should have only Alice after selection
			Expect(finalResult.Size()).To(Equal(1))

			docs, err := finalResult.GetDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs[0]["name"]).To(Equal("Alice"))
		})
	})

	Context("Performance Characteristics", func() {
		It("should handle large deltas efficiently", func() {
			// Build simple pipeline
			graph.AddInput(NewInput("data"))
			graph.AddToChain(NewProjection(NewFieldProjection("id")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			// Create large delta
			largeDelta := NewDocumentZSet()
			for i := 0; i < 1000; i++ {
				doc, err := newDocumentFromPairs("id", int64(i), "data", fmt.Sprintf("item_%d", i))
				Expect(err).NotTo(HaveOccurred())
				largeDelta, err = largeDelta.AddDocument(doc, 1)
				Expect(err).NotTo(HaveOccurred())
			}

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: largeDelta,
			}

			// Should complete without error
			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should process all items
			Expect(result.Size()).To(Equal(1000))
		})

		It("should demonstrate incremental efficiency", func() {
			// Set up incremental gather to show state efficiency
			graph.AddInput(NewInput("events"))
			extractEval := NewGroupByKey("category", "value")
			setEval := NewAggregateList("category", "values")
			graph.AddToChain(NewGather(extractEval, setEval))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			context := NewIncrementalExecutionContext(executor)

			// Process many small deltas
			for i := 0; i < 100; i++ {
				doc, err := newDocumentFromPairs("category", "A", "value", int64(i))
				Expect(err).NotTo(HaveOccurred())
				delta, err := SingletonZSet(doc)
				Expect(err).NotTo(HaveOccurred())

				deltaInputs := map[string]*DocumentZSet{
					graph.inputs[0]: delta,
				}

				_, err = context.ProcessDelta(deltaInputs)
				Expect(err).NotTo(HaveOccurred())
			}

			// Final result should have all values in one group
			cumulative := context.GetCumulativeOutput()
			Expect(cumulative.Size()).To(Equal(1))

			docs, err := cumulative.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			values := docs[0]["values"].([]any)
			Expect(values).To(HaveLen(100))
		})
	})

	Context("Edge Cases and Robustness", func() {
		It("should handle documents with complex nested structures", func() {
			graph.AddInput(NewInput("complex"))
			graph.AddToChain(NewProjection(NewFieldProjection("metadata")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			// Complex nested document
			complexDoc, err := newDocumentFromPairs(
				"id", int64(1),
				"metadata", map[string]any{
					"tags": []any{"urgent", "customer"},
					"stats": map[string]any{
						"views": int64(42),
						"score": 3.14,
					},
				},
			)
			Expect(err).NotTo(HaveOccurred())

			delta, err := SingletonZSet(complexDoc)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			Expect(result.Size()).To(Equal(1))

			docs, err := result.GetDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Verify nested structure preserved
			metadata := docs[0]["metadata"].(map[string]any)
			tags := metadata["tags"].([]any)
			Expect(tags).To(ContainElement("urgent"))
			Expect(tags).To(ContainElement("customer"))
		})

		It("should handle mixed positive and negative multiplicities", func() {
			graph.AddInput(NewInput("data"))
			graph.AddToChain(NewProjection(NewFieldProjection("value")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			doc, err := newDocumentFromPairs("value", int64(42))
			Expect(err).NotTo(HaveOccurred())

			// Delta with mixed multiplicities
			mixedDelta := NewDocumentZSet()
			err = mixedDelta.AddDocumentMutate(doc, 5) // Add 5
			Expect(err).NotTo(HaveOccurred())
			err = mixedDelta.AddDocumentMutate(doc, -2) // Remove 2
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: mixedDelta,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Net effect should be +3
			expectedDoc := Document{"value": int64(42)}
			mult, err := result.GetMultiplicity(expectedDoc)
			Expect(err).NotTo(HaveOccurred())
			Expect(mult).To(Equal(3))
		})

		It("should handle operations that produce empty results", func() {
			graph.AddInput(NewInput("data"))
			graph.AddToChain(NewSelection("impossible", NewFieldFilter("nonexistent_field", "impossible_value")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			doc, err := newDocumentFromPairs("existing_field", "some_value")
			Expect(err).NotTo(HaveOccurred())
			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should produce empty result (selection filters everything out)
			Expect(result.IsZero()).To(BeTrue())
		})
	})

	Context("Integration with Rewrite Engine", func() {
		It("should work correctly with all optimization rules applied", func() {
			// Create a graph that exercises multiple optimization rules
			graph.AddInput(NewInput("stream"))

			// Add I->D pair (will be eliminated)
			graph.AddToChain(NewIntegrator())
			graph.AddToChain(NewDifferentiator())

			// Add redundant distincts (will be optimized)
			graph.AddToChain(NewDistinct())
			graph.AddToChain(NewDistinct())

			// Add fuseable operations (will be fused)
			graph.AddToChain(NewSelection("filter", NewFieldFilter("active", true)))
			graph.AddToChain(NewProjection(NewFieldProjection("name")))

			// Add gather (will be incrementalized)
			extractEval := NewGroupByKey("name", "name")
			setEval := NewAggregateList("name", "names")
			graph.AddToChain(NewGather(extractEval, setEval))

			originalChainLength := len(graph.chain)
			Expect(originalChainLength).To(Equal(7))

			// Apply all optimizations
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should be heavily optimized
			Expect(len(graph.chain)).To(BeNumerically("<", originalChainLength))

			// Should create valid executor
			executor, err = NewLinearChainExecutor(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should execute correctly
			doc, err := newDocumentFromPairs("active", true, "name", "Alice", "extra", "data")
			Expect(err).NotTo(HaveOccurred())
			delta, err := SingletonZSet(doc)
			Expect(err).NotTo(HaveOccurred())

			deltaInputs := map[string]*DocumentZSet{
				graph.inputs[0]: delta,
			}

			result, err := executor.ProcessDelta(deltaInputs)
			Expect(err).NotTo(HaveOccurred())

			// Should produce correct result
			Expect(result.Size()).To(Equal(1))

			docs, err := result.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))

			// Verify the gather result
			resultDoc := docs[0]
			Expect(resultDoc).To(HaveKey("name"))
			Expect(resultDoc).To(HaveKey("names"))
			Expect(resultDoc["name"]).To(Equal("Alice"))

			names := resultDoc["names"].([]any)
			Expect(names).To(HaveLen(1))
			Expect(names[0]).To(Equal("Alice"))
		})
	})
})
