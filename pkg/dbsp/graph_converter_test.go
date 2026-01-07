package dbsp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GraphConverter", func() {
	Context("ToSnapshotGraph", func() {
		It("should convert incremental join to snapshot join", func() {
			// Build incremental graph with IncrementalJoinOp.
			inputs := []string{"users", "projects"}
			incrementalGraph := NewChainGraph()
			incrementalGraph.AddInput(NewInput(inputs[0]))
			incrementalGraph.AddInput(NewInput(inputs[1]))
			incrementalGraph.SetJoin(NewIncrementalJoin(NewFlexibleJoin("id", inputs), inputs))

			// Convert to snapshot.
			snapshotGraph, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify join was converted.
			Expect(snapshotGraph.joinNode).NotTo(BeEmpty())
			joinOp := snapshotGraph.nodes[snapshotGraph.joinNode].Op
			Expect(joinOp).To(BeAssignableToTypeOf(&JoinOp{}))
		})

		It("should convert incremental binary join to snapshot binary join", func() {
			// Build incremental graph with IncrementalBinaryJoinOp.
			inputs := []string{"users", "projects"}
			incrementalGraph := NewChainGraph()
			incrementalGraph.AddInput(NewInput(inputs[0]))
			incrementalGraph.AddInput(NewInput(inputs[1]))
			incrementalGraph.SetJoin(NewIncrementalBinaryJoin(NewFlexibleJoin("id", inputs), inputs))

			// Convert to snapshot.
			snapshotGraph, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify join was converted.
			Expect(snapshotGraph.joinNode).NotTo(BeEmpty())
			joinOp := snapshotGraph.nodes[snapshotGraph.joinNode].Op
			Expect(joinOp).To(BeAssignableToTypeOf(&BinaryJoinOp{}))
		})

		It("should preserve snapshot gather", func() {
			// Build incremental graph with GatherOp.
			incrementalGraph := NewChainGraph()
			incrementalGraph.AddInput(NewInput("sales"))
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			incrementalGraph.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert to snapshot.
			snapshotGraph, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify gather was converted.
			Expect(snapshotGraph.chain).To(HaveLen(1))
			gatherOp := snapshotGraph.nodes[snapshotGraph.chain[0]].Op
			Expect(gatherOp).To(BeAssignableToTypeOf(&GatherOp{}))
		})

		It("should preserve linear operators", func() {
			// Build incremental graph with linear operators.
			incrementalGraph := NewChainGraph()
			incrementalGraph.AddInput(NewInput("users"))
			incrementalGraph.AddToChain(NewProjection(NewFieldProjection("name", "age")))
			incrementalGraph.AddToChain(NewSelection(NewRangeFilter("age", 18, 100)))
			incrementalGraph.AddToChain(NewDistinct())

			// Convert to snapshot.
			snapshotGraph, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify all operators preserved.
			Expect(snapshotGraph.chain).To(HaveLen(3))
			Expect(snapshotGraph.nodes[snapshotGraph.chain[0]].Op).To(BeAssignableToTypeOf(&ProjectionOp{}))
			Expect(snapshotGraph.nodes[snapshotGraph.chain[1]].Op).To(BeAssignableToTypeOf(&SelectionOp{}))
			Expect(snapshotGraph.nodes[snapshotGraph.chain[2]].Op).To(BeAssignableToTypeOf(&DistinctOp{}))
		})

		It("should reject graphs with structural operators", func() {
			// Build incremental graph with IntegratorOp.
			incrementalGraph := NewChainGraph()
			incrementalGraph.AddInput(NewInput("stream"))
			incrementalGraph.AddToChain(NewIntegrator())

			// Should fail to convert.
			_, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("IntegratorOp cannot be converted"))
		})

		It("should handle complex incremental pipeline", func() {
			// Build complex incremental pipeline.
			inputs := []string{"users", "projects"}
			incrementalGraph := NewChainGraph()
			incrementalGraph.AddInput(NewInput(inputs[0]))
			incrementalGraph.AddInput(NewInput(inputs[1]))
			incrementalGraph.SetJoin(NewIncrementalBinaryJoin(NewFlexibleJoin("user_id", inputs), inputs))
			incrementalGraph.AddToChain(NewProjection(NewFieldProjection("left_name", "right_amount")))
			incrementalGraph.AddToChain(NewSelection(NewRangeFilter("right_amount", 1000, 10000)))
			keyExt, valueExt, aggregator := createGatherEvaluators("left_name", "right_amount", "name", "amounts")
			incrementalGraph.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert to snapshot.
			snapshotGraph, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify structure.
			Expect(snapshotGraph.Arity()).To(Equal(2))
			Expect(snapshotGraph.joinNode).NotTo(BeEmpty())
			Expect(snapshotGraph.chain).To(HaveLen(3))

			// Verify operators converted correctly.
			joinOp := snapshotGraph.nodes[snapshotGraph.joinNode].Op
			Expect(joinOp).To(BeAssignableToTypeOf(&BinaryJoinOp{}))

			gatherOp := snapshotGraph.nodes[snapshotGraph.chain[2]].Op
			Expect(gatherOp).To(BeAssignableToTypeOf(&GatherOp{}))
		})

		It("should handle already-snapshot operators gracefully", func() {
			// Build graph that's already snapshot.
			snapshotGraph1 := NewChainGraph()
			snapshotGraph1.AddInput(NewInput("sales"))
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			snapshotGraph1.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert again (should be idempotent).
			snapshotGraph2, err := ToSnapshotGraph(snapshotGraph1)
			Expect(err).NotTo(HaveOccurred())

			// Should still have snapshot operators.
			gatherOp := snapshotGraph2.nodes[snapshotGraph2.chain[0]].Op
			Expect(gatherOp).To(BeAssignableToTypeOf(&GatherOp{}))
		})
	})

	Context("ToIncrementalGraph", func() {
		It("should convert snapshot join to incremental join", func() {
			// Build snapshot graph with JoinOp.
			inputs := []string{"users", "projects"}
			snapshotGraph := NewChainGraph()
			snapshotGraph.AddInput(NewInput(inputs[0]))
			snapshotGraph.AddInput(NewInput(inputs[1]))
			snapshotGraph.SetJoin(NewJoin(NewFlexibleJoin("id", inputs), inputs))

			// Convert to incremental.
			incrementalGraph, err := ToIncrementalGraph(snapshotGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify join was converted.
			Expect(incrementalGraph.joinNode).NotTo(BeEmpty())
			joinOp := incrementalGraph.nodes[incrementalGraph.joinNode].Op
			Expect(joinOp).To(BeAssignableToTypeOf(&IncrementalJoinOp{}))
		})

		It("should convert snapshot binary join to incremental binary join", func() {
			// Build snapshot graph with BinaryJoinOp.
			inputs := []string{"users", "projects"}
			snapshotGraph := NewChainGraph()
			snapshotGraph.AddInput(NewInput(inputs[0]))
			snapshotGraph.AddInput(NewInput(inputs[1]))
			snapshotGraph.SetJoin(NewBinaryJoin(NewFlexibleJoin("id", inputs), inputs))

			// Convert to incremental.
			incrementalGraph, err := ToIncrementalGraph(snapshotGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify join was converted.
			Expect(incrementalGraph.joinNode).NotTo(BeEmpty())
			joinOp := incrementalGraph.nodes[incrementalGraph.joinNode].Op
			Expect(joinOp).To(BeAssignableToTypeOf(&IncrementalBinaryJoinOp{}))
		})

		It("should preserve snapshot gather", func() {
			// Build snapshot graph with GatherOp.
			snapshotGraph := NewChainGraph()
			snapshotGraph.AddInput(NewInput("sales"))
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			snapshotGraph.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert to incremental.
			incrementalGraph, err := ToIncrementalGraph(snapshotGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify gather was converted.
			Expect(incrementalGraph.chain).To(HaveLen(1))
			gatherOp := incrementalGraph.nodes[incrementalGraph.chain[0]].Op
			Expect(gatherOp).To(BeAssignableToTypeOf(&GatherOp{}))
		})

		It("should preserve linear operators", func() {
			// Build snapshot graph with linear operators.
			snapshotGraph := NewChainGraph()
			snapshotGraph.AddInput(NewInput("users"))
			snapshotGraph.AddToChain(NewProjection(NewFieldProjection("name", "age")))
			snapshotGraph.AddToChain(NewSelection(NewRangeFilter("age", 18, 100)))
			snapshotGraph.AddToChain(NewDistinct())

			// Convert to incremental.
			incrementalGraph, err := ToIncrementalGraph(snapshotGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify all operators preserved.
			Expect(incrementalGraph.chain).To(HaveLen(3))
			Expect(incrementalGraph.nodes[incrementalGraph.chain[0]].Op).To(BeAssignableToTypeOf(&ProjectionOp{}))
			Expect(incrementalGraph.nodes[incrementalGraph.chain[1]].Op).To(BeAssignableToTypeOf(&SelectionOp{}))
			Expect(incrementalGraph.nodes[incrementalGraph.chain[2]].Op).To(BeAssignableToTypeOf(&DistinctOp{}))
		})
	})

	Context("Round-trip conversion", func() {
		It("should handle incremental -> snapshot -> incremental round-trip", func() {
			// Build incremental graph.
			inputs := []string{"users", "projects"}
			incrementalGraph1 := NewChainGraph()
			incrementalGraph1.AddInput(NewInput(inputs[0]))
			incrementalGraph1.AddInput(NewInput(inputs[1]))
			incrementalGraph1.SetJoin(NewIncrementalBinaryJoin(NewFlexibleJoin("id", inputs), inputs))
			incrementalGraph1.AddToChain(NewProjection(NewFieldProjection("left_name", "right_title")))
			keyExt, valueExt, aggregator := createGatherEvaluators("left_name", "right_title", "name", "titles")
			incrementalGraph1.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert to snapshot.
			snapshotGraph, err := ToSnapshotGraph(incrementalGraph1)
			Expect(err).NotTo(HaveOccurred())

			// Convert back to incremental.
			incrementalGraph2, err := ToIncrementalGraph(snapshotGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify structure preserved.
			Expect(incrementalGraph2.Arity()).To(Equal(incrementalGraph1.Arity()))
			Expect(incrementalGraph2.joinNode).NotTo(BeEmpty())
			Expect(incrementalGraph2.chain).To(HaveLen(len(incrementalGraph1.chain)))

			// Verify operator types match original.
			joinOp := incrementalGraph2.nodes[incrementalGraph2.joinNode].Op
			Expect(joinOp).To(BeAssignableToTypeOf(&IncrementalBinaryJoinOp{}))

			gatherOp := incrementalGraph2.nodes[incrementalGraph2.chain[1]].Op
			Expect(gatherOp).To(BeAssignableToTypeOf(&GatherOp{}))
		})

		It("should handle snapshot -> incremental -> snapshot round-trip", func() {
			// Build snapshot graph.
			snapshotGraph1 := NewChainGraph()
			snapshotGraph1.AddInput(NewInput("sales"))
			snapshotGraph1.AddToChain(NewProjection(NewFieldProjection("dept", "amount")))
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			snapshotGraph1.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert to incremental.
			incrementalGraph, err := ToIncrementalGraph(snapshotGraph1)
			Expect(err).NotTo(HaveOccurred())

			// Convert back to snapshot.
			snapshotGraph2, err := ToSnapshotGraph(incrementalGraph)
			Expect(err).NotTo(HaveOccurred())

			// Verify structure preserved.
			Expect(snapshotGraph2.Arity()).To(Equal(snapshotGraph1.Arity()))
			Expect(snapshotGraph2.chain).To(HaveLen(len(snapshotGraph1.chain)))

			// Verify operator types match original.
			gatherOp := snapshotGraph2.nodes[snapshotGraph2.chain[1]].Op
			Expect(gatherOp).To(BeAssignableToTypeOf(&GatherOp{}))
		})
	})

	Context("Functional equivalence", func() {
		It("should produce equivalent results for snapshot and incremental gather", func() {
			// Build snapshot graph.
			snapshotGraph := NewChainGraph()
			snapshotGraph.AddInput(NewInput("sales"))
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			snapshotGraph.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Convert to incremental.
			incrementalGraph, err := ToIncrementalGraph(snapshotGraph)
			Expect(err).NotTo(HaveOccurred())

			// Create test data.
			sale1, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
			Expect(err).NotTo(HaveOccurred())
			sale2, err := newDocumentFromPairs("dept", "Engineering", "amount", int64(1500))
			Expect(err).NotTo(HaveOccurred())
			sale3, err := newDocumentFromPairs("dept", "Marketing", "amount", int64(800))
			Expect(err).NotTo(HaveOccurred())

			stateSales := NewDocumentZSet()
			err = stateSales.AddDocumentMutate(sale1, 1)
			Expect(err).NotTo(HaveOccurred())
			err = stateSales.AddDocumentMutate(sale2, 1)
			Expect(err).NotTo(HaveOccurred())
			err = stateSales.AddDocumentMutate(sale3, 1)
			Expect(err).NotTo(HaveOccurred())

			// Execute with snapshot executor.
			snapshotExecutor, err := NewSnapshotExecutor(snapshotGraph, logger)
			Expect(err).NotTo(HaveOccurred())

			inputs := map[string]*DocumentZSet{
				snapshotGraph.inputIdx[snapshotGraph.inputs[0]]: stateSales,
			}
			snapshotResult, err := snapshotExecutor.Process(inputs)
			Expect(err).NotTo(HaveOccurred())

			// Execute with incremental executor (processing full state as single delta).
			incrementalExecutor, err := NewExecutor(incrementalGraph, logger)
			Expect(err).NotTo(HaveOccurred())

			incrementalInputs := map[string]*DocumentZSet{
				incrementalGraph.inputIdx[incrementalGraph.inputs[0]]: stateSales,
			}
			incrementalResult, err := incrementalExecutor.Process(incrementalInputs)
			Expect(err).NotTo(HaveOccurred())

			// Results should be identical.
			Expect(snapshotResult.Size()).To(Equal(incrementalResult.Size()))
			Expect(snapshotResult.UniqueCount()).To(Equal(incrementalResult.UniqueCount()))

			snapshotDocs, err := snapshotResult.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())
			incrementalDocs, err := incrementalResult.GetUniqueDocuments()
			Expect(err).NotTo(HaveOccurred())

			// Compare documents (order may differ, so convert to map).
			snapshotMap := make(map[string]Document)
			for _, doc := range snapshotDocs {
				dept := doc["department"].(string)
				snapshotMap[dept] = doc
			}

			incrementalMap := make(map[string]Document)
			for _, doc := range incrementalDocs {
				dept := doc["department"].(string)
				incrementalMap[dept] = doc
			}

			Expect(snapshotMap).To(HaveLen(len(incrementalMap)))
			for dept, snapshotDoc := range snapshotMap {
				incrementalDoc, ok := incrementalMap[dept]
				Expect(ok).To(BeTrue(), "Department %s not found in incremental result", dept)

				// Compare amounts lists (order-independent).
				snapshotAmounts := snapshotDoc["amounts"].([]any)
				incrementalAmounts := incrementalDoc["amounts"].([]any)
				Expect(snapshotAmounts).To(ConsistOf(incrementalAmounts))
			}
		})
	})
})
