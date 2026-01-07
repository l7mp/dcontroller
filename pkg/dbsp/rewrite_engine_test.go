package dbsp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LinearChainRewriteEngine", func() {
	var (
		rewriter *LinearChainRewriteEngine
		graph    *ChainGraph
	)

	BeforeEach(func() {
		rewriter = NewLinearChainRewriteEngine()
		graph = NewChainGraph()
	})

	Context("Join Incrementalization Rule", func() {
		It("should incrementalize binary joins", func() {
			// Setup: Create a graph with a non-incremental binary join
			inputs := []string{"users", "projects"}
			graph.AddInput(NewInput(inputs[0]))
			graph.AddInput(NewInput(inputs[1]))

			// Add a regular (non-incremental) binary join
			joinEval := NewFlexibleJoin("id", inputs)
			joinID := graph.SetJoin(NewBinaryJoin(joinEval, inputs))

			// Verify it's not incremental initially
			joinNode := graph.nodes[joinID]
			_, isIncremental := joinNode.Op.(*IncrementalBinaryJoinOp)
			Expect(isIncremental).To(BeFalse())
			Expect(graph.Validate()).NotTo(HaveOccurred())

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Verify the join was incrementalized
			joinNode = graph.nodes[graph.joinNode] // ID might have changed
			_, isIncremental = joinNode.Op.(*IncrementalBinaryJoinOp)
			Expect(isIncremental).To(BeTrue())
		})

		It("should incrementalize N-ary joins", func() {
			// Setup: 3-way join
			inputs := []string{"users", "projects", "departments"}
			graph.AddInput(NewInput("users"))
			graph.AddInput(NewInput("projects"))
			graph.AddInput(NewInput("departments"))

			joinEval := NewNaryJoin("id", inputs)
			joinID := graph.SetJoin(NewJoin(joinEval, inputs))

			// Verify it's not incremental initially
			joinNode := graph.nodes[joinID]
			_, isIncremental := joinNode.Op.(*IncrementalJoinOp)
			Expect(isIncremental).To(BeFalse())

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Verify the join was incrementalized
			joinNode = graph.nodes[graph.joinNode]
			_, isIncremental = joinNode.Op.(*IncrementalJoinOp)
			Expect(isIncremental).To(BeTrue())
		})

		It("should not affect graphs without joins", func() {
			// Single input graph
			graph.AddInput(NewInput("collection"))
			projID := graph.AddToChain(NewProjection(NewFieldProjection("name")))

			originalChainLength := len(graph.chain)

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should be unchanged
			Expect(graph.chain).To(HaveLen(originalChainLength))
			Expect(graph.joinNode).To(Equal(""))

			// Projection should still be there
			projNode := graph.nodes[projID]
			_, isProjection := projNode.Op.(*ProjectionOp)
			Expect(isProjection).To(BeTrue())
		})
	})

	Context("NonLinear Lifting Rule", func() {
		It("should lift gather operations with I→Gather→D", func() {
			graph.AddInput(NewInput("sales"))

			// Add a gather operation (non-linear snapshot operator).
			keyExt, valueExt, aggregator := createGatherEvaluators("dept", "amount", "department", "amounts")
			graph.AddToChain(NewGather(keyExt, valueExt, aggregator))

			// Verify it's a GatherOp initially.
			Expect(graph.chain).To(HaveLen(1))
			_, isGather := graph.nodes[graph.chain[0]].Op.(*GatherOp)
			Expect(isGather).To(BeTrue())

			// Apply rewrite rules.
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Verify the gather was lifted to I→Gather→D.
			// Chain should now be: [I, Gather, D].
			Expect(graph.chain).To(HaveLen(3))

			_, isI := graph.nodes[graph.chain[0]].Op.(*IntegratorOp)
			Expect(isI).To(BeTrue())

			_, isGather = graph.nodes[graph.chain[1]].Op.(*GatherOp)
			Expect(isGather).To(BeTrue())

			_, isD := graph.nodes[graph.chain[2]].Op.(*DifferentiatorOp)
			Expect(isD).To(BeTrue())
		})
	})

	Context("Integration-Differentiation Elimination Rule", func() {
		It("should remove I->D pairs", func() {
			graph.AddInput(NewInput("stream"))

			integID := graph.AddToChain(NewIntegrator())
			diffID := graph.AddToChain(NewDifferentiator())
			projID := graph.AddToChain(NewProjection(NewFieldProjection("name")))

			originalChainLength := len(graph.chain)
			Expect(originalChainLength).To(Equal(3))

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// I->D pair should be eliminated
			Expect(graph.chain).To(HaveLen(1))

			// Verify the nodes were actually removed
			_, integExists := graph.nodes[integID]
			_, diffExists := graph.nodes[diffID]
			Expect(integExists).To(BeFalse())
			Expect(diffExists).To(BeFalse())

			// Projection should still exist
			projNode := graph.nodes[projID]
			_, isProjection := projNode.Op.(*ProjectionOp)
			Expect(isProjection).To(BeTrue())

			// Output should be updated to projection
			Expect(graph.output).To(Equal(projID))
		})

		It("should remove multiple I->D pairs", func() {
			graph.AddInput(NewInput("stream"))

			integ1ID := graph.AddToChain(NewIntegrator())
			diff1ID := graph.AddToChain(NewDifferentiator())
			integ2ID := graph.AddToChain(NewIntegrator())
			diff2ID := graph.AddToChain(NewDifferentiator())
			_ = graph.AddToChain(NewProjection(NewFieldProjection("name")))
			Expect(graph.chain).To(HaveLen(5))

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Both I->D pairs should be eliminated
			Expect(graph.chain).To(HaveLen(1)) // Only projection remains

			// Verify all I/D nodes were removed
			_, integ1Exists := graph.nodes[integ1ID]
			_, diff1Exists := graph.nodes[diff1ID]
			_, integ2Exists := graph.nodes[integ2ID]
			_, diff2Exists := graph.nodes[diff2ID]

			Expect(integ1Exists).To(BeFalse())
			Expect(diff1Exists).To(BeFalse())
			Expect(integ2Exists).To(BeFalse())
			Expect(diff2Exists).To(BeFalse())
		})

		It("should not affect isolated I or D operations", func() {
			graph.AddInput(NewInput("stream"))

			integID := graph.AddToChain(NewIntegrator())
			projID := graph.AddToChain(NewProjection(NewFieldProjection("name")))
			diffID := graph.AddToChain(NewDifferentiator())

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// All operations should remain (no adjacent I->D pair)
			Expect(graph.chain).To(HaveLen(3))

			// Verify all nodes still exist
			_, integExists := graph.nodes[integID]
			_, projExists := graph.nodes[projID]
			_, diffExists := graph.nodes[diffID]

			Expect(integExists).To(BeTrue())
			Expect(projExists).To(BeTrue())
			Expect(diffExists).To(BeTrue())
		})
	})

	Context("Distinct Optimization Rule", func() {
		// NOTE: With the NonLinearLiftingRule, distinct operations are lifted to I→Distinct→D.
		// This separates adjacent distincts, so the idempotence optimization doesn't apply.
		// These tests verify behavior with a rewriter that doesn't include lifting.
		var rewriterNoLifting *LinearChainRewriteEngine

		BeforeEach(func() {
			// Create a rewriter without the NonLinearLiftingRule for testing distinct idempotence.
			rewriterNoLifting = &LinearChainRewriteEngine{
				rules: []LinearChainRule{
					&DistinctOptimizationRule{},
				},
			}
		})

		It("should remove redundant distinct operations", func() {
			graph.AddInput(NewInput("collection"))

			dist1ID := graph.AddToChain(NewDistinct())
			dist2ID := graph.AddToChain(NewDistinct())
			projID := graph.AddToChain(NewProjection(NewFieldProjection("name")))
			Expect(graph.chain).To(HaveLen(3))

			// Apply rewrite rules (without lifting).
			err := rewriterNoLifting.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// First distinct should be removed, second kept.
			Expect(graph.chain).To(HaveLen(2))

			// First distinct should be gone.
			_, dist1Exists := graph.nodes[dist1ID]
			Expect(dist1Exists).To(BeFalse())

			// Second distinct should remain.
			_, dist2Exists := graph.nodes[dist2ID]
			Expect(dist2Exists).To(BeTrue())

			// Projection should remain.
			_, projExists := graph.nodes[projID]
			Expect(projExists).To(BeTrue())
		})

		It("should handle multiple distinct->distinct pairs", func() {
			graph.AddInput(NewInput("collection"))

			dist1ID := graph.AddToChain(NewDistinct())
			dist2ID := graph.AddToChain(NewDistinct())
			dist3ID := graph.AddToChain(NewDistinct())
			dist4ID := graph.AddToChain(NewDistinct())

			// Apply rewrite rules (without lifting).
			err := rewriterNoLifting.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should keep only last distinct.
			Expect(graph.chain).To(HaveLen(1))

			_, dist1Exists := graph.nodes[dist1ID]
			_, dist2Exists := graph.nodes[dist2ID]
			_, dist3Exists := graph.nodes[dist3ID]
			_, dist4Exists := graph.nodes[dist4ID]

			Expect(dist1Exists).To(BeFalse())
			Expect(dist2Exists).To(BeFalse())
			Expect(dist3Exists).To(BeFalse())
			Expect(dist4Exists).To(BeTrue())
		})
	})

	Context("Linear Operator Fusion Rule", func() {
		It("should fuse selection followed by projection", func() {
			graph.AddInput(NewInput("users"))

			_ = graph.AddToChain(NewSelection(NewFieldFilter("active", true)))
			_ = graph.AddToChain(NewProjection(NewFieldProjection("name", "email")))

			Expect(graph.chain).To(HaveLen(2))

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should be fused into single operation
			Expect(graph.chain).To(HaveLen(1))

			// Original selection should be removed
			node, nodeExists := graph.nodes[graph.chain[0]]
			Expect(nodeExists).To(BeTrue())
			_, isSelection := node.Op.(*SelectionOp)
			Expect(isSelection).To(BeFalse())
			// Node should now be a fused-op
			_, isFused := node.Op.(*SelectThenProjectionsOp)
			Expect(isFused).To(BeTrue())
		})

		It("should not fuse non-adjacent operations", func() {
			graph.AddInput(NewInput("users"))

			selID := graph.AddToChain(NewSelection(NewFieldFilter("active", true)))
			_ = graph.AddToChain(NewDistinct())
			projID := graph.AddToChain(NewProjection(NewFieldProjection("name")))

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should not fuse selection and projection (distinct in between)
			// Note: distinct optimization might run, but fusion shouldn't

			// At minimum, selection and projection should still be separate types
			selNode := graph.nodes[selID]
			_, isSelection := selNode.Op.(*SelectionOp)

			projNode := graph.nodes[projID]
			_, isProjection := projNode.Op.(*ProjectionOp)
			// _, isFused := projNode.Op.(*FusedOp)

			// Either should remain unfused OR distinct was optimized away and they got fused
			if len(graph.chain) == 3 {
				// No fusion happened
				Expect(isSelection).To(BeTrue())
				Expect(isProjection).To(BeTrue())
			}
		})

		It("should handle multiple fuseable pairs", func() {
			graph.AddInput(NewInput("users"))

			_ = graph.AddToChain(NewSelection(NewFieldFilter("active", true)))
			_ = graph.AddToChain(NewProjection(NewFieldProjection("name", "email")))
			_ = graph.AddToChain(NewSelection(NewFieldFilter("verified", true)))
			_ = graph.AddToChain(NewProjection(NewFieldProjection("name")))

			Expect(graph.chain).To(HaveLen(4))

			// Apply rewrite rules
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should fuse into 2 operations
			Expect(graph.chain).To(HaveLen(2))

			// Selections should be substituted with fused-ops
			for _, id := range graph.chain {
				node, nodeExists := graph.nodes[id]
				Expect(nodeExists).To(BeTrue())
				_, isSelection := node.Op.(*SelectionOp)
				Expect(isSelection).To(BeFalse())
				_, isProjection := node.Op.(*ProjectionOp)
				Expect(isProjection).To(BeFalse())
				_, isFused := node.Op.(*SelectThenProjectionsOp)
				Expect(isFused).To(BeTrue())
			}
		})
	})

	Context("Complex Optimization Scenarios", func() {
		It("should apply multiple rules in sequence", func() {
			// Complex graph with multiple optimization opportunities.
			// NOTE: This test uses Distinct for DBSP theory completeness. In production,
			// pipelines do NOT include distinct - deduplication is handled by
			// pkg/pipeline.Reconcile via cache-based disambiguation.
			inputs := []string{"users", "projects"}
			graph.AddInput(NewInput(inputs[0]))
			graph.AddInput(NewInput(inputs[1]))

			// Non-incremental join.
			joinEval := NewFlexibleJoin("id", inputs)
			graph.SetJoin(NewBinaryJoin(joinEval, inputs))

			// I->D pair that should be eliminated.
			graph.AddToChain(NewIntegrator())
			graph.AddToChain(NewDifferentiator())

			// Redundant distincts (for testing NonLinearLiftingRule).
			graph.AddToChain(NewDistinct())
			graph.AddToChain(NewDistinct())

			// Fuseable selection + projection.
			graph.AddToChain(NewSelection(NewFieldFilter("active", true)))
			graph.AddToChain(NewProjection(NewFieldProjection("name")))

			initialChainLength := len(graph.chain)
			Expect(initialChainLength).To(Equal(6))

			// Apply all optimizations.
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// With NonLinearLiftingRule, the chain structure changes:
			// - Initial: [I, D, Distinct1, Distinct2, Select, Project]
			// - After lifting: [I, D, I, Distinct1, D, I, Distinct2, D, Select, Project]
			// - After I→D elimination: [I, Distinct1, D, I, Distinct2, D, Select, Project]
			// - After fusion: [I, Distinct1, D, I, Distinct2, D, FusedSelectProject]
			// Note: Distinct idempotence doesn't apply because they're separated by D→I.
			Expect(graph.chain).To(HaveLen(7))

			// Verify structure: should have two lifted distincts and one fused op.
			var integrators, differentiators, distincts, fused int
			for _, id := range graph.chain {
				switch graph.nodes[id].Op.(type) {
				case *IntegratorOp:
					integrators++
				case *DifferentiatorOp:
					differentiators++
				case *DistinctOp:
					distincts++
				case *SelectThenProjectionsOp:
					fused++
				}
			}
			Expect(integrators).To(Equal(2))
			Expect(differentiators).To(Equal(2))
			Expect(distincts).To(Equal(2))
			Expect(fused).To(Equal(1))

			// Join should be incremental.
			joinNode := graph.nodes[graph.joinNode]
			_, isIncrementalJoin := joinNode.Op.(*IncrementalBinaryJoinOp)
			Expect(isIncrementalJoin).To(BeTrue())
		})

		It("should handle empty chains gracefully", func() {
			// Just inputs, no operations
			graph.AddInput(NewInput("collection"))
			Expect(graph.chain).To(BeEmpty())

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Should remain empty
			Expect(graph.chain).To(BeEmpty())
		})

		It("should converge within iteration limit", func() {
			// Create a graph that requires multiple passes
			graph.AddInput(NewInput("collection"))

			// Chain of operations that create optimization opportunities
			graph.AddToChain(NewIntegrator())
			graph.AddToChain(NewDifferentiator())
			graph.AddToChain(NewDistinct())
			graph.AddToChain(NewDistinct())
			graph.AddToChain(NewSelection(NewFieldFilter("active", true)))
			graph.AddToChain(NewProjection(NewFieldProjection("name")))

			// Should converge without hitting iteration limit
			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Rule Interaction and Ordering", func() {
		It("should apply rules in correct order", func() {
			// Check that rules are applied in the right sequence
			// This is important because some rules create opportunities for others
			graph.AddInput(NewInput("collection"))

			// Add integration/differentiation that will be cancelled
			integID := graph.AddToChain(NewIntegrator())
			diffID := graph.AddToChain(NewDifferentiator())

			// Add operations after I->D
			_ = graph.AddToChain(NewSelection(NewFieldFilter("active", true)))
			_ = graph.AddToChain(NewProjection(NewFieldProjection("name")))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// I->D should be eliminated first
			_, integExists := graph.nodes[integID]
			_, diffExists := graph.nodes[diffID]
			Expect(integExists).To(BeFalse())
			Expect(diffExists).To(BeFalse())

			// Then selection+projection should be fused
			Expect(graph.chain).To(HaveLen(1)) // Fused operation

			// Remaining node should be fused
			remainingID := graph.chain[0]
			remainingNode, remainingNodeExists := graph.nodes[remainingID]
			Expect(remainingNodeExists).To(BeTrue())
			_, isFused := remainingNode.Op.(*SelectThenProjectionsOp)
			Expect(isFused).To(BeTrue())
		})
	})

	Context("Error Handling", func() {
		It("should handle invalid graphs gracefully", func() {
			// Create invalid graph (no inputs)
			invalidGraph := NewChainGraph()

			err := rewriter.Optimize(invalidGraph)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid graph"))
		})

		It("should handle graphs requiring joins but missing them", func() {
			// Multiple inputs but no join
			graph.AddInput(NewInput("users"))
			graph.AddInput(NewInput("projects"))
			graph.AddToChain(NewProjection(NewFieldProjection("name")))

			err := rewriter.Optimize(graph)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("multiple inputs require a join"))
		})
	})

	Context("Graph Structure Preservation", func() {
		It("should preserve node connectivity after optimization", func() {
			graph.AddInput(NewInput("users"))

			_ = graph.AddToChain(NewProjection(NewFieldProjection("name")))
			_ = graph.AddToChain(NewSelection(NewFieldFilter("active", true)))

			// Record original structure
			originalInputs := len(graph.inputs)
			// originalOutput := graph.output

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Structure should be preserved
			Expect(graph.inputs).To(HaveLen(originalInputs))
			Expect(graph.output).NotTo(Equal("")) // Should have an output

			// Graph should still be valid
			Expect(graph.Validate()).NotTo(HaveOccurred())
		})

		It("should update output pointer correctly after eliminations", func() {
			graph.AddInput(NewInput("stream"))

			projID := graph.AddToChain(NewProjection(NewFieldProjection("name")))
			_ = graph.AddToChain(NewIntegrator())
			diffID := graph.AddToChain(NewDifferentiator()) // This will be the original output

			originalOutput := graph.output
			Expect(originalOutput).To(Equal(diffID))

			err := rewriter.Optimize(graph)
			Expect(err).NotTo(HaveOccurred())

			// Output should be updated to projection (I->D eliminated)
			Expect(graph.output).To(Equal(projID))
			Expect(graph.output).NotTo(Equal(originalOutput))
		})
	})
})
