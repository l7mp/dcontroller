//nolint:gosec
package dbsp

// # Run all benchmarks
// go test -bench=. -benchmem -timeout=30s

// # Run specific size comparisons
// go test -bench=BenchmarkComparison -benchmem -timeout=30s

// # Run just join comparisons
// go test -bench=BenchmarkJoinScenarios -benchmem

// # Get detailed memory profiles
// go test -bench=BenchmarkMemoryUsage -benchmem

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var rnd *rand.Rand = rand.New(rand.NewSource(42))

// BenchmarkData holds the test data and configurations
type BenchmarkData struct {
	largeDelta       *DocumentZSet
	mediumDelta      *DocumentZSet
	smallDelta       *DocumentZSet
	snapshotGraph    *ChainGraph
	incrementalGraph *ChainGraph
	optimizedGraph   *ChainGraph
}

// setupBenchmarkData creates test data and graphs for benchmarking
func setupBenchmarkData() *BenchmarkData {
	// Create test data of different sizes
	largeDelta := createTestData(10000) // 10K documents
	mediumDelta := createTestData(1000) // 1K documents
	smallDelta := createTestData(100)   // 100 documents

	// Create three versions of the same graph: snapshot, incremental, optimized
	snapshotGraph := createSnapshotGraph()
	incrementalGraph := createIncrementalGraph()
	optimizedGraph := createOptimizedGraph()

	return &BenchmarkData{
		largeDelta:       largeDelta,
		mediumDelta:      mediumDelta,
		smallDelta:       smallDelta,
		snapshotGraph:    snapshotGraph,
		incrementalGraph: incrementalGraph,
		optimizedGraph:   optimizedGraph,
	}
}

// createTestData generates a DocumentZSet with realistic data
func createTestData(size int) *DocumentZSet {
	delta := NewDocumentZSet()

	departments := []string{"Engineering", "Marketing", "Sales", "HR", "Finance"}
	locations := []string{"NYC", "SF", "LA", "Austin", "Seattle"}
	skills := [][]any{
		{"Go", "Python", "JavaScript"},
		{"Marketing", "Analytics", "Content"},
		{"Sales", "CRM", "Negotiation"},
		{"Management", "Leadership", "Strategy"},
	}

	for i := 0; i < size; i++ {
		// Create realistic employee documents
		doc, err := newDocumentFromPairs(
			"id", int64(i),
			"name", fmt.Sprintf("Employee_%d", i),
			"department", departments[rnd.Intn(len(departments))],
			"location", locations[rnd.Intn(len(locations))],
			"salary", int64(50000+rnd.Intn(100000)),
			"active", rnd.Float32() > 0.1, // 90% active
			"skills", skills[rnd.Intn(len(skills))],
			"hire_date", fmt.Sprintf("2020-%02d-%02d", rnd.Intn(12)+1, rnd.Intn(28)+1),
			"performance_score", rnd.Float64()*5.0,
			"metadata", map[string]any{
				"last_login": fmt.Sprintf("2024-%02d-%02d", rnd.Intn(12)+1, rnd.Intn(28)+1),
				"projects":   rnd.Intn(10),
			},
		)
		if err != nil {
			panic(err)
		}

		// Add with random multiplicity (1-3) to test Z-set properties
		multiplicity := rnd.Intn(3) + 1
		err = delta.AddDocumentMutate(doc, multiplicity)
		if err != nil {
			panic(err)
		}
	}

	return delta
}

// createSnapshotGraph creates a graph using only snapshot operators
func createSnapshotGraph() *ChainGraph {
	graph := NewChainGraph()

	// Input: employee data
	graph.AddInput(NewInput("employees"))

	// Filter: active employees only
	graph.AddToChain(NewSelection(NewFieldFilter("active", true)))

	// Project: extract relevant fields
	graph.AddToChain(NewProjection(NewFieldProjection("name", "department", "salary", "skills")))

	// Unwind: flatten skills array
	extractor, transformer := createUnwindEvaluators("skills", "skill")
	graph.AddToChain(NewUnwind(extractor, transformer))

	// Project: clean up after unwind
	graph.AddToChain(NewProjection(NewFieldProjection("name", "department", "salary", "skill")))

	// Gather: group by department, collect salaries
	keyExt, valueExt, aggregator := createGatherEvaluators("department", "salary", "department", "salaries")
	graph.AddToChain(NewGather(keyExt, valueExt, aggregator))

	if err := graph.Validate(); err != nil {
		panic(fmt.Sprintf("Invalid snapshot graph: %v", err))
	}

	return graph
}

// createIncrementalGraph creates the same logical graph but with incremental operators
func createIncrementalGraph() *ChainGraph {
	graph := NewChainGraph()

	// Input: employee data
	graph.AddInput(NewInput("employees"))

	// Filter: active employees only (linear, already incremental)
	graph.AddToChain(NewSelection(NewFieldFilter("active", true)))

	// Project: extract relevant fields (linear, already incremental)
	graph.AddToChain(NewProjection(NewFieldProjection("name", "department", "salary", "skills")))

	// Unwind: flatten skills array (linear, already incremental)
	extractor, transformer := createUnwindEvaluators("skills", "skill")
	graph.AddToChain(NewUnwind(extractor, transformer))

	// Project: clean up after unwind (linear, already incremental)
	graph.AddToChain(NewProjection(NewFieldProjection("name", "department", "salary", "skill")))

	// Gather: group by department, collect salaries (use snapshot version)
	keyExt, valueExt, aggregator := createGatherEvaluators("department", "salary", "department", "salaries")
	graph.AddToChain(NewGather(keyExt, valueExt, aggregator))

	if err := graph.Validate(); err != nil {
		panic(fmt.Sprintf("Invalid incremental graph: %v", err))
	}

	return graph
}

// createOptimizedGraph creates an incremental graph and applies all optimizations
func createOptimizedGraph() *ChainGraph {
	graph := NewChainGraph()

	// Input: employee data
	graph.AddInput(NewInput("employees"))

	// Add some operations that will be optimized
	graph.AddToChain(NewIntegrator())     // Will be eliminated in I->D pairs
	graph.AddToChain(NewDifferentiator()) // Will be eliminated in I->D pairs

	// Filter: active employees only
	graph.AddToChain(NewSelection(NewFieldFilter("active", true)))

	// Project: extract relevant fields (will be fused with selection)
	graph.AddToChain(NewProjection(NewFieldProjection("name", "department", "salary", "skills")))

	// Add redundant distincts (will be optimized)
	graph.AddToChain(NewDistinct())
	graph.AddToChain(NewDistinct())

	// Unwind: flatten skills array
	extractor, transformer := createUnwindEvaluators("skills", "skill")
	graph.AddToChain(NewUnwind(extractor, transformer))

	// Project: clean up after unwind
	graph.AddToChain(NewProjection(NewFieldProjection("name", "department", "salary", "skill")))

	// Gather: group by department, collect salaries (will be incrementalized)
	keyExt, valueExt, aggregator := createGatherEvaluators("department", "salary", "department", "salaries")
	graph.AddToChain(NewGather(keyExt, valueExt, aggregator))

	// Apply all optimizations
	rewriter := NewLinearChainRewriteEngine()
	if err := rewriter.Optimize(graph); err != nil {
		panic(fmt.Sprintf("Failed to optimize graph: %v", err))
	}

	if err := graph.Validate(); err != nil {
		panic(fmt.Sprintf("Invalid optimized graph: %v", err))
	}

	return graph
}

// Global benchmark data to avoid setup overhead
var benchData *BenchmarkData

func init() {
	benchData = setupBenchmarkData()
}

// Benchmark snapshot processing with small delta
func BenchmarkSnapshotSmall(b *testing.B) {
	executor, err := NewExecutor(benchData.snapshotGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.snapshotGraph.inputs[0]: benchData.smallDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result // Prevent optimization
	}
}

// Benchmark incremental processing with small delta
func BenchmarkIncrementalSmall(b *testing.B) {
	executor, err := NewExecutor(benchData.incrementalGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.incrementalGraph.inputs[0]: benchData.smallDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// DON'T reset - this defeats incremental computation!
		// Only reset at the very beginning
		if i == 0 {
			executor.Reset()
		}

		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

// Benchmark optimized processing with small delta
func BenchmarkOptimizedSmall(b *testing.B) {
	executor, err := NewExecutor(benchData.optimizedGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.optimizedGraph.inputs[0]: benchData.smallDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == 0 {
			executor.Reset()
		}

		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

// Medium-sized benchmarks
func BenchmarkSnapshotMedium(b *testing.B) {
	executor, err := NewExecutor(benchData.snapshotGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.snapshotGraph.inputs[0]: benchData.mediumDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

// Better benchmark that simulates realistic incremental workload
func BenchmarkRealisticIncremental(b *testing.B) {
	b.Run("MultipleSmallDeltas", func(b *testing.B) {
		// This simulates the real incremental use case: many small updates
		executor, err := NewExecutor(benchData.incrementalGraph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		// Create smaller deltas (10 docs each)
		smallDeltas := make([]*DocumentZSet, 10)
		for i := range smallDeltas {
			smallDeltas[i] = createTestData(10) // 10 docs per delta
		}

		b.ResetTimer()
		b.ReportAllocs()

		executor.Reset() // Reset once at start

		for i := 0; i < b.N; i++ {
			deltaInputs := map[string]*DocumentZSet{
				benchData.incrementalGraph.inputs[0]: smallDeltas[i%len(smallDeltas)],
			}

			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("SingleLargeDelta", func(b *testing.B) {
		// Compare with snapshot approach: one large delta
		executor, err := NewExecutor(benchData.snapshotGraph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		deltaInputs := map[string]*DocumentZSet{
			benchData.snapshotGraph.inputs[0]: benchData.smallDelta,
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})
}

func BenchmarkOptimizedMedium(b *testing.B) {
	executor, err := NewExecutor(benchData.optimizedGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.optimizedGraph.inputs[0]: benchData.mediumDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		executor.Reset()

		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

// Large-sized benchmarks
func BenchmarkSnapshotLarge(b *testing.B) {
	executor, err := NewExecutor(benchData.snapshotGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.snapshotGraph.inputs[0]: benchData.largeDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkIncrementalLarge(b *testing.B) {
	executor, err := NewExecutor(benchData.incrementalGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.incrementalGraph.inputs[0]: benchData.largeDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		executor.Reset()

		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOptimizedLarge(b *testing.B) {
	executor, err := NewExecutor(benchData.optimizedGraph, logger)
	if err != nil {
		b.Fatalf("Failed to create executor: %v", err)
	}

	deltaInputs := map[string]*DocumentZSet{
		benchData.optimizedGraph.inputs[0]: benchData.largeDelta,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		executor.Reset()

		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("Processing failed: %v", err)
		}
		_ = result
	}
}

// prepopulateJoinState processes baseline data to establish initial state for incremental benchmarks
func prepopulateJoinState(executor *Executor, graph *ChainGraph, users, projects *DocumentZSet) error {
	deltaInputs := map[string]*DocumentZSet{
		graph.inputIdx[graph.inputs[0]]: users,
		graph.inputIdx[graph.inputs[1]]: projects,
	}
	_, err := executor.Process(deltaInputs)
	return err
}

// createSingleUserDelta creates a delta with a single new user
func createSingleUserDelta(userID int64) *DocumentZSet {
	delta := NewDocumentZSet()
	doc, err := newDocumentFromPairs(
		"user_id", userID,
		"name", fmt.Sprintf("User_%d", userID),
		"department", fmt.Sprintf("Dept_%d", userID%10),
	)
	if err != nil {
		panic(err)
	}
	err = delta.AddDocumentMutate(doc, 1)
	if err != nil {
		panic(err)
	}
	return delta
}

// createUserUpdateDelta creates a delta that updates an existing user (delete old + add new)
func createUserUpdateDelta(userID int64, oldDept string, newDept string) *DocumentZSet {
	delta := NewDocumentZSet()

	// Delete old version (multiplicity -1)
	oldDoc, err := newDocumentFromPairs(
		"user_id", userID,
		"name", fmt.Sprintf("User_%d", userID),
		"department", oldDept,
	)
	if err != nil {
		panic(err)
	}
	err = delta.AddDocumentMutate(oldDoc, -1)
	if err != nil {
		panic(err)
	}

	// Add new version (multiplicity +1)
	newDoc, err := newDocumentFromPairs(
		"user_id", userID,
		"name", fmt.Sprintf("User_%d", userID),
		"department", newDept,
	)
	if err != nil {
		panic(err)
	}
	err = delta.AddDocumentMutate(newDoc, 1)
	if err != nil {
		panic(err)
	}

	return delta
}

// createUserDeleteDelta creates a delta that deletes a user (multiplicity -1)
func createUserDeleteDelta(userID int64, dept string) *DocumentZSet {
	delta := NewDocumentZSet()
	doc, err := newDocumentFromPairs(
		"user_id", userID,
		"name", fmt.Sprintf("User_%d", userID),
		"department", dept,
	)
	if err != nil {
		panic(err)
	}
	err = delta.AddDocumentMutate(doc, -1)
	if err != nil {
		panic(err)
	}
	return delta
}

// Benchmark with joins (binary join scenario)
func BenchmarkJoinScenarios(b *testing.B) {
	// Setup join benchmark data - baseline state
	baselineUsers := createUserData(1000)
	baselineProjects := createProjectData(500)

	// User ID for delta operations (outside existing range)
	newUserID := int64(10000)
	// Existing user for update/delete operations
	existingUserID := int64(42)
	existingUserDept := fmt.Sprintf("Dept_%d", existingUserID%10)

	// Empty delta for projects (no changes)
	emptyProjectDelta := NewDocumentZSet()

	// Benchmark: Add single user to existing state
	b.Run("AddUser_Snapshot", func(b *testing.B) {
		graph := createJoinGraph(false, false) // snapshot join
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		// Create single user delta
		singleUserDelta := createSingleUserDelta(newUserID)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Snapshot: re-processes entire baseline + delta every time
			allUsers, _ := baselineUsers.Add(singleUserDelta)
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: allUsers,
				graph.inputIdx[graph.inputs[1]]: baselineProjects,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("AddUser_Incremental", func(b *testing.B) {
		graph := createJoinGraph(true, false) // incremental join
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		// Pre-populate state with baseline data (outside timer)
		if err := prepopulateJoinState(executor, graph, baselineUsers, baselineProjects); err != nil {
			b.Fatalf("Failed to prepopulate state: %v", err)
		}

		// Create single user delta
		singleUserDelta := createSingleUserDelta(newUserID)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Incremental: only processes delta against existing state
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: singleUserDelta,
				graph.inputIdx[graph.inputs[1]]: emptyProjectDelta,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("AddUser_Optimized", func(b *testing.B) {
		graph := createJoinGraph(true, true) // incremental + optimized
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		// Pre-populate state with baseline data
		if err := prepopulateJoinState(executor, graph, baselineUsers, baselineProjects); err != nil {
			b.Fatalf("Failed to prepopulate state: %v", err)
		}

		// Create single user delta
		singleUserDelta := createSingleUserDelta(newUserID)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: singleUserDelta,
				graph.inputIdx[graph.inputs[1]]: emptyProjectDelta,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	// Benchmark: Update single user in existing state
	b.Run("UpdateUser_Snapshot", func(b *testing.B) {
		graph := createJoinGraph(false, false)
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		// Create update delta (old dept -> new dept)
		updateDelta := createUserUpdateDelta(existingUserID, existingUserDept, "NewDept")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Snapshot: re-processes entire state
			allUsers, _ := baselineUsers.Add(updateDelta)
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: allUsers,
				graph.inputIdx[graph.inputs[1]]: baselineProjects,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("UpdateUser_Incremental", func(b *testing.B) {
		graph := createJoinGraph(true, false)
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		if err := prepopulateJoinState(executor, graph, baselineUsers, baselineProjects); err != nil {
			b.Fatalf("Failed to prepopulate state: %v", err)
		}

		updateDelta := createUserUpdateDelta(existingUserID, existingUserDept, "NewDept")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: updateDelta,
				graph.inputIdx[graph.inputs[1]]: emptyProjectDelta,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("UpdateUser_Optimized", func(b *testing.B) {
		graph := createJoinGraph(true, true)
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		if err := prepopulateJoinState(executor, graph, baselineUsers, baselineProjects); err != nil {
			b.Fatalf("Failed to prepopulate state: %v", err)
		}

		updateDelta := createUserUpdateDelta(existingUserID, existingUserDept, "NewDept")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: updateDelta,
				graph.inputIdx[graph.inputs[1]]: emptyProjectDelta,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	// Benchmark: Delete single user from existing state
	b.Run("DeleteUser_Snapshot", func(b *testing.B) {
		graph := createJoinGraph(false, false)
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		deleteDelta := createUserDeleteDelta(existingUserID, existingUserDept)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Snapshot: re-processes entire state
			allUsers, _ := baselineUsers.Add(deleteDelta)
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: allUsers,
				graph.inputIdx[graph.inputs[1]]: baselineProjects,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("DeleteUser_Incremental", func(b *testing.B) {
		graph := createJoinGraph(true, false)
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		if err := prepopulateJoinState(executor, graph, baselineUsers, baselineProjects); err != nil {
			b.Fatalf("Failed to prepopulate state: %v", err)
		}

		deleteDelta := createUserDeleteDelta(existingUserID, existingUserDept)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: deleteDelta,
				graph.inputIdx[graph.inputs[1]]: emptyProjectDelta,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})

	b.Run("DeleteUser_Optimized", func(b *testing.B) {
		graph := createJoinGraph(true, true)
		executor, err := NewExecutor(graph, logger)
		if err != nil {
			b.Fatalf("Failed to create executor: %v", err)
		}

		if err := prepopulateJoinState(executor, graph, baselineUsers, baselineProjects); err != nil {
			b.Fatalf("Failed to prepopulate state: %v", err)
		}

		deleteDelta := createUserDeleteDelta(existingUserID, existingUserDept)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			deltaInputs := map[string]*DocumentZSet{
				graph.inputIdx[graph.inputs[0]]: deleteDelta,
				graph.inputIdx[graph.inputs[1]]: emptyProjectDelta,
			}
			result, err := executor.Process(deltaInputs)
			if err != nil {
				b.Fatalf("Processing failed: %v", err)
			}
			_ = result
		}
	})
}

// Helper functions for join benchmarks
func createUserData(size int) *DocumentZSet {
	delta := NewDocumentZSet()

	for i := 0; i < size; i++ {
		doc, err := newDocumentFromPairs(
			"user_id", int64(i),
			"name", fmt.Sprintf("User_%d", i),
			"department", fmt.Sprintf("Dept_%d", i%10),
		)
		if err != nil {
			panic(err)
		}

		err = delta.AddDocumentMutate(doc, 1)
		if err != nil {
			panic(err)
		}
	}

	return delta
}

func createProjectData(size int) *DocumentZSet {
	delta := NewDocumentZSet()

	for i := 0; i < size; i++ {
		doc, err := newDocumentFromPairs(
			"user_id", int64(i*2), // 50% overlap with users
			"project", fmt.Sprintf("Project_%d", i),
			"budget", int64(10000+i*1000),
		)
		if err != nil {
			panic(err)
		}

		err = delta.AddDocumentMutate(doc, 1)
		if err != nil {
			panic(err)
		}
	}

	return delta
}

func createJoinGraph(incremental, optimize bool) *ChainGraph {
	inputs := []string{"users", "projects"}
	graph := NewChainGraph()
	graph.AddInput(NewInput(inputs[0]))
	graph.AddInput(NewInput(inputs[1]))

	// Add join
	joinEval := NewFlexibleJoin("user_id", inputs)
	if incremental {
		graph.SetJoin(NewIncrementalBinaryJoin(joinEval, inputs))
	} else {
		graph.SetJoin(NewBinaryJoin(joinEval, inputs))
	}

	// Add some post-join processing
	graph.AddToChain(NewProjection(NewFieldProjection("left_name", "right_project", "right_budget")))
	graph.AddToChain(NewSelection(NewRangeFilter("right_budget", 15000, 1000000)))

	if optimize {
		rewriter := NewLinearChainRewriteEngine()
		if err := rewriter.Optimize(graph); err != nil {
			panic(err)
		}
	}

	return graph
}

// Memory allocation benchmark
func BenchmarkMemoryUsage(b *testing.B) {
	b.Run("DocumentCreation", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			doc, err := newDocumentFromPairs(
				"id", int64(i),
				"name", fmt.Sprintf("User_%d", i),
				"metadata", map[string]any{"score": float64(i) * 1.5},
			)
			if err != nil {
				b.Fatalf("Document creation failed: %v", err)
			}
			_ = doc
		}
	})

	b.Run("ZSetOperations", func(b *testing.B) {
		doc, _ := newDocumentFromPairs("id", int64(1), "name", "test")

		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			zset := NewDocumentZSet()
			err := zset.AddDocumentMutate(doc, 1)
			if err != nil {
				b.Fatalf("ZSet operation failed: %v", err)
			}
			_ = zset
		}
	})
}

// Comparison benchmark that runs all three approaches and reports ratios
func BenchmarkComparison(b *testing.B) {
	testSizes := []struct {
		name string
		data *DocumentZSet
	}{
		{"Small", benchData.smallDelta},
		{"Medium", benchData.mediumDelta},
		{"Large", benchData.largeDelta},
	}

	for _, size := range testSizes {
		b.Run(size.name, func(b *testing.B) {
			// Measure each approach
			snapshotTime := benchmarkApproach(b, benchData.snapshotGraph, size.data, "Snapshot")
			incrementalTime := benchmarkApproach(b, benchData.incrementalGraph, size.data, "Incremental")
			optimizedTime := benchmarkApproach(b, benchData.optimizedGraph, size.data, "Optimized")

			// Report performance ratios
			b.Logf("%s Results:", size.name)
			b.Logf("  Snapshot:    %v", snapshotTime)
			b.Logf("  Incremental: %v (%.2fx)", incrementalTime, float64(snapshotTime)/float64(incrementalTime))
			b.Logf("  Optimized:   %v (%.2fx)", optimizedTime, float64(snapshotTime)/float64(optimizedTime))
		})
	}
}

func benchmarkApproach(b *testing.B, graph *ChainGraph, data *DocumentZSet, name string) time.Duration {
	executor, err := NewExecutor(graph, logger)
	if err != nil {
		b.Fatalf("Failed to create %s executor: %v", name, err)
	}

	deltaInputs := map[string]*DocumentZSet{
		graph.inputs[0]: data,
	}

	start := time.Now()

	// Run a fixed number of iterations for consistent measurement
	iterations := 100
	for i := 0; i < iterations; i++ {
		executor.Reset()

		result, err := executor.Process(deltaInputs)
		if err != nil {
			b.Fatalf("%s processing failed: %v", name, err)
		}
		_ = result
	}

	return time.Since(start) / time.Duration(iterations)
}

func BenchmarkGatherComparison(b *testing.B) {
	testDoc, _ := newDocumentFromPairs("dept", "Engineering", "amount", int64(1000))
	testData, _ := SingletonZSet(testDoc)

	b.Run("SnapshotGather", func(b *testing.B) {
		keyExt, valueExt, agg := createGatherEvaluators("dept", "amount", "dept", "amounts")
		op := NewGather(keyExt, valueExt, agg)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := op.Process(testData)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SnapshotGather", func(b *testing.B) {
		keyExt, valueExt, agg := createGatherEvaluators("dept", "amount", "dept", "amounts")
		op := NewGather(keyExt, valueExt, agg)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := op.Process(testData)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
