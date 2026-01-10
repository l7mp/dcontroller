// Package pipeline implements declarative data processing pipelines that transform
// Kubernetes resources using DBSP-based incremental computation.
//
// Pipelines define how source Kubernetes resources are joined, filtered, and
// aggregated to produce target view objects. They support complex relational
// operations while maintaining incremental update semantics for efficiency.
//
// Pipeline operations:
//   - @join: Combine multiple resource types with boolean conditions (must be first if present).
//   - @select: Filter objects based on boolean expressions.
//   - @project: Transform object structure and extract fields.
//   - @unwind: Expand array fields into multiple objects.
//   - @gather: Collect multiple objects into aggregated results.
//
// Example usage:
//
//	pipeline, _ := pipeline.New("my-op", target, sources,
//	    opv1a1.Pipeline{
//	        Expressions: []expression.Expression{
//	            {Op: "@join", Args: ...},
//	            {Op: "@select", Args: ...},
//	        },
//	    }, logger)
package pipeline

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/dbsp"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/util"
)

var _ Evaluator = &Pipeline{}

var ObjectKey = toolscache.MetaObjectToName

// Evaluator is a query that knows how to evaluate itself on a given delta and how to print itself.
type Evaluator interface {
	Evaluate(object.Delta) ([]object.Delta, error)
	Sync() ([]object.Delta, error)
	fmt.Stringer
	// GetTargetCache returns the pipeline's internal target cache (primarily for testing).
	GetTargetCache() *cache.Store
	// GetSourceCache returns the pipeline's internal source cache for a given GVK (primarily for testing).
	GetSourceCache(schema.GroupVersionKind) *cache.Store
}

// Pipeline is query that knows how to evaluate itself.
// Pipeline is not reentrant: Evaluate() and Sync() must not be called concurrently.
type Pipeline struct {
	operator         string
	config           opv1a1.Pipeline
	executor         *dbsp.Executor
	graph            *dbsp.ChainGraph
	rewriter         *dbsp.LinearChainRewriteEngine
	sources          []schema.GroupVersionKind
	sourceCache      map[schema.GroupVersionKind]*cache.Store
	target           schema.GroupVersionKind
	targetCache      *cache.Store
	snapshotGraph    *dbsp.ChainGraph
	snapshotExecutor *dbsp.SnapshotExecutor
	mu               sync.Mutex // Protects against concurrent Evaluate/Sync calls
	log              logr.Logger
}

// New creates a new pipeline from the set of base objects and a seralized pipeline that writes
// into a given target.
func New(operator string, target schema.GroupVersionKind, sources []schema.GroupVersionKind, config opv1a1.Pipeline, log logr.Logger) (Evaluator, error) {
	// Check if first operation is @join when multiple sources exist.
	hasJoin := len(config.Ops) > 0 && config.Ops[0].OpType() == "@join"
	if len(sources) > 1 && !hasJoin {
		return nil, errors.New("invalid controller configuration: controllers " +
			"defined on multiple base resources must specify @join as the first operation in the pipeline")
	}

	p := &Pipeline{
		operator:    operator,
		config:      config,
		graph:       dbsp.NewChainGraph(),
		rewriter:    dbsp.NewLinearChainRewriteEngine(),
		sources:     sources,
		sourceCache: make(map[schema.GroupVersionKind]*cache.Store),
		target:      target,
		targetCache: cache.NewStore(),
		log:         log,
	}

	// Add inputs
	for _, src := range sources {
		p.graph.AddInput(dbsp.NewInput(src.Kind))
	}

	// Process operations.
	startIdx := 0

	// Add optional Join (if first operation is @join).
	if hasJoin {
		expr := config.Ops[0].GetExpression()
		joinOp := p.NewJoinOp(expr, sources)
		p.graph.SetJoin(joinOp)
		startIdx = 1
	}

	// Add the remaining operations to the aggregation chain.
	for i := startIdx; i < len(config.Ops); i++ {
		pipelineOp := config.Ops[i]
		expr := pipelineOp.GetExpression()
		if expr == nil {
			return nil, NewPipelineError(fmt.Errorf("pipeline operation %s has no expression", pipelineOp.OpType()))
		}

		var op dbsp.Operator
		switch pipelineOp.OpType() {
		case "@select":
			// @select is one-to-one or one-to-zero
			op = p.NewSelectionOp(expr)

		case "@project":
			// @project is one-to-one
			op = p.NewProjectionOp(expr)

		case "@unwind", "@demux":
			// @demux is one to many
			o, err := p.NewUnwindOp(expr)
			if err != nil {
				return nil, NewPipelineError(fmt.Errorf("failed to instantiate unwind op: %w", err))
			}
			op = o
		case "@gather", "@mux":
			// @mux is many to one
			o, err := p.NewGatherOp(expr)
			if err != nil {
				return nil, NewPipelineError(fmt.Errorf("failed to instantiate gather op: %w", err))
			}
			op = o

		default:
			return nil, NewPipelineError(fmt.Errorf("unknown pipeline op: %s", pipelineOp.OpType()))
		}

		p.graph.AddToChain(op)
	}

	p.log.V(2).Info("pipeline initialization ready", "num-inputs", len(sources), "graph", p.graph.String())

	// Optimize
	if err := p.rewriter.Optimize(p.graph); err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to optimize pipeline: %w", err))
	}

	// Create executor
	executor, err := dbsp.NewExecutor(p.graph, p.log)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to create chain executor: %w", err))
	}
	p.executor = executor

	p.log.V(2).Info("pipeline optimization ready", "graph", p.graph.String())

	return p, nil
}

// String stringifies a pipeline.
func (p *Pipeline) String() string {
	return p.graph.String()
}

// GetTargetCache returns the pipeline's internal target cache.
// This is primarily useful for testing to synchronize external state with the pipeline's view.
func (p *Pipeline) GetTargetCache() *cache.Store {
	return p.targetCache
}

// GetSourceCache returns the pipeline's internal source cache for a given GVK.
// This is primarily useful for testing to synchronize external state with the pipeline's view.
// Returns nil if no cache exists for the given GVK.
func (p *Pipeline) GetSourceCache(gvk schema.GroupVersionKind) *cache.Store {
	return p.sourceCache[gvk]
}

// Evaluate processes an pipeline on the given delta.
func (p *Pipeline) Evaluate(delta object.Delta) ([]object.Delta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.V(2).Info("processing event", "event-type", delta.Type, "object", ObjectKey(delta.Object))

	// Init
	zset, err := p.ConvertDeltaToZSet(delta)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to convert delta to DBSP zset: %w", err))
	}

	// Prepare the input zset (one entry for each input): add an empty zset to each input and
	// init the input for the changed object
	dzset := make(map[string]*dbsp.DocumentZSet, len(p.sources))
	for _, src := range p.sources {
		dzset[src.Kind] = dbsp.NewDocumentZSet()
	}
	key := delta.Object.GetKind()
	dzset[key] = zset

	p.log.V(8).Info("input zset ready", "object", ObjectKey(delta.Object), "zset", zset.String())

	// Run the DBSP executor
	res, err := p.executor.Process(dzset)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to evaluate the DBSP graph: %w", err))
	}

	rawDeltas, err := p.ConvertZSetToDelta(res, p.target)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to convert DBSP zset to delta: %w", err))
	}

	deltas, err := p.Reconcile(rawDeltas)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to update target cache from delta: %w", err))
	}

	p.log.V(1).Info("eval ready", "event-type", delta.Type, "object", object.Dump(delta.Object),
		"result", util.Stringify(deltas))

	return deltas, nil
}

// Sync performs state-of-the-world reconciliation by computing the delta needed to bring the
// target state up to date with the current source state.
//
// This is used for periodic sources that need to re-render the entire target state periodically.
// On first call, it converts the incremental graph to a snapshot graph and caches both the graph
// and executor. On subsequent calls, it:
//  1. Converts source cache to ZSet (current input state)
//  2. Runs snapshot executor to compute required target state
//  3. Subtracts current target cache from required state to get delta
//  4. Applies delta to target cache to keep it synchronized
//  5. Returns delta as []object.Delta
func (p *Pipeline) Sync() ([]object.Delta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// On first run, convert incremental graph to snapshot graph and create executor.
	if p.snapshotGraph == nil {
		var err error
		p.snapshotGraph, err = dbsp.ToSnapshotGraph(p.graph)
		if err != nil {
			return nil, NewPipelineError(fmt.Errorf("failed to convert pipeline to snapshot mode: %w", err))
		}

		p.snapshotExecutor, err = dbsp.NewSnapshotExecutor(p.snapshotGraph, p.log)
		if err != nil {
			return nil, NewPipelineError(fmt.Errorf("failed to create snapshot executor: %w", err))
		}

		p.log.V(2).Info("initialized snapshot executor for state-of-the-world reconciliation")
	}

	// Step 1: Convert source caches to ZSets (current input state).
	sourceZSets := make(map[string]*dbsp.DocumentZSet)
	for i, gvk := range p.sources {
		cache, ok := p.sourceCache[gvk]
		if !ok {
			// No cache for this source yet, use empty ZSet.
			sourceZSets[p.snapshotGraph.GetInput(gvk.Kind).Name()] = dbsp.NewDocumentZSet()
			continue
		}

		// Convert cache objects to ZSet.
		zset := dbsp.NewDocumentZSet()
		for _, obj := range cache.List() {
			objCopy := object.DeepCopy(obj)
			object.RemoveUID(objCopy)
			if err := zset.AddDocumentMutate(objCopy.UnstructuredContent(), 1); err != nil {
				return nil, NewPipelineError(
					fmt.Errorf("failed to convert source cache %d (%s) to zset: %w", i, gvk.Kind, err))
			}
		}
		sourceZSets[p.snapshotGraph.GetInput(gvk.Kind).Name()] = zset
	}

	// Step 2: Run snapshot executor to compute required target state.
	requiredState, err := p.snapshotExecutor.Process(sourceZSets)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to execute snapshot pipeline: %w", err))
	}

	p.log.V(2).Info("snapshot execution complete", "required-docs", requiredState.Size())

	// Step 3: Convert target cache to ZSet (current target state).
	currentState := dbsp.NewDocumentZSet()
	for _, obj := range p.targetCache.List() {
		objCopy := object.DeepCopy(obj)
		object.RemoveUID(objCopy)
		if err := currentState.AddDocumentMutate(objCopy.UnstructuredContent(), 1); err != nil {
			return nil, NewPipelineError(
				fmt.Errorf("failed to convert target cache to zset: %w", err))
		}
	}

	// Step 4: Compute diff: required - current.
	diffZSet, err := requiredState.Subtract(currentState)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to compute state diff: %w", err))
	}

	p.log.V(2).Info("computed state diff", "diff-size", diffZSet.Size())

	// Step 5: Convert diff ZSet back to deltas.
	rawDeltas, err := p.ConvertZSetToDelta(diffZSet, p.target)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to convert diff zset to delta: %w", err))
	}

	// Step 6: Apply deltas to target cache and reconcile.
	deltas, err := p.Reconcile(rawDeltas)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to reconcile target cache: %w", err))
	}

	p.log.V(2).Info("sync ready", "deltas", len(deltas))

	return deltas, nil
}
