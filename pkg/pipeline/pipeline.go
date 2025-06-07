package pipeline

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/dbsp"
	"github.com/l7mp/dcontroller/pkg/util"
)

var (
	_         Evaluator = &Pipeline{}
	ObjectKey           = toolscache.MetaObjectToName
)

// Evaluator is a query that knows how to evaluate itself on a given delta and how to print itself.
type Evaluator interface {
	Evaluate(cache.Delta) ([]cache.Delta, error)
	fmt.Stringer
}

// Pipeline is query that knows how to evaluate itself.
type Pipeline struct {
	executor    *dbsp.Executor
	graph       *dbsp.ChainGraph
	rewriter    *dbsp.LinearChainRewriteEngine
	sources     []schema.GroupVersionKind
	sourceCache map[schema.GroupVersionKind]*cache.Store
	target      string
	targetCache *cache.Store
	log         logr.Logger
}

// NewPipeline creates a new pipeline from the set of base objects and a seralized pipeline that writes into a given target.
func NewPipeline(target string, sources []schema.GroupVersionKind, config opv1a1.Pipeline, log logr.Logger) (Evaluator, error) {
	if len(sources) > 1 && config.Join == nil {
		return nil, errors.New("invalid controller configuration: controllers " +
			"defined on multiple base resources must specify a Join in the pipeline")
	}

	p := &Pipeline{
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
		p.graph.AddInput(dbsp.NewInput(src.String()))
	}

	// // Add optional Join
	// if config.Join != nil {
	// 	joinOp, err := p.makeJoin(config.Join)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	p.graph.SetJoin(joinOp)
	// }

	// Add the the Aggregation chain
	if config.Aggregation != nil {
		for _, e := range config.Aggregation.Expressions {
			var op dbsp.Operator

			switch e.Op {
			case "@select":
				// @select is one-to-one or one-to-zero
				op = p.NewSelectionOp(&e)

			case "@project":
				// @project is one-to-one
				op = p.NewProjectionOp(&e)

			case "@unwind", "@demux":
				// @demux is one to many
				o, err := p.NewUnwindOp(&e)
				if err != nil {
					return nil, NewPipelineError(fmt.Errorf("failed to instantiate unwind op: %w", err))
				}
				op = o
			case "@gather", "@mux":
				// @mux is many to one
				o, err := p.NewGatherOp(&e)
				if err != nil {
					return nil, NewPipelineError(fmt.Errorf("failed to instantiate gather op: %w", err))
				}
				op = o

			default:
				return nil, NewPipelineError(errors.New("unknown aggregation op"))
			}

			p.graph.AddToChain(op)
		}
	}

	p.log.Info("pipeline initialization ready", "num-inputs", len(sources), "graph", p.graph.String())

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

	p.log.Info("pipeline optimization ready", "graph", p.graph.String())

	return p, nil
}

func (p *Pipeline) String() string {
	return p.graph.String()
}

// Evaluate processes an pipeline expression on the given delta.
func (p *Pipeline) Evaluate(delta cache.Delta) ([]cache.Delta, error) {
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
		dzset[src.String()] = dbsp.NewDocumentZSet()
	}
	key := delta.Object.GroupVersionKind().String()
	dzset[key] = zset

	// p.log.V(4).Info("prepared input zset", "object", ObjectKey(delta.Object),
	// 	"input", util.Stringify(dzset))

	// Run the DBSP executor
	res, err := p.executor.ProcessDelta(dzset)
	if err != nil {
		return nil, NewPipelineError(err)
	}

	rawDeltas, err := p.ConvertZSetToDelta(res, p.target)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to convert DBSP zset to delta: %w", err))
	}

	deltas, err := p.Reconcile(rawDeltas)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to update target cache from delta: %w", err))
	}

	p.log.V(1).Info("eval ready", "event-type", delta.Type, "object", ObjectKey(delta.Object),
		"result", util.Stringify(rawDeltas))

	return deltas, nil
}
