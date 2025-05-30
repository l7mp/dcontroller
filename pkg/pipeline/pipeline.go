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
	executor *dbsp.LinearChainExecutor
	graph    *dbsp.LinearChainGraph
	rewriter *dbsp.LinearChainRewriteEngine
	target   string
	log      logr.Logger
}

// NewPipeline creates a new pipeline from the set of base objects and a seralized pipeline that writes into a given target.
func NewPipeline(target string, sources []schema.GroupVersionKind, config opv1a1.Pipeline, log logr.Logger) (Evaluator, error) {
	if len(sources) > 1 && config.Join == nil {
		return nil, errors.New("invalid controller configuration: controllers " +
			"defined on multiple base resources must specify a Join in the pipeline")
	}

	p := &Pipeline{
		graph:    dbsp.NewLinearChainGraph(),
		rewriter: dbsp.NewLinearChainRewriteEngine(),
		target:   target,
		log:      log,
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
				op = p.makeSelect(&e)

			// case "@project":
			// 	// @project is one-to-one
			// 	op = p.makeProject(e)

			// case "@unwind", "@demux":
			// 	// @demux is one to many
			// 	op = p.makeUnwind(e)

			// case "@gather", "@mux":
			// 	// @mux is many to one
			// 	op = p.makeGather(e)

			default:
				return nil, NewPipelineError(errors.New("unknown aggregation op"))
			}

			p.graph.AddToChain(op)
		}
	}

	// Optimize
	if err := p.rewriter.Optimize(p.graph); err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to optimize pipeline: %w", err))
	}

	// Create executor
	executor, err := dbsp.NewLinearChainExecutor(p.graph)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to create chain executor: %w", err))
	}
	p.executor = executor

	p.log.Info("pipeline setup ready", "graph", p.graph.String())

	return p, nil
}

func (p *Pipeline) String() string {
	return p.graph.String()
}

// Evaluate processes an pipeline expression on the given delta.
func (p *Pipeline) Evaluate(delta cache.Delta) ([]cache.Delta, error) {
	p.log.V(2).Info("processing event", "event-type", delta.Type, "object", ObjectKey(delta.Object))

	// Init
	dzset, err := ConvertDeltaToZSet(delta)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to init delta: %w", err))
	}

	fmt.Println(dzset)

	// Run the DBSP executor
	res, err := p.executor.ProcessDelta(dzset)
	if err != nil {
		return nil, NewPipelineError(err)
	}

	ds, err := ConvertZSetToDelta(res, p.target)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to init delta: %w", err))
	}

	p.log.V(1).Info("eval ready", "event-type", delta.Type, "object", ObjectKey(delta.Object),
		"result", util.Stringify(ds))

	return ds, nil
}

// func collapseDeltas(de []dbsp.DocumentEntry) []dbsp.DocumentEntry {
// 	uniq := map[string]cache.Delta{}
// 	for _, entry := range de {
// 		doc := entry.Document
// 		mult := entry.Multiplicity
// 		key := ObjectKey(delta.Object).String()
// 		if d, ok := uniq[key]; ok && d.Type == cache.Deleted && (delta.Type == cache.Added ||
// 			delta.Type == cache.Updated || delta.Type == cache.Upserted) {
// 			// del events come first
// 			uniq[key] = cache.Delta{Type: cache.Updated, Object: delta.Object}
// 		} else {
// 			uniq[key] = delta
// 		}
// 	}

// 	// first the deletes, then the updates and finally the adds
// 	ret := []cache.Delta{}
// 	for _, t := range []cache.DeltaType{cache.Deleted, cache.Updated, cache.Added, cache.Upserted, cache.Replaced, cache.Sync} {
// 		for _, v := range uniq {
// 			if v.Type == t {
// 				ret = append(ret, v)
// 			}
// 		}
// 	}

// 	ret := []cache.Delta{}
// 	for _, d := range res {
// 		retd, err := Normalize(eng, d)
// 		if err != nil {
// 			return nil, err
// 		}
// 		ret = append(ret, retd)
// 	}

// 	return ret
// }
