package pipeline

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
				op = p.NewSelectOp(&e)

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
	executor, err := dbsp.NewExecutor(p.graph, p.log)
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

// Reconcile processes a delta set containing only unrdered(!) add/delete ops into a proper
// ordered(!) upsert/delete delta list.
//
// DBSP outputs onordered zsets so there is no way to know for documents that map to the same
// primary key whether an add or a delete comes first, and the two orders yield different
// results. To remove this ambiguity, we maintain a target cache that contains the latest known
// state of the target view and we take the (doc->+/-1) pairs in any order from the zset result
// set. The rules are as follows:
//
// - for additions (doc->+1), we extract the primary key from doc and immediately upsert doc into
// the cache with that key and add the upsert delta to our result set, possibly overwriting any
// previous delta for the same key
//
// - for deletions (doc->-1), we again extract the primary key from doc and first we fetch the
// current entry doc' from the cache and check if doc==doc'. If there is no entry in the cache for
// the key or the latest state equals the doc to be deleted, we add the delete to the cache and the
// result delta, otherwise we drop the delete event and move on.
func (p *Pipeline) Reconcile(ds []cache.Delta) ([]cache.Delta, error) {
	deltaCache := map[string]cache.Delta{}

	for _, d := range ds {
		key := client.ObjectKeyFromObject(d.Object).String()

		switch d.Type {
		case cache.Added:
			// Addition: Always upsert, may overwrite previous delta
			if err := p.targetCache.Add(d.Object); err != nil {
				return nil, err
			}

			d.Type = cache.Upserted
			deltaCache[key] = d

		case cache.Deleted:
			// Deletion: Delete, but only if there is no entry in the target cache for
			// that object or the previous entry was for the exact same document
			obj, exists, err := p.targetCache.Get(d.Object)
			if err != nil {
				return nil, err
			}

			same := false
			if exists {
				eq, err := dbsp.DeepEqual(obj.UnstructuredContent(), d.Object.UnstructuredContent())
				if err != nil {
					return nil, err
				}
				same = eq
			}

			if !exists || same {
				d.Type = cache.Deleted
				deltaCache[key] = d
			}

		default:
			return nil, fmt.Errorf("unknown delta in zset: %s", d.Type)
		}
	}

	// convert delta cache back to delta
	res := []cache.Delta{}
	for _, d := range deltaCache {
		res = append(res, d)
	}

	return res, nil
}
