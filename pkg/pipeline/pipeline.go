package pipeline

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/util"
)

var _ Evaluator = &Pipeline{}

// Evaluator is a query that knows how to evaluate itself on a given delta and how to print itself.
type Evaluator interface {
	Evaluate(cache.Delta) ([]cache.Delta, error)
	fmt.Stringer
}

// Pipeline is query that knows how to evaluate itself.
type Pipeline struct {
	*Join
	*Aggregation
	engine Engine
}

// NewPipeline creates a new pipeline from the set of base objects and a seralized pipeline that writes into a given target.
func NewPipeline(target string, sources []gvk, config opv1a1.Pipeline, log logr.Logger) (Evaluator, error) {
	if len(sources) > 1 && config.Join == nil {
		return nil, errors.New("invalid controller configuration: controllers " +
			"defined on multiple base resources must specify a Join in the pipeline")
	}

	engine := NewDefaultEngine(target, sources, log)
	return &Pipeline{
		Join:        NewJoin(engine, config.Join),
		Aggregation: NewAggregation(engine, config.Aggregation),
		engine:      engine,
	}, nil
}

func (p *Pipeline) String() string {
	ss := ""
	if p.Join != nil {
		ss = p.Join.String()
	}
	if p.Aggregation != nil {
		ss = ss + "," + p.Aggregation.String()
	}
	return fmt.Sprintf("pipeline:[%s]", ss)
}

// Evaluate processes an pipeline expression on the given delta.
func (p *Pipeline) Evaluate(delta cache.Delta) ([]cache.Delta, error) {
	eng := p.engine
	eng.Log().V(2).Info("processing event", "event-type", delta.Type, "object", ObjectKey(delta.Object))

	if !eng.IsValidEvent(delta) {
		eng.Log().V(4).Info("aggregation: ignoring nil/duplicate event",
			"event-type", delta.Type)
		return []cache.Delta{}, nil
	}

	var deltas []cache.Delta

	// process the join
	if p.Join != nil {
		ds, err := eng.EvaluateJoin(p.Join, delta)
		if err != nil {
			return nil, err
		}
		deltas = ds
	} else {
		deltas = []cache.Delta{delta}
	}

	// process the aggregation on each delta
	res := []cache.Delta{}
	if p.Aggregation != nil {
		for _, d := range deltas {
			ds, err := eng.EvaluateAggregation(p.Aggregation, d)
			if err != nil {
				return nil, err
			}
			res = append(res, ds...)
		}

		// the aggregation may collapse objects that come out as different (delete+add)
		// from the join pipeline
		res = collapseDeltas(res)
	} else {
		res = deltas
	}

	eng.Log().V(1).Info("eval ready", "event-type", delta.Type, "object", ObjectKey(delta.Object),
		"result", util.Stringify(res))

	return res, nil
}

func collapseDeltas(ds []cache.Delta) []cache.Delta {
	uniq := map[string]cache.Delta{}

	for _, delta := range ds {
		key := ObjectKey(delta.Object).String()
		if d, ok := uniq[key]; ok && d.Type == cache.Deleted && (delta.Type == cache.Added ||
			delta.Type == cache.Updated || delta.Type == cache.Upserted) {
			// del events come first
			uniq[key] = cache.Delta{Type: cache.Updated, Object: delta.Object}
		} else {
			uniq[key] = delta
		}
	}

	// first the deletes, then the updates and finally the adds
	ret := []cache.Delta{}
	for _, t := range []cache.DeltaType{cache.Deleted, cache.Updated, cache.Added, cache.Upserted, cache.Replaced, cache.Sync} {
		for _, v := range uniq {
			if v.Type == t {
				ret = append(ret, v)
			}
		}
	}

	return ret
}
