package pipeline

import (
	"fmt"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/util"
)

// Pipeline is an optional join followed by an aggregation.
type Pipeline struct {
	*Join
	Aggregation
}

func (p *Pipeline) String() string {
	ss := p.Aggregation.String()
	if p.Join != nil {
		ss = p.Join.String() + "," + ss
	}
	return fmt.Sprintf("pipeline:[%s]", ss)
}

// Evaluate processes an pipeline expression on the given delta.
func (p *Pipeline) Evaluate(eng Engine, delta cache.Delta) ([]cache.Delta, error) {
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

	// process the aggregation on each of the deltas
	res := []cache.Delta{}
	for _, d := range deltas {
		da, err := eng.EvaluateAggregation(&p.Aggregation, d)
		if err != nil {
			return nil, err
		}
		res = append(res, da)
	}

	// the aggregation may collapse objects that come out as different (delete+add) from the
	// join pipeline
	res = collapseDeltas(res)

	eng.Log().Info("eval ready", "pipeline", p.String(), "result", util.Stringify(res))

	return res, nil
}

func collapseDeltas(ds []cache.Delta) []cache.Delta {
	uniq := map[string]cache.Delta{}

	for _, delta := range ds {
		key := ObjectKey(delta.Object).String()
		if d, ok := uniq[key]; ok && d.Type == cache.Deleted && (delta.Type == cache.Added || delta.Type == cache.Updated) {
			// del events come first
			uniq[key] = cache.Delta{Type: cache.Updated, Object: delta.Object}
		} else {
			uniq[key] = delta
		}
	}

	// first the deletes, then the updates and finally the adds
	ret := []cache.Delta{}
	for _, t := range []cache.DeltaType{cache.Deleted, cache.Updated, cache.Added, cache.Replaced, cache.Sync} {
		for _, v := range uniq {
			if v.Type == t {
				ret = append(ret, v)
			}
		}
	}

	return ret
}
