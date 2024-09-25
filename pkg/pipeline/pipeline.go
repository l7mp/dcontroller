package pipeline

import (
	"encoding/json"
	"fmt"
	"hsnlab/dcontroller/pkg/cache"
	"hsnlab/dcontroller/pkg/util"
)

// Pipeline is an optional join followed by an aggregation.
type Pipeline struct {
	*Join        `json:",inline"`
	*Aggregation `json:",inline"`
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
func (p *Pipeline) Evaluate(eng Engine, delta cache.Delta) ([]cache.Delta, error) {
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

	eng.Log().V(1).Info("eval ready", "event-type", delta.Type,
		"object", ObjectKey(delta.Object), "result", util.Stringify(res))

	return res, nil
}

func (p *Pipeline) DeepCopyInto(d *Pipeline) {
	j, err := json.Marshal(p)
	if err != nil {
		d = nil
		return
	}

	if err := json.Unmarshal(j, d); err != nil {
		d = nil
		return
	}
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
