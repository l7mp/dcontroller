package pipeline

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-logr/logr"
	toolscache "k8s.io/client-go/tools/cache"

	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/util"
)

var ObjectKey = toolscache.MetaObjectToName

type DefaultEngine struct {
	targetView string                  // the view to put the output objects into
	baseviews  []string                // views (potentially more than one for joins) input is coming from
	views      map[string]*cache.Store // internal view cache
	log        logr.Logger
}

func NewDefaultEngine(targetView string, baseviews []string, log logr.Logger) *DefaultEngine {
	return &DefaultEngine{
		targetView: targetView,
		baseviews:  baseviews,
		views:      make(map[string]*cache.Store),
		log:        log,
	}
}

func (eng *DefaultEngine) Log() logr.Logger { return eng.log }
func (eng *DefaultEngine) View() string     { return eng.targetView }

func (eng *DefaultEngine) EvaluateAggregation(a *Aggregation, delta cache.Delta) (cache.Delta, error) {
	if delta.IsUnchanged() {
		return delta, nil
	}

	if err := eng.procUpdate(delta); err != nil {
		return cache.Delta{}, NewAggregationError(a.String(), err)
	}

	content := delta.Object.UnstructuredContent()
	for _, s := range a.Expressions {
		res, err := eng.evalStage(&s, content)
		if err != nil {
			return cache.Delta{}, NewAggregationError(a.String(), err)
		}

		content = res

		if content == nil {
			// @select shortcuts the iteration
			return cache.NilDelta, nil
		}

	}

	obj, err := Normalize(eng, content)
	if err != nil {
		return cache.Delta{}, NewAggregationError(a.String(), err)
	}

	d := cache.Delta{Object: obj, Type: delta.Type}

	eng.Log().V(4).Info("eval ready", "aggregation", a.String(), "result", d)

	return d, nil
}

func (eng *DefaultEngine) evalStage(e *Expression, u Unstructured) (Unstructured, error) {
	if e.Arg == nil {
		return nil, NewAggregationError(e.String(),
			fmt.Errorf("no expression found in aggregation stage %s", e.String()))
	}

	switch e.Op {
	case "@select":
		res, err := e.Arg.Evaluate(evalCtx{object: u, log: eng.log})
		if err != nil {
			return nil, err
		}

		b, err := asBool(res)
		if err != nil {
			return nil, NewAggregationError(e.String(),
				fmt.Errorf("expected conditional expression to "+
					"evaluate to boolean: %w", err))
		}

		// default is no change
		var v Unstructured
		if b {
			v = u
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", u)

		return v, nil

	case "@project":
		res, err := e.Arg.Evaluate(evalCtx{object: u, log: eng.log})
		if err != nil {
			return nil, err
		}

		v, err := asObject(res)
		if err != nil {
			return nil, NewAggregationError(e.String(), err)
		}

		eng.log.V(4).Info("eval ready", "expression", e.String(), "result", v)

		return v, nil

	default:
		return nil, NewAggregationError(e.String(),
			errors.New("unknown aggregation stage"))
	}
}
func (eng *DefaultEngine) EvaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error) {
	ds, err := eng.evaluateJoin(j, delta)
	if err != nil {
		return ds, err
	}

	// clean up __id (must do outside the main join routine because evaluateJoin calls itself
	// on Updates)
	for _, d := range ds {
		if d.Object != nil {
			delete(d.Object.UnstructuredContent(), "__id")
		}
	}

	return ds, nil
}

func (eng *DefaultEngine) evaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error) {
	eng.log.V(1).Info("join: processing event", "event-type", delta.Type, "object", delta.Object)

	view := delta.Object.GetKind()
	eng.initViewStore(view)

	ds := make([]cache.Delta, 0)
	switch delta.Type {
	case cache.Added:
		os, err := eng.evalJoin(j, delta.Object)
		if err != nil {
			return nil, NewJoinError(j.String(),
				fmt.Errorf("processing event %q: could not evaluate join for new object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := eng.views[view].Add(delta.Object); err != nil {
			return nil, NewJoinError(j.String(),
				fmt.Errorf("processing event %q: could not add object %s to store: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		for _, o := range os {
			ds = append(ds, cache.Delta{Type: cache.Added, Object: o})
		}

	case cache.Updated, cache.Replaced:
		eng.log.V(1).Info("join: replacing event with a Delete followed by an Add",
			"event-type", delta.Type, "object", delta.Object)

		delDeltas, err := eng.evaluateJoin(j, cache.Delta{Type: cache.Deleted, Object: delta.Object})
		if err != nil {
			return nil, NewJoinError(j.String(), err)
		}

		addDeltas, err := eng.evaluateJoin(j, cache.Delta{Type: cache.Added, Object: delta.Object})
		if err != nil {
			return nil, NewJoinError(j.String(), err)
		}

		// consolidate: objects both in the deleted and added cache are updated
		a, m, d := diffDeltas(delDeltas, addDeltas)
		ds = append(ds, d...)
		ds = append(ds, m...)
		ds = append(ds, a...)

	case cache.Deleted:
		old, ok, err := eng.views[view].GetByKey(ObjectKey(delta.Object).String())
		if err != nil {
			return nil, NewJoinError(j.String(), err)
		}
		if !ok {
			eng.log.V(1).Info("join: ignoring delete event for an unknown object",
				"event-type", delta.Type, "object", ObjectKey(delta.Object))
			return []cache.Delta{}, nil
		}

		eng.log.V(1).Info("join: delete using existing object", "object", old)

		os, err := eng.evalJoin(j, old)
		if err != nil {
			return nil, NewJoinError(j.String(),
				fmt.Errorf("procesing event %q: could not evaluate join for deleted object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := eng.views[view].Delete(old); err != nil {
			return nil, NewJoinError(j.String(),
				fmt.Errorf("procesing event %q: could not delete object %s from store: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		for _, o := range os {
			ds = append(ds, cache.Delta{Type: cache.Deleted, Object: o})
		}

	default:
		eng.log.V(1).Info("join: ignoring event", "event-type", delta.Type)

		return []cache.Delta{}, nil
	}

	eng.log.V(1).Info("join: ready", "event-type", delta.Type, "result", util.Stringify(ds))

	return ds, nil
}

func (eng *DefaultEngine) evalJoin(j *Join, obj *object.Object) ([]*object.Object, error) {
	res, err := eng.product(obj, func(obj *object.Object, current []*object.Object) (*object.Object, bool, error) {
		// temporary view name: Normalize will eventually recast the object into the target view
		newObj := object.New("__tmp_join_view")
		input := newObj.UnstructuredContent()
		ids := []string{}
		for _, v := range current {
			if v == nil {
				continue
			}
			input[v.GetKind()] = v.UnstructuredContent()
			ids = append(ids, fmt.Sprintf("%s:%s:%s", v.GetKind(), v.GetNamespace(), v.GetName()))
		}

		// set id: this is needed so that we can disambiguate objects in diffDeltas
		slices.Sort(ids)
		input["__id"] = strings.Join(ids, "--")

		// evalutate conditional expression on the input
		res, err := j.Expression.Evaluate(evalCtx{object: input, log: eng.log})
		if err != nil {
			return nil, false, NewExpressionError(j.Expression.Op, j.Expression.Raw, err)
		}

		arg, err := asBool(res)
		if err != nil {
			return nil, false, NewExpressionError(j.Expression.Op, j.Expression.Raw, err)
		}

		if !arg {
			return nil, false, nil
		}

		// just to make sure
		newObj.SetUnstructuredContent(input)

		// add input to the join list
		return newObj.DeepCopy(), true, nil
	})
	if err != nil {
		return nil, NewExpressionError(j.Expression.Op, j.Expression.Raw, err)
	}

	eng.log.V(4).Info("eval ready", "expression", j.String(), "result", res)

	return res, nil
}

// product takes an object and a condition expression, generates the Cartesian product of the
// object stored in all the baseviews, applies the expression to each combination, and if it
// evalutates to true then it adds the combined object to the result set
type joinEvalFunc = func(*object.Object, []*object.Object) (*object.Object, bool, error)

func (eng *DefaultEngine) product(obj *object.Object, eval joinEvalFunc) ([]*object.Object, error) {
	if len(eng.baseviews) < 2 {
		return nil, errors.New("at least two views required")
	}

	current, ret := []*object.Object{}, []*object.Object{}
	err := eng.recurseProd(obj, current, &ret, eval, 0) // pass slice ref: append reallocates it!
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (eng *DefaultEngine) recurseProd(obj *object.Object, current []*object.Object, ret *([]*object.Object), eval joinEvalFunc, depth int) error {
	if depth == len(eng.baseviews) {
		newObj, ok, err := eval(obj, current)
		if err != nil {
			return err
		}
		if ok {
			*ret = append(*ret, newObj)
		}
		return nil
	}

	// skip object's view
	if obj.GetKind() == eng.baseviews[depth] {
		next := make([]*object.Object, len(current))
		copy(next, current)
		next = append(next, obj)
		return eng.recurseProd(obj, next, ret, eval, depth+1)
	}

	store, ok := eng.views[eng.baseviews[depth]]
	if !ok {
		// no element seen yet: go on with an empty object
		next := make([]*object.Object, len(current))
		copy(next, current)
		next = append(next, nil)
		return eng.recurseProd(obj, next, ret, eval, depth+1)
	}

	for _, o := range store.List() {
		next := make([]*object.Object, len(current))
		copy(next, current)
		next = append(next, o)
		err := eng.recurseProd(obj, next, ret, eval, depth+1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (eng *DefaultEngine) initViewStore(view string) {
	if _, ok := eng.views[view]; !ok {
		eng.views[view] = cache.NewStore()
	}
}

func (eng *DefaultEngine) procUpdate(delta cache.Delta) error {
	view := delta.Object.GetKind()
	eng.initViewStore(view)

	switch delta.Type {
	case cache.Added:
		return eng.views[view].Add(delta.Object)
	case cache.Replaced, cache.Updated:
		return eng.views[view].Update(delta.Object)
	case cache.Deleted:
		return eng.views[view].Delete(delta.Object)
	case cache.Sync:
		// ignore
	}

	return nil
}

// for testing
func (eng *DefaultEngine) add(objs ...*object.Object) {
	for _, o := range objs {
		eng.initViewStore(o.GetKind())
		eng.views[o.GetKind()].Add(o)
	}
	return
}

func diffDeltas(dels, adds []cache.Delta) ([]cache.Delta, []cache.Delta, []cache.Delta) {
	a, m, d := []cache.Delta{}, []cache.Delta{}, []cache.Delta{}

	for _, delta := range dels {
		if !containsDelta(adds, delta) {
			d = append(d, cache.Delta{Type: cache.Deleted, Object: delta.Object})
		}
	}

	for _, delta := range adds {
		if containsDelta(dels, delta) {
			m = append(m, cache.Delta{Type: cache.Updated, Object: delta.Object})
		} else {
			a = append(a, cache.Delta{Type: cache.Added, Object: delta.Object})
		}
	}

	return a, m, d
}

func containsDelta(ds []cache.Delta, delta cache.Delta) bool {
	return slices.ContainsFunc(ds, func(n cache.Delta) bool {
		id1, ok1 := delta.Object.UnstructuredContent()["__id"]
		id2, ok2 := n.Object.UnstructuredContent()["__id"]
		if !ok1 || !ok2 {
			return false
		}
		return id1 == id2
	})
}
