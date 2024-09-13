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

// defaultEngine is the default implementation of the pipeline engine.
type defaultEngine struct {
	targetView    string               // the views/objects to work on
	baseviews     []GVK                // the view to put the output objects into
	baseViewStore map[GVK]*cache.Store // internal view cache
	log           logr.Logger
}

func NewDefaultEngine(targetView string, baseviews []GVK, log logr.Logger) Engine {
	return &defaultEngine{
		targetView:    targetView,
		baseviews:     baseviews,
		baseViewStore: make(map[GVK]*cache.Store),
		log:           log,
	}
}

func (eng *defaultEngine) Log() logr.Logger { return eng.log }
func (eng *defaultEngine) View() string     { return eng.targetView }

func (eng *defaultEngine) WithObjects(objs ...object.Object) {
	for _, o := range objs {
		gvk := o.GetObjectKind().GroupVersionKind()
		eng.initViewStore(gvk)
		eng.baseViewStore[gvk].Add(o)
	}
	return
}

func (eng *defaultEngine) EvaluateAggregation(a *Aggregation, delta cache.Delta) ([]cache.Delta, error) {
	if delta.IsUnchanged() {
		return []cache.Delta{delta}, nil
	}

	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	eng.initViewStore(gvk)

	// find out whether an upsert is an update/replace or an add
	delta = eng.handleUpsertEvent(delta)

	// update local view cache
	var ds []cache.Delta
	switch delta.Type {
	case cache.Added:
		eng.log.V(1).Info("aggregation: add using new object", "object", delta.Object)

		o, err := eng.evalAggregation(a, object.DeepCopy(delta.Object))
		if err != nil {
			return nil, NewAggregationError(a.String(),
				fmt.Errorf("processing event %q: could not evaluate aggregation for new object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := eng.baseViewStore[gvk].Add(delta.Object); err != nil {
			return nil, NewAggregationError(a.String(),
				fmt.Errorf("processing event %q: could not add object %s to store: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		eng.Log().Info(fmt.Sprintf("%#v", eng.baseViewStore[gvk].List()))
		eng.Log().Info("$$$$$$$$$$$$$$$$$$$$$")
		ds = []cache.Delta{{Type: cache.Added, Object: o}}

	case cache.Updated, cache.Replaced:
		eng.log.V(1).Info("aggregate: replacing event with a Delete followed by an Add",
			"event-type", delta.Type, "object", delta.Object)

		delDeltas, err := eng.EvaluateAggregation(a, cache.Delta{Type: cache.Deleted, Object: delta.Object})
		if err != nil {
			return nil, NewAggregationError(a.String(), err)
		}
		delDelta := cache.NilDelta
		if len(delDeltas) == 1 {
			delDelta = delDeltas[0]
		}

		addDeltas, err := eng.EvaluateAggregation(a, cache.Delta{Type: cache.Added, Object: delta.Object})
		if err != nil {
			return nil, NewAggregationError(a.String(), err)
		}
		addDelta := cache.NilDelta
		if len(addDeltas) == 1 {
			addDelta = addDeltas[0]
		}

		// consolidate: objects both in the deleted and added cache are updated
		if delDelta.IsUnchanged() && addDelta.IsUnchanged() {
			// nothing happened: object wasn't in the view and it it still isn't
			ds = []cache.Delta{{Type: cache.Updated, Object: nil}}
		} else if delDelta.IsUnchanged() && !addDelta.IsUnchanged() {
			// object added into the view and it it still isn't
			ds = []cache.Delta{addDelta}
		} else if !delDelta.IsUnchanged() && addDelta.IsUnchanged() {
			// object removed from the view
			ds = []cache.Delta{delDelta}
		} else if ObjectKey(delDelta.Object) == ObjectKey(addDelta.Object) {
			// object updated
			ds = []cache.Delta{{Type: cache.Updated, Object: addDelta.Object}}
		} else {
			// aggregation affects the name and the name has changed!
			ds = []cache.Delta{delDelta, addDelta}
		}

	case cache.Deleted:
		old, ok, err := eng.baseViewStore[gvk].GetByKey(ObjectKey(delta.Object).String())
		if err != nil {
			return nil, NewAggregationError(a.String(), err)
		}
		if !ok {
			eng.log.V(1).Info("aggregation: ignoring delete event for an unknown object",
				"event-type", delta.Type, "object", ObjectKey(delta.Object))
			return nil, nil
		}

		eng.log.V(1).Info("aggregation: delete using existing object", "object", old)

		o, err := eng.evalAggregation(a, object.DeepCopy(old))
		if err != nil {
			return nil, NewAggregationError(a.String(),
				fmt.Errorf("processing event %q: could not evaluate aggregation for deleted object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		eng.Log().Info("@@@@@@@@@@@@@@@@@2")
		eng.Log().Info(fmt.Sprintf("%#v", eng.baseViewStore[gvk].List()))

		if err := eng.baseViewStore[gvk].Delete(old); err != nil {
			return nil, NewAggregationError(a.String(),
				fmt.Errorf("procesing event %q: could not delete object %s from store: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		eng.Log().Info(fmt.Sprintf("%#v", eng.baseViewStore[gvk].List()))
		eng.Log().Info("@@@@@@@@@@@@@@@@@2")

		ds = []cache.Delta{{Type: cache.Deleted, Object: o}}

	default:
		eng.log.V(1).Info("aggregate: ignoring event", "event-type", delta.Type)

		return []cache.Delta{}, nil
	}

	eng.log.V(1).Info("aggregation: ready", "event-type", delta.Type, "result", util.Stringify(ds))

	return ds, nil
}

func (eng *defaultEngine) evalAggregation(a *Aggregation, obj object.Object) (object.Object, error) {
	content := obj.UnstructuredContent()
	for _, s := range a.Expressions {
		res, err := eng.evalStage(&s, content)
		if err != nil {
			return nil, err
		}

		content = res

		if content == nil {
			// @select shortcuts the iteration
			return nil, nil
		}

	}

	obj, err := Normalize(eng, content)
	if err != nil {
		return nil, err
	}

	eng.Log().V(4).Info("eval ready", "aggregation", a.String(), "result", obj)

	return obj, nil
}

func (eng *defaultEngine) evalStage(e *Expression, u Unstructured) (Unstructured, error) {
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
func (eng *defaultEngine) EvaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error) {
	ds, err := eng.evaluateJoin(j, delta)
	if err != nil {
		return ds, err
	}

	return ds, nil
}

func (eng *defaultEngine) evaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error) {
	eng.log.V(1).Info("join: processing event", "event-type", delta.Type, "object", ObjectKey(delta.Object))

	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	eng.initViewStore(gvk)

	// find out whether an upsert is an update/replace or an add
	delta = eng.handleUpsertEvent(delta)

	ds := make([]cache.Delta, 0)
	switch delta.Type {
	case cache.Added:
		os, err := eng.evalJoin(j, delta.Object)
		if err != nil {
			return nil, NewJoinError(j.String(),
				fmt.Errorf("processing event %q: could not evaluate join for new object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := eng.baseViewStore[gvk].Add(delta.Object); err != nil {
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
		old, ok, err := eng.baseViewStore[gvk].GetByKey(ObjectKey(delta.Object).String())
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

		if err := eng.baseViewStore[gvk].Delete(old); err != nil {
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

func (eng *defaultEngine) evalJoin(j *Join, obj object.Object) ([]object.Object, error) {
	res, err := eng.product(obj, func(obj object.Object, current []object.Object) (object.Object, bool, error) {
		// temporary view name: Normalize will eventually recast the object into the target view
		newObj := object.NewViewObject("__tmp_join_view")
		input := newObj.UnstructuredContent()
		ids := []string{}
		for _, v := range current {
			if v == nil {
				continue
			}
			// this may break when working on native K8s objects in different groups
			// that have the same kind (don't do join on native objects!)
			kind := v.GetObjectKind().GroupVersionKind().Kind
			input[kind] = v.UnstructuredContent()
			ids = append(ids, fmt.Sprintf("%s:%s:%s", kind, v.GetNamespace(), v.GetName()))
		}

		// set id: this is needed so that we can disambiguate objects in diffDeltas
		slices.Sort(ids)
		input["metadata"] = map[string]any{"name": strings.Join(ids, "--")}

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
		// newObj.SetUnstructuredContent(input)
		object.SetContent(newObj, input)

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
type joinEvalFunc = func(object.Object, []object.Object) (object.Object, bool, error)

func (eng *defaultEngine) product(obj object.Object, eval joinEvalFunc) ([]object.Object, error) {
	if len(eng.baseviews) < 2 {
		return nil, errors.New("at least two views required")
	}

	current, ret := []object.Object{}, []object.Object{}
	err := eng.recurseProd(obj, current, &ret, eval, 0) // pass slice ref: append reallocates it!
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (eng *defaultEngine) recurseProd(obj object.Object, current []object.Object, ret *([]object.Object), eval joinEvalFunc, depth int) error {
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
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk == eng.baseviews[depth] {
		next := make([]object.Object, len(current))
		copy(next, current)
		next = append(next, obj)
		return eng.recurseProd(obj, next, ret, eval, depth+1)
	}

	store, ok := eng.baseViewStore[eng.baseviews[depth]]
	if !ok {
		// no element seen yet: go on with an empty object
		next := make([]object.Object, len(current))
		copy(next, current)
		next = append(next, nil)
		return eng.recurseProd(obj, next, ret, eval, depth+1)
	}

	for _, o := range store.List() {
		next := make([]object.Object, len(current))
		copy(next, current)
		next = append(next, o)
		err := eng.recurseProd(obj, next, ret, eval, depth+1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (eng *defaultEngine) initViewStore(gvk GVK) {
	if _, ok := eng.baseViewStore[gvk]; !ok {
		eng.baseViewStore[gvk] = cache.NewStore()
	}
}

// find out whether an upsert is an add or an update/replace
func (eng *defaultEngine) handleUpsertEvent(delta cache.Delta) cache.Delta {
	if delta.Type != cache.Upserted {
		return delta
	}

	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	if _, exists, err := eng.baseViewStore[gvk].Get(delta.Object); err != nil || !exists {
		return cache.Delta{Type: cache.Added, Object: delta.Object}
	}

	return cache.Delta{Type: cache.Updated, Object: delta.Object}
}

// helpers
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
		if delta.Object == nil || n.Object == nil {
			return false
		}
		return delta.Object.GetObjectKind().GroupVersionKind() ==
			n.Object.GetObjectKind().GroupVersionKind() &&
			delta.Object.GetName() == n.Object.GetName()
	})
}
