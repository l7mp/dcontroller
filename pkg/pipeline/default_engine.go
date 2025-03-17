package pipeline

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/go-logr/logr"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/hsnlab/dcontroller/pkg/cache"
	"github.com/hsnlab/dcontroller/pkg/expression"
	"github.com/hsnlab/dcontroller/pkg/object"
	"github.com/hsnlab/dcontroller/pkg/util"
)

type unstruct = map[string]any

var ObjectKey = toolscache.MetaObjectToName

// defaultEngine is the default implementation of the pipeline engine.
type defaultEngine struct {
	targetView    string               // the views/objects to work on
	baseviews     []gvk                // the view to put the output objects into
	baseViewStore map[gvk]*cache.Store // internal view cache
	log           logr.Logger
}

func NewDefaultEngine(targetView string, baseviews []gvk, log logr.Logger) Engine {
	return &defaultEngine{
		targetView:    targetView,
		baseviews:     baseviews,
		baseViewStore: make(map[gvk]*cache.Store),
		log:           log,
	}
}

func (eng *defaultEngine) Log() logr.Logger { return eng.log }
func (eng *defaultEngine) View() string     { return eng.targetView }

func (eng *defaultEngine) WithObjects(objs ...object.Object) {
	for _, o := range objs {
		gvk := o.GetObjectKind().GroupVersionKind()
		eng.initViewStore(gvk)
		eng.baseViewStore[gvk].Add(o) //nolint:errcheck
	}
}

func (eng *defaultEngine) IsValidEvent(delta cache.Delta) bool {
	if delta.Object == nil {
		return false
	}

	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	eng.initViewStore(gvk)

	if delta.Type == cache.Added || delta.Type == cache.Updated ||
		delta.Type == cache.Upserted || delta.Type == cache.Replaced {
		obj, ok, err := eng.baseViewStore[gvk].GetByKey(ObjectKey(delta.Object).String())
		if err == nil && ok {
			// duplicate->not-valid
			return !object.DeepEqual(delta.Object, obj)
		}
	}

	return true
}

func (eng *defaultEngine) EvaluateAggregation(a *Aggregation, delta cache.Delta) ([]cache.Delta, error) {
	if delta.IsUnchanged() {
		return []cache.Delta{delta}, nil
	}

	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	eng.initViewStore(gvk)

	if !eng.IsValidEvent(delta) {
		eng.log.V(4).Info("aggregation: ignoring duplicate event", "GVK", gvk,
			"event-type", delta.Type)
		return []cache.Delta{}, nil
	}

	// find out whether an upsert is an update/replace or an add
	delta = eng.handleUpsertEvent(delta)

	// update the local view caches
	ds := []cache.Delta{}
	switch delta.Type {
	case cache.Added:
		if err := eng.baseViewStore[gvk].Add(delta.Object); err != nil {
			return nil, fmt.Errorf("processing event %q: could not add object %s to store: %w",
				delta.Type, ObjectKey(delta.Object), err)
		}

		ds = append(ds, cache.Delta{
			Type:   delta.Type,
			Object: object.DeepCopy(delta.Object),
		})

		eng.log.V(6).Info("aggregation: add", "new-object", object.Dump(delta.Object))

	case cache.Updated, cache.Replaced:
		old, ok, err := eng.baseViewStore[gvk].GetByKey(ObjectKey(delta.Object).String())
		if err != nil {
			return nil, err
		}
		if !ok {
			eng.log.V(4).Info("aggregation: ignoring update event for an unknown object",
				"event-type", delta.Type, "object", ObjectKey(delta.Object))
			return nil, nil
		}

		if err := eng.baseViewStore[gvk].Update(delta.Object); err != nil {
			return nil, fmt.Errorf("processing event %q: could not add object %s to store: %w",
				delta.Type, ObjectKey(delta.Object), err)
		}

		ds = append(ds, []cache.Delta{
			{
				Type:   cache.Deleted,
				Object: old, // GgetByKey performs the deepcop
			}, {
				Type:   cache.Added,
				Object: object.DeepCopy(delta.Object),
			},
		}...)

		eng.log.V(6).Info("aggregation: update", "old-object", object.Dump(old),
			"new-object", object.Dump(delta.Object))

	case cache.Deleted:
		old, ok, err := eng.baseViewStore[gvk].GetByKey(ObjectKey(delta.Object).String())
		if err != nil {
			return nil, err
		}
		if !ok {
			eng.log.V(4).Info("aggregation: ignoring delete event for an unknown object",
				"event-type", delta.Type, "object", ObjectKey(delta.Object))
			return nil, nil
		}

		if err := eng.baseViewStore[gvk].Delete(old); err != nil {
			return nil, fmt.Errorf("procesing event %q: could not delete object %s from store: %w",
				delta.Type, ObjectKey(delta.Object), err)
		}

		ds = append(ds, cache.Delta{
			Type:   cache.Deleted,
			Object: old, // GgetByKey performs the deepcop
		})

		eng.log.V(6).Info("aggregation: delete using existing object", "object", old)

	default:
		eng.log.V(4).Info("aggregate: ignoring event", "event-type", delta.Type)

		return []cache.Delta{}, nil
	}

	// no more touching of the local cache from this point!

	// process the deltas
	res := []cache.Delta{}
	for _, d := range ds {
		resds, err := eng.doAggregation(a, d)
		if err != nil {
			return nil, fmt.Errorf("processing event %q: could not evaluate aggregation %s: %w",
				delta.Type, a.String(), err)
		}

		res = append(res, resds...)
	}

	res = eng.consolidateDeltas(res)

	eng.log.V(4).Info("aggregation: ready", "event-type", delta.Type, "result", util.Stringify(res))

	return res, nil
}

// doAggregation performs the actual aggregation by processing each stage.
func (eng *defaultEngine) doAggregation(a *Aggregation, delta cache.Delta) ([]cache.Delta, error) {
	var res []cache.Delta
	input := []cache.Delta{delta}
	for _, s := range a.Stages {
		res = []cache.Delta{}
		for _, d := range input {
			if d.IsUnchanged() {
				continue
			}
			resds, err := eng.EvaluateStage(s, d)
			if err != nil {
				return nil, err
			}
			res = append(res, resds...)
		}
		input = res
	}

	ret := []cache.Delta{}
	for _, d := range res {
		retd, err := Normalize(eng, d)
		if err != nil {
			return nil, err
		}
		ret = append(ret, retd)
	}

	return ret, nil
}

// EvaluateStage evaluates a single aggregation stage.
func (eng *defaultEngine) EvaluateStage(s *Stage, delta cache.Delta) ([]cache.Delta, error) {
	e := s.Expression
	u := delta.Object.UnstructuredContent()

	ret := []cache.Delta{}
	switch e.Op {
	// @select is one-to-one or one-to-zero
	case "@select":
		res, err := e.Arg.Evaluate(expression.EvalCtx{Object: u, Log: eng.log})
		if err != nil {
			return nil, err
		}

		b, err := expression.AsBool(res)
		if err != nil {
			return nil, fmt.Errorf("expected conditional expression to evaluate to "+
				"boolean: %w", err)
		}

		// default is no change
		if b {
			ret = packDeltas(eng.targetView, delta.Type, []unstruct{u})
		}

	// @project is one-to-one
	case "@project":
		res, err := e.Arg.Evaluate(expression.EvalCtx{Object: u, Log: eng.log})
		if err != nil {
			return nil, err
		}

		u, err := expression.AsObject(res)
		if err != nil {
			return nil, err
		}

		ret = packDeltas(eng.targetView, delta.Type, []unstruct{u})

	// @demux is one to many
	case "@unwind", "@demux":
		name, err := expression.GetJSONPathRaw("$.metadata.name", u)
		if err != nil {
			return nil, errors.New("valid .metadata.name required")
		}

		arg, err := e.Arg.Evaluate(expression.EvalCtx{Object: u, Log: eng.log})
		if err != nil {
			return nil, err
		}

		list := []any{}
		if arg != nil {
			list, err = expression.AsList(arg)
			if err != nil {
				return nil, err
			}
		}

		vs := []unstruct{}
		for i, elem := range list {
			a := deepCopy(u)
			v, ok := a.(unstruct)
			if !ok {
				return nil, errors.New("could not deepcopy object content")
			}

			// the elem to the corresponding jsonpath
			jp, err := e.Arg.GetLiteralString()
			if err != nil {
				return nil, err
			}

			// must use the low-level jsonpath setter so that we retain the original object
			if err := expression.SetJSONPathRaw(jp, elem, v); err != nil {
				return nil, err
			}

			// add index to name
			if err := expression.SetJSONPathRaw("$.metadata.name", fmt.Sprintf("%s-%d", name, i), v); err != nil {
				return nil, fmt.Errorf("could not add index to .metadata.name")
			}

			v, err = expression.AsObject(v)
			if err != nil {
				return nil, err
			}

			vs = append(vs, v)
		}

		ret = packDeltas(eng.targetView, delta.Type, vs)

	// @mux is many to one
	// this Op is riddled with corner cases, use only if you know what you are doing
	case "@gather", "@mux":
		args, err := expression.AsExpOrExpList(e.Arg)
		if err != nil {
			return nil, err
		}

		if len(args) != 2 {
			return nil, errors.New("expected two arguments")
		}
		idPath := args[0]
		elemPath := args[1]

		// object's ID
		id, err := idPath.Evaluate(expression.EvalCtx{Object: u, Log: eng.log})
		if err != nil {
			return nil, fmt.Errorf("error querying object id: %w", err)
		}
		if id == nil {
			return nil, errors.New("empty object id")
		}

		// object's elem
		elem, err := elemPath.Evaluate(expression.EvalCtx{Object: u, Log: eng.log})
		if err != nil {
			return nil, fmt.Errorf("error querying object element: %w", err)
		}
		if elem == nil {
			return nil, errors.New("empty object element")
		}

		// iterate without the added/deleted item to find out what's in the cache
		gathered := []any{}
		for _, o := range s.inCache.List() {
			// ignore cached object on Delete
			if client.ObjectKeyFromObject(o) == client.ObjectKeyFromObject(delta.Object) {
				continue
			}

			oid, err := idPath.Evaluate(expression.EvalCtx{Object: o.UnstructuredContent(), Log: eng.log})
			if err != nil {
				return nil, err
			}

			if !reflect.DeepEqual(id, oid) {
				continue
			}

			// store.List deepcopies
			oelem, err := elemPath.Evaluate(expression.EvalCtx{Object: o.UnstructuredContent(), Log: eng.log})
			if err != nil {
				return nil, err
			}
			gathered = append(gathered, oelem)
		}

		var t cache.DeltaType
		switch delta.Type {
		case cache.Added:
			// we generate an Added delta even when the list is updated:
			// delta-consolidation will turn that into an Upsert eventually
			t = cache.Added
			gathered = append(gathered, elem)

			if err := s.inCache.Add(delta.Object); err != nil {
				return nil, err
			}
		case cache.Deleted:
			// if resultant list is non-empty, we generate an Upsert delta (Added will
			// be turned into an IUpsert eventually), otherwise we generate a Delete
			if len(gathered) > 0 {
				t = cache.Added
			} else {
				t = cache.Deleted
			}

			if err := s.inCache.Delete(delta.Object); err != nil {
				return nil, err
			}
		default:
		}

		if err := expression.SetJSONPathRawExp(&elemPath, gathered, u); err != nil {
			return nil, err
		}

		ret = packDeltas(eng.targetView, t, []unstruct{u})

	default:
		return nil, NewAggregationError(
			errors.New("unknown aggregation op"))
	}

	eng.log.V(5).Info("eval ready", "aggregation", e.String(), "result", util.Stringify(ret))

	return ret, nil
}

// consolidate: objects both in the deleted and added cache are updated
func (eng *defaultEngine) consolidateDeltas(ds []cache.Delta) []cache.Delta {
	res := []cache.Delta{}

	// build indices into deltas
	addidx, delidx := map[string]*cache.Delta{}, map[string]*cache.Delta{}
	for i, d := range ds {
		if d.IsUnchanged() {
			continue
		}
		switch d.Type {
		case cache.Added:
			// decomposed update aggregations may yield multiple Added deltas for the
			// same object, this may yield spurious results - use the latest
			addidx[d.Object.GetName()] = &ds[i]

		case cache.Deleted:
			// decomposed update aggregations may yield multiple Deleted deltas for the
			// same object, this may yield spurious results - use the latest
			delidx[d.Object.GetName()] = &ds[i]

		default:
			eng.log.Info("ignoring delta of unknown type", "object",
				d.Object.GetName(), "delta-type", d.Type)
			continue
		}
	}

	// 1. deleted && !added -> deleted
	for name, del := range delidx {
		if _, ok := addidx[name]; !ok {
			res = append(res, *del)
		}
	}

	// 2. deleted && added && deleted!=added -> updated
	for name, del := range delidx {
		if add, ok := addidx[name]; ok && !object.DeepEqual(add.Object, del.Object) {
			res = append(res, cache.Delta{Type: cache.Updated, Object: add.Object})
		}
	}

	// 3. !deleted && added -> added (use upsert to be on the safe side)
	for name, add := range addidx {
		if _, ok := delidx[name]; !ok {
			res = append(res, cache.Delta{
				Type:   cache.Upserted,
				Object: add.Object,
			})
		}
	}

	return res
}

func (eng *defaultEngine) EvaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error) {
	ds, err := eng.evaluateJoin(j, delta)
	if err != nil {
		return ds, err
	}

	return ds, nil
}

func (eng *defaultEngine) evaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error) {
	eng.log.V(5).Info("join: processing event", "event-type", delta.Type, "object", ObjectKey(delta.Object))

	gvk := delta.Object.GetObjectKind().GroupVersionKind()
	eng.initViewStore(gvk)

	if !eng.IsValidEvent(delta) {
		eng.log.V(4).Info("aggregation: ignoring duplicate event", "GVK", gvk,
			"event-type", delta.Type)
		return []cache.Delta{}, nil
	}

	// find out whether an upsert is an update/replace or an add
	delta = eng.handleUpsertEvent(delta)

	ds := make([]cache.Delta, 0)
	switch delta.Type {
	case cache.Added:
		os, err := eng.evalJoin(j, delta.Object)
		if err != nil {
			return nil, NewJoinError(
				fmt.Errorf("processing event %q: could not evaluate join for new object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := eng.baseViewStore[gvk].Add(delta.Object); err != nil {
			return nil, NewJoinError(
				fmt.Errorf("processing event %q: could not add object %s to store: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		for _, o := range os {
			ds = append(ds, cache.Delta{Type: cache.Added, Object: o})
		}

	case cache.Updated, cache.Replaced:
		eng.log.V(2).Info("join: replacing event with a Delete followed by an Add",
			"event-type", delta.Type, "object", delta.Object)

		delDeltas, err := eng.evaluateJoin(j, cache.Delta{Type: cache.Deleted, Object: delta.Object})
		if err != nil {
			return nil, NewJoinError(err)
		}

		addDeltas, err := eng.evaluateJoin(j, cache.Delta{Type: cache.Added, Object: delta.Object})
		if err != nil {
			return nil, NewJoinError(err)
		}

		// consolidate: objects both in the deleted and added cache are updated
		a, m, d := diffDeltas(delDeltas, addDeltas)
		ds = append(ds, d...)
		ds = append(ds, m...)
		ds = append(ds, a...)

	case cache.Deleted:
		old, ok, err := eng.baseViewStore[gvk].GetByKey(ObjectKey(delta.Object).String())
		if err != nil {
			return nil, NewJoinError(err)
		}
		if !ok {
			eng.log.V(4).Info("join: ignoring delete event for an unknown object",
				"event-type", delta.Type, "object", ObjectKey(delta.Object))
			return []cache.Delta{}, nil
		}

		eng.log.V(4).Info("join: delete using existing object", "object", old)

		os, err := eng.evalJoin(j, old)
		if err != nil {
			return nil, NewJoinError(
				fmt.Errorf("procesing event %q: could not evaluate join for deleted object %s: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		if err := eng.baseViewStore[gvk].Delete(old); err != nil {
			return nil, NewJoinError(
				fmt.Errorf("procesing event %q: could not delete object %s from store: %w",
					delta.Type, ObjectKey(delta.Object), err))
		}

		for _, o := range os {
			ds = append(ds, cache.Delta{Type: cache.Deleted, Object: o})
		}

	default:
		eng.log.V(4).Info("join: ignoring event", "event-type", delta.Type)

		return []cache.Delta{}, nil
	}

	eng.log.V(4).Info("join: ready", "event-type", delta.Type, "result", util.Stringify(ds))

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
		res, err := j.Expression.Evaluate(expression.EvalCtx{Object: input, Log: eng.log})
		if err != nil {
			return nil, false, expression.NewExpressionError(&j.Expression, err)
		}

		arg, err := expression.AsBool(res)
		if err != nil {
			return nil, false, expression.NewExpressionError(&j.Expression, err)
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
		return nil, expression.NewExpressionError(&j.Expression, err)
	}

	eng.log.V(5).Info("eval ready", "expression", j.String(), "result", res)

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

func (eng *defaultEngine) initViewStore(gvk gvk) {
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

func deepCopy(value any) any {
	switch v := value.(type) {
	case bool, int64, float64, string:
		return v
	case []any:
		newList := make([]any, len(v))
		for i, item := range v {
			newList[i] = deepCopy(item)
		}
		return newList
	case map[string]any:
		newMap := make(map[string]any)
		for k, item := range v {
			newMap[k] = deepCopy(item)
		}
		return newMap
	default:
		return v
	}
}

func packDeltas(targetView string, deltaType cache.DeltaType, vs []unstruct) []cache.Delta {
	ret := []cache.Delta{}
	for _, v := range vs {
		obj := object.NewViewObject(targetView)
		obj.SetUnstructuredContent(v)
		ret = append(ret, cache.Delta{
			Type:   deltaType,
			Object: obj,
		})
	}
	return ret
}
