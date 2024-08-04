package pipeline

import (
	"errors"
	"reflect"

	"github.com/go-logr/logr"

	"hsnlab/dcontroller-runtime/pkg/object"
)

type Engine struct {
	view   string
	inputs []ObjectContent
	views  [][]ObjectContent // only used for joins
	vars   ObjectContent
	stack  []any // for holding object contexts on which to eval JSONpath exprs
	log    logr.Logger
}

func NewEngine(view string, log logr.Logger) *Engine {
	return &Engine{view: view, vars: ObjectContent{}, stack: []any{}, log: log}
}

func (eng *Engine) WithInput(us []ObjectContent) *Engine {
	eng.inputs = us
	return eng
}

func (eng *Engine) WithObjects(objs []*object.Object) *Engine {
	us := make([]ObjectContent, len(objs))
	for i := range objs {
		us[i] = objs[i].UnstructuredContent()
	}
	eng.inputs = us
	return eng
}

func (eng *Engine) Normalize(arg any) ([]*object.Object, error) {
	ret := []*object.Object{}

	objs, err := asList(arg)
	if err != nil {
		return nil, NewInvalidObjectError("normalize: evaluation result must " +
			"be a list of unstructured objects")
	}

	for _, res := range objs {
		content, ok := res.(ObjectContent)
		if !ok {
			return nil, NewInvalidObjectError("normalize: evaluation result must " +
				"be an unstructured object")
		}

		obj := object.New(eng.view)

		// metadata: must exist
		meta, ok := content["metadata"]
		if !ok {
			return nil, NewInvalidObjectError("no .metadata in object")
		}
		metaMap, ok := meta.(ObjectContent)
		if !ok {
			return nil, NewInvalidObjectError("invalid .metadata in object")
		}

		// name must be defined
		name, ok := metaMap["name"]
		if !ok {
			return nil, NewInvalidObjectError("missing name")
		}
		if reflect.ValueOf(name).Kind() != reflect.String {
			return nil, NewInvalidObjectError("name must be a string")
		}
		nameStr := name.(string)
		if nameStr == "" {
			return nil, NewInvalidObjectError("empty name in aggregation result")
		}
		obj.SetName(nameStr)

		// namespace: can be empty
		namespace, ok := metaMap["namespace"]
		if !ok {
			obj.SetNamespace("")
		} else {
			if reflect.ValueOf(namespace).Kind() != reflect.String {
				return nil, NewInvalidObjectError("namespace must be a string")
			}
			namespaceStr := namespace.(string)
			obj.SetNamespace(namespaceStr)
		}

		obj.SetUnstructuredContent(content)

		ret = append(ret, obj)
	}

	return ret, nil
}

func (eng *Engine) pushStack(arg any) {
	eng.stack = append(eng.stack, arg)
}

func (eng *Engine) popStack() any {
	if len(eng.stack) == 0 {
		return nil
	}
	ret := eng.stack[len(eng.stack)-1]
	eng.stack = eng.stack[:len(eng.stack)-1]
	return ret
}

// return the top of the stack but do not remove it
func (eng *Engine) peekStack() any {
	if len(eng.stack) == 0 {
		return nil
	}
	return eng.stack[len(eng.stack)-1]
}

func (eng *Engine) isStackEmpty() bool {
	return len(eng.stack) == 0
}

// returns a list of arguments to for a list command (@map, @filter, etc) to iterate on
//   - if there is explicit argument list, use that
//   - if there is no argument list, use the provided objects (useful for the top-level aggregation
//     command)
func (eng *Engine) prepareListCommandArgs(args []Expression) ([]any, error) {
	ret := []any{}

	// if no args, use the object context
	if len(args) == 0 {
		for i := range eng.inputs {
			ret = append(ret, eng.inputs[i])
		}
		return ret, nil
	}

	// use the explicit args provided by the expression
	rawArg, err := args[0].Evaluate(eng)
	if err != nil {
		return nil, errors.New("failed to evaluate arguments")
	}

	ret, err = asList(rawArg)
	if err != nil {
		return nil, errors.New("invlaid arguments: expecting a list")
	}

	return ret, nil
}

// cartesianProduct takes n slices of objects (n >= 2) and a condition expression, generates the
// Cartesian product of the elements of the slices, applies the expression to to each combination,
// and if it evalutates to true then it adds it to the result set.
func (eng *Engine) cartesianProduct(cond func(current []ObjectContent) (ObjectContent, bool, error)) ([]ObjectContent, error) {
	ret := []ObjectContent{}
	if len(eng.views) < 2 {
		return nil, errors.New("at least two views required")
	}

	current := []ObjectContent{}
	err := eng.recurseCP(current, ret, cond, 0)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (eng *Engine) recurseCP(current, ret []ObjectContent, cond func(current []ObjectContent) (ObjectContent, bool, error), depth int) error {
	if depth == len(eng.views)-1 {
		newObj, ok, err := cond(current)
		if err != nil {
			return err
		}
		if ok {
			ret = append(ret, newObj)
		}
		return nil
	}

	for _, o := range eng.views[depth] {
		next := make([]ObjectContent, len(current))
		copy(next, current)
		next = append(next, o)
		err := eng.recurseCP(next, ret, cond, depth+1)
		if err != nil {
			return err
		}
	}

	return nil
}
