package pipeline

import (
	"reflect"

	"github.com/go-logr/logr"

	"hsnlab/dcontroller-runtime/pkg/object"
)

type Engine struct {
	view   string
	inputs []map[string]any
	vars   map[string]any
	stack  []any // for holding object contexts on which to eval JSONpath exprs
	log    logr.Logger
}

func NewEngine(view string, log logr.Logger) *Engine {
	return &Engine{view: view, vars: map[string]any{}, stack: []any{}, log: log}
}

func (eng *Engine) WithInput(us []map[string]any) *Engine {
	eng.inputs = us
	return eng
}

func (eng *Engine) WithObjects(objs []*object.Object) *Engine {
	us := make([]map[string]any, len(objs))
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
		content, ok := res.(map[string]any)
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
		metaMap, ok := meta.(map[string]any)
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

// func (eng *Engine) pushStack(arg any) {
// 	eng.stack = arg
// }

// func (eng *Engine) popStack() any {
// 	ret := eng.stack
// 	eng.stack = nil
// 	return ret
// }

// func (eng *Engine) hasObjCtx() bool {
// 	return eng.stack != nil
// }

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
