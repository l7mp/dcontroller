package pipeline

import (
	"reflect"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
)

type GVK = schema.GroupVersionKind

type Engine interface {
	// EvaluateAggregation evaluates an aggregation pipeline.
	EvaluateAggregation(a *Aggregation, delta cache.Delta) ([]cache.Delta, error)
	// View returns the target view of the engine.
	View() string
	// WithObjects sets some base objects in the cache for testing.
	WithObjects(objects ...object.Object)
	// Log returns a logger.
	Log() logr.Logger
	// EvaluateJoin evaluates a join expression.
	EvaluateJoin(j *Join, delta cache.Delta) ([]cache.Delta, error)
}

func Normalize(eng Engine, content Unstructured) (object.Object, error) {
	// Normalize always produces Views!
	obj := object.NewViewObject(eng.View())

	// metadata: must exist
	meta, ok := content["metadata"]
	if !ok {
		return nil, NewInvalidObjectError("no .metadata in object")
	}
	metaMap, ok := meta.(Unstructured)
	if !ok {
		return nil, NewInvalidObjectError("invalid .metadata in object")
	}

	// name must be defined
	name, ok := metaMap["name"]
	if !ok {
		return nil, NewInvalidObjectError("missing .metadata.name")
	}
	if reflect.ValueOf(name).Kind() != reflect.String {
		return nil, NewInvalidObjectError(".metadata.name must be a string")
	}
	nameStr := name.(string)
	if nameStr == "" {
		return nil, NewInvalidObjectError("empty .metadata.name in aggregation result")
	}
	obj.SetName(nameStr)

	// namespace: can be empty
	namespace, ok := metaMap["namespace"]
	if !ok {
		obj.SetNamespace("")
	} else {
		if reflect.ValueOf(namespace).Kind() != reflect.String {
			return nil, NewInvalidObjectError(".metadata.namespace must be a string")
		}
		namespaceStr := namespace.(string)
		obj.SetNamespace(namespaceStr)
	}

	obj.SetUnstructuredContent(content)

	return obj, nil
}
