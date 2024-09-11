package object

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type UnstructuredContent = map[string]any

type Unstructured interface {
	// UnstructuredContent returns a non-nil map with this object's contents. Values may be
	// []any, map[string]any, or any primitive type. Contents are typically serialized to and
	// from JSON. SetUnstructuredContent should be used to mutate the contents.
	UnstructuredContent() UnstructuredContent
	// SetUnstructuredContent updates the object content to match the provided map.
	SetUnstructuredContent(UnstructuredContent)
}

// Object is a general resource, can be either an unstructured.Unstructured or a ViewObject.
type Object interface {
	client.Object
	Unstructured
}

func DeepEqual(a, b Object) bool {
	return equality.Semantic.DeepEqual(a, b)
}

func DeepCopyInto(in, out Object) {
	if in == nil || out == nil {
		return
	}
	out.SetUnstructuredContent(runtime.DeepCopyJSON(in.UnstructuredContent()))
	// get view right in out
	fmt.Printf("---------%#v\n", in.GetObjectKind().GroupVersionKind())

	out.GetObjectKind().SetGroupVersionKind(in.GetObjectKind().GroupVersionKind())
	fmt.Printf("---------%#v\n", out.GetObjectKind().GroupVersionKind())
	return
}

func DeepCopy(in *ViewObject) Object {
	if in == nil {
		return nil
	}
	out := new(ViewObject)
	DeepCopyInto(in, out)
	return out
}
