package object

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
)

type Object = *unstructured.Unstructured
type ObjectList = *unstructured.UnstructuredList

func NewObject(view string) Object {
	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(map[string]any{})
	obj.SetGroupVersionKind(viewapiv1.NewGVK(view))
	return obj
}

// SetName is a shortcut to SetNamespace(ns) followed by SetNamespace(name).
func SetName(obj Object, ns, name string) {
	obj.SetNamespace(ns)
	obj.SetName(name)
}

// SetContent is similar to SetUnstructuredContent but it preserves the GVK, the name and the namespace.
func SetContent(obj Object, content map[string]any) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	ns, name := obj.GetNamespace(), obj.GetName()
	obj.SetUnstructuredContent(content)
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	SetName(obj, ns, name)
}

func DeepEqual(a, b Object) bool {
	return equality.Semantic.DeepEqual(a, b)
}

func DeepCopyInto(in, out Object) {
	if in == nil || out == nil {
		return
	}
	out.SetUnstructuredContent(runtime.DeepCopyJSON(in.UnstructuredContent()))
	out.GetObjectKind().SetGroupVersionKind(in.GetObjectKind().GroupVersionKind())
	return
}

func DeepCopy(in Object) Object {
	if in == nil {
		return nil
	}

	out := new(unstructured.Unstructured)
	DeepCopyInto(in, out)
	return out
}
