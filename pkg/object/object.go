package object

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "hsnlab/dcontroller/pkg/api/view/v1alpha1"
)

type Object = *unstructured.Unstructured
type ObjectList = *unstructured.UnstructuredList

func New() Object {
	return &unstructured.Unstructured{}
}

func NewViewObject(view string) Object {
	obj := New()
	obj.SetUnstructuredContent(map[string]any{})
	obj.SetGroupVersionKind(viewv1a1.NewGVK(view))
	return obj
}

func NewViewObjectFromNativeObject(view string, clientObj client.Object) (Object, error) {
	unstructuredObj := &unstructured.Unstructured{}
	var err error
	unstructuredObj.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(clientObj)
	if err != nil {
		return nil, err
	}

	obj := NewViewObject(view)
	SetName(obj, clientObj.GetNamespace(), clientObj.GetName())
	SetContent(obj, unstructuredObj.UnstructuredContent())
	return obj, nil
}

// SetName is a shortcut to SetNamespace(ns) followed by SetNamespace(name).
func SetName(obj Object, ns, name string) {
	obj.SetNamespace(ns)
	obj.SetName(name)
}

// SetContent is similar to SetUnstructuredContent but it preserves the GVK, the name and the
// namespace and deep-copies the content.
func SetContent(obj Object, content map[string]any) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	ns, name := obj.GetNamespace(), obj.GetName()
	obj.SetUnstructuredContent(runtime.DeepCopyJSON(content))
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

func NewViewObjectList(view string) ObjectList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(viewv1a1.NewGVK(view))
	return list
}

func AppendToListItem(list client.ObjectList, obj client.Object) {
	listu, ok := list.(ObjectList)
	if !ok {
		return
	}
	u, ok := obj.(Object)
	if !ok {
		return
	}

	listu.Items = append(listu.Items, *DeepCopy(u))
}
