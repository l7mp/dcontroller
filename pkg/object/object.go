package object

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// Object is an alias for a Kubernetes unstructured object used internally as an object
// representation.
type Object = *unstructured.Unstructured

// ObjectList is an alias for a Kubernetes unstructured lists.
type ObjectList = *unstructured.UnstructuredList //nolint:revive

// New create a new object.
func New() Object {
	return &unstructured.Unstructured{}
}

// NewViewObject initializes an empty object in the given view and sets the GVK.
func NewViewObject(view string) Object {
	obj := New()
	obj.SetUnstructuredContent(map[string]any{})
	obj.SetGroupVersionKind(viewv1a1.NewGVK(view))
	return obj
}

// NewViewObjectFromNativeObject creates a view object from a client.Object.
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

// DeepEqual compares to Objects.
func DeepEqual(a, b Object) bool {
	return equality.Semantic.DeepEqual(a, b)
}

// DeepCopyInto copies an Object into another one.
func DeepCopyInto(in, out Object) {
	if in == nil || out == nil {
		return
	}
	out.SetUnstructuredContent(runtime.DeepCopyJSON(in.UnstructuredContent()))
	out.GetObjectKind().SetGroupVersionKind(in.GetObjectKind().GroupVersionKind())
}

// DeepCopy copies an Object.
func DeepCopy(in Object) Object {
	if in == nil {
		return nil
	}

	out := new(unstructured.Unstructured)
	DeepCopyInto(in, out)
	return out
}

// NewViewObjectList creates an empty object list.
func NewViewObjectList(view string) ObjectList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(viewv1a1.NewGVK(view))
	return list
}

// AppendToListItem appends an object to a list.
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
