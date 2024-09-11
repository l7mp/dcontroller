package object

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
)

// Object allows a docontroller view to be manipulated as a generic Kubernetes controller runtime
// object.
type ViewObject struct {
	unstructured.Unstructured
	View string
}

func NewViewObject(view string) *ViewObject {
	obj := ViewObject{Unstructured: unstructured.Unstructured{}, View: view}
	obj.SetGroupVersionKind(viewapiv1.NewGVK(view))
	return &obj
}

func NewFromUnstructured(u *unstructured.Unstructured) *ViewObject {
	return NewViewObject(u.GetKind()).WithName(u.GetNamespace(), u.GetName()).
		WithContent(u.UnstructuredContent())
}

func NewViewObjectFromNativeObject(view string, clientObj client.Object) (*ViewObject, error) {
	unstructuredObj := &unstructured.Unstructured{}
	var err error
	unstructuredObj.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(clientObj)
	if err != nil {
		return nil, err
	}

	obj := NewViewObject(view).WithName(clientObj.GetNamespace(), clientObj.GetName())
	obj.SetUnstructuredContent(unstructuredObj.UnstructuredContent())
	return obj, nil
}

func (obj *ViewObject) WithName(namespace, name string) *ViewObject {
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}

func (obj *ViewObject) WithContent(m UnstructuredContent) *ViewObject {
	namespace := obj.GetNamespace()
	if ns, ok, err := unstructured.NestedString(m, "metadata", "namespace"); err == nil && ok {
		namespace = ns
	}
	name := obj.GetName()
	if n, ok, err := unstructured.NestedString(m, "metadata", "name"); err == nil && ok {
		name = n
	}
	obj.SetUnstructuredContent(m)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	return obj
}

// Implement schema.ObjectKind
func (obj *ViewObject) GetObjectKind() schema.ObjectKind { return obj }

func (in *ViewObject) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// Implement client.Object
func (obj *ViewObject) GetAPIVersion() string {
	return viewapiv1.GroupVersion.String()
}

func (obj *ViewObject) SetAPIVersion(_ string) {
	obj.Unstructured.SetAPIVersion(viewapiv1.GroupVersion.String())
}

func (obj *ViewObject) GetKind() string {
	return obj.View
}

func (obj *ViewObject) SetKind(view string) {
	obj.Unstructured.SetKind(view)
	obj.View = view
}

func (obj *ViewObject) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.SetAPIVersion(gvk.GroupVersion().String())
	obj.SetKind(gvk.Kind)
}

func (obj *ViewObject) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(obj.View)
	return gvk
}

func (obj *ViewObject) GetID() string {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}.String()
}

// implement Unstructured
func (obj *ViewObject) SetUnstructuredContent(content map[string]any) {
	gvk := obj.GroupVersionKind()
	obj.Unstructured.SetUnstructuredContent(content)
	obj.SetGroupVersionKind(gvk)
}

// func (a *ViewObject) DeepEqual(b *ViewObject) bool {
// 	return a.View == b.View && equality.Semantic.DeepEqual(a, b)
// }

func (a *ViewObject) GroupResource() schema.GroupResource {
	return schema.GroupResource{Group: viewapiv1.GroupVersion.Group, Resource: a.View}
}
