package object

import (
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "hsnlab/dcontroller-runtime/pkg/api/v1"
)

var _ client.Object = &Object{}
var _ schema.ObjectKind = &Object{}
var _ metav1.ListInterface = &Object{}
var _ runtime.Unstructured = &Object{}

// Object allows a docontroller view to be manipulated as a generic Kubernetes controller runtime
// object.
type Object struct {
	unstructured.Unstructured
	View string
}

func New(view string) *Object {
	obj := Object{Unstructured: unstructured.Unstructured{}, View: view}
	obj.SetGroupVersionKind(apiv1.NewGVK(view))
	return &obj
}

func NewFromNativeObject(view string, clientObj client.Object) (*Object, error) {
	unstructuredObj := &unstructured.Unstructured{}
	var err error
	unstructuredObj.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(clientObj)
	if err != nil {
		return nil, err
	}

	obj := New(view).WithName(clientObj.GetNamespace(), clientObj.GetName())
	obj.SetUnstructuredContent(unstructuredObj.Object)
	return obj, nil
}

func (obj *Object) WithName(namespace, name string) *Object {
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}

func (obj *Object) WithContent(content map[string]any) *Object {
	gvk := obj.GroupVersionKind()
	namespace, name := obj.GetNamespace(), obj.GetName()
	obj.SetUnstructuredContent(runtime.DeepCopyJSON(content))
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	return obj
}

// Implement schema.ObjectKind
func (obj *Object) GetObjectKind() schema.ObjectKind { return obj }

func (in *Object) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// Implement client.Object
func (obj *Object) GetAPIVersion() string {
	return apiv1.GroupVersion.String()
}

func (obj *Object) SetAPIVersion(_ string) {
	obj.Unstructured.SetAPIVersion(apiv1.GroupVersion.String())
}

func (obj *Object) GetKind() string {
	return obj.View
}

func (obj *Object) SetKind(_ string) {
	obj.Unstructured.SetKind(obj.View)
}

func (obj *Object) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.SetAPIVersion(gvk.GroupVersion().String())
	obj.SetKind(obj.View)
}

func (obj *Object) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(obj.View)
	return gvk
}

func (obj *Object) GetID() string {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}.String()
}

// implement runtime.Unstructured
func (obj *Object) SetUnstructuredContent(content map[string]any) {
	gvk := obj.GroupVersionKind()
	obj.Unstructured.SetUnstructuredContent(content)
	obj.SetGroupVersionKind(gvk)
}

// deep* utils
func (in *Object) DeepCopyInto(out *Object) {
	if in == nil || out == nil {
		return
	}
	*out = *in
	out.Unstructured.Object = runtime.DeepCopyJSON(in.Unstructured.Object)
	return
}

func (in *Object) DeepCopy() *Object {
	if in == nil {
		return nil
	}
	out := new(Object)
	in.DeepCopyInto(out)
	return out
}

func (a *Object) DeepEqual(b *Object) bool {
	return a.View == b.View && equality.Semantic.DeepEqual(a, b)
}

func (a *Object) GroupResource() schema.GroupResource {
	return schema.GroupResource{Group: apiv1.GroupVersion.Group, Resource: a.View}
}
