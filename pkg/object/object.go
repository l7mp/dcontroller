package object

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	apiv1 "hsnlab/dcontroller-runtime/pkg/api/v1"
)

var _ runtime.Object = &Object{}
var _ schema.ObjectKind = &Object{}

type Object struct {
	unstructured.Unstructured
	View string
}

func New(view, namespace, name string, content map[string]any) *Object {
	o := Object{Unstructured: unstructured.Unstructured{}, View: view}
	o.SetUnstructuredContent(content)
	o.SetNamespace(namespace)
	o.SetName(name)

	return &o
}

func (obj *Object) GetObjectKind() schema.ObjectKind { return obj }

func (in *Object) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(Object)
	*out = *in
	in.Unstructured.DeepCopyInto(&out.Unstructured)
	return out
}

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
	obj.SetKind(gvk.Kind)
}

func (obj *Object) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(obj.GetKind())
	return gvk
}

func (obj *Object) GetID() string {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}.String()
}
