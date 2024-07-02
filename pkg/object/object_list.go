package object

import (
	apiv1 "hsnlab/dcontroller-runtime/pkg/api/v1"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ runtime.Unstructured = &ObjectList{}

// var _ metav1.ListInterface = &ObjectList{}
var _ runtime.Object = &ObjectList{}
var _ schema.ObjectKind = &ObjectList{}

// ObjectList allows lists of views to be manipulated generically.
type ObjectList struct {
	View string

	Object map[string]interface{}

	// Items is a list of unstructured objects.
	Items []Object `json:"items"`
}

func NewList(view string) *ObjectList {
	obj := ObjectList{View: view}
	obj.SetGroupVersionKind(apiv1.NewGVK(view))
	return &obj
}

// Implement schema.ObjectKind
func (obj *ObjectList) GetObjectKind() schema.ObjectKind { return obj }

func (in *ObjectList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// Implement client.Object
func (l *ObjectList) GetAPIVersion() string {
	return apiv1.GroupVersion.String()
}

func (l *ObjectList) SetAPIVersion(_ string) {
	l.setNestedField(apiv1.GroupVersion.String(), "apiVersion")
}

func (l *ObjectList) GetKind() string {
	return l.View
}

func (l *ObjectList) SetKind(_ string) {
	l.setNestedField(l.View, "kind")
}

func (l *ObjectList) GetResourceVersion() string {
	ret, ok, err := unstructured.NestedString(l.Object, "metadata", "resourceVersion")
	if err != nil || !ok {
		return ""
	}
	return ret
}

func (l *ObjectList) SetResourceVersion(version string) {
	l.setNestedField(version, "metadata", "resourceVersion")
}

func (l *ObjectList) GetSelfLink() string {
	return l.getNestedString(l.Object, "metadata", "selfLink")
}

func (l *ObjectList) SetSelfLink(selfLink string) {
	l.setNestedField(selfLink, "metadata", "selfLink")
}

func (l *ObjectList) GetContinue() string {
	return l.getNestedString(l.Object, "metadata", "continue")
}

func (l *ObjectList) SetContinue(c string) {
	l.setNestedField(c, "metadata", "continue")
}

func (l *ObjectList) GetRemainingItemCount() *int64 {
	return l.getNestedInt64Pointer(l.Object, "metadata", "remainingItemCount")
}

func (l *ObjectList) SetRemainingItemCount(c *int64) {
	if c == nil {
		unstructured.RemoveNestedField(l.Object, "metadata", "remainingItemCount")
	} else {
		l.setNestedField(*c, "metadata", "remainingItemCount")
	}
}

func (l *ObjectList) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	l.SetAPIVersion(gvk.GroupVersion().String())
	l.SetKind(l.View)
}

func (l *ObjectList) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(l.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(l.View)
	return gvk
}

// implement runtime.Unstructured
func (l *ObjectList) IsList() bool { return true }

func (l *ObjectList) EachListItem(fn func(runtime.Object) error) error {
	for i := range l.Items {
		if err := fn(&l.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

func (l *ObjectList) EachListItemWithAlloc(fn func(runtime.Object) error) error {
	for i := range l.Items {
		if err := fn(New(l.Items[i].View).WithContent(l.Items[i].Object)); err != nil {
			return err
		}
	}
	return nil
}

func (l *ObjectList) NewEmptyInstance() runtime.Unstructured {
	out := new(ObjectList)
	if l != nil {
		out.SetGroupVersionKind(l.GroupVersionKind())
	}
	return out
}

func (l *ObjectList) UnstructuredContent() map[string]interface{} {
	out := make(map[string]interface{}, len(l.Object)+1)

	// shallow copy every property
	for k, v := range l.Object {
		out[k] = v
	}

	items := make([]interface{}, len(l.Items))
	for i, item := range l.Items {
		items[i] = item.UnstructuredContent()
	}
	out["items"] = items
	return out
}

func (l *ObjectList) SetUnstructuredContent(content map[string]interface{}) {
	l.Object = content
	if content == nil {
		l.Items = nil
		return
	}
	items, ok := l.Object["items"].([]interface{})
	if !ok || items == nil {
		items = []interface{}{}
	}
	objectItems := make([]Object, 0, len(items))
	newItems := make([]interface{}, 0, len(items))
	for _, item := range items {
		o, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		v, ok, err := unstructured.NestedString(o, "view")
		if err != nil || !ok {
			continue
		}
		objectItems = append(objectItems, Object{View: v, Unstructured: unstructured.Unstructured{Object: o}})
		newItems = append(newItems, o)
	}
	l.Items = objectItems
	l.Object["items"] = newItems
}

// deep* utils
func (in *ObjectList) DeepCopyInto(out *ObjectList) {
	if in == nil || out == nil {
		return
	}

	*out = *in
	out.Object = runtime.DeepCopyJSON(in.Object)
	out.Items = make([]Object, len(in.Items))
	for i := range in.Items {
		in.Items[i].DeepCopyInto(&out.Items[i])
	}
}

func (in *ObjectList) DeepCopy() *ObjectList {
	out := new(ObjectList)
	in.DeepCopyInto(out)
	return out
}

func (a *ObjectList) DeepEqual(b *ObjectList) bool {
	if a.View != b.View || !equality.Semantic.DeepEqual(a.Object, b.Object) || len(a.Items) != len(b.Items) {
		return false
	}

	for i := range a.Items {
		if !a.Items[i].DeepEqual(&b.Items[i]) {
			return false
		}
	}

	return true
}

// helpers
func (l *ObjectList) setNestedField(value interface{}, fields ...string) {
	if l.Object == nil {
		l.Object = make(map[string]interface{})
	}
	unstructured.SetNestedField(l.Object, value, fields...)
}

func (l *ObjectList) getNestedString(obj map[string]interface{}, fields ...string) string {
	val, found, err := unstructured.NestedString(obj, fields...)
	if !found || err != nil {
		return ""
	}
	return val
}

func (l *ObjectList) getNestedInt64Pointer(obj map[string]interface{}, fields ...string) *int64 {
	val, found, err := unstructured.NestedInt64(obj, fields...)
	if !found || err != nil {
		return nil
	}
	return &val
}
