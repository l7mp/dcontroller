package object

import (
	apiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ runtime.Unstructured = &ViewObjectList{}

// var _ metav1.ListInterface = &ViewObjectList{}
var _ runtime.Object = &ViewObjectList{}
var _ schema.ObjectKind = &ViewObjectList{}

// ViewObjectList allows lists of views to be manipulated generically.
type ViewObjectList struct {
	View string

	Object map[string]interface{}

	// Items is a list of unstructured objects.
	Items []ViewObject `json:"items"`
}

func NewViewObjectList(view string) *ViewObjectList {
	obj := ViewObjectList{View: view}
	obj.SetGroupVersionKind(apiv1.NewGVK(view))
	return &obj
}

// Implement schema.ObjectKind
func (obj *ViewObjectList) GetObjectKind() schema.ObjectKind { return obj }

func (in *ViewObjectList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// Implement client.Object
func (l *ViewObjectList) GetAPIVersion() string {
	return apiv1.GroupVersion.String()
}

func (l *ViewObjectList) SetAPIVersion(_ string) {
	l.setNestedField(apiv1.GroupVersion.String(), "apiVersion")
}

func (l *ViewObjectList) GetKind() string {
	return l.View
}

func (l *ViewObjectList) SetKind(kind string) {
	l.setNestedField(l.View, kind)
}

func (l *ViewObjectList) GetResourceVersion() string {
	ret, ok, err := unstructured.NestedString(l.Object, "metadata", "resourceVersion")
	if err != nil || !ok {
		return ""
	}
	return ret
}

func (l *ViewObjectList) SetResourceVersion(version string) {
	l.setNestedField(version, "metadata", "resourceVersion")
}

func (l *ViewObjectList) GetSelfLink() string {
	return l.getNestedString(l.Object, "metadata", "selfLink")
}

func (l *ViewObjectList) SetSelfLink(selfLink string) {
	l.setNestedField(selfLink, "metadata", "selfLink")
}

func (l *ViewObjectList) GetContinue() string {
	return l.getNestedString(l.Object, "metadata", "continue")
}

func (l *ViewObjectList) SetContinue(c string) {
	l.setNestedField(c, "metadata", "continue")
}

func (l *ViewObjectList) GetRemainingItemCount() *int64 {
	return l.getNestedInt64Pointer(l.Object, "metadata", "remainingItemCount")
}

func (l *ViewObjectList) SetRemainingItemCount(c *int64) {
	if c == nil {
		unstructured.RemoveNestedField(l.Object, "metadata", "remainingItemCount")
	} else {
		l.setNestedField(*c, "metadata", "remainingItemCount")
	}
}

func (l *ViewObjectList) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	l.SetAPIVersion(gvk.GroupVersion().String())
	l.SetKind(l.View)
}

func (l *ViewObjectList) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(l.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(l.View)
	return gvk
}

// implement runtime.Unstructured
func (l *ViewObjectList) IsList() bool { return true }

func (l *ViewObjectList) EachListItem(fn func(runtime.Object) error) error {
	for i := range l.Items {
		if err := fn(&l.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

func (l *ViewObjectList) EachListItemWithAlloc(fn func(runtime.Object) error) error {
	for i := range l.Items {
		if err := fn(NewViewObject(l.Items[i].View).WithContent(l.Items[i].Object)); err != nil {
			return err
		}
	}
	return nil
}

func (l *ViewObjectList) NewEmptyInstance() runtime.Unstructured {
	out := new(ViewObjectList)
	if l != nil {
		out.SetGroupVersionKind(l.GroupVersionKind())
	}
	return out
}

func (l *ViewObjectList) UnstructuredContent() map[string]interface{} {
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

func (l *ViewObjectList) SetUnstructuredContent(content map[string]interface{}) {
	l.Object = content
	if content == nil {
		l.Items = nil
		return
	}
	items, ok := l.Object["items"].([]interface{})
	if !ok || items == nil {
		items = []interface{}{}
	}
	objectItems := make([]ViewObject, 0, len(items))
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
		objectItems = append(objectItems, ViewObject{View: v, Unstructured: unstructured.Unstructured{Object: o}})
		newItems = append(newItems, o)
	}
	l.Items = objectItems
	l.Object["items"] = newItems
}

// deep* utils
func (in *ViewObjectList) DeepCopyInto(out *ViewObjectList) {
	if in == nil || out == nil {
		return
	}

	*out = *in
	out.Object = runtime.DeepCopyJSON(in.Object)
	out.Items = make([]ViewObject, len(in.Items))
	for i := range in.Items {
		DeepCopyInto(&in.Items[i], &out.Items[i])
	}
}

func (in *ViewObjectList) DeepCopy() *ViewObjectList {
	out := new(ViewObjectList)
	in.DeepCopyInto(out)
	return out
}

func (a *ViewObjectList) DeepEqual(b *ViewObjectList) bool {
	if a.View != b.View || !equality.Semantic.DeepEqual(a.Object, b.Object) || len(a.Items) != len(b.Items) {
		return false
	}

	for i := range a.Items {
		if !DeepEqual(&a.Items[i], &b.Items[i]) {
			return false
		}
	}

	return true
}

// helpers
func (l *ViewObjectList) setNestedField(value interface{}, fields ...string) {
	if l.Object == nil {
		l.Object = make(map[string]interface{})
	}
	unstructured.SetNestedField(l.Object, value, fields...)
}

func (l *ViewObjectList) getNestedString(obj map[string]interface{}, fields ...string) string {
	val, found, err := unstructured.NestedString(obj, fields...)
	if !found || err != nil {
		return ""
	}
	return val
}

func (l *ViewObjectList) getNestedInt64Pointer(obj map[string]interface{}, fields ...string) *int64 {
	val, found, err := unstructured.NestedInt64(obj, fields...)
	if !found || err != nil {
		return nil
	}
	return &val
}
