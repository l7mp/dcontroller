package object

import (
	"crypto/sha256"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// NewUID creates a stable UID from GVK+namespace+name.
func NewUID(gvk schema.GroupVersionKind, namespace, name string) types.UID {
	key := fmt.Sprintf("%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace, name)
	hash := sha256.Sum256([]byte(key))

	// Format as UUID-like string (just for aesthetics).
	return types.UID(fmt.Sprintf("%x-%x-%x-%x-%x",
		hash[0:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16]))
}

// WithUID sets a deterministic UID if one doesn't exist.  For view objects, UIDs are generated
// from GVK+namespace+name, for native objects it does not do anything.
func WithUID(obj Object) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if !viewv1a1.IsViewKind(gvk) || obj.GetUID() != "" {
		return
	}
	obj.SetUID(NewUID(gvk, obj.GetNamespace(), obj.GetName()))
}

// RemoveUID removes the UID from view objects if one exists. For native objects it does not do
// anything.
func RemoveUID(obj Object) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if !viewv1a1.IsViewKind(gvk) || obj.GetUID() == "" {
		return
	}
	unstructured.RemoveNestedField(obj.UnstructuredContent(), "metadata", "uid")
}
