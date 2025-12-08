package cache

// Utility functions that use the discovery client.

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

// NewViewObjectList creates an empty object list.
func NewViewObjectList(operator, view string) object.ObjectList {
	list := &unstructured.UnstructuredList{}
	discovery := NewViewDiscovery()
	objGVK := viewv1a1.GroupVersionKind(operator, view)
	listGVK := discovery.ListGVKFromObjectGVK(objGVK)
	list.SetGroupVersionKind(listGVK)
	return list
}

// AppendToListItem appends an object to a list.
func AppendToListItem(list client.ObjectList, obj client.Object) {
	listu, ok := list.(object.ObjectList)
	if !ok {
		return
	}
	u, ok := obj.(object.Object)
	if !ok {
		return
	}

	listu.Items = append(listu.Items, *object.DeepCopy(u))
}
