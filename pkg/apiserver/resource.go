package apiserver

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// Resource defines a native or a view resource type for sources and targets.
type Resource struct {
	// GVK is the GroupVersionKind for the resource.
	GVK schema.GroupVersionKind
	// APIResource is the discovered API resource def for a native object.
	APIResource *metav1.APIResource
	// HasStatus is true if the resource has a status field.
	HasStatus bool
}

// findAPIResource is a helper to obtain an API Resource from a GVK. For views the properties are
// hardcoded, for native resources use the discovery API.
func (s *APIServer) findAPIResource(gvk schema.GroupVersionKind) (*Resource, error) {
	// Do not handle native objects.
	if !viewv1a1.IsViewKind(gvk) {
		return nil, fmt.Errorf("native API resource %q refused (only view resources are supported)",
			gvk.String())
	}

	// Handle view objects.
	name := strings.ToLower(gvk.Kind)
	return &Resource{
		GVK: gvk,
		APIResource: &metav1.APIResource{
			Name:         name, // lower-case kind (to avoid pluralization irregularities)
			SingularName: name,
			Namespaced:   true,
			Group:        gvk.Group,
			Version:      gvk.Version,
			Kind:         gvk.Kind,
		},
		HasStatus: true,
	}, nil
}

// listGVK returns the List GVK for a GVK.
func listGVK(gvk schema.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	}
}
