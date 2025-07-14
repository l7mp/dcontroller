package apiserver

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

type Resource struct {
	GVK         schema.GroupVersionKind
	APIResource *metav1.APIResource
	HasStatus   bool
}

// findAPIResource is a helper to obtain an API Resource from a GVK. For views properties are
// hardcoded, for native resource we use the discovery API.
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

func listGVK(gvk schema.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	}
}
