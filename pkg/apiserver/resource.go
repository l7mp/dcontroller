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

// findAPIResource is a helper to get APIResource: for views properties are hardcoded, for native
// resource we use the discovery API.
func (s *APIServer) findAPIResource(gvk schema.GroupVersionKind) (*Resource, error) {
	// Handle view objects.
	if gvk.Group == viewv1a1.GroupVersion.Group {
		name := strings.ToLower(gvk.Kind)
		return &Resource{
			GVK: gvk,
			APIResource: &metav1.APIResource{
				Name:         name, // lower-case kind (to avoid pluralization irregularities)
				SingularName: name,
				Namespaced:   true,
				Group:        viewv1a1.GroupVersion.Group,
				Version:      viewv1a1.GroupVersion.Version,
				Kind:         gvk.Kind,
			},
			HasStatus: true,
		}, nil
	}

	// Handle native objects.
	resourceList, err := s.discoveryClient.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		return nil, fmt.Errorf("failed to get server resources for %s: %w", gvk.GroupVersion().String(), err)
	}

	resource := &Resource{GVK: gvk}
	for i, r := range resourceList.APIResources {
		if r.Kind == gvk.Kind {
			resource.APIResource = &resourceList.APIResources[i]
			break
		}
	}

	if resource.APIResource == nil {
		return nil, fmt.Errorf("resource with Kind %s not found in group version %s", gvk.Kind,
			gvk.GroupVersion().String())
	}

	// status discovery
	mainResourceName := ""
	for _, r := range resourceList.APIResources {
		if r.Kind == gvk.Kind && !strings.Contains(r.Name, "/") {
			mainResourceName = r.Name
			break
		}
	}

	if mainResourceName != "" {
		// Look for the status subresource
		statusSubresourceName := mainResourceName + "/status"
		for _, r := range resourceList.APIResources {
			if r.Name == statusSubresourceName {
				resource.HasStatus = true
				break
			}
		}
	}

	return resource, nil
}

func listGVK(gvk schema.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	}
}
