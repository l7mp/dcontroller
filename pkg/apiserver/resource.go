package apiserver

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

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

// genOpenAPIDef generates an OpenAPI definition for a particular GVK.
func (s *APIServer) genOpenAPIDef(gvk schema.GroupVersionKind, ref openapicommon.ReferenceCallback) openapicommon.OpenAPIDefinition {
	return openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: fmt.Sprintf("Generic %s view resource", gvk.Kind),
				Type:        []string{"object"},
				Properties: map[string]spec.Schema{
					"apiVersion": {
						SchemaProps: spec.SchemaProps{
							Type:        []string{"string"},
							Description: "APIVersion defines the versioned schema of this representation of an object.",
						},
					},
					"kind": {
						SchemaProps: spec.SchemaProps{
							Type:        []string{"string"},
							Description: "Kind is a string value representing the REST resource this object represents.",
						},
					},
					"metadata": {
						SchemaProps: spec.SchemaProps{
							Description: "Standard object metadata.",
							Default:     map[string]any{}, // Optional
							// Resolved from generatedopenapi.GetOpenAPIDefinitions.
							Ref: ref("k8s.io.apimachinery.pkg.apis.meta.v1.ObjectMeta"),
						},
					},
				},
				// Enforce apiVersion, kind, and metadata at the schema level:
				Required: []string{"apiVersion", "kind", "metadata"},
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					// Crucial: Links this schema definition to the GVK
					"x-kubernetes-group-version-kind": []interface{}{
						map[string]interface{}{
							"group":   gvk.Group,
							"version": gvk.Version,
							"kind":    gvk.Kind,
						},
					},
					// Preserve all fields not explicitly defined in 'properties'.
					"x-kubernetes-preserve-unknown-fields": true,
				},
			},
		},
		Dependencies: []string{
			"k8s.io.apimachinery.pkg.apis.meta.v1.ObjectMeta",
		},
	}
}

// genOpenAPIDef generates an OpenAPI definition for a particular GVK.
func (s *APIServer) genOpenAPIListDef(gvk schema.GroupVersionKind, ref openapicommon.ReferenceCallback, defName string) openapicommon.OpenAPIDefinition {
	return openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: fmt.Sprintf("List of %s view resources.", gvk.Kind),
				Type:        []string{"object"},
				Required:    []string{"items"}, // A list resource must have an 'items' array
				Properties: map[string]spec.Schema{
					"apiVersion": {
						SchemaProps: spec.SchemaProps{Type: []string{"string"}},
					},
					"kind": {
						SchemaProps: spec.SchemaProps{Type: []string{"string"}},
					},
					"metadata": {
						SchemaProps: spec.SchemaProps{
							Ref: ref("k8s.io.apimachinery.pkg.apis.meta.v1.ListMeta"),
						},
					},
					"items": {
						SchemaProps: spec.SchemaProps{
							Type: []string{"array"},
							Items: &spec.SchemaOrArray{
								Schema: &spec.Schema{
									SchemaProps: spec.SchemaProps{
										Ref: ref(defName), // Each item is a View resource
									},
								},
							},
						},
					},
				},
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-group-version-kind": []interface{}{
						map[string]interface{}{
							"group":   gvk.Group,
							"version": gvk.Version,
							"kind":    listGVK(gvk).Kind,
						},
					},
				},
			},
		},
		Dependencies: []string{
			"k8s.io.apimachinery.pkg.apis.meta.v1.ListMeta",
			defName, // Depends on the TestView definition
		},
	}
}

// genOpenAPIUnstructDef generates an OpenAPI definition for unstructured.Unstructured.
func (s *APIServer) genOpenAPIUnstructDef(_ openapicommon.ReferenceCallback) openapicommon.OpenAPIDefinition {
	return openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "Unstructured allows objects that do not have Golang structs registered to be manipulated dynamically.",
				Type:        []string{"object"},
				// No specific properties are defined here, as it's truly unstructured at this level.
				// The actual fields will be handled by 'x-kubernetes-preserve-unknown-fields'.
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-preserve-unknown-fields": true,
				},
			},
		},
		// No specific dependencies for the generic Unstructured definition itself,
		// unless you wanted to mandate apiVersion/kind/metadata here, but that's
		// usually better handled in the GVK-specific definition.
	}
}

// genOpenAPIUnstructListDef generates an OpenAPI definition for unstructured.UnstructuredList.
func (s *APIServer) genOpenAPIUnstructListDef(ref openapicommon.ReferenceCallback, def string) openapicommon.OpenAPIDefinition {
	return openapicommon.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "UnstructuredList allows lists of objects that do not have Golang structs registered to be manipulated dynamically.",
				Type:        []string{"object"},
				Properties: map[string]spec.Schema{
					// Standard list fields
					"apiVersion": {
						SchemaProps: spec.SchemaProps{
							Type:        []string{"string"},
							Description: "APIVersion defines the versioned schema of this object.",
						},
					},
					"kind": {
						SchemaProps: spec.SchemaProps{
							Type:        []string{"string"},
							Description: "Kind is the REST resource for this object.",
						},
					},
					"metadata": {
						SchemaProps: spec.SchemaProps{
							Description: "Standard list metadata.",
							Ref:         ref("k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta"),
						},
					},
					"items": {
						SchemaProps: spec.SchemaProps{
							Type: []string{"array"},
							Items: &spec.SchemaOrArray{
								Schema: &spec.Schema{
									SchemaProps: spec.SchemaProps{
										// Items are generic Unstructured objects
										Ref: ref(def),
									},
								},
							},
						},
					},
				},
				Required: []string{"items"}, // A list must have items
			},
		},
		Dependencies: []string{
			"k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta",
			def, // Depends on the generic Unstructured definition
		},
	}
}

func listGVK(gvk schema.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	}
}
