package apiserver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

func (s *APIServer) getOpenAPIv2Handler() openapicommon.GetOpenAPIDefinitions {
	return func(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
		// Called by Kubernetes' OpenAPI system each time someone requests the spec
		s.mu.RLock()
		cachedOpenAPIDefs := s.cachedOpenAPIDefs
		s.mu.RUnlock()

		// If cache is non-empty, return the cached spec
		if cachedOpenAPIDefs != nil {
			return cachedOpenAPIDefs
		}

		defs := s.generateOpenAPIv2Defs(ref)

		s.mu.RLock()
		s.cachedOpenAPIDefs = defs
		s.mu.RUnlock()

		return defs
	}
}

func (s *APIServer) generateOpenAPIv2Defs(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Rebuild the spec and set cache
	var gvks []schema.GroupVersionKind
	for _, groupGVKs := range s.groupGVKs {
		for gvk := range groupGVKs {
			gvks = append(gvks, gvk)
		}
	}

	// Base definitions from Kubernetes for common types
	defs := generatedopenapi.GetOpenAPIDefinitions(ref)

	unstructuredDefName := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
	defs[unstructuredDefName] = s.genOpenAPIUnstructDef(ref)

	unstructuredListDefName := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"
	defs[unstructuredListDefName] = s.genOpenAPIUnstructListDef(ref, unstructuredDefName)

	for _, gvk := range gvks {
		if !viewv1a1.IsViewKind(gvk) {
			continue
		}

		// resource
		defName := fmt.Sprintf("%s.%s.%s", gvk.Group, gvk.Version, gvk.Kind)
		defs[defName] = s.genOpenAPIDef(gvk, ref)

		// resourcelist
		listDefName := fmt.Sprintf("%s/%s.%s", gvk.Group, gvk.Version, listGVK(gvk).Kind)
		defs[listDefName] = s.genOpenAPIListDef(gvk, ref, defName)
	}

	return defs
}

func (s *APIServer) getOpenAPIv3Handler() openapicommon.GetOpenAPIDefinitions {
	return func(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
		// Called by Kubernetes' OpenAPI system each time someone requests the spec
		s.mu.RLock()
		cachedOpenAPIV3Defs := s.cachedOpenAPIV3Defs
		s.mu.RUnlock()

		// If cache is non-empty, return the cached spec
		if cachedOpenAPIV3Defs != nil {
			return cachedOpenAPIV3Defs
		}

		defs := s.generateOpenAPIv3Defs(ref)
		s.mu.RLock()
		s.cachedOpenAPIV3Defs = defs
		s.mu.RUnlock()

		return defs
	}
}

func (s *APIServer) generateOpenAPIv3Defs(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Rebuild the spec and set cache
	var gvks []schema.GroupVersionKind
	for _, groupGVKs := range s.groupGVKs {
		for gvk := range groupGVKs {
			gvks = append(gvks, gvk)
		}
	}

	// Add base definitions from Kubernetes for common types like ObjectMeta, ListMeta
	// and common APIs from core/v1 and apps/v1
	defs := generatedopenapi.GetOpenAPIDefinitions(ref)

	unstructuredDefName := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
	defs[unstructuredDefName] = s.genOpenAPIUnstructDef(ref)

	unstructuredListDefName := "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"
	defs[unstructuredListDefName] = s.genOpenAPIUnstructListDef(ref, unstructuredDefName)

	for _, gvk := range gvks {
		if !viewv1a1.IsViewKind(gvk) {
			continue
		}

		// resource e.g., "view.dcontroller.io.v1alpha1.MyView"
		defName := fmt.Sprintf("%s.%s.%s", gvk.Group, gvk.Version, gvk.Kind)
		defs[defName] = s.genOpenAPIDef(gvk, ref) // Same as v2

		// resourcelist
		listDefName := fmt.Sprintf("%s/%s.%s", gvk.Group, gvk.Version, listGVK(gvk).Kind)
		defs[listDefName] = s.genOpenAPIListDef(gvk, ref, defName) // Same as v2
	}

	return defs
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
							Description: "Standard object metadata.",
							Default:     map[string]any{}, // Optional
							// Resolved from generatedopenapi.GetOpenAPIDefinitions.
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
