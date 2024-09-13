package view

// import (
// 	"fmt"

// 	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
// 	"k8s.io/apimachinery/pkg/runtime/schema"
// 	"sigs.k8s.io/controller-runtime/pkg/predicate"

// 	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
// 	"hsnlab/dcontroller-runtime/pkg/manager"
// )

// type Predicate = predicate.TypedPredicate[*unstructured.Unstructured]

// type ResourceRef struct {
// 	Group string `json:"apiGroup"`
// 	Kind  string `json:"kind"`
// 	// Options are additional config for watching the resource, like predicates, etc.
// 	Options []ResourceRefOption `json:"options"`
// }

// type ResourceRefOption struct{}

// // GetGVKByGroupKind returns a full GVK for a Group and a Kind. The function is smart enough to
// // handle view references.
// func GetGVKByGroupKind(m *manager.Manager, gr schema.GroupKind) (schema.GroupVersionKind, error) {
// 	if gr.Group == viewapiv1.GroupVersion.Group {
// 		return schema.GroupVersionKind{
// 			Group:   viewapiv1.GroupVersion.Group,
// 			Kind:    gr.Kind,
// 			Version: viewapiv1.GroupVersion.Version,
// 		}, nil
// 	}

// 	// standarg Kubernetes object
// 	mapper := m.GetRESTMapper()
// 	gvk, err := mapper.KindFor(schema.GroupVersionResource{Group: gr.Group, Resource: gr.Kind})
// 	if err != nil {
// 		return schema.GroupVersionKind{}, fmt.Errorf("error finding GVK: %w", err)
// 	}

// 	return gvk, nil
// }
