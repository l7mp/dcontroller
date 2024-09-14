package view

import (
	// corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type Source struct {
	Group         *string               `json:"apiGroup"`
	Version       *string               `json:"version"`
	Kind          string                `json:"kind"`
	Namespace     *string               `json:"namespace"`
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"` // https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/predicate#LabelSelectorPredicate
}

func (s *Source) GetSource(mgr runtimeManager.Manager) source.Source {
	return nil
}

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
