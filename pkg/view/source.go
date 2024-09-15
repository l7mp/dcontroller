package view

import (
	// corev1 "k8s.io/api/core/v1"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
)

type Source struct {
	Group         *string               `json:"apiGroup"`
	Version       *string               `json:"version"`
	Kind          string                `json:"kind"`
	Namespace     *string               `json:"namespace"`
	LabelSelector *metav1.LabelSelector `json:"labelSelector"`
	Predicate     *Predicate            `json:"predicate"`
}

func (s *Source) GetSource(mgr runtimeManager.Manager) (source.TypedSource[Request], error) {
	// gvk to watch
	gvk, err := s.GetGVK(mgr)
	if err != nil {
		return nil, err
	}

	var obj client.Object = &unstructured.Unstructured{}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	// prepare the predicate
	ps := []predicate.TypedPredicate[client.Object]{}
	if s.Predicate != nil {
		p, err := s.Predicate.ToPredicate()
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}

	if s.LabelSelector != nil {
		lp, err := predicate.LabelSelectorPredicate(*s.LabelSelector)
		if err != nil {
			return nil, err
		}
		ps = append(ps, lp)
	}

	if s.Namespace != nil {
		np := predicate.NewPredicateFuncs(func(object client.Object) bool {
			return object.GetNamespace() == *s.Namespace
		})
		ps = append(ps, np)
	}

	// generic handler
	src := source.TypedKind(mgr.GetCache(), obj, EventHandler[client.Object]{}, ps...)

	return src, nil
}

func (s *Source) GetGVK(mgr runtimeManager.Manager) (schema.GroupVersionKind, error) {
	if s.Group == nil || *s.Group == viewapiv1.GroupVersion.Group {
		// this will be a View, version is enforced
		return GetGVKByGroupKind(mgr, schema.GroupKind{Group: viewapiv1.GroupVersion.Group, Kind: s.Kind})
	}

	// this will be a standard Kubernetes object
	if s.Version == nil {
		return GetGVKByGroupKind(mgr, schema.GroupKind{Group: *s.Group, Kind: s.Kind})
	}
	return schema.GroupVersionKind{
		Group:   *s.Group,
		Version: *s.Version,
		Kind:    s.Kind,
	}, nil
}

// GetGVKByGroupKind returns a full GVK for a Group and a Kind. The function is smart enough to
// handle view references.
func GetGVKByGroupKind(m runtimeManager.Manager, gr schema.GroupKind) (schema.GroupVersionKind, error) {
	if gr.Group == viewapiv1.GroupVersion.Group {
		return schema.GroupVersionKind{
			Group:   viewapiv1.GroupVersion.Group,
			Kind:    gr.Kind,
			Version: viewapiv1.GroupVersion.Version,
		}, nil
	}

	// standard Kubernetes object
	mapper := m.GetRESTMapper()
	gvk, err := mapper.KindFor(schema.GroupVersionResource{Group: gr.Group, Resource: gr.Kind})
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("cannot find GVK for %s: %w", gr, err)
	}

	return gvk, nil
}
