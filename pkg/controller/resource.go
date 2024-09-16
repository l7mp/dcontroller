package view

import (
	// corev1 "k8s.io/api/core/v1"
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	viewv1a1 "hsnlab/dcontroller-runtime/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/util"
)

type Resource struct {
	Group   *string `json:"apiGroup"`
	Version *string `json:"version"`
	Kind    string  `json:"kind"`
}

func (r *Resource) GetGVK(mgr runtimeManager.Manager) (schema.GroupVersionKind, error) {
	if r.Group == nil || *r.Group == viewv1a1.GroupVersion.Group {
		// this will be a View, version is enforced
		return GetGVKByGroupKind(mgr, schema.GroupKind{Group: viewv1a1.GroupVersion.Group, Kind: r.Kind})
	}

	// this will be a standard Kubernetes object
	if r.Version == nil {
		return GetGVKByGroupKind(mgr, schema.GroupKind{Group: *r.Group, Kind: r.Kind})
	}
	return schema.GroupVersionKind{
		Group:   *r.Group,
		Version: *r.Version,
		Kind:    r.Kind,
	}, nil
}

func GetGVKByGroupKind(m runtimeManager.Manager, gr schema.GroupKind) (schema.GroupVersionKind, error) {
	if gr.Group == viewv1a1.GroupVersion.Group {
		return schema.GroupVersionKind{
			Group:   viewv1a1.GroupVersion.Group,
			Kind:    gr.Kind,
			Version: viewv1a1.GroupVersion.Version,
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

type Source struct {
	Resource
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

type TargetType string

const (
	Updater TargetType = "Updater"
	Patcher TargetType = "Patcher"
)

type Target struct {
	Resource
	Type TargetType `json:"target,omitempty"`
}

// Write enforces a delta on a target. The behavior depends on the target type:
//   - For Updaters the delta is enforced as is to the target
//   - For Patchers the delta object is applied as a strategic merge patch: for Add and Update
//     deltas the target is patched with the delta object, while for Delete the delta object
//     content is removed from the target using a strategic merge patch.
func (t *Target) Write(ctx context.Context, mgr runtimeManager.Manager, delta cache.Delta) error {
	if delta.Object == nil {
		return errors.New("write: empty object in delta")
	}

	// gvk to watch
	gvk, err := t.GetGVK(mgr)
	if err != nil {
		return err
	}

	// make sure delta object gets the correct GVK applied
	delta.Object.SetGroupVersionKind(gvk)

	switch t.Type {
	case "Updater", "":
		return t.update(ctx, mgr, delta)
	case "Patcher":
		return t.patch(ctx, mgr, delta)
	default:
		return fmt.Errorf("unknown target type: %s", t.Type)
	}
}

func (t *Target) update(ctx context.Context, mgr runtimeManager.Manager, delta cache.Delta) error {
	c := mgr.GetClient()
	log := mgr.GetLogger().WithName("target-updater")

	switch delta.Type {
	case cache.Added:
		log.V(4).Info("create", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		return c.Create(ctx, delta.Object)
	case cache.Updated, cache.Replaced:
		log.Info("update", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		return c.Update(ctx, delta.Object)
	case cache.Deleted:
		log.V(4).Info("delete", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		return c.Delete(ctx, delta.Object)
	default:
		log.V(2).Info("target: ignoring delta", "type", delta.Type)
		return nil
	}
}

func (t *Target) patch(ctx context.Context, mgr runtimeManager.Manager, delta cache.Delta) error {
	c := mgr.GetClient()
	log := mgr.GetLogger().WithName("target-patcher")

	switch delta.Type {
	case cache.Added, cache.Updated, cache.Replaced:
		log.V(4).Info("update-patch", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		patch, err := json.Marshal(delta.Object.UnstructuredContent())
		if err != nil {
			return err
		}

		return c.Patch(ctx, delta.Object, client.RawPatch(types.StrategicMergePatchType, patch))
	case cache.Deleted:
		// apply the patch locally so that we fully control the behavior
		patch := removeNested(delta.Object.UnstructuredContent())

		// make sure we do not remove crucial metadata: the GVK and the namespace/name
		gvk := delta.Object.GroupVersionKind()
		gr := schema.GroupVersion{Group: gvk.Group, Version: gvk.Version}
		unstructured.SetNestedField(patch, gr.String(), "apiVersion")
		unstructured.SetNestedField(patch, gvk.Kind, "kind")
		unstructured.SetNestedField(patch, delta.Object.GetNamespace(), "metadata", "namespace")
		unstructured.SetNestedField(patch, delta.Object.GetName(), "metadata", "name")

		b, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		log.V(4).Info("delete-patch", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object),
			"patch", util.Stringify(patch), "raw-patch", string(b))
		return c.Patch(context.Background(), delta.Object, client.RawPatch(types.StrategicMergePatchType, b))
	default:
		log.V(2).Info("target: ignoring delta", "type", delta.Type)
		return nil

	}
}

func removeNested(m map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range m {
		if nestedMap, ok := v.(map[string]any); ok {
			result[k] = removeNested(nestedMap)
		} else if nestedSlice, ok := v.([]any); ok {
			// TODO: handle nested slices!!!!
			result[k] = nestedSlice
		} else {
			result[k] = nil
		}
	}
	return result
}
