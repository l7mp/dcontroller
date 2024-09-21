package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	runtimeSource "sigs.k8s.io/controller-runtime/pkg/source"

	viewv1a1 "hsnlab/dcontroller-runtime/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/util"
)

type Resource struct {
	Group   *string `json:"apiGroup,omitempty"`
	Version *string `json:"version,omitempty"`
	Kind    string  `json:"kind"`
}

func (r *Resource) String(mgr runtimeManager.Manager) string {
	gvk, err := r.GetGVK(mgr)
	if err != nil {
		return ""
	}
	// do not use the standard notation: it adds spaces
	gr := gvk.Group
	if gr == "" {
		gr = "core"
	}
	return fmt.Sprintf("%s/%s:%s", gr, gvk.Version, gvk.Kind)
}

func (r *Resource) GetGVK(mgr runtimeManager.Manager) (schema.GroupVersionKind, error) {
	if r.Kind == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("empty Kind in %s", util.Stringify(*r))
	}

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
	Namespace     *string               `json:"namespace,omitempty"`
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
	Predicate     *Predicate            `json:"predicate,omitempty"`
}

type source struct {
	*Source
	manager runtimeManager.Manager
	log     logr.Logger
}

func NewSource(mgr runtimeManager.Manager, s *Source) *source {
	src := &source{
		manager: mgr,
		Source:  s,
	}

	log := mgr.GetLogger().WithName("source").WithValues("name", s.Resource.String(mgr))
	src.log = log

	return src
}

func (s *source) String() string { return s.Resource.String(s.manager) }

func (s *source) GetSource() (runtimeSource.TypedSource[Request], error) {
	// gvk to watch
	gvk, err := s.GetGVK(s.manager)
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
	src := runtimeSource.TypedKind(s.manager.GetCache(), obj, EventHandler[client.Object]{}, ps...)

	s.log.V(4).Info("watch source: ready", "GVK", gvk.String(), "predicate-num", len(ps))

	return src, nil
}

type TargetType string

const (
	Updater TargetType = "Updater"
	Patcher TargetType = "Patcher"
)

type Target struct {
	Resource
	Type TargetType `json:"type,omitempty"`
}

type target struct {
	*Target
	manager runtimeManager.Manager
	log     logr.Logger
}

func NewTarget(mgr runtimeManager.Manager, t *Target) *target {
	target := &target{
		manager: mgr,
		Target:  t,
	}

	log := mgr.GetLogger().WithName("target").WithValues("name", t.Resource.String(mgr))
	target.log = log

	return target
}

func (t *target) String() string {
	return fmt.Sprintf("%s<type:%s>", t.Resource.String(t.manager), t.Type)
}

// Write enforces a delta on a target. The behavior depends on the target type:
//   - For Updaters the delta is enforced as is to the target
//   - For Patchers the delta object is applied as a strategic merge patch: for Add and Update
//     deltas the target is patched with the delta object, while for Delete the delta object
//     content is removed from the target using a strategic merge patch.
func (t *target) Write(ctx context.Context, delta cache.Delta) error {
	if delta.Object == nil {
		return errors.New("write: empty object in delta")
	}

	// gvk to watch
	gvk, err := t.GetGVK(t.manager)
	if err != nil {
		return err
	}

	// make a private copy of the Object
	delta.Object = object.DeepCopy(delta.Object)

	// make sure delta object gets the correct GVK applied
	delta.Object.SetGroupVersionKind(gvk)

	switch t.Type {
	case "Updater", "":
		return t.update(ctx, delta)
	case "Patcher":
		return t.patch(ctx, delta)
	default:
		return fmt.Errorf("unknown target type: %s", t.Type)
	}
}

func (t *target) update(ctx context.Context, delta cache.Delta) error {
	c := t.manager.GetClient()

	switch delta.Type {
	case cache.Added:
		t.log.V(4).Info("create", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		return c.Create(ctx, delta.Object)
	case cache.Updated, cache.Replaced:
		t.log.Info("update", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		return c.Update(ctx, delta.Object)
	case cache.Deleted:
		t.log.V(4).Info("delete", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))
		return c.Delete(ctx, delta.Object)
	default:
		t.log.V(2).Info("target: ignoring delta", "type", delta.Type)
		return nil
	}
}

func (t *target) patch(ctx context.Context, delta cache.Delta) error {
	c := t.manager.GetClient()

	switch delta.Type {
	case cache.Added, cache.Updated, cache.Replaced:
		t.log.V(4).Info("update-patch", "event-type", delta.Type,
			"key", client.ObjectKeyFromObject(delta.Object).String(),
			"patch", object.Dump(delta.Object))

		patch, err := json.Marshal(object.DeepCopy(delta.Object).UnstructuredContent())
		if err != nil {
			return err
		}

		oldObj := object.New()
		oldObj.SetGroupVersionKind(delta.Object.GroupVersionKind())
		oldObj.SetName(delta.Object.GetName())
		oldObj.SetNamespace(delta.Object.GetNamespace())
		if err := t.manager.GetClient().Get(ctx, client.ObjectKeyFromObject(oldObj), oldObj); err != nil {
			return err
		}

		// TODO: strategic merge patch fails with error "unable to find api field in struct
		// Unstructured for the json field \"metadata\""}"
		// return c.Patch(ctx, result, client.RawPatch(types.StrategicMergePatchType, patch))

		// fall back to simple merge patches
		return c.Patch(ctx, oldObj, client.RawPatch(types.MergePatchType, patch))

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

		t.log.V(4).Info("delete-patch", "event-type", delta.Type,
			"object", client.ObjectKeyFromObject(delta.Object),
			"patch", util.Stringify(patch), "raw-patch", string(b))

		return c.Patch(context.Background(), delta.Object, client.RawPatch(types.StrategicMergePatchType, b))
	default:
		t.log.V(2).Info("target: ignoring delta", "type", delta.Type)

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
