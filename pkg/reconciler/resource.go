package reconciler

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	runtimePredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	runtimeSource "sigs.k8s.io/controller-runtime/pkg/source"

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "hsnlab/dcontroller/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller/pkg/cache"
	"hsnlab/dcontroller/pkg/object"
	"hsnlab/dcontroller/pkg/predicate"
	"hsnlab/dcontroller/pkg/util"
)

type Resource interface {
	fmt.Stringer
	GetGVK() (schema.GroupVersionKind, error)
}

type resource struct {
	mgr      runtimeManager.Manager
	resource opv1a1.Resource
}

func NewResource(mgr runtimeManager.Manager, r opv1a1.Resource) Resource {
	return &resource{
		mgr:      mgr,
		resource: r,
	}
}

func (r *resource) String() string {
	gvk, err := r.GetGVK()
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

func (r *resource) GetGVK() (schema.GroupVersionKind, error) {
	if r.resource.Kind == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("empty Kind in %s", util.Stringify(*r))
	}

	if r.resource.Group == nil || *r.resource.Group == viewv1a1.GroupVersion.Group {
		// this will be a View, version is enforced
		return r.getGVKByGroupKind(schema.GroupKind{Group: viewv1a1.GroupVersion.Group, Kind: r.resource.Kind})
	}

	// this will be a standard Kubernetes object
	if r.resource.Version == nil {
		return r.getGVKByGroupKind(schema.GroupKind{Group: *r.resource.Group, Kind: r.resource.Kind})
	}
	return schema.GroupVersionKind{
		Group:   *r.resource.Group,
		Version: *r.resource.Version,
		Kind:    r.resource.Kind,
	}, nil
}

func (r *resource) getGVKByGroupKind(gr schema.GroupKind) (schema.GroupVersionKind, error) {
	if gr.Group == viewv1a1.GroupVersion.Group {
		return schema.GroupVersionKind{
			Group:   viewv1a1.GroupVersion.Group,
			Kind:    gr.Kind,
			Version: viewv1a1.GroupVersion.Version,
		}, nil
	}

	// standard Kubernetes object
	mapper := r.mgr.GetRESTMapper()
	gvk, err := mapper.KindFor(schema.GroupVersionResource{Group: gr.Group, Resource: gr.Kind})
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("cannot find GVK for %s: %w", gr, err)
	}

	return gvk, nil
}

// Source is a generic watch source that knows how to create controller runtime sources.
type Source interface {
	Resource
	GetSource() (runtimeSource.TypedSource[Request], error)
	fmt.Stringer
}

type source struct {
	Resource
	mgr    runtimeManager.Manager
	source opv1a1.Source
	log    logr.Logger
}

func NewSource(mgr runtimeManager.Manager, s opv1a1.Source) Source {
	src := &source{
		mgr:      mgr,
		source:   s,
		Resource: NewResource(mgr, s.Resource),
	}

	log := mgr.GetLogger().WithName("source").WithValues("name", src.Resource.String())
	src.log = log

	return src
}

func (s *source) String() string { return s.Resource.String() }

func (s *source) GetSource() (runtimeSource.TypedSource[Request], error) {
	// gvk to watch
	gvk, err := s.GetGVK()
	if err != nil {
		return nil, err
	}

	var obj client.Object = &unstructured.Unstructured{}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	// prepare the predicate
	ps := []runtimePredicate.TypedPredicate[client.Object]{}
	if s.source.Predicate != nil {
		p, err := predicate.FromPredicate(*s.source.Predicate)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}

	if s.source.LabelSelector != nil {
		lp, err := predicate.FromLabelSelector(*s.source.LabelSelector)
		if err != nil {
			return nil, err
		}
		ps = append(ps, lp)
	}

	if s.source.Namespace != nil {
		ps = append(ps, predicate.FromNamespace(*s.source.Namespace))
	}

	// generic handler
	src := runtimeSource.TypedKind(s.mgr.GetCache(), obj, EventHandler[client.Object]{}, ps...)

	s.log.V(4).Info("watch source: ready", "GVK", gvk.String(), "predicate-num", len(ps))

	return src, nil
}

// Target is a generic writer that knows how to create controller runtime objects in a target resource.
type Target interface {
	Resource
	Write(context.Context, cache.Delta) error
	fmt.Stringer
}

type target struct {
	Resource
	mgr    runtimeManager.Manager
	target opv1a1.Target
	log    logr.Logger
}

func NewTarget(mgr runtimeManager.Manager, t opv1a1.Target) Target {
	target := &target{
		Resource: NewResource(mgr, t.Resource),
		mgr:      mgr,
		target:   t,
	}

	log := mgr.GetLogger().WithName("target").WithValues("name", target.Resource.String())
	target.log = log

	return target
}

func (t *target) String() string {
	return fmt.Sprintf("%s<type:%s>", t.Resource.String(), t.target.Type)
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
	gvk, err := t.GetGVK()
	if err != nil {
		return err
	}

	// make a private copy of the Object
	delta.Object = object.DeepCopy(delta.Object)

	// make sure delta object gets the correct GVK applied
	delta.Object.SetGroupVersionKind(gvk)

	switch t.target.Type {
	case "Updater", "":
		return t.update(ctx, delta)
	case "Patcher":
		return t.patch(ctx, delta)
	default:
		return fmt.Errorf("unknown target type: %s", t.target.Type)
	}
}

func (t *target) update(ctx context.Context, delta cache.Delta) error {
	c := t.mgr.GetClient()

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
	c := t.mgr.GetClient()

	switch delta.Type {
	case cache.Added, cache.Updated, cache.Replaced:
		t.log.V(4).Info("update-patch", "event-type", delta.Type,
			"key", client.ObjectKeyFromObject(delta.Object).String())

		patch, err := json.Marshal(object.DeepCopy(delta.Object).UnstructuredContent())
		if err != nil {
			return err
		}

		oldObj := object.New()
		oldObj.SetGroupVersionKind(delta.Object.GroupVersionKind())
		oldObj.SetName(delta.Object.GetName())
		oldObj.SetNamespace(delta.Object.GetNamespace())
		if err := c.Get(ctx, client.ObjectKeyFromObject(oldObj), oldObj); err != nil {
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

		if err := c.Patch(context.Background(), delta.Object, client.RawPatch(types.StrategicMergePatchType, b)); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}

		return nil

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
