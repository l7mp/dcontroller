package reconciler

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/util"
)

// Target is a generic writer that knows how to create controller runtime objects in a target resource.
type Target interface {
	Resource
	Write(context.Context, object.Delta, object.Object) error
	fmt.Stringer
}

type target struct {
	Resource
	mgr    runtimeManager.Manager
	target opv1a1.Target
	log    logr.Logger
}

// NewTarget creates a new target resource.
func NewTarget(mgr runtimeManager.Manager, operator string, t opv1a1.Target) Target {
	target := &target{
		Resource: NewResource(mgr, operator, t.Resource),
		mgr:      mgr,
		target:   t,
	}

	log := mgr.GetLogger().WithName("target").WithValues("name", target.Resource.String())
	target.log = log

	return target
}

// String stringifies a target.
func (t *target) String() string {
	return fmt.Sprintf("%s<type:%s>", t.Resource.String(), t.target.Type)
}

// Write enforces a delta on a target. The behavior depends on the target type:
//   - For Updaters the delta is enforced as is to the target.
//   - For Patchers the delta object is applied as a strategic merge patch: for Add and Update
//     deltas the target is patched with the delta object, while for Delete the delta object
//     content is removed from the target using a strategic merge patch.
//
// The originalObject parameter is the object snapshot from the reconcile request, used for
// optimistic concurrency control in Patcher mode. If nil, the current object is fetched.
func (t *target) Write(ctx context.Context, delta object.Delta, originalObject object.Object) error {
	if delta.Object == nil {
		return errors.New("write: empty object in delta")
	}

	gvk, err := t.GetGVK()
	if err != nil {
		return err
	}

	// make a private copy of the Object
	delta.Object = object.DeepCopy(delta.Object)

	// make sure delta object gets the correct GVK applied
	delta.Object.SetGroupVersionKind(gvk)

	switch t.target.Type {
	case opv1a1.Updater, "":
		return t.update(ctx, delta)
	case opv1a1.Patcher:
		return t.patch(ctx, delta, originalObject)
	default:
		return fmt.Errorf("unknown target type: %s", t.target.Type)
	}
}

func (t *target) update(ctx context.Context, delta object.Delta) error {
	t.log.V(5).Info("updating target", "delta-type", delta.Type, "object", object.Dump(delta.Object))

	c := t.mgr.GetClient()

	//nolint:nolintlint
	switch delta.Type { //nolint:exhaustive
	case object.Added, object.Upserted, object.Updated, object.Replaced:
		t.log.V(2).Info("add/upsert", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))

		gvk, err := t.GetGVK()
		if err != nil {
			return err
		}
		obj := object.New()
		obj.SetGroupVersionKind(gvk)
		obj.SetName(delta.Object.GetName())
		obj.SetNamespace(delta.Object.GetNamespace())

		// WARNING: the Update target cannot be used to delete labels and annotations, use
		// the Patcher target for that (this is because we don't want the user to remove
		// important labels/annotations accidentally and taking care of each in the
		// pipeline may be too difficult)
		//
		// Use our own CreateOrUpdate that will also update the status
		res, err := CreateOrUpdate(context.TODO(), c, obj, func() error {
			// remove stuff that's no longer there
			for k := range obj.UnstructuredContent() {
				if k == "metadata" {
					continue
				}
				if _, ok, _ := unstructured.NestedFieldNoCopy(delta.Object.UnstructuredContent(), k); !ok {
					unstructured.RemoveNestedField(obj.UnstructuredContent(), k)
				}
			}

			// then update the content with new keys: metadata and status will be handled separately
			for k, v := range delta.Object.UnstructuredContent() {
				if k == "metadata" {
					continue
				}

				if err := unstructured.SetNestedField(obj.UnstructuredContent(), v, k); err != nil {
					t.log.Error(err, "failed to update object field during update",
						"object", client.ObjectKeyFromObject(obj).String(), "key", k)
					continue
				}
			}

			mergeMetadata(obj, delta.Object)

			// restore metadata
			obj.SetGroupVersionKind(gvk)
			obj.SetName(delta.Object.GetName())
			obj.SetNamespace(delta.Object.GetNamespace())

			return nil
		})

		// NotFound errors are ignored: the object has disappeared while we were working in it
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("create/update resource %s failed with operation code %s: %w",
					client.ObjectKeyFromObject(delta.Object).String(), res, err)
			}
			t.log.V(2).Info("add/upsert: object has disappeared (probably harmless)",
				"event-type", delta.Type)
		}

	case object.Deleted:
		t.log.V(2).Info("delete", "event-type", delta.Type, "object", client.ObjectKeyFromObject(delta.Object))

		// NotFound errors are ignored: the object has disappeared while we were working in it
		if err := c.Delete(ctx, delta.Object); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete resource %s failed: %w",
					client.ObjectKeyFromObject(delta.Object).String(), err)
			}
			t.log.V(2).Info("delete: object has disappeared (probably harmless)",
				"event-type", delta.Type)
		}

	default:
		t.log.V(3).Info("target: ignoring delta", "type", delta.Type)
	}

	return nil
}

func (t *target) patch(ctx context.Context, delta object.Delta, originalObject object.Object) error {
	t.log.V(5).Info("patching target", "delta-type", delta.Type, "object", object.Dump(delta.Object))

	c := t.mgr.GetClient()

	//nolint:nolintlint
	switch delta.Type { //nolint:exhaustive
	case object.Added, object.Updated, object.Upserted, object.Replaced:
		t.log.V(4).Info("update-patch", "event-type", delta.Type,
			"key", client.ObjectKeyFromObject(delta.Object).String())

		// Check if we can use the original object snapshot (same GVK, namespace, name)
		useOriginal := originalObject != nil &&
			originalObject.GroupVersionKind() == delta.Object.GroupVersionKind() &&
			originalObject.GetNamespace() == delta.Object.GetNamespace() &&
			originalObject.GetName() == delta.Object.GetName()

		var obj object.Object
		if useOriginal {
			// Use the original object snapshot (has correct resourceVersion)
			obj = object.DeepCopy(originalObject)
			t.log.V(4).Info("using original object snapshot for patch",
				"resourceVersion", obj.GetResourceVersion())
		} else {
			// Fetch current target object
			obj = object.New()
			obj.SetGroupVersionKind(delta.Object.GroupVersionKind())
			obj.SetName(delta.Object.GetName())
			obj.SetNamespace(delta.Object.GetNamespace())
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				return err
			}
			t.log.V(4).Info("fetched target object for patch",
				"resourceVersion", obj.GetResourceVersion())
		}

		// Apply delta changes to the object in-place using merge-patch semantics (RFC 7386).
		// This properly merges nested objects and arrays rather than replacing them.
		if err := object.Patch(obj, delta.Object.UnstructuredContent()); err != nil {
			return err
		}

		// Restore critical metadata that must not be overwritten
		obj.SetGroupVersionKind(delta.Object.GroupVersionKind())
		obj.SetName(delta.Object.GetName())
		obj.SetNamespace(delta.Object.GetNamespace())

		// Use our custom Update function which handles both spec and status correctly.
		// This uses optimistic concurrency control via resourceVersion (Kubernetes will reject
		// if the object has changed). NotFound errors are ignored: the object has disappeared
		// while we were working on it.
		if err := Update(ctx, c, obj); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("update resource %s failed: %w",
					client.ObjectKeyFromObject(delta.Object).String(), err)
			}
			t.log.V(2).Info("update-patch: object has disappeared (probably harmless)",
				"event-type", delta.Type)
		}

	case object.Deleted:
		t.log.V(4).Info("delete-patch", "event-type", delta.Type,
			"key", client.ObjectKeyFromObject(delta.Object).String())

		// Check if we can use the original object snapshot (same GVK, namespace, name)
		useOriginal := originalObject != nil &&
			originalObject.GroupVersionKind() == delta.Object.GroupVersionKind() &&
			originalObject.GetNamespace() == delta.Object.GetNamespace() &&
			originalObject.GetName() == delta.Object.GetName()

		var obj object.Object
		if useOriginal {
			// Use the original object snapshot (has correct resourceVersion)
			obj = object.DeepCopy(originalObject)
			t.log.V(4).Info("using original object snapshot for delete-patch",
				"resourceVersion", obj.GetResourceVersion())
		} else {
			// Fetch current target object
			obj = object.New()
			obj.SetGroupVersionKind(delta.Object.GroupVersionKind())
			obj.SetName(delta.Object.GetName())
			obj.SetNamespace(delta.Object.GetNamespace())
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				// NotFound errors are ignored: the object has disappeared
				if apierrors.IsNotFound(err) {
					t.log.V(2).Info("delete-patch: object has disappeared (probably harmless)",
						"event-type", delta.Type)
					return nil
				}
				return err
			}
			t.log.V(4).Info("fetched target object for delete-patch",
				"resourceVersion", obj.GetResourceVersion())
		}

		// Apply the delete patch locally so that we fully control the behavior.
		// The removeNestedMap function converts all leaf values to nil, which is
		// the merge-patch semantics for deletion (RFC 7386).
		patch := removeNestedMap(delta.Object.UnstructuredContent())

		// Make sure we do not remove crucial metadata: the GVK and the namespace/name
		gvk := delta.Object.GroupVersionKind()
		gr := schema.GroupVersion{Group: gvk.Group, Version: gvk.Version}
		unstructured.SetNestedField(patch, gr.String(), "apiVersion")                            //nolint:errcheck
		unstructured.SetNestedField(patch, gvk.Kind, "kind")                                     //nolint:errcheck
		unstructured.SetNestedField(patch, delta.Object.GetNamespace(), "metadata", "namespace") //nolint:errcheck
		unstructured.SetNestedField(patch, delta.Object.GetName(), "metadata", "name")           //nolint:errcheck

		t.log.V(5).Info("delete-patch content", "patch", util.Stringify(patch))

		// Apply delete patch to the fetched/cached object in-place
		if err := object.Patch(obj, patch); err != nil {
			return err
		}

		// Restore critical metadata that must not be overwritten
		obj.SetGroupVersionKind(delta.Object.GroupVersionKind())
		obj.SetName(delta.Object.GetName())
		obj.SetNamespace(delta.Object.GetNamespace())

		// Use our custom Update function which handles both spec and status correctly.
		// This uses optimistic concurrency control via resourceVersion (Kubernetes will reject
		// if the object has changed). NotFound errors are ignored: the object has disappeared
		// while we were working on it.
		if err := Update(ctx, c, obj); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("update resource %s after delete-patch failed: %w",
				client.ObjectKeyFromObject(delta.Object).String(), err)
		}

	default:
		t.log.V(2).Info("target: ignoring delta", "type", delta.Type)
	}

	return nil
}

func removeNestedMap(m map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range m {
		switch x := v.(type) {
		case bool, int64, float64, string:
			result[k] = nil
		case map[string]any:
			result[k] = removeNestedMap(x)
		case []any:
			result[k] = removeNestedList(x)
		}
	}
	return result
}

func removeNestedList(l []any) []any {
	result := make([]any, len(l))
	for k, v := range l {
		switch x := v.(type) {
		case bool, int64, float64, string:
			result[k] = nil
		case map[string]any:
			result[k] = removeNestedMap(x)
		case []any:
			result[k] = removeNestedList(x)
		}
	}
	return result
}

func mergeMetadata(obj, new object.Object) {
	labels := obj.GetLabels()
	newLabels := new.GetLabels()
	if newLabels != nil {
		if labels == nil {
			labels = map[string]string{}
		}
		for k, v := range newLabels {
			labels[k] = v
		}
		obj.SetLabels(labels)
	}

	annotations := obj.GetAnnotations()
	newAnnotations := new.GetAnnotations()
	if newAnnotations != nil {
		if annotations == nil {
			annotations = map[string]string{}
		}
		for k, v := range newAnnotations {
			annotations[k] = v
		}
		obj.SetAnnotations(annotations)
	}
}
