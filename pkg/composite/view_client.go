package composite

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dcontroller/pkg/object"
)

var _ client.WithWatch = &viewClient{}

// viewClient implements client.WithWatch by delegating to ViewCache.
type viewClient struct {
	cache *ViewCache
}

// GetClient returns a controller-runtime client backed by this ViewCache.
func (vc *ViewCache) GetClient() client.WithWatch {
	return &viewClient{cache: vc}
}

// Client interface implementation.

// Get retrieves an obj for the given object key from the ViewCache.
func (c *viewClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.cache.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects from the ViewCache.
func (c *viewClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.cache.List(ctx, list, opts...)
}

// Create saves the object obj in the ViewCache.
func (c *viewClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	viewObj, ok := obj.(object.Object)
	if !ok {
		return fmt.Errorf("object must implement object.Object interface")
	}
	return c.cache.Add(viewObj)
}

// Delete deletes the given obj from the ViewCache.
func (c *viewClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	viewObj, ok := obj.(object.Object)
	if !ok {
		return fmt.Errorf("object must implement object.Object interface")
	}
	return c.cache.Delete(viewObj)
}

// Update updates the given obj in the ViewCache.
func (c *viewClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	newObj, ok := obj.(object.Object)
	if !ok {
		return fmt.Errorf("object must implement object.Object interface")
	}

	// For updates, we need to get the old object first
	oldObj := object.NewViewObject(object.GetOperator(newObj), newObj.GetKind())
	object.SetName(oldObj, newObj.GetNamespace(), newObj.GetName())

	// Get the current version from cache
	if err := c.cache.Get(ctx, client.ObjectKeyFromObject(newObj), oldObj); err != nil {
		return fmt.Errorf("cannot update object with key %s: %w", client.ObjectKeyFromObject(newObj), err)
	}

	return c.cache.Update(oldObj, newObj)
}

// Patch patches the given obj in the ViewCache. Note that obj is NOT updated to the new content.
func (c *viewClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	o, ok := obj.(object.Object)
	if !ok {
		return errors.New("object must be an object.Object")
	}

	// Get current object
	current := object.NewViewObject(object.GetOperator(o), o.GetKind())
	if err := c.cache.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
		return fmt.Errorf("cannot patch object with key %s: %w", client.ObjectKeyFromObject(obj), err)
	}

	// strategic merge patch not supported in views, falling back to merge-patch
	j, err := patch.Data(obj)
	if err != nil {
		return fmt.Errorf("cannot decode JSON patch: %w", err)
	}

	newContent := map[string]any{}
	if err := json.Unmarshal(j, &newContent); err != nil {
		return fmt.Errorf("cannot parse JSON patch: %w", err)
	}

	newObj := object.DeepCopy(current)
	if err := object.Patch(newObj, newContent); err != nil {
		return err
	}

	return c.cache.Update(current, newObj)
}

// DeleteAllOf deletes all objects of the given type matching the given options.
func (c *viewClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	// Convert delete options to list options
	var listOpts []client.ListOption
	for _, opt := range opts {
		switch o := opt.(type) {
		case client.InNamespace:
			listOpts = append(listOpts, o)
		case client.MatchingLabels:
			listOpts = append(listOpts, o)
		case client.MatchingFields:
			listOpts = append(listOpts, o)
		case client.MatchingLabelsSelector:
			listOpts = append(listOpts, o)
		case client.MatchingFieldsSelector:
			listOpts = append(listOpts, o)
		}
	}

	// List all matching objects
	viewObj, ok := obj.(object.Object)
	if !ok {
		return fmt.Errorf("object must implement object.Object interface")
	}

	list := NewViewObjectList(object.GetOperator(viewObj), viewObj.GetKind())
	if err := c.cache.List(ctx, list, listOpts...); err != nil {
		return err
	}

	// Delete each object
	for _, item := range list.Items {
		if err := c.cache.Delete(&item); err != nil {
			return err
		}
	}

	return nil
}

// Status returns a client which can update status subresource.
func (c *viewClient) Status() client.StatusWriter {
	return &viewSubResourceClient{
		client: c,
		subres: "status",
	}
}

// Scheme returns the scheme this client is using (ViewCache doesn't have a scheme).
func (c *viewClient) Scheme() *runtime.Scheme {
	return nil
}

// RESTMapper returns the rest mapper (ViewCache doesn't have a REST mapper).
func (c *viewClient) RESTMapper() meta.RESTMapper {
	return nil
}

// GroupVersionKindFor returns the GroupVersionKind for the given object.
func (c *viewClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	if gvkObj, ok := obj.(schema.ObjectKind); ok {
		if gvk := gvkObj.GroupVersionKind(); !gvk.Empty() {
			return gvk, nil
		}
	}
	return schema.GroupVersionKind{}, fmt.Errorf("unable to determine GroupVersionKind for object type %T", obj)
}

// IsObjectNamespaced returns true if the object is namespaced.
func (c *viewClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	// All view objects are namespaced
	return true, nil
}

// SubResource returns a client for the specified subresource.
func (c *viewClient) SubResource(subResource string) client.SubResourceClient {
	return &viewSubResourceClient{
		client: c,
		subres: subResource,
	}
}

// Watch interface implementation.

// Watch watches objects of type obj and sends events on the returned channel.
func (c *viewClient) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	return c.cache.Watch(ctx, list, opts...)
}

// viewSubResourceClient implements client.SubResourceClient for ViewCache.
type viewSubResourceClient struct {
	client *viewClient
	subres string
}

// Get retrieves the subresource for the given object.
func (sr *viewSubResourceClient) Get(ctx context.Context, obj, subResource client.Object, _ ...client.SubResourceGetOption) error {
	o, ok := obj.(object.Object)
	if !ok {
		return errors.New("object must be an object.Object")
	}

	so, ok := subResource.(object.Object)
	if !ok {
		return errors.New("sub-resource must be an object.Object")
	}

	current := object.NewViewObject(object.GetOperator(o), o.GetKind())
	if err := sr.client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
		return err
	}

	status, ok, err := unstructured.NestedMap(current.UnstructuredContent(), sr.subres)
	if err != nil {
		return fmt.Errorf("cannot load status sub-resource in object argument: %w", err)
	} else if !ok {
		return errors.New("no status sub-resource in object argument")
	}

	so.SetUnstructuredContent(status)

	return nil
}

// Create creates the subresource for the given object.
func (sr *viewSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	o, ok := obj.(object.Object)
	if !ok {
		return errors.New("object must be an object.Object")
	}

	so, ok := subResource.(object.Object)
	if !ok {
		return errors.New("sub-resource must be an object.Object")
	}

	sub, ok, err := unstructured.NestedMap(so.UnstructuredContent(), sr.subres)
	if err != nil {
		return fmt.Errorf("cannot load %s sub-resource: %w", sr.subres, err)
	} else if !ok {
		return errors.New("no status sub-resource in object argument")
	}

	current := object.NewViewObject(object.GetOperator(o), o.GetKind())
	if err := sr.client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
		return err
	}

	if err := unstructured.SetNestedMap(current.UnstructuredContent(), sub, sr.subres); err != nil {
		return fmt.Errorf("failed to set sub-resource %s to sub-resource: %w", sr.subres, err)
	}

	return sr.client.Update(ctx, current)
}

// Update updates the subresource for the given object.
func (sr *viewSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	o, ok := obj.(object.Object)
	if !ok {
		return errors.New("object must be an object.Object")
	}

	current := object.NewViewObject(object.GetOperator(o), o.GetKind())
	if err := sr.client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
		return err
	}

	sub, ok, err := unstructured.NestedMap(o.UnstructuredContent(), sr.subres)
	if err != nil {
		return fmt.Errorf("cannot load %s sub-resource: %w", sr.subres, err)
	} else if !ok {
		return errors.New("no status sub-resource in object argument")
	}

	if err := unstructured.SetNestedMap(current.UnstructuredContent(), sub, sr.subres); err != nil {
		return fmt.Errorf("cannot set sub-resource %s: %w", sr.subres, err)
	}

	// For ViewCache, status is just part of the object - update the whole object
	return sr.client.Update(ctx, obj)
}

// Patch patches the subresource for the given object.
func (sr *viewSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	// For ViewCache, status is just part of the object - patch the whole object
	return sr.client.Patch(ctx, obj, patch)
}
