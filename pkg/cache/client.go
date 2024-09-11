package cache

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"hsnlab/dcontroller-runtime/pkg/object"
)

type ReadOnlyClient interface {
	client.Reader
	Watch(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) (watch.Interface, error)
}

type cacheClient struct {
	*Cache
}

func (c *Cache) NewClient() ReadOnlyClient {
	return &cacheClient{c}
}

// Get retrieves an obj for the given object. The paraamter obj must be a struct pointer with a
// valid kind (view), key must be a namespace/name pair, and obj will be updated with the response.
func (c *cacheClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.Cache == nil {
		return apierrors.NewInternalError(errors.New("invalid cache"))
	}

	o, ok := obj.(object.Object)
	if !ok {
		return apierrors.NewBadRequest("obj must be object.Object")
	}

	return c.Cache.get(key, o)
}

// List retrieves list of objects for a view. The list parameter must be a client.ObjectList with a
// kind (view) specified. On a successful call, Items field in the list will be populated with the
// result.
func (c *cacheClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.Cache == nil {
		return apierrors.NewInternalError(errors.New("invalid cache"))
	}

	l, ok := list.(object.ObjectList)
	if !ok {
		return apierrors.NewBadRequest("list must be *object.ObjectList")
	}

	return c.Cache.list(l)
}

// Watch creates a watch for the specified resource. The list parameter must be a client.ObjectList
// with a kind (view) specified.
func (c *cacheClient) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	if c.Cache == nil {
		return nil, apierrors.NewInternalError(errors.New("invalid cache"))
	}

	l, ok := list.(object.ObjectList)
	if !ok {
		return nil, apierrors.NewBadRequest("list must be *object.ObjectList")
	}

	return c.Cache.watch(ctx, l.GetObjectKind().GroupVersionKind())
}

func (c *Cache) get(key client.ObjectKey, obj object.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		return apierrors.NewBadRequest("GVK not registered")
	}

	item, exists, err := indexer.GetByKey(key.String())
	if err != nil {
		return err
	}

	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{
			Group:    obj.GetObjectKind().GroupVersionKind().Group,
			Resource: obj.GetObjectKind().GroupVersionKind().Kind,
		}, key.String())
	}

	object.DeepCopyInto(item.(object.Object), obj)

	return nil
}

// TODO: implement ListOptions
func (c *Cache) list(list object.ObjectList) error {
	gvk := list.GetObjectKind().GroupVersionKind()

	c.mu.RLock()
	indexer, exists := c.caches[gvk]
	c.mu.RUnlock()

	if !exists {
		return apierrors.NewBadRequest("GVK not registered")
	}

	for _, item := range indexer.List() {
		object.AppendToListItem(list, item.(object.Object))
	}
	return nil
}
