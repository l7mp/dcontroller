package cache

import (
	"context"
	"errors"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

var _ client.Client = &CompositeClient{}

// CompositeClient is a controlller runtime client that uses a view cache to manage view resources
// and delegates native resources to a default client.
type CompositeClient struct {
	client.Client
	compositeCache Cache
	viewClient     *ViewCacheClient
	log            logr.Logger
}

// NewCompositeClient creates a composite client: views are served through the viewcache, native
// Kubernetes resources served from a native client (can be a split client).
func NewCompositeClient(config *rest.Config, options ClientOptions) (*CompositeClient, error) {
	var nativeClient client.Client
	if config != nil {
		c, err := client.New(config, options)
		if err != nil {
			return nil, err
		}
		nativeClient = c
	}
	return &CompositeClient{
		Client: nativeClient,
		log:    logr.New(nil),
	}, nil
}

func NewClientForCache(config *rest.Config, cache Cache, options ClientOptions) (*CompositeClient, error) {
	var nativeClient client.Client
	if config != nil {
		c, err := client.New(config, options)
		if err != nil {
			return nil, err
		}
		nativeClient = c
	}
	c := &CompositeClient{
		Client: nativeClient,
		log:    logr.New(nil),
	}
	c.SetCache(cache)
	return c, nil
}

// SetClient sets the native client in the composite client.
func (c *CompositeClient) SetClient(client client.Client) {
	c.Client = client
}

// SetCache sets the cache for the composite client.
func (c *CompositeClient) SetCache(cache Cache) {
	c.compositeCache = cache
	if viewCache, ok := cache.(*CompositeCache); ok {
		c.viewClient = viewCache.GetViewCache().GetClient().(*ViewCacheClient)
	}
}

// SetClient sets the cache in the composite client.
func (c *CompositeClient) GetCache() Cache {
	return c.compositeCache
}

// split client:
// client.Reader: implemented by the cache.Reader in the native manager.client
// client.Writer: views are written to the viewcache, rest handled by the default client

// Create saves the object obj.
func (c *CompositeClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewClient.Create(ctx, obj, opts...)
	}
	return c.Client.Create(ctx, obj, opts...)
}

// Delete deletes the given obj.
func (c *CompositeClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewClient.Delete(ctx, obj, opts...)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

// Update updates the given obj.
func (c *CompositeClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewClient.Update(ctx, obj, opts...)
	}
	return c.Client.Update(ctx, obj, opts...)
}

// Patch patches the given obj.
func (c *CompositeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewClient.Patch(ctx, obj, patch, opts...)
	}

	return c.Client.Patch(ctx, obj, patch, opts...)
}

// DeleteAllOf deletes all objects of the given type matching the given options.
func (c *CompositeClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewClient.DeleteAllOf(ctx, obj, opts...)
	}
	return c.Client.DeleteAllOf(ctx, obj, opts...)
}

// Get retrieves an obj for the given object key.
func (c *CompositeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.compositeCache.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options.
func (c *CompositeClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.compositeCache.List(ctx, list, opts...)
}

// Watch watches objects of type obj and sends events on the returned channel.
func (c *CompositeClient) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	if viewv1a1.IsViewKind(list.GetObjectKind().GroupVersionKind()) {
		return c.viewClient.Watch(ctx, list, opts...)
	}
	return nil, apierrors.NewInternalError(errors.New("native K8s client does not support watch"))
}

// Status implements StatusClient. Note that normally this would not be needed since the default
// view-object client already writes the status if requested, but still needed because native
// objects' status can only be updated via the status-writer
func (c *CompositeClient) Status() client.SubResourceWriter {
	return &compositeSubResourceClient{
		viewSubResourceClient: c.viewClient.SubResource("status"),
		SubResourceClient:     c.SubResource("status"),
	}
}

// compositeSubResourceClient implements a status client.
type compositeSubResourceClient struct {
	viewSubResourceClient client.SubResourceClient
	client.SubResourceClient
}

// Get returns the status on the given obj.
func (c *compositeSubResourceClient) Get(ctx context.Context, obj, subResource client.Object, opts ...client.SubResourceGetOption) error {
	if viewv1a1.IsViewKind(subResource.GetObjectKind().GroupVersionKind()) {
		return c.viewSubResourceClient.Get(ctx, obj, subResource, opts...)
	}
	return c.SubResourceClient.Get(ctx, obj, subResource, opts...)
}

// Create saves the status on the given obj.
func (c *compositeSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if viewv1a1.IsViewKind(subResource.GetObjectKind().GroupVersionKind()) {
		return c.viewSubResourceClient.Create(ctx, obj, subResource, opts...)
	}
	return c.SubResourceClient.Create(ctx, obj, subResource, opts...)
}

// Update updates the status of the given obj.
func (c *compositeSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewSubResourceClient.Update(ctx, obj, opts...)
	}
	return c.SubResourceClient.Update(ctx, obj, opts...)
}

// Patch patches the status of the given obj.
func (c *compositeSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if viewv1a1.IsViewKind(obj.GetObjectKind().GroupVersionKind()) {
		return c.viewSubResourceClient.Patch(ctx, obj, patch, opts...)
	}

	return c.SubResourceClient.Patch(ctx, obj, patch, opts...)
}
