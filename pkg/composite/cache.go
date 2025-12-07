package composite

// Composite cache is a cache that serves views from the view cache and the rest from the default
// Kubernetes cache.

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ cache.Cache = &CompositeCache{}

// ViewCacheInterface extends cache.Cache with view-specific operations.
// Both ViewCache and DelegatingViewCache implement this interface.
type ViewCacheInterface interface {
	cache.Cache
	// GetClient returns a client for this view cache.
	GetClient() client.WithWatch
	// Add adds an object to the cache.
	Add(obj object.Object) error
	// Update updates an object in the cache.
	Update(oldObj, newObj object.Object) error
	// Delete removes an object from the cache.
	Delete(obj object.Object) error
	// Watch watches for changes to objects.
	Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error)
}

// CompositeCache is a cache for storing view objects. It delegates native objects to a default
// cache.
type CompositeCache struct {
	defaultCache cache.Cache
	viewCache    ViewCacheInterface
	logger, log  logr.Logger
}

// CacheOptions are generic caching options.
type CacheOptions struct {
	cache.Options
	// DefaultCache is the controller-runtime cache used for anything that is not a view.
	DefaultCache cache.Cache
	// ViewCache is the view cache used for anything that is a view.
	ViewCache cache.Cache
	// Logger is for logging. Currently only the viewcache generates log messages.
	Logger logr.Logger
}

// NewCompositeCache creates a new composite cache. If the config is not nil it also creates a
// controller-runtime cache for storing native resources.
func NewCompositeCache(config *rest.Config, opts CacheOptions) (*CompositeCache, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	defaultCache := opts.DefaultCache
	if defaultCache == nil && config != nil {
		dc, err := cache.New(config, opts.Options)
		if err != nil {
			return nil, err
		}
		defaultCache = dc
	}

	var viewCache ViewCacheInterface
	if opts.ViewCache != nil {
		// Use the provided view cache (can be ViewCache or DelegatingViewCache)
		if vc, ok := opts.ViewCache.(ViewCacheInterface); ok {
			viewCache = vc
		} else {
			// Fallback: create a new ViewCache if provided cache doesn't implement the interface
			viewCache = NewViewCache(opts)
		}
	} else {
		// Create a new ViewCache if none provided
		viewCache = NewViewCache(opts)
	}

	return &CompositeCache{
		defaultCache: defaultCache,
		viewCache:    viewCache,
		logger:       logger,
		log:          logger.WithName("cache"),
	}, nil
}

// GetLogger returns the logger of the cache.
func (cc *CompositeCache) GetLogger() logr.Logger {
	return cc.logger
}

// GetDefaultCache returns the cache used for storing native objects.
func (cc *CompositeCache) GetDefaultCache() cache.Cache {
	return cc.defaultCache
}

// GetViewCache returns the cache used for storing view objects.
func (cc *CompositeCache) GetViewCache() ViewCacheInterface {
	return cc.viewCache
}

// GetInformer fetches or constructs an informer for the given object.
func (cc *CompositeCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(6).Info("get-informer", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.GetInformer(ctx, obj)
	}
	return cc.defaultCache.GetInformer(ctx, obj)
}

// GetInformerForKind is similar to GetInformer, except that it takes a group-version-kind instead
// of the underlying object.
func (cc *CompositeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	cc.log.V(6).Info("get-informer-for-kind", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.GetInformerForKind(ctx, gvk)
	}
	return cc.defaultCache.GetInformerForKind(ctx, gvk)
}

// RemoveInformer removes an informer entry and stops it if it was running.
func (cc *CompositeCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(6).Info("remove-informer", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.RemoveInformer(ctx, obj)
	}
	return cc.defaultCache.RemoveInformer(ctx, obj)
}

// Start runs all the informers known to this cache until the context is closed. It blocks.
func (cc *CompositeCache) Start(ctx context.Context) error {
	cc.log.V(1).Info("starting")

	// we must run this in a goroutine, otherwise the default cache cannot start up
	// ignore the returned error: viewcache.Start() never errs
	go cc.viewCache.Start(ctx) //nolint:errcheck

	return cc.defaultCache.Start(ctx)
}

// WaitForCacheSync waits for all the caches to sync. Returns false if it could not sync a cache.c
func (cc *CompositeCache) WaitForCacheSync(ctx context.Context) bool {
	return cc.viewCache.WaitForCacheSync(ctx) && cc.defaultCache.WaitForCacheSync(ctx)
}

// IndexField adds an index with the given field name on the given object type.
func (cc *CompositeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.IndexField(ctx, obj, field, extractValue)
	}
	return cc.defaultCache.IndexField(ctx, obj, field, extractValue)
}

// Get retrieves an obj for the given object key from the cache.
func (cc *CompositeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("get", "gvk", gvk, "key", key)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.Get(ctx, key, obj, opts...)
	}
	return cc.defaultCache.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options.
func (cc *CompositeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk := list.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("list", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.List(ctx, list, opts...)
	}
	return cc.defaultCache.List(ctx, list, opts...)
}
