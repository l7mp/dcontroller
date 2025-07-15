package composite

// Composite cache is a cache that serves views from the view cache and the rest from the default
// Kubernetes cache.

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

// Ensure CompositeCache implements cache.Cache
var _ cache.Cache = &CompositeCache{}

type CompositeCache struct {
	defaultCache cache.Cache
	viewCache    *ViewCache
	logger, log  logr.Logger
}

// CacheOptions are generic caching options
type CacheOptions struct {
	cache.Options
	// DefaultCache is the controller-runtime cache used for anything that is not a view.
	DefaultCache cache.Cache
	// Logger is for logging. Currently only the viewcache generates log messages.
	Logger logr.Logger
}

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

	return &CompositeCache{
		defaultCache: defaultCache,
		viewCache:    NewViewCache(opts),
		logger:       logger,
		log:          logger.WithName("cache"),
	}, nil
}

func (cc *CompositeCache) GetLogger() logr.Logger {
	return cc.logger
}

func (cc *CompositeCache) GetDefaultCache() cache.Cache {
	return cc.defaultCache
}

func (cc *CompositeCache) GetViewCache() *ViewCache {
	return cc.viewCache
}

func (cc *CompositeCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(6).Info("get-informer", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.GetInformer(ctx, obj)
	}
	return cc.defaultCache.GetInformer(ctx, obj)
}

func (cc *CompositeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	cc.log.V(6).Info("get-informer-for-kind", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.GetInformerForKind(ctx, gvk)
	}
	return cc.defaultCache.GetInformerForKind(ctx, gvk)
}

func (cc *CompositeCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(6).Info("remove-informer", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.RemoveInformer(ctx, obj)
	}
	return cc.defaultCache.RemoveInformer(ctx, obj)
}

func (cc *CompositeCache) Start(ctx context.Context) error {
	cc.log.V(1).Info("starting")

	// we must run this in a goroutine, otherwise the default cache cannot start up
	// ignore the returned error: viewcache.Start() never errs
	go cc.viewCache.Start(ctx) //nolint:errcheck

	return cc.defaultCache.Start(ctx)
}

func (cc *CompositeCache) WaitForCacheSync(ctx context.Context) bool {
	return cc.viewCache.WaitForCacheSync(ctx) && cc.defaultCache.WaitForCacheSync(ctx)
}

func (cc *CompositeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.IndexField(ctx, obj, field, extractValue)
	}
	return cc.defaultCache.IndexField(ctx, obj, field, extractValue)
}

func (cc *CompositeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("get", "gvk", gvk, "key", key)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.Get(ctx, key, obj, opts...)
	}
	return cc.defaultCache.Get(ctx, key, obj, opts...)
}

func (cc *CompositeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk := list.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("list", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.List(ctx, list, opts...)
	}
	return cc.defaultCache.List(ctx, list, opts...)
}
