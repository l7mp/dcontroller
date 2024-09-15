package cache

// composite cache is a cache that serves views from the view cache and the rest from the default
// Kubernetes cache

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
)

// Ensure CompositeCache implements cache.Cache
var _ cache.Cache = &CompositeCache{}

type CompositeCache struct {
	defaultCache cache.Cache
	viewCache    *ViewCache
	log          logr.Logger
}

// Options are generic caching options
type Options struct {
	cache.Options
	// DefaultCache is the controller-runtime cache used for anything that is not a view.
	DefaultCache cache.Cache
	// Logger is for logging. Currently only the viewcache generates log messages.
	Logger logr.Logger
}

func NewCompositeCache(config *rest.Config, opts Options) (*CompositeCache, error) {
	defaultCache := opts.DefaultCache
	if opts.DefaultCache == nil {
		dc, err := cache.New(config, opts.Options)
		if err != nil {
			return nil, err
		}
		defaultCache = dc
	}

	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	return &CompositeCache{
		defaultCache: defaultCache,
		viewCache:    NewViewCache(opts),
		log:          logger.WithName("compositecache"),
	}, nil
}

func (cc *CompositeCache) GetDefaultCache() cache.Cache {
	return cc.defaultCache
}

func (cc *CompositeCache) GetViewCache() *ViewCache {
	return cc.viewCache
}

func (cc *CompositeCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("get-informer", "gvk", gvk)

	if gvk.Group == viewapiv1.GroupVersion.Group {
		return cc.viewCache.GetInformer(ctx, obj)
	}
	return cc.defaultCache.GetInformer(ctx, obj)
}

func (cc *CompositeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	cc.log.V(3).Info("get-informer-for-kind", "gvk", gvk)

	if gvk.Group == viewapiv1.GroupVersion.Group {
		return cc.viewCache.GetInformerForKind(ctx, gvk)
	}
	return cc.defaultCache.GetInformerForKind(ctx, gvk)
}

func (cc *CompositeCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(3).Info("get-informer-for-kind", "gvk", gvk)

	if gvk.Group == viewapiv1.GroupVersion.Group {
		return cc.viewCache.RemoveInformer(ctx, obj)
	}
	return cc.defaultCache.RemoveInformer(ctx, obj)
}

func (cc *CompositeCache) Start(ctx context.Context) error {
	cc.log.V(3).Info("starting")
	if err := cc.viewCache.Start(ctx); err != nil {
		return err
	}
	return cc.defaultCache.Start(ctx)
}

func (cc *CompositeCache) WaitForCacheSync(ctx context.Context) bool {
	return cc.viewCache.WaitForCacheSync(ctx) && cc.defaultCache.WaitForCacheSync(ctx)
}

func (cc *CompositeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Group == viewapiv1.GroupVersion.Group {
		return cc.viewCache.IndexField(ctx, obj, field, extractValue)
	}
	return cc.defaultCache.IndexField(ctx, obj, field, extractValue)
}

func (cc *CompositeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(3).Info("get", "gvk", gvk, "key", key)

	if gvk.Group == viewapiv1.GroupVersion.Group {
		return cc.viewCache.Get(ctx, key, obj, opts...)
	}
	return cc.defaultCache.Get(ctx, key, obj, opts...)
}

func (cc *CompositeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk := list.GetObjectKind().GroupVersionKind()

	cc.log.V(3).Info("list", "gvk", gvk)

	if gvk.Group == viewapiv1.GroupVersion.Group {
		return cc.viewCache.List(ctx, list, opts...)
	}
	return cc.defaultCache.List(ctx, list, opts...)
}
