// Package manager provides constructor functions for creating Kubernetes controller-runtime
// managers with integrated support for Δ-controller's composite client and cache system.
//
// The constructors automatically configure the composite client and composite cache, enabling
// controllers to work with both native Kubernetes resources and view objects through a unified
// interface.
//
// Use New() for a manager with Kubernetes API access, or NewHeadless() for a standalone manager
// that only handles view resources.
//
// Example usage:
//
//	import "github.com/l7mp/dcontroller/pkg/manager"
//
//	mgr, _ := manager.New(cfg, manager.Options{
//	    Scheme: scheme,
//	    Logger: logger,
//	})
//	return mgr.Start(ctx)
package manager

import (
	"errors"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeMgr "sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/l7mp/dcontroller/pkg/cache"
)

// Re-export controller-runtime types for convenience.
// Users can use manager.Options and manager.Manager without importing controller-runtime.
type (
	Options = runtimeMgr.Options
	Manager = runtimeMgr.Manager
)

// CacheInjector can be used to inject an existing cache into a manager configuration. The new
// cache will be a composite cache comprised by a new controller-runtime cache for native
// Kubernetes resources and a new delegating view cache created on top of the shared view
// cache. This allows the per-manager caches (and hence the operators created on top of the
// managers) to have independent lifetimes.
func CacheInjector(sharedCache ctrlCache.Cache) ctrlCache.NewCacheFunc {
	return func(config *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
		compositeCache, ok := sharedCache.(*cache.CompositeCache)
		if !ok {
			return nil, errors.New("expecting CompositeCache")
		}

		// Get the shared view cache (must be *ViewCache)
		sharedViewCache, ok := compositeCache.GetViewCache().(*cache.ViewCache)
		if !ok {
			return nil, errors.New("shared cache view cache must be *ViewCache")
		}

		delegatingViewCache := cache.NewDelegatingViewCache(sharedViewCache,
			cache.CacheOptions{Options: opts})

		compositeCache, err := cache.NewCompositeCache(config, cache.CacheOptions{
			ViewCache: delegatingViewCache,
			Options:   opts,
		})
		if err != nil {
			return nil, err
		}

		return compositeCache, nil
	}
}

// New creates a new delta-controller manager with composite cache and client.  A composite manager
// uses a split cache: native Kubernetes objects are managed by the API server, while view objects
// are served from an in-memory view cache. The client automatically routes operations to the
// appropriate backend based on the resource type. For native Kubernetes object support, provide a
// valid REST config. Set config to nil to create a headless manager.
func New(config *rest.Config, opts Options) (Manager, error) {
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	if config == nil {
		return NewHeadless(opts)
	}

	logger := opts.Logger
	if opts.NewCache == nil {
		// cache is not injected, setup our own infrastructure
		opts.NewCache = func(config *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
			return cache.NewCompositeCache(config, cache.CacheOptions{
				Options: opts,
				Logger:  logger,
			})
		}

		// Make sure unstructured objects are served through the cache (the default is to
		// obtain them directly from the API server).
		opts.Client = client.Options{Cache: &client.CacheOptions{Unstructured: true}}
		// This, apparently, only affects the Writer of the split client!
		opts.NewClient = func(config *rest.Config, options client.Options) (client.Client, error) {
			return cache.NewCompositeClient(config, options)
		}
	} else {
		// cache is being injected: override the client getter to generate clients for the
		// injected cache
		opts.NewClient = func(config *rest.Config, options client.Options) (client.Client, error) {
			return cache.NewCompositeClient(config, options)
		}
	}

	mgr, err := runtimeMgr.New(config, opts)
	if err != nil {
		return nil, err
	}

	// Pass the composite cache in to the client.
	c, ok := mgr.GetClient().(*cache.CompositeClient)
	if !ok {
		return nil, errors.New("client must be a composite client")
	}
	c.SetCache(mgr.GetCache())

	return mgr, nil
}

// NewHeadless creates a headless delta-controller manager with no upstream Kubernetes API access.
// A headless manager handles only view resources stored in an in-memory ViewCache.  This is useful
// for standalone operation, testing, or when only view resources are needed.
func NewHeadless(opts Options) (Manager, error) {
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	logger := opts.Logger

	// We can create a static cache since we do not need to wait until NewCache/NewClient is
	// called by the controller runtime to reveal the cache options and client options.
	if opts.NewCache == nil {
		c := cache.NewViewCache(cache.CacheOptions{Logger: logger})
		opts.NewCache = func(_ *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
			return c, nil
		}

		opts.NewClient = func(config *rest.Config, options client.Options) (client.Client, error) {
			return c.GetClient(), nil
		}
	}

	return runtimeMgr.New(&rest.Config{}, opts)
}
