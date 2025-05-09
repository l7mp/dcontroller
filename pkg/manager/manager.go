package manager

import (
	"errors"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/l7mp/dcontroller/pkg/cache"
)

// Options provides various settings to custimize the manager. Most options come verbatim from the
// controller runtime package.
type Options struct {
	manager.Options

	// Manager is a controller runtime manager. If set, the dmanager will wrap the specified
	// manager instead of creating a new one.
	Manager manager.Manager
}

// Manager is a wrapper around the controller-runtime Manager. The main difference is a custom
// split cache: only native objects are managed by the API server, view objects are served from a
// special view cache. The client returned by the GetClient call is smart enough to know when to
// send requests to the API server (via the default cache) and when to use the view cache.
type Manager struct {
	manager.Manager
}

// New creates a new manager.
func New(config *rest.Config, opts Options) (*Manager, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	// If runtime manager is provided by the caller, use that
	if opts.Manager != nil {
		return &Manager{Manager: opts.Manager}, nil
	}

	// Otherwise create a new manager overriding the cache and the client created by the base
	// manager.
	if opts.NewCache == nil {
		opts.NewCache = func(config *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
			return cache.NewCompositeCache(config, cache.Options{
				Options: opts,
				Logger:  logger,
			})
		}
	}

	// Override the client created by the base manager with the custom split client.
	if opts.NewClient == nil {
		// Make sure unstructured objects are served through the cache (the default is to
		// obtain them directly from the API server).
		opts.Client = client.Options{
			Cache: &client.CacheOptions{
				Unstructured: true,
			},
		}
		// This, apparently, only affects the Writer of the split client!
		opts.NewClient = NewCompositeClient
	}

	mgr, err := manager.New(config, opts.Options)
	if err != nil {
		return nil, err
	}

	// pass the composite cache in to the client
	c, ok := mgr.GetClient().(*compositeClient)
	if !ok {
		return nil, errors.New("cache must be a composite client")
	}
	c.setCache(mgr.GetCache()) //nolint:errcheck

	return &Manager{Manager: mgr}, nil
}

// // GetCache returns the view cache. Use manager.Manager.GetCache() to obtain the cache of the
// // controller runtime manager.
// func (m *Manager) GetCache() *cache.ViewCache {
// 	return m.cache
// }
