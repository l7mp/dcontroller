package manager

import (
	"errors"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"hsnlab/dcontroller/pkg/cache"
)

// Manager is a wrapper around the controller-runtime Manager. The main difference is a custom
// split cache: only native objects are loaded from the API server (via the cache), view objects
// always go to a special view cache. The client returned by the GetClient call is smart enough to
// know when to send requests to the API server (via the default cache) and when to use the view
// cache.
type Manager struct {
	manager.Manager
}

// New creates a new manager
func New(mgr manager.Manager, config *rest.Config, options manager.Options) (*Manager, error) {
	logger := options.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	// If runtime manager is provided by the caller, use that
	if mgr != nil {
		return &Manager{Manager: mgr}, nil
	}

	// Otherwise create a new manager and override the cache and the client to be created by
	// the base manager
	if options.NewCache == nil {
		options.NewCache = func(config *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
			return cache.NewCompositeCache(config, cache.Options{
				Options: opts,
				Logger:  logger,
			})
		}
	}

	// override the client created by the base manager with the manager's custom split client
	if options.NewClient == nil {
		// make sure unstructured objects are served through the cache (the default
		// is to obtain them directly from the API server)
		options.Client = client.Options{
			Cache: &client.CacheOptions{
				Unstructured: true,
			},
		}
		// this, apparently, only affects the Writer of the split client!
		options.NewClient = NewCompositeClient
	}

	m, err := manager.New(config, options)
	if err != nil {
		return nil, err
	}
	mgr = m

	// pass the composite cache in to the client
	c, ok := mgr.GetClient().(*compositeClient)
	if !ok {
		return nil, errors.New("cache must be a composite client")
	}
	c.setCache(mgr.GetCache())

	return &Manager{Manager: mgr}, nil
}

// // GetCache returns the view cache. Use manager.Manager.GetCache() to obtain the cache of the
// // controller runtime manager.
// func (m *Manager) GetCache() *cache.ViewCache {
// 	return m.cache
// }
