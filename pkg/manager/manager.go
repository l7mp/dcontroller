package manager

import (
	"errors"

	"k8s.io/client-go/rest"
	ctrlCache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"hsnlab/dcontroller-runtime/pkg/cache"
)

// Options allows to pass options into the Manager constructor.
type Options struct {
	manager.Options
	// Manager can be used to set a controller-runtime Manager to wrap.
	Manager manager.Manager
}

// Manager is a wrapper around the controller-runtime Manager. The main difference is a custom
// split cache: only native objects are loaded from the API server (via the cache), view objects
// always go to a special view cache. The client returned by the GetClient call is smart enough to
// know when to send requests to the API server (via the default cache) and when to use the view
// cache.
type Manager struct {
	manager.Manager
	// views map[string]view.View
	// triggers map[string]trigger.Trigger
}

// New creates a new manager
func New(config *rest.Config, options Options) (*Manager, error) {
	mgr := options.Manager
	if options.Manager == nil {
		// override the cache created by the base manager with the composite cache
		if options.NewCache == nil {
			options.NewCache = func(config *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
				return cache.NewCompositeCache(config, cache.Options{
					Options: opts,
					Logger:  &options.Logger,
				})
			}
		}

		// override the client created by the base manager with the manager's custom split client
		if options.NewClient == nil {
			// make sure unstructured objects are served throught the cache (the default is to
			// obtain these directly from the API server)
			options.Client.Cache.Unstructured = true
			// this, apparently, only affects the Writer of the split client!
			options.NewClient = NewCompositeClient
		}

		m, err := manager.New(config, options.Options)
		if err != nil {
			return nil, err
		}
		mgr = m

		if options.NewClient == nil {
			c, ok := mgr.GetClient().(*compositeClient)
			if !ok {
				return nil, errors.New("cache must be a composite client")
			}
			c.setCache(mgr.GetCache())
		}
	}

	return &Manager{
		Manager: mgr,
		// views:   map[string]view.View{},
	}, nil
}

// // GetCache returns the view cache. Use manager.Manager.GetCache() to obtain the cache of the
// // controller runtime manager.
// func (m *Manager) GetCache() *cache.ViewCache {
// 	return m.cache
// }
