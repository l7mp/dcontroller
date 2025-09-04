// Package manager provides an enhanced Kubernetes controller-runtime manager
// with integrated support for Δ-controller's composite client and cache system.
//
// The manager wraps the standard controller-runtime Manager to provide seamless
// integration with the composite client system, enabling controllers to work
// with both native Kubernetes resources and view objects through a unified interface.
//
// Key components:
//   - Manager: Enhanced manager with composite client integration.
//   - FakeManager: Testing implementation with mock clients and caches.
//
// Features:
//   - Automatic composite client and cache setup.
//   - Transparent view object support.
//   - Compatible with standard controller-runtime patterns.
//   - Enhanced testing utilities with fake implementations.
//   - Support for both native and view resource types.
//
// The manager automatically configures the composite client as the default
// client, ensuring that all controllers receive the enhanced client capabilities
// without requiring code changes.
//
// Example usage:
//
//	mgr, _ := manager.New(cfg, manager.Options{
//	    Options: ctrl.Options{
//	        Scheme: scheme,
//	    },
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
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/l7mp/dcontroller/pkg/composite"
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

// New creates a new delta-controller manager. If called with a nil config, create a headless
// manager, otherwise create a composite manager. A headless manager runs with no upstream
// Kubernetes API access so it handles only view resources, not native objects. For native
// Kubernetes object support, create a composite manager by providing a valid REST config to access
// a full Kubernetes API server.
func New(config *rest.Config, opts Options) (*Manager, error) {
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	// If runtime manager is provided by the caller, use that
	if opts.Manager != nil {
		return &Manager{Manager: opts.Manager}, nil
	}

	// If no Kubernetes config is provided, fire up a standalone manager.
	if config == nil {
		return newHeadlessManager(opts)
	}

	return newManager(config, opts)
}

// newHeadlessManager creates a new headless manager.
func newHeadlessManager(opts Options) (*Manager, error) {
	logger := opts.Logger

	// We can create a static cache since we do not need to wait until NewCache/NewClient is
	// callled by the controller runtime to reveal the cache options and client options.
	c := composite.NewViewCache(composite.CacheOptions{Logger: logger})
	if opts.NewCache == nil {
		opts.NewCache = func(_ *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
			return c, nil
		}
	}

	if opts.NewClient == nil {
		opts.NewClient = func(config *rest.Config, options client.Options) (client.Client, error) {
			return c.GetClient(), nil
		}
	}

	mgr, err := manager.New(&rest.Config{}, opts.Options)
	if err != nil {
		return nil, err
	}

	return &Manager{Manager: mgr}, nil
}

// newManager creates a new manager backed by a Kubernetes API server.
func newManager(config *rest.Config, opts Options) (*Manager, error) {
	logger := opts.Logger
	if opts.NewCache == nil {
		opts.NewCache = func(config *rest.Config, opts ctrlCache.Options) (ctrlCache.Cache, error) {
			return composite.NewCompositeCache(config, composite.CacheOptions{
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
		opts.NewClient = func(config *rest.Config, options client.Options) (client.Client, error) {
			return composite.NewCompositeClient(config, options) // returns *CompositeClient
		}
	}

	mgr, err := manager.New(config, opts.Options)
	if err != nil {
		return nil, err
	}

	// pass the composite cache in to the client
	c, ok := mgr.GetClient().(*composite.CompositeClient)
	if !ok {
		return nil, errors.New("cache must be a composite client")
	}
	c.SetCache(mgr.GetCache())

	return &Manager{Manager: mgr}, nil
}
