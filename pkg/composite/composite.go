// Package composite provides a unified client, cache, and discovery system that seamlessly
// handles both native Kubernetes resources and Δ-controller view objects.
//
// The composite system creates a transparent abstraction layer that allows controllers to work
// with view objects using the same APIs as native Kubernetes resources.  View objects are
// maintained in an in-memory cache with full CRUD and watch capabilities, while native resources
// are delegated to the standard Kubernetes API server (if available).
//
// Key components:
//   - CompositeClient: Unified client interface for both views and native resources.
//   - CompositeCache: Split caching system with view cache and native resource cache.
//   - CompositeDiscoveryClient: Unified API discovery for views and native resources.
//   - ClientMultiplexer: Routes client operations based on resource type.
//   - ViewCache: Specialized cache for view objects with informer support.
//
// The composite system automatically determines whether a resource is a view or native Kubernetes
// resource based on its GroupVersionKind and routes operations accordingly.  This enables
// transparent operation where controllers don't need to distinguish between view and native
// resources.
package composite

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

type ClientOptions = client.Options

// Options for creating composite API clients.
type Options struct {
	CacheOptions
	ClientOptions
	Logger logr.Logger
}

// APIClient bundles multiple API machinery components into a single client, including a regular
// client, a discovery client, a cache and a REST mapper.
type APIClient struct {
	Client     client.Client
	Cache      cache.Cache
	Discovery  discovery.DiscoveryInterface
	RESTMapper meta.RESTMapper
	Log        logr.Logger
}

// NewCompositeAPIClient creates a composite API client with all components.
func NewCompositeAPIClient(config *rest.Config, operator string, opts Options) (*APIClient, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	log := logger.WithName("composite-api")

	// Create native discovery client
	var nativeDiscovery discovery.DiscoveryInterface
	if config != nil {
		var err error
		nativeDiscovery, err = discovery.NewDiscoveryClientForConfig(config)
		if err != nil {
			return nil, err
		}
	}

	// Create composite discovery
	compositeDiscovery := NewCompositeDiscoveryClient(nativeDiscovery)

	// Create composite RESTMapper
	compositeRESTMapper := NewCompositeRESTMapper(compositeDiscovery)

	// Create composite cache
	compositeCache, err := NewCompositeCache(config, viewv1a1.Group(operator), CacheOptions{
		Options:      opts.Options,
		DefaultCache: opts.DefaultCache,
		Logger:       logger,
	})
	if err != nil {
		return nil, err
	}

	// Create composite client
	compositeClient, err := NewCompositeClient(config, viewv1a1.Group(operator), opts.ClientOptions)
	if err != nil {
		return nil, err
	}

	if config == nil {
		log.Info("native Kubernetes resources unavailable: no REST config provided")
	}

	return &APIClient{
		Client:     compositeClient,
		Cache:      compositeCache,
		Discovery:  compositeDiscovery,
		RESTMapper: compositeRESTMapper,
		Log:        logger,
	}, nil
}

// GetDiscovery returns the composite discovery client with view-specific extensions.
func (c *APIClient) GetDiscovery() *CompositeDiscoveryClient {
	if cd, ok := c.Discovery.(*CompositeDiscoveryClient); ok {
		return cd
	}
	return nil
}

// GetCache returns the cache for the bundle.
func (c *APIClient) GetCache() *CompositeCache {
	if cc, ok := c.Cache.(*CompositeCache); ok {
		return cc
	}
	return nil
}

// GetCache returns the client for the bundle.
func (c *APIClient) GetClient() *CompositeClient {
	if cc, ok := c.Client.(*CompositeClient); ok {
		return cc
	}
	return nil
}
