package composite

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClientOptions = client.Options

// Options for creating composite API clients
type Options struct {
	CacheOptions
	ClientOptions
	Logger logr.Logger
}

// APIClient bundles all composite API machinery components
type APIClient struct {
	Client     client.Client
	Cache      cache.Cache
	Discovery  discovery.DiscoveryInterface
	RESTMapper meta.RESTMapper
	Log        logr.Logger
}

// NewCompositeAPIClient creates a complete composite API client with all components
func NewCompositeAPIClient(config *rest.Config, opts Options) (*APIClient, error) {
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
	compositeCache, err := NewCompositeCache(config, CacheOptions{
		Options:      opts.Options,
		DefaultCache: opts.DefaultCache,
		Logger:       logger,
	})
	if err != nil {
		return nil, err
	}

	// Create composite client
	compositeClient, err := NewCompositeClient(config, opts.ClientOptions)
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

// GetDiscovery returns the composite discovery client with view-specific extensions
func (c *APIClient) GetDiscovery() *CompositeDiscoveryClient {
	if cd, ok := c.Discovery.(*CompositeDiscoveryClient); ok {
		return cd
	}
	return nil
}

// GetCache returns the composite cache
func (c *APIClient) GetCache() *CompositeCache {
	if cc, ok := c.Cache.(*CompositeCache); ok {
		return cc
	}
	return nil
}

func (c *APIClient) GetClient() *CompositeClient {
	if cc, ok := c.Client.(*CompositeClient); ok {
		return cc
	}
	return nil
}
