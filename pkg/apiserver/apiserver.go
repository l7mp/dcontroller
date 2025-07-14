package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/l7mp/dcontroller/pkg/composite"
)

// APIServer manages a Kubernetes API server with dynamic GVK registration. Currently all view
// resources per each running dcontroller operator are available via the API server. Only view
// resources can be queries, native Kubernetes API groups (e.g., "core/v1" and "apps/v1") must be
// queried from the default API server.
type APIServer struct {
	// Configuration
	config           Config
	server           *genericapiserver.GenericAPIServer
	insecureListener net.Listener
	insecureServer   *http.Server
	delegatingClient client.Client
	delegatingCache  *composite.ViewCache // for implementing Watch
	discoveryClient  composite.ViewDiscoveryInterface
	scheme           *runtime.Scheme
	codecs           runtime.NegotiatedSerializer

	// State management
	mu                  sync.RWMutex
	groupGVKs           GroupGVKs
	cachedOpenAPIDefs   map[string]openapicommon.OpenAPIDefinition
	cachedOpenAPIV3Defs map[string]openapicommon.OpenAPIDefinition

	// Lifecycle management
	running bool

	log logr.Logger
}

// NewAPIServer creates a new API server instance with the provided config. The argument mgr is the
// controller-runtime manager that is used to obtain the clients (the delegaitng client and the
// discovery client) that will serve resource requests and the REST config for API discovery (if
// any). If no address/port is given in the config the API server listens on the localhost:18443.
func NewAPIServer(mgr manager.Manager, config Config) (*APIServer, error) {
	if mgr == nil {
		return nil, errors.New("manager: required argument")
	}

	log := mgr.GetLogger()
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("apiserver")

	discoveryClient := config.DiscoveryClient
	if discoveryClient != nil {
		discoveryClient = composite.NewViewDiscovery()
	}

	cache, ok := mgr.GetCache().(*composite.CompositeCache)
	if !ok {
		return nil, fmt.Errorf("expected a delta-controller manager, got %T", mgr)
	}

	s := &APIServer{
		config:           config,
		delegatingClient: mgr.GetClient(),
		delegatingCache:  cache.GetViewCache(),
		discoveryClient:  discoveryClient,
		groupGVKs:        make(GroupGVKs),
		log:              log,
	}

	// Build and run the server
	if err := s.buildServer(); err != nil {
		return nil, fmt.Errorf("failed to build server: %w", err)
	}

	return s, nil
}

// GetServerAddress returns the address and the port of the running API server.
func (s *APIServer) GetServerAddress() string {
	if s.config.Addr == nil {
		return "<unknown>"
	}
	return s.config.Addr.String()
}

// GetScheme returns the scheme used by the API server.
func (s *APIServer) GetScheme() *runtime.Scheme {
	return s.scheme
}

// Start begins the API server lifecycle with automatic restart capability. It blocks.
func (s *APIServer) Start(ctx context.Context) error {
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()

	if running {
		return fmt.Errorf("API server already running at %s", s.GetServerAddress())
	}

	// Create new context for this server instance
	return s.runServerInstance(ctx)
}

// runServerInstance runs a single instance of the API server
func (s *APIServer) runServerInstance(ctx context.Context) error {
	s.log.Info("starting API server", "addr", s.GetServerAddress())

	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	if s.config.UseHTTP {
		s.log.V(2).Info("starting insecure API server", "addr", s.insecureListener.Addr())

		s.insecureServer = &http.Server{Handler: s.server.Handler} //nolint:gosec
		go func() {
			if err := s.insecureServer.Serve(s.insecureListener); err != nil && err != http.ErrServerClosed {
				s.log.Error(err, "HTTP server error")
			}
		}()

		go func() {
			<-ctx.Done()
			if err := s.insecureServer.Shutdown(context.Background()); err != nil {
				s.log.Error(err, "failed to shutdown HTTP server")
			}
		}()
	}

	prepared := s.server.PrepareRun()
	return prepared.RunWithContext(ctx)
}
