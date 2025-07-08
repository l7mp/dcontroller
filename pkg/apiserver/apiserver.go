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
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/util"
)

// APIServer manages a Kubernetes API server with dynamic GVK registration.
type APIServer struct {
	// Configuration
	config           Config
	server           *genericapiserver.GenericAPIServer
	insecureListener net.Listener
	insecureServer   *http.Server
	delegatingClient client.Client
	discoveryClient  composite.ViewDiscoveryInterface
	scheme           *runtime.Scheme
	codecs           runtime.NegotiatedSerializer

	// State management
	mu        sync.RWMutex
	groupGVKs GroupGVKs

	// Lifecycle management
	running    bool
	currentCtx context.Context
	cancelFunc context.CancelFunc

	log logr.Logger
}

// NewAPIServer creates a new API server instance with the provided config. The argument mgr is the
// controller-runtime manager that is used to obtain the clients (the delegaitng client and the
// discovery client) that will serve resource requests and the REST config for API discovery (if
// any). If no address/port is given in the config the API server listens on the localhost:18443.
func NewAPIServer(mgr manager.Manager, config Config) (*APIServer, error) {
	log := mgr.GetLogger()
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("apiserver")

	if mgr == nil {
		return nil, errors.New("manager: required argument")
	}

	discoveryClient := config.DiscoveryClient
	cfg := mgr.GetConfig()
	if discoveryClient != nil && cfg != nil {
		client, err := discovery.NewDiscoveryClientForConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create discovery client: %w", err)
		}
		discoveryClient = composite.NewCompositeDiscoveryClient(client)
	}

	return &APIServer{
		config:           config,
		delegatingClient: mgr.GetClient(),
		discoveryClient:  discoveryClient,
		groupGVKs:        make(GroupGVKs),
		log:              log,
	}, nil
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
	s.currentCtx, s.cancelFunc = context.WithCancel(ctx)
	return s.runServerInstance(s.currentCtx)
}

// runServerInstance runs a single instance of the API server
func (s *APIServer) runServerInstance(ctx context.Context) error {
	// Get current GVK snapshot
	s.mu.Lock()
	currentGVKs := make([]schema.GroupVersionKind, 0, len(s.groupGVKs))
	for _, groupGVKs := range s.groupGVKs {
		for gvk, _ := range groupGVKs {
			currentGVKs = append(currentGVKs, gvk)
		}
	}
	s.mu.Unlock()

	// Build and run the server
	if err := s.buildServer(currentGVKs); err != nil {
		return fmt.Errorf("failed to build server: %w", err)
	}

	s.log.Info("starting API server", "addr", s.GetServerAddress(), "GVKs", util.Stringify(currentGVKs))

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

		s.insecureServer = &http.Server{Handler: s.server.Handler}
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
