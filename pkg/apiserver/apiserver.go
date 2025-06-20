package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/util"
)

const DefaultAPIServerPort = 18443

// APIServer manages a Kubernetes API server with dynamic GVK registration.
type APIServer struct {
	// Configuration
	bindAddress      net.IP
	bindPort         int
	delegatingClient client.Client
	discoveryClient  discovery.DiscoveryInterface
	restConfig       *rest.Config

	// State management
	mu   sync.RWMutex
	gvks map[schema.GroupVersionKind]bool

	// Lifecycle management
	running     bool
	currentCtx  context.Context
	cancelFunc  context.CancelFunc
	restartChan chan struct{}

	log logr.Logger
}

// NewAPIServer creates a new API server instance. The argument mgr is the controller-runtime
// manager that is used to obtain the clients (the delegaitng client and the discovery client) that
// will serve resource requests and the REST config for API discovery (if any), addr is the address
// the API server listens on (Default: localhost), and port ois the port the API server listens on
// (Default: 18443).
func NewAPIServer(mgr manager.Manager, addr string, port int) (*APIServer, error) {
	log := mgr.GetLogger()
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("apiserver")

	if mgr == nil {
		return nil, errors.New("manager: required argument")
	}

	if port == 0 {
		port = DefaultAPIServerPort
	}

	bindAddr := net.ParseIP(addr)
	if bindAddr == nil {
		bindAddr = net.ParseIP("127.0.0.1")
	}

	var discoveryClient discovery.DiscoveryInterface
	cfg := mgr.GetConfig()
	if cfg == nil {
		log.Info("failed to obtain REST config: native resources unavailable")
	} else {
		client, err := discovery.NewDiscoveryClientForConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create discovery client: %w", err)
		}
		discoveryClient = client
	}

	return &APIServer{
		bindAddress:      bindAddr,
		bindPort:         port,
		delegatingClient: mgr.GetClient(),
		discoveryClient:  discoveryClient,
		gvks:             make(map[schema.GroupVersionKind]bool),
		log:              log,
	}, nil
}

// RegisterGVK adds a new GVK to the server (silently ignores if already registered)
func (s *APIServer) RegisterGVK(gvk schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.gvks[gvk] {
		// Already registered, silently return
		return nil
	}

	if s.discoveryClient == nil && gvk.GroupVersion() != viewv1a1.GroupVersion {
		return errors.New("native Kubernetes resources unavailable")
	}

	s.log.V(1).Info("registering GVK", "GVK", gvk.String())
	s.gvks[gvk] = true

	return nil
}

// UnregisterGVK removes a GVK from the server (silently ignores if not registered)
func (s *APIServer) UnregisterGVK(gvk schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.gvks[gvk] {
		// Not registered, silently return
		return nil
	}

	s.log.V(1).Info("unregistering GVK", "GVK", gvk.String())
	delete(s.gvks, gvk)

	return nil
}

// Start begins the API server lifecycle with automatic restart capability. It blocks.
func (s *APIServer) Start(ctx context.Context) error {
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()

	if running {
		s.Shutdown()
	}

	s.log.Info("starting API server", "address", fmt.Sprintf("%s:%d", s.bindAddress.String(), s.bindPort))

	// Create new context for this server instance
	s.currentCtx, s.cancelFunc = context.WithCancel(ctx)
	errChan := make(chan error, 1)
	if err := s.runServerInstance(s.currentCtx, errChan); err != nil {
		return fmt.Errorf("API server startup failed: %w", err)
	}

	select {
	case <-ctx.Done():
		s.log.Info("stopping API server")
		return nil
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("API server runtime error: %w", err)
		}
		return nil
	}
}

// Shutdown gracefully stops the API server
func (s *APIServer) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancelFunc != nil {
		s.log.Info("shutting down API server")
		s.cancelFunc()
	}
}

// runServerInstance runs a single instance of the API server
func (s *APIServer) runServerInstance(ctx context.Context, errChan chan error) error {
	s.mu.Lock()

	// Get current GVK snapshot
	currentGVKs := make([]schema.GroupVersionKind, 0, len(s.gvks))
	gvks := make([]string, 0, len(s.gvks))
	for gvk := range s.gvks {
		currentGVKs = append(currentGVKs, gvk)
		gvks = append(gvks, gvk.String())
	}

	s.mu.Unlock()

	s.log.V(2).Info("starting API server instance", "GVKs", util.Stringify(gvks))

	// Build and run the server
	server, err := s.buildServer(currentGVKs)
	if err != nil {
		return fmt.Errorf("failed to build server: %w", err)
	}

	// Run server in goroutine and wait for completion or restart signal
	go func() {
		defer close(errChan)

		prepared := server.PrepareRun()

		s.mu.Lock()
		s.running = true
		s.mu.Unlock()

		errChan <- prepared.RunWithContext(ctx)

		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	return nil
}
