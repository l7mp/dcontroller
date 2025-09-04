// Package apiserver implements a Kubernetes API extension server that provides REST endpoints
// for Δ-controller view resources.
//
// The API server extends the Kubernetes API server pattern to serve custom view resources
// dynamically. It provides a complete REST API implementation with support for standard
// Kubernetes operations (GET, LIST, CREATE, UPDATE, DELETE, WATCH) on view objects.
//
// Key components:
//   - APIServer: Main server struct that handles HTTP requests and routing.
//   - ClientDelegatedStorage: Storage implementation that delegates to controller-runtime clients.
//   - CompositeCodec: Custom encoding/decoding for view objects.
//   - Registry: Dynamic API group and resource registration.
//
// The server supports both secure (HTTPS) and insecure (HTTP) modes, with configurable
// authentication and authorization. It integrates with the composite client system to
// serve view objects from the view cache while delegating native Kubernetes resources
// to the standard API server.
//
// Example usage:
//
//	config := apiserver.Config{
//	    DelegatingClient: client,
//	    UseHTTP: true,
//	    Logger: logger,
//	}
//	server, _ := apiserver.NewAPIServer(config)
//	return server.Start(ctx)
package apiserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// APIServer manages a Kubernetes API server with dynamic GVK registration. Currently all view
// resources per each running operator are available via the API server. Only view resources can be
// queried, native Kubernetes API groups (e.g., "core/v1" and "apps/v1") must be queried from the
// native Kubernetes API server.
type APIServer struct {
	// Configuration
	config           Config
	server           *genericapiserver.GenericAPIServer
	insecureListener net.Listener
	insecureServer   *http.Server
	delegatingClient client.Client
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

// NewAPIServer creates a new API server instance with the provided config.
func NewAPIServer(config Config) (*APIServer, error) {
	log := config.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("apiserver")

	s := &APIServer{
		config:           config,
		delegatingClient: config.DelegatingClient,
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

// Start initiates the API server lifecycle. It blocks.
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
