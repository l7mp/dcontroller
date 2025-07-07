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
	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/compatibility"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/util"
)

const DefaultAPIServerPort = 18443

type Config struct {
	*genericapiserver.RecommendedConfig

	// Addr is the server address
	Addr *net.TCPAddr

	// UseHTTP switches the API server to insecure serving mode.
	UseHTTP bool

	// DiscoveryClient allows to inject a REST discovery client into the API server. Used
	// mostly for testing,
	DiscoveryClient discovery.DiscoveryInterface
}

// NewDefaultConfig creates a RecommendedConfig with sensible defaults, either using secure serving
// (HTTPS) and insecure serving (HTTP) that can be used for testing.
func NewDefaultConfig(addr string, port int, insecure bool) (Config, error) {
	if addr == "" {
		addr = "localhost"
	}
	if port == 0 {
		port = DefaultAPIServerPort
	}

	bindAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return Config{}, fmt.Errorf("failed to resolve server address: %w", err)
	}

	// Create base config
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	config := genericapiserver.NewConfig(codecs)

	// Create loopback config
	config.LoopbackClientConfig = &rest.Config{}
	if insecure {
		config.LoopbackClientConfig.Host = fmt.Sprintf("http://%s", bindAddr.String())
	} else {
		config.LoopbackClientConfig.Host = fmt.Sprintf("https://%s", bindAddr.String())
		config.LoopbackClientConfig.TLSClientConfig = rest.TLSClientConfig{Insecure: insecure}
	}

	// Set other required fields
	config.EffectiveVersion = compatibility.NewEffectiveVersionFromString("1.33", "", "")

	return Config{
		RecommendedConfig: &genericapiserver.RecommendedConfig{Config: *config},
		Addr:              bindAddr,
		UseHTTP:           insecure,
	}, nil
}

// APIServer manages a Kubernetes API server with dynamic GVK registration.
type APIServer struct {
	// Configuration
	config           Config
	server           *genericapiserver.GenericAPIServer
	insecureListener net.Listener
	insecureServer   *http.Server
	delegatingClient client.Client
	discoveryClient  discovery.DiscoveryInterface
	scheme           *runtime.Scheme

	// State management
	mu   sync.RWMutex
	gvks map[schema.GroupVersionKind]bool

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
		gvks:             make(map[schema.GroupVersionKind]bool),
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

// RegisterGVK adds a new GVK to the server (silently ignores if already registered)
func (s *APIServer) RegisterGVK(gvk schema.GroupVersionKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.gvks[gvk] {
		// Already registered, silently return
		return nil
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
	currentGVKs := make([]schema.GroupVersionKind, 0, len(s.gvks))
	gvks := make([]string, 0, len(s.gvks))
	for gvk := range s.gvks {
		currentGVKs = append(currentGVKs, gvk)
		gvks = append(gvks, gvk.String())
	}
	s.mu.Unlock()

	// Build and run the server
	server, scheme, err := s.buildServer(currentGVKs)
	if err != nil {
		return fmt.Errorf("failed to build server: %w", err)
	}
	s.server = server
	s.scheme = scheme

	s.log.Info("starting API server", "addr", s.GetServerAddress(), "GVKs", util.Stringify(gvks))

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

		// s.insecureServer = &http.Server{Handler: server.UnprotectedHandler()}
		s.insecureServer = &http.Server{Handler: server.Handler}
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

	prepared := server.PrepareRun()
	return prepared.RunWithContext(ctx)
}
