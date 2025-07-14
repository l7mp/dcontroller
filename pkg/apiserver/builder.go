package apiserver

import (
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	openapiendpoints "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// buildServer creates a new API server instance.
func (s *APIServer) buildServer() error {
	// Step 1: Create minimal scheme with unstructured types
	scheme, codecs, err := s.createSchemeAndCodecs()
	if err != nil {
		return fmt.Errorf("failed to create scheme: %w", err)
	}
	s.scheme = scheme
	s.codecs = codecs

	// Step 2: Apply customizations to the config
	config, err := s.createServerConfig()
	if err != nil {
		return fmt.Errorf("failed to create server config: %w", err)
	}

	// Step 3: Create the GenericAPIServer
	server, err := config.Complete().New("dcontroller-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	s.server = server

	// Step 4: Register initial GVKs added before the server starts iup
	if err := s.initGVKs(); err != nil {
		return fmt.Errorf("failed to register initial GVKs: %w", err)
	}

	s.log.V(2).Info("server built successfully")

	return nil
}

// createSchemeAndCodecs creates a minimal scheme with unstructured types and codecs
func (s *APIServer) createSchemeAndCodecs() (*runtime.Scheme, runtime.NegotiatedSerializer, error) {
	scheme := runtime.NewScheme()

	// Add client-go scheme for core types and meta types
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, serializer.CodecFactory{}, fmt.Errorf("failed to add client-go scheme: %w", err)
	}

	// Create codecs
	codecs := NewCompositeCodecFactory(serializer.NewCodecFactory(scheme), scheme)

	return scheme, codecs, nil
}

// createServerConfig applies our customizations to the provided config and sets defaults
func (s *APIServer) createServerConfig() (*genericapiserver.RecommendedConfig, error) {
	config := s.config

	// Apply secure serving options
	secureAddr := config.Addr.IP
	securePort := config.Addr.Port
	if config.UseHTTP {
		secureAddr = net.ParseIP("127.0.0.1")
		// use a random port for the mandatory TLS server
		securePort = rand.IntN(15000) + 32768 //nolint:gosec
	}
	secureServingOptions := &genericoptions.SecureServingOptions{
		BindAddress: secureAddr,
		BindPort:    securePort,
		ServerCert: genericoptions.GeneratableKeyCert{
			PairName: "apiserver",
			// CertKey:  genericoptions.CertKey{},
		},
	}

	if err := secureServingOptions.ApplyTo(&config.SecureServing); err != nil {
		return nil, fmt.Errorf("failed to apply secure serving: %w", err)
	}

	// Create the HTTP middleware for the inecure HTTP server
	config.BuildHandlerChainFunc = func(apiHandler http.Handler, c *genericapiserver.Config) http.Handler {
		handler := genericfilters.WithWaitGroup(apiHandler, c.LongRunningFunc, c.NonLongRunningRequestWaitGroup)
		middleware := genericapifilters.WithRequestInfo(handler, c.RequestInfoResolver)
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dump, err := httputil.DumpRequest(r, true) // true = include body
			if err != nil {
				dump = []byte{}
			}
			s.log.Info("HTTP Request", "method", r.Method, "path", r.URL.Path, "content", string(dump))
			middleware.ServeHTTP(w, r)
		})
		return handler
	}

	// Ensure secure serving is configured
	if config.UseHTTP {
		insecureAddr := &net.TCPAddr{
			IP:   config.Addr.IP,
			Port: config.Addr.Port,
		}
		listener, err := net.Listen("tcp", insecureAddr.String())
		if err != nil {
			return nil, fmt.Errorf("failed to open insecure server socket: %w", err)
		}
		s.insecureListener = listener
	}

	// Override/set required fields for our dynamic server
	config.Serializer = s.codecs

	// Build OpenAPI config specs: inject our dynamic OpenAPI handler
	namer := openapiendpoints.NewDefinitionNamer(s.scheme)
	openAPIConfig := genericapiserver.DefaultOpenAPIConfig(s.getOpenAPIv2Handler(), namer)
	openAPIConfig.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:   "Dynamic API Server for dcontroller",
			Version: "v1.0.0",
		},
	}
	config.OpenAPIConfig = openAPIConfig

	openAPIV3Config := genericapiserver.DefaultOpenAPIV3Config(s.getOpenAPIv3Handler(), namer)
	openAPIV3Config.Info = openAPIConfig.Info // Reuse the same info
	config.OpenAPIV3Config = openAPIV3Config

	return config.RecommendedConfig, nil
}

func (s *APIServer) initGVKs() error {
	groupGVKs := map[string][]schema.GroupVersionKind{}
	for group, gvks := range s.groupGVKs {
		for gvk := range gvks {
			groupGVKs[group] = append(groupGVKs[group], gvk)
		}
	}

	for group, gvkList := range groupGVKs {
		if err := s.RegisterAPIGroup(group, gvkList); err != nil {
			return fmt.Errorf("failed to register API group %s: %w", group, err)
		}
	}

	return nil
}
