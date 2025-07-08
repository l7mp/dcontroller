package apiserver

import (
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	// metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
)

// buildServer creates a new API server instance with the given GVKs using the provided config
func (s *APIServer) buildServer(gvks []schema.GroupVersionKind) error {
	// Step 1: Create minimal scheme with unstructured types
	scheme, codecs, err := s.createSchemeAndCodecs(gvks)
	if err != nil {
		return fmt.Errorf("failed to create scheme: %w", err)
	}
	s.scheme = scheme
	s.codecs = codecs

	// Step 2: Apply customizations to the config
	config, err := s.createServerConfig(gvks)
	if err != nil {
		return fmt.Errorf("failed to create server config: %w", err)
	}

	// Step 3: Create the GenericAPIServer
	server, err := config.Complete().New("dynamic-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	s.server = server

	// Step 4: Register API groups for native Kubernetes resources
	if err := s.registerAPIGroups(gvks); err != nil {
		return fmt.Errorf("failed to register API groups: %w", err)
	}

	s.log.V(2).Info("server built successfully", "GVKs", len(gvks))

	return nil
}

// createSchemeAndCodecs creates a minimal scheme with unstructured types and codecs
func (s *APIServer) createSchemeAndCodecs(gvks []schema.GroupVersionKind) (*runtime.Scheme, runtime.NegotiatedSerializer, error) {
	scheme := runtime.NewScheme()

	// Add client-go scheme for core types and meta types
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, serializer.CodecFactory{}, fmt.Errorf("failed to add client-go scheme: %w", err)
	}

	// metainternalversion.AddToScheme(scheme)

	// Group GVKs by GroupVersion for registration
	groupVersions := make(map[schema.GroupVersion][]schema.GroupVersionKind)
	for _, gvk := range gvks {
		gv := gvk.GroupVersion()
		groupVersions[gv] = append(groupVersions[gv], gvk)
	}

	// Register unstructured types for each GroupVersion
	for gv, gvkList := range groupVersions {
		// Register unstructured types for each kind in this GroupVersion
		for _, gvk := range gvkList {
			// Register meta types
			metav1.AddToGroupVersion(scheme, gv)

			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(listGVK(gvk), &unstructured.UnstructuredList{})
			s.log.V(4).Info("registered unstructured types in scheme", "GVK", gvk.String(),
				"list-GVK", listGVK(gvk))
		}

		// Set version priority for this group
		allVersions := []schema.GroupVersion{gv}
		if err := scheme.SetVersionPriority(allVersions...); err != nil {
			return nil, serializer.CodecFactory{},
				fmt.Errorf("failed to set version priority for %s: %w", gv.String(), err)
		}
	}

	// Create codecs
	codecs := NewCompositeCodecFactory(serializer.NewCodecFactory(scheme), scheme, s.discoveryClient)

	return scheme, codecs, nil
}

// createServerConfig applies our customizations to the provided config and sets defaults
func (s *APIServer) createServerConfig(gvks []schema.GroupVersionKind) (*genericapiserver.RecommendedConfig, error) {
	config := s.config

	// Apply secure serving options
	secureAddr := config.Addr.IP
	securePort := config.Addr.Port
	if config.UseHTTP {
		secureAddr = net.ParseIP("127.0.0.1")
		// use a random port for the mandatory TLS server
		securePort = rand.IntN(15000) + 32768
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
	config.Config.BuildHandlerChainFunc = func(apiHandler http.Handler, c *genericapiserver.Config) http.Handler {
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
	config.Config.Serializer = s.codecs

	// Update OpenAPI configuration
	openAPINamer := openapiendpoints.NewDefinitionNamer(s.scheme)
	openAPIConfig := genericapiserver.DefaultOpenAPIConfig(s.getOpenAPIDefinitions(gvks), openAPINamer)
	openAPIConfig.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:   "Dynamic API Server for dcontroller",
			Version: "v1.0.0",
		},
	}
	config.Config.OpenAPIConfig = openAPIConfig

	openAPIV3Config := genericapiserver.DefaultOpenAPIV3Config(s.getOpenAPIDefinitions(gvks), openAPINamer)
	openAPIV3Config.Info = openAPIConfig.Info // Reuse info
	config.Config.OpenAPIV3Config = openAPIV3Config

	return config.RecommendedConfig, nil
}
