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
	// Step 1: Create minimal scheme with unstructured types.
	scheme, codecs, err := s.createSchemeAndCodecs()
	if err != nil {
		return fmt.Errorf("failed to create scheme: %w", err)
	}
	s.scheme = scheme
	s.codecs = codecs

	// Step 2: Apply customizations to the config.
	config, err := s.createServerConfig()
	if err != nil {
		return fmt.Errorf("failed to create server config: %w", err)
	}

	// Step 3: Create the GenericAPIServer.
	server, err := config.Complete().New("dcontroller-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	s.server = server

	// Step 4: Create dynamic discovery handlers.
	// These handlers intercept /apis/<group> and /apis/<group>/<version> discovery requests.
	// For paths they don't handle, delegate to GoRestfulContainer (which has /apis from DiscoveryGroupManager).
	// IMPORTANT: We must NOT delegate to Director here because that would create a loop:
	//   Director → NonGoRestfulMux (our handler) → dynamicHandlers → Director (LOOP!)
	// Chain: dynamicVersionHandler → dynamicGroupHandler → GoRestfulContainer
	goRestfulContainer := server.Handler.GoRestfulContainer
	s.dynamicGroupHandler = newDynamicGroupHandler(goRestfulContainer, s.log)
	s.dynamicVersionHandler = newDynamicVersionHandler(s.dynamicGroupHandler, s.log)

	// Step 5: Create resourceHandler for CRUD operations.
	// This handler dynamically routes resource requests (GET, LIST, CREATE, etc.) to their storage.
	// Chain: resourceHandler -> dynamicVersionHandler -> dynamicGroupHandler -> go-restful
	resourceHandler := newResourceHandler(resourceHandlerConfig{
		delegatingClient: s.delegatingClient,
		scheme:           s.scheme,
		codecs:           s.codecs,
		delegate:         s.dynamicVersionHandler,
		log:              s.log,
	})
	s.resourceHandler = resourceHandler

	// Step 6: Install resourceHandler into NonGoRestfulMux to intercept /apis/* requests.
	// This is BEFORE go-restful processes them, similar to how CRDs work.
	// Note: We only intercept /apis/ (with prefix), not /apis itself.
	// The /apis endpoint is served by go-restful's DiscoveryGroupManager which we populate via AddGroup().
	// Request flow: FullHandlerChain -> NonGoRestfulMux (resourceHandler for /apis/*) -> go-restful (for /apis and fallback) -> Director
	server.Handler.NonGoRestfulMux.HandlePrefix("/apis/", resourceHandler)

	// Step 7: Register initial GVKs added before the server starts up.
	if err := s.initGVKs(); err != nil {
		return fmt.Errorf("failed to register initial GVKs: %w", err)
	}

	s.log.V(2).Info("server built successfully")

	return nil
}

// createSchemeAndCodecs creates a minimal scheme with unstructured types and codecs
func (s *APIServer) createSchemeAndCodecs() (*runtime.Scheme, runtime.NegotiatedSerializer, error) {
	scheme := runtime.NewScheme()

	// Add client-go scheme for core types and meta types.
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, serializer.CodecFactory{}, fmt.Errorf("failed to add client-go scheme: %w", err)
	}

	// Create codecs.
	codecs := NewCompositeCodecFactory(serializer.NewCodecFactory(scheme), scheme)

	return scheme, codecs, nil
}

// createServerConfig applies our customizations to the provided config and sets defaults
func (s *APIServer) createServerConfig() (*genericapiserver.RecommendedConfig, error) {
	config := s.config

	// Apply secure serving options.
	secureAddr := config.Addr.IP
	securePort := config.Addr.Port

	if config.HTTPMode {
		secureAddr = net.ParseIP("127.0.0.1")
		// use a random port for the mandatory TLS server
		securePort = rand.IntN(15000) + 32768 //nolint:gosec
	}
	secureServingOptions := &genericoptions.SecureServingOptions{
		BindAddress: secureAddr,
		BindPort:    securePort,
		ServerCert: genericoptions.GeneratableKeyCert{
			CertKey: genericoptions.CertKey{
				CertFile: config.CertFile,
				KeyFile:  config.KeyFile,
			},
		},
	}

	if err := secureServingOptions.ApplyTo(&config.SecureServing); err != nil {
		return nil, fmt.Errorf("failed to apply secure serving: %w", err)
	}

	// Configure authentication
	if config.Authenticator != nil {
		config.Authentication.Authenticator = config.Authenticator
	}

	// Configure authorization
	if config.Authorizer != nil {
		config.Authorization.Authorizer = config.Authorizer
	}

	// Configure request info resolver if not already set
	if config.RequestInfoResolver == nil {
		config.RequestInfoResolver = genericapiserver.NewRequestInfoResolver(&config.Config)
	}

	// Create the HTTP middleware for the inecure HTTP server.
	// Note: Middleware is built bottom-up but executes top-down (outer to inner)
	config.BuildHandlerChainFunc = func(apiHandler http.Handler, c *genericapiserver.Config) http.Handler {
		handler := genericfilters.WithWaitGroup(apiHandler, c.LongRunningFunc, c.NonLongRunningRequestWaitGroup)

		// Add audit handler
		handler = genericapifilters.WithAudit(handler, c.AuditBackend, c.AuditPolicyRuleEvaluator, c.LongRunningFunc)
		handler = genericapifilters.WithAuditInit(handler)

		// Add authorization (needs RequestInfo in context)
		if c.Authorization.Authorizer != nil {
			handler = genericapifilters.WithAuthorization(handler, c.Authorization.Authorizer, c.Serializer)
		}

		// Add authentication (needs RequestInfo in context)
		if c.Authentication.Authenticator != nil {
			// Create failed authentication handler
			failedHandler := genericapifilters.Unauthorized(c.Serializer)
			failedHandler = genericapifilters.WithFailedAuthenticationAudit(failedHandler, c.AuditBackend, c.AuditPolicyRuleEvaluator)

			handler = genericapifilters.WithAuthentication(handler, c.Authentication.Authenticator, failedHandler, c.Authentication.APIAudiences, c.Authentication.RequestHeaderConfig)
		}

		// Add RequestInfo parser (must come AFTER auth/authz in chain building, so it runs BEFORE them in execution)
		handler = genericapifilters.WithRequestInfo(handler, c.RequestInfoResolver)

		// Add logging wrapper (outermost)
		nextHandler := handler // Capture handler before reassignment to avoid infinite loop
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dump, err := httputil.DumpRequest(r, true) // true = include body
			if err != nil {
				dump = []byte{}
			}
			s.log.Info("HTTP Request", "method", r.Method, "path", r.URL.Path, "content", string(dump))
			nextHandler.ServeHTTP(w, r)
		})
		return handler
	}

	// Ensure secure serving is configured.
	if config.HTTPMode {
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

	// Override/set required fields for our dynamic server.
	config.Serializer = s.codecs

	// Build OpenAPI config specs: inject our dynamic OpenAPI handler.
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

// initGVKs registers the GVKs added before the API server starts.
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
