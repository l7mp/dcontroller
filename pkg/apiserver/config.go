package apiserver

import (
	"fmt"
	"net"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/compatibility"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dcontroller/pkg/cache"
)

// DefaultAPIServerPort defines the default port used for the API server.
const DefaultAPIServerPort = 18443

// Config defines the configuration for the embedded API server.
type Config struct {
	*genericapiserver.RecommendedConfig

	// Addr is the server address.
	Addr *net.TCPAddr

	// HTTPMode switches the API server to HTTP (no TLS).
	HTTPMode bool

	// Insecure allows accepting self-signed TLS certificates (HTTPS only).
	// When true, internal controllers skip TLS certificate verification.
	Insecure bool

	// DelegatingClient allows to inject a controller runtime client into the API server that
	// will be used by the server to serve requests.
	DelegatingClient client.Client

	// DiscoveryClient allows to inject a REST discovery client into the API server. Used
	// mostly for testing.
	DiscoveryClient cache.ViewDiscoveryInterface

	// Authenticator and authorizer
	Authenticator authenticator.Request
	Authorizer    authorizer.Authorizer

	// CERT files for TLS
	CertFile, KeyFile string

	// Logger provides a logger for the API server.
	Logger logr.Logger
}

// NewDefaultConfig creates an API server configuration with sensible defaults.
// - httpMode: use HTTP instead of HTTPS (no TLS)
// - insecure: skip TLS certificate verification for self-signed certificates (HTTPS only)
func NewDefaultConfig(addr string, port int, client client.Client, httpMode, insecure bool, log logr.Logger) (Config, error) {
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

	// Create base config.
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	config := genericapiserver.NewConfig(codecs)

	// Create loopback config.
	config.LoopbackClientConfig = &rest.Config{}
	if httpMode {
		config.LoopbackClientConfig.Host = fmt.Sprintf("http://%s", bindAddr.String())
	} else {
		config.LoopbackClientConfig.Host = fmt.Sprintf("https://%s", bindAddr.String())
		// Skip TLS verification if using self-signed certificates.
		config.LoopbackClientConfig.TLSClientConfig = rest.TLSClientConfig{Insecure: insecure}
	}

	// Set other required fields
	config.EffectiveVersion = compatibility.NewEffectiveVersionFromString("1.33", "", "")

	return Config{
		RecommendedConfig: &genericapiserver.RecommendedConfig{Config: *config},
		Addr:              bindAddr,
		HTTPMode:          httpMode,
		Insecure:          insecure,
		DelegatingClient:  client,
		Logger:            log,
	}, nil
}

// String returns the status for the API server.
func (c *Config) String() string {
	return fmt.Sprintf("{addr:%s:%s,insecure:%t}", c.Addr.Network(), c.Addr.String(), c.HTTPMode)
}
