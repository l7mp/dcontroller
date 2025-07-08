package apiserver

import (
	"fmt"
	"net"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/compatibility"

	"github.com/l7mp/dcontroller/pkg/composite"
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
	DiscoveryClient composite.ViewDiscoveryInterface
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
