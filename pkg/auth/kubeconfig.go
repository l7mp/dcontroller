package auth

import (
	"fmt"
	"net/url"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// KubeconfigOptions configures kubeconfig generation
type KubeconfigOptions struct {
	ClusterName      string
	ContextName      string
	DefaultNamespace string
	Insecure         bool
	HTTPMode         bool
}

// DefaultKubeconfigOptions returns sensible defaults for kubeconfig generation
func DefaultKubeconfigOptions() *KubeconfigOptions {
	return &KubeconfigOptions{
		ClusterName: "dcontroller",
		ContextName: "dcontroller",
	}
}

// CreateKubeconfig creates a complete kubeconfig structure for a user with a JWT token.
// This can be written to disk using clientcmd.WriteToFile().
func CreateKubeconfig(addr, username, token string, opts *KubeconfigOptions) *clientcmdapi.Config {
	if opts == nil {
		opts = DefaultKubeconfigOptions()
	}

	config := clientcmdapi.NewConfig()

	server := url.URL{Scheme: "https", Host: addr}
	if opts.HTTPMode {
		server.Scheme = "http"
	}

	// Add cluster
	config.Clusters[opts.ClusterName] = &clientcmdapi.Cluster{
		Server:                server.String(),
		InsecureSkipTLSVerify: opts.Insecure,
	}

	// Add user with token
	config.AuthInfos[username] = &clientcmdapi.AuthInfo{
		Token: token,
	}

	// Add context
	config.Contexts[opts.ContextName] = &clientcmdapi.Context{
		Cluster:   opts.ClusterName,
		AuthInfo:  username,
		Namespace: opts.DefaultNamespace,
	}
	config.CurrentContext = opts.ContextName

	return config
}

// GenerateKubeconfig creates a kubeconfig and returns it as a YAML string.
func GenerateKubeconfig(addr, username, token string, opts *KubeconfigOptions) (string, error) {
	config := CreateKubeconfig(addr, username, token, opts)
	yaml, err := clientcmd.Write(*config)
	if err != nil {
		return "", err
	}
	return string(yaml), nil
}

// WriteKubeconfig creates a kubeconfig and writes it to a file.
func WriteKubeconfig(filename, addr, username, token string, opts *KubeconfigOptions) error {
	config := CreateKubeconfig(addr, username, token, opts)
	return clientcmd.WriteToFile(*config, filename)
}

// ExtractTokenFromKubeconfig reads a kubeconfig file and extracts the bearer token
// from the current context's user.
func ExtractTokenFromKubeconfig(kubeconfigPath string) (string, error) {
	config, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return "", fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	if config.CurrentContext == "" {
		return "", fmt.Errorf("no current context set in kubeconfig")
	}

	context, ok := config.Contexts[config.CurrentContext]
	if !ok {
		return "", fmt.Errorf("current context %q not found in kubeconfig", config.CurrentContext)
	}

	authInfo, ok := config.AuthInfos[context.AuthInfo]
	if !ok {
		return "", fmt.Errorf("user %q not found in kubeconfig", context.AuthInfo)
	}

	if authInfo.Token == "" {
		return "", fmt.Errorf("no token found for user %q", context.AuthInfo)
	}

	return authInfo.Token, nil
}

// CreateRestConfig creates an in-memory rest.Config for programmatic client usage.
// This is useful for tests and tools that need to create dynamic clients.
func CreateRestConfig(server, token string, insecure bool) *rest.Config {
	return &rest.Config{
		Host:        server,
		BearerToken: token,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: insecure,
		},
	}
}
