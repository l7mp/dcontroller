/*
Copyright 2022-2025 The l7mp.io team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/go-logr/logr"
	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/l7mp/dcontroller/internal/buildinfo"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	"github.com/l7mp/dcontroller/pkg/auth"
	"github.com/l7mp/dcontroller/pkg/kubernetes/controllers"
)

const APIServerPort = 8443

var (
	scheme           = runtime.NewScheme()
	version          = "dev"
	commitHash       = "n/a"
	buildDate        = "<unknown>"
	logger, setupLog logr.Logger
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(opv1a1.AddToScheme(scheme))
}

func main() {
	opts := zap.Options{
		Development:     true,
		DestWriter:      os.Stderr,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
	}

	rootCmd := &cobra.Command{
		Use:   "dctl",
		Short: "dcontroller admin CLI tool",
		Long:  "Admin tool for managing the Δ-controller API server and user access",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logger = zap.New(zap.UseFlagOptions(&opts))
			return nil
		},
	}

	goFlags := flag.NewFlagSet("", flag.ContinueOnError)
	opts.BindFlags(goFlags)
	rootCmd.PersistentFlags().AddGoFlagSet(goFlags)

	rootCmd.AddCommand(generateKeysCmd())
	rootCmd.AddCommand(startServerCmd())
	rootCmd.AddCommand(generateConfigCmd())
	rootCmd.AddCommand(getConfigCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// ============================================================================
// Command: generate-keys
// ============================================================================

type genKeysConfig struct {
	hostnames         []string
	keyFile, certFile string
}

func generateKeysCmd() *cobra.Command {
	cfg := genKeysConfig{}
	cmd := &cobra.Command{
		Use:   "generate-keys",
		Short: "Generate RSA key pair for the Δ-controller API server",
		Long:  "Generate a new RSA key pair for signing and validating JWT tokens",
		Example: `  # Generate keys with default names (localhost)
  dctl generate-keys

  # Generate keys for a specific hostname
  dctl generate-keys --hostname=api.example.com

  # Generate keys for multiple hostnames and IPs
  dctl generate-keys --hostname=api.example.com --hostname=134.112.161.48 --hostname=localhost

  # Generate keys with custom output files
  dctl generate-keys --hostname=example.com --tls-key-file=/etc/server.key --tls-cert-file=/etc/server.crt`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerateKeys(cmd, cfg)
		},
	}

	cmd.Flags().StringSliceVar(&cfg.hostnames, "hostname", []string{"localhost"}, "Hostname or IP address (can be specified multiple times)")
	cmd.Flags().StringVar(&cfg.keyFile, "tls-key-file", "apiserver.key", "TLS key output file")
	cmd.Flags().StringVar(&cfg.certFile, "tls-cert-file", "apiserver.crt", "TLS certificate output file")

	return cmd
}

func runGenerateKeys(_ *cobra.Command, cfg genKeysConfig) error {
	fmt.Println("🔑 Generating RSA key pair...")

	cert, key, err := auth.GenerateSelfSignedCertWithSANs(cfg.hostnames)
	if err != nil {
		return fmt.Errorf("failed to generate keys: %w", err)
	}

	if err := auth.WriteCertAndKey(cfg.keyFile, cfg.certFile, key, cert); err != nil {
		return fmt.Errorf("failed to write key/cert into file %q/%q: %w", cfg.keyFile, cfg.certFile, err)
	}

	fmt.Println("✅ Successfully generated keys:")
	fmt.Printf("   TLS key:  %s\n", cfg.keyFile)
	fmt.Printf("   TLS cert: %s\n", cfg.certFile)
	fmt.Printf("   SANs:     %v\n", cfg.hostnames)

	return nil
}

// ============================================================================
// Command: start
// ============================================================================

type apiServerConfig struct {
	addr                                                          string
	port                                                          int
	httpMode                                                      bool
	configFile                                                    string
	disableAPIServer, disableAuthentication, enableLeaderElection bool
	certFile, keyFile, metricsAddr, probeAddr                     string
}

func startServerCmd() *cobra.Command {
	cfg := apiServerConfig{}

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the Δ-controller operator",
		Long:  "Start the Δ-controller operator",
		Example: `  # Start with default settings (requires TLS cert/key)
  dctl start --tls-cert-file=apiserver.crt --tls-key-file=apiserver.key

  # Start without authentication (for development/testing)
  dctl start --disable-authentication

  # Start in HTTP mode (no TLS)
  dctl start --http --disable-authentication

  # Start on specific address and port
  dctl start --addr=0.0.0.0 --port=8443 --tls-cert-file=/etc/server.crt --tls-key-file=/etc/server.key`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStartServer(cmd, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.addr, "addr", "localhost", "API server bind address")
	cmd.Flags().IntVar(&cfg.port, "port", 8443, "API server port")
	cmd.Flags().BoolVar(&cfg.httpMode, "http", false, "Use insecure HTTP (no TLS)")
	cmd.Flags().StringVar(&cfg.certFile, "tls-cert-file", "apiserver.crt",
		"TLS cert file for secure mode and JWT validation (latter not required if --disable-authentication is set)")
	cmd.Flags().StringVar(&cfg.keyFile, "tls-key-file", "apiserver.key", "TLS key file for secure mode")
	cmd.Flags().BoolVar(&cfg.disableAuthentication, "disable-authentication", false,
		"Disable authentication/authorization (WARNING: allows unrestricted access)")
	cmd.Flags().StringVar(&cfg.configFile, "config", "", "Config file (optional)")
	cmd.Flags().BoolVar(&cfg.disableAPIServer, "disable-api-server", false, "Disable the embedded API server.")
	cmd.Flags().StringVar(&cfg.metricsAddr, "metrics-bind-address", ":8080",
		"The address the metric endpoint binds to.")
	cmd.Flags().StringVar(&cfg.probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	cmd.Flags().BoolVar(&cfg.enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")

	return cmd
}

func runStartServer(_ *cobra.Command, cfg apiServerConfig) error {
	ctrl.SetLogger(logger.WithName("dcontroller"))
	setupLog = logger.WithName("setup")

	buildInfo := buildinfo.BuildInfo{Version: version, CommitHash: commitHash, BuildDate: buildDate}
	setupLog.Info(fmt.Sprintf("starting Δ-controller operator %s", buildInfo.String()))

	// Create an operator controller to watch and reconcile Operator CRDs
	c, err := controllers.NewOpController(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: cfg.metricsAddr,
		},
		HealthProbeBindAddress: cfg.probeAddr,
		LeaderElection:         cfg.enableLeaderElection,
		LeaderElectionID:       "92062b70.l7mp.io",
		Logger:                 logger,
	})
	if err != nil {
		setupLog.Error(err, "unable to create an operator controller")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()

	if !cfg.disableAPIServer {
		clientMpx := c.GetClient()
		apiServerCfg, err := apiserver.NewDefaultConfig("0.0.0.0", APIServerPort, clientMpx, cfg.httpMode, logger)
		if err != nil {
			setupLog.Error(err, "failed to create the config for the embedded API server")
			os.Exit(1)
		}

		// Configure authentication and authorization unless explicitly disabled or running in HTTP-only mode
		if cfg.httpMode || cfg.disableAuthentication {
			setupLog.Info("⚠️  WARNING: Running API server without authentication - unrestricted access enabled")
		} else {
			// Load TLS key/cert
			if err := checkCert(cfg.certFile, cfg.keyFile); err != nil {
				return fmt.Errorf("failed to load TLS key/cert: %w", err)
			}
			// Load public key
			publicKey, err := auth.LoadPublicKey(cfg.certFile)
			if err != nil {
				return fmt.Errorf("failed to load public key: %w (hint: generate keys with "+
					"'dctl generate-keys' or use --disable-authentication)", err)
			}

			apiServerCfg.Authenticator = auth.NewJWTAuthenticator(publicKey)
			apiServerCfg.Authorizer = auth.NewCompositeAuthorizer()
			setupLog.Info("API server authentication and authorization enabled")
			apiServerCfg.CertFile = cfg.certFile
			apiServerCfg.KeyFile = cfg.keyFile
		}

		apiServer, err := apiserver.NewAPIServer(apiServerCfg)
		if err != nil {
			setupLog.Error(err, "failed to create the embedded API server")
			os.Exit(1)
		}

		c.SetAPIServer(apiServer)

		go func() {
			if err := apiServer.Start(ctx); err != nil {
				setupLog.Error(err, "embedded API server error")
			}
		}()
	}

	setupLog.Info("starting the operator controller")
	if err := c.Start(ctx); err != nil {
		setupLog.Error(err, "operator error")
	}

	return nil
}

// ============================================================================
// Command: generate-config
// ============================================================================

type generateConfigConfig struct {
	username           string
	namespaces         string
	rules              string
	rulesFile          string
	expiry             time.Duration
	output             string
	keyFile            string
	serverAddress      string
	defaultNamespace   string
	insecure, httpMode bool
}

func generateConfigCmd() *cobra.Command {
	cfg := generateConfigConfig{}

	cmd := &cobra.Command{
		Use:   "generate-config",
		Short: "Generate kubeconfig with JWT token for a user",
		Long:  "Generate a complete kubeconfig file with embedded JWT token for accessing the API server",
		Example: `  # Generate kubeconfig to stdout (redirect to file)
  dctl generate-config --user=alice --namespaces=default > dctl.config
  export KUBECONFIG=./dctl.config

  # Generate kubeconfig to specific file
  dctl generate-config --user=alice --namespaces=default --output=alice.config

  # Generate admin config with full access
  dctl generate-config --user=admin --namespaces="*" > admin.config

  # Read-only access to specific resources
  dctl generate-config --user=viewer --namespaces=team-a \
    --rules='[{"verbs":["get","list","watch"],"apiGroups":[""],"resources":["pods","services"]}]' \
    > viewer.config

  # Load rules from file
  dctl generate-config --user=bob --namespaces=team-b --rules-file=rules.json > bob.config

  # Multiple namespaces
  dctl generate-config --user=developer --namespaces=dev,staging > dev.config`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerateConfig(cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.username, "user", "", "Username (required)")
	cmd.Flags().StringVar(&cfg.namespaces, "namespaces", "", "Comma-separated allowed namespaces, or * for all (empty = no restrictions)")
	cmd.Flags().StringVar(&cfg.rules, "rules", "", "RBAC PolicyRules as JSON array (empty = full access)")
	cmd.Flags().StringVar(&cfg.rulesFile, "rules-file", "", "Path to file containing RBAC PolicyRules JSON")
	cmd.Flags().DurationVar(&cfg.expiry, "expiry", 24*365*time.Hour, "Token expiry duration")
	cmd.Flags().StringVar(&cfg.output, "output", "", "Output kubeconfig file (default: stdout)")
	cmd.Flags().StringVar(&cfg.keyFile, "tls-key-file", "apiserver.key",
		"TLS key file specifying the private key for access token generation")
	cmd.Flags().StringVar(&cfg.serverAddress, "server-address", "localhost:8443", "API server address:port pair")
	cmd.Flags().StringVar(&cfg.defaultNamespace, "namespace", "", "Default namespace in context")
	cmd.Flags().BoolVar(&cfg.insecure, "insecure", false, "Skip TLS verification")
	cmd.Flags().BoolVar(&cfg.httpMode, "http", false, "Use insecure HTTP (no TLS)")

	cmd.MarkFlagRequired("user") //nolint:errcheck // flag exists

	return cmd
}

func runGenerateConfig(cfg generateConfigConfig) error {
	// Parse namespaces
	var namespacesList []string
	if cfg.namespaces != "" {
		namespacesList = parseCommaSeparated(cfg.namespaces)
	}

	// Parse RBAC rules
	var rulesList []rbacv1.PolicyRule
	if cfg.rulesFile != "" {
		// Load from file
		rulesData, err := os.ReadFile(cfg.rulesFile)
		if err != nil {
			return fmt.Errorf("failed to read rules file: %w", err)
		}
		if err := json.Unmarshal(rulesData, &rulesList); err != nil {
			return fmt.Errorf("failed to parse rules from file: %w", err)
		}
	} else if cfg.rules != "" {
		// Parse from inline JSON
		if err := json.Unmarshal([]byte(cfg.rules), &rulesList); err != nil {
			return fmt.Errorf("failed to parse rules JSON: %w", err)
		}
	}
	// If both are empty, rulesList will be nil/empty = full access

	// Load private key
	privateKey, err := auth.LoadPrivateKey(cfg.keyFile)
	if err != nil {
		return fmt.Errorf("failed to load private key: %w\nHint: Generate keys with 'dctl generate-keys'", err)
	}

	// Generate JWT token
	generator := auth.NewTokenGenerator(privateKey)
	token, err := generator.GenerateToken(cfg.username, namespacesList, rulesList, cfg.expiry)
	if err != nil {
		return fmt.Errorf("failed to generate token: %w", err)
	}

	// Create kubeconfig
	kubeconfigOpts := &auth.KubeconfigOptions{
		ClusterName:      "dcontroller",
		ContextName:      "dcontroller",
		DefaultNamespace: cfg.defaultNamespace,
		Insecure:         cfg.insecure,
		HTTPMode:         cfg.httpMode,
	}

	// Output to stdout or file
	if cfg.output == "" {
		// Output to stdout
		kubeconfigYAML, err := auth.GenerateKubeconfig(cfg.serverAddress, cfg.username, token, kubeconfigOpts)
		if err != nil {
			return fmt.Errorf("failed to generate kubeconfig: %w", err)
		}
		fmt.Print(kubeconfigYAML)
	} else {
		// Output to file
		if err := auth.WriteKubeconfig(cfg.output, cfg.serverAddress, cfg.username, token, kubeconfigOpts); err != nil {
			return fmt.Errorf("failed to write kubeconfig: %w", err)
		}
		// Print success message to stderr (so it doesn't interfere with file redirects)
		fmt.Fprintf(os.Stderr, "✅ Successfully generated kubeconfig\n")
		fmt.Fprintf(os.Stderr, "   User: %s\n", cfg.username)
		fmt.Fprintf(os.Stderr, "   File: %s\n", cfg.output)
		if len(namespacesList) > 0 {
			fmt.Fprintf(os.Stderr, "   Namespaces: %v\n", namespacesList)
		} else {
			fmt.Fprintf(os.Stderr, "   Namespaces: <none - no restrictions>\n")
		}
		if len(rulesList) > 0 {
			fmt.Fprintf(os.Stderr, "   Rules: %d RBAC policy rules\n", len(rulesList))
			for i, rule := range rulesList {
				fmt.Fprintf(os.Stderr, "     [%d] verbs=%v apiGroups=%v resources=%v\n",
					i+1, rule.Verbs, rule.APIGroups, rule.Resources)
			}
		} else {
			fmt.Fprintf(os.Stderr, "   Rules: <none - full access>\n")
		}
		fmt.Fprintf(os.Stderr, "   Expiry: %s\n", cfg.expiry)
		fmt.Fprintf(os.Stderr, "   Server: %s\n", cfg.serverAddress)
	}

	return nil
}

// ============================================================================
// Command: get-config
// ============================================================================

type getConfigConfig struct{ kubeconfig, certFile string }

func getConfigCmd() *cobra.Command {
	cfg := getConfigConfig{}

	cmd := &cobra.Command{
		Use:   "get-config",
		Short: "Display user information from a kubeconfig file",
		Long:  "Decode and display user information and token claims from a kubeconfig file",
		Example: `  # Get info from KUBECONFIG env var
  export KUBECONFIG=./dctl.config
  dctl get-config

  # Get info from specific kubeconfig file
  dctl get-config --kubeconfig=./dctl.config`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGetConfig(cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.kubeconfig, "kubeconfig", "", "Path to kubeconfig file (defaults to KUBECONFIG env var)")
	cmd.Flags().StringVar(&cfg.certFile, "tls-cert-file", "apiserver.crt",
		"TLS certificate file to be used to obtain the public key for token checks")

	return cmd
}

func runGetConfig(cfg getConfigConfig) error {
	// Get kubeconfig path from flag or environment
	kubeconfigPath := cfg.kubeconfig
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv("KUBECONFIG")
	}
	if kubeconfigPath == "" {
		return fmt.Errorf("no kubeconfig specified: set KUBECONFIG env var or use --kubeconfig flag")
	}

	// Extract token from kubeconfig
	token, err := auth.ExtractTokenFromKubeconfig(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to extract token from kubeconfig: %w", err)
	}

	// Load public key
	publicKey, err := auth.LoadPublicKey(cfg.certFile)
	if err != nil {
		return fmt.Errorf("failed to load public key: %w", err)
	}

	// Parse token
	claims := &auth.Claims{}
	jwtToken, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})
	if err != nil {
		return err
	}

	// Display user info
	fmt.Println("👤 User Information:")
	fmt.Printf("   Username:   %s\n", claims.Username)
	if len(claims.Namespaces) > 0 {
		fmt.Printf("   Namespaces: %v\n", claims.Namespaces)
	} else {
		fmt.Printf("   Namespaces: <none - no restrictions>\n")
	}
	if len(claims.Rules) > 0 {
		fmt.Printf("   Rules: %d RBAC policy rules\n", len(claims.Rules))
		for i, rule := range claims.Rules {
			fmt.Printf("     [%d] verbs=%v apiGroups=%v resources=%v\n",
				i+1, rule.Verbs, rule.APIGroups, rule.Resources)
			if len(rule.ResourceNames) > 0 {
				fmt.Printf("         resourceNames=%v\n", rule.ResourceNames)
			}
		}
	} else {
		fmt.Printf("   Rules: <none - full access>\n")
	}
	fmt.Println()
	fmt.Println("⏱️  Token Metadata:")
	fmt.Printf("   Issuer:     %s\n", claims.Issuer)
	fmt.Printf("   Issued At:  %s\n", claims.IssuedAt.Format(time.RFC3339))
	fmt.Printf("   Expires At: %s\n", claims.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("   Not Before: %s\n", claims.NotBefore.Format(time.RFC3339))

	// Check expiry
	if claims.ExpiresAt != nil && time.Now().After(claims.ExpiresAt.Time) {
		fmt.Println("❌ Token is EXPIRED")
		return fmt.Errorf("token expired")
	}

	if jwtToken != nil && jwtToken.Valid {
		fmt.Println("✅ Token is VALID")
	} else {
		fmt.Println("❌ Token is INVALID")
	}

	return nil
}

// ============================================================================
// Helper functions
// ============================================================================

func parseCommaSeparated(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, item := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// checkCert loads a TLS certificate and private key from the specified files, validates that they
// are a matching pair, and logs the certificate's details if successful.
func checkCert(certFile, keyFile string) error {
	// 1. Load the raw bytes from the certificate and key files.
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return fmt.Errorf("failed to read certificate file %q: %w", certFile, err)
	}

	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return fmt.Errorf("failed to read private key file %q: %w", keyFile, err)
	}

	// 2. The core validation step: Attempt to create a tls.Certificate object.
	// This function will fail if the PEM blocks are malformed or if the private key
	// does not match the public key in the certificate.
	_, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("failed to validate certificate and key pair: %w", err)
	}

	// 3. If validation was successful, proceed to log the certificate's details.
	// We can be confident now that the certPEM contains a valid certificate.
	block, _ := pem.Decode(certPEM)
	cert, _ := x509.ParseCertificate(block.Bytes)

	ipStrings := make([]string, len(cert.IPAddresses))
	for i, ip := range cert.IPAddresses {
		ipStrings[i] = ip.String()
	}

	setupLog.Info("validated TLS certificate and key pair", "cert_path", certFile, "key_path", keyFile,
		"subject", cert.Subject.CommonName, "dns_names", cert.DNSNames, "ip_addresses", ipStrings,
		"valid-to", cert.NotAfter)

	return nil
}
