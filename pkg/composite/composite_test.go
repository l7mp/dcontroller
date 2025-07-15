package composite

import (
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	"go.uber.org/zap/zapcore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

// A "functional" API server fake and fake config
func createLocalTestServerConfig() *rest.Config {
	// Create a test server that always returns 200 OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"kind": "Status", "status": "Success"}`))
	}))

	return &rest.Config{
		Host: server.URL,
	}
}

func TestCompositeAPIClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Composite API Suite")
}

var _ = Describe("NewCompositeAPIClient", func() {
	Describe("Basic creation", func() {
		It("should create a composite API client with nil config", func() {
			opts := Options{
				CacheOptions: CacheOptions{
					Logger: logger,
				},
				ClientOptions: client.Options{},
				Logger:        logger,
			}

			apiClient, err := NewCompositeAPIClient(nil, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(apiClient).NotTo(BeNil())

			// Verify all components are created
			Expect(apiClient.Client).NotTo(BeNil())
			Expect(apiClient.Cache).NotTo(BeNil())
			Expect(apiClient.Discovery).NotTo(BeNil())
			Expect(apiClient.RESTMapper).NotTo(BeNil())

			// Verify components are of the correct composite types
			compositeDiscovery := apiClient.GetDiscovery()
			Expect(compositeDiscovery).NotTo(BeNil())
			Expect(compositeDiscovery).To(BeAssignableToTypeOf(&CompositeDiscoveryClient{}))

			compositeCache := apiClient.GetCache()
			Expect(compositeCache).NotTo(BeNil())
			Expect(compositeCache).To(BeAssignableToTypeOf(&CompositeCache{}))

			compositeClient := apiClient.GetClient()
			Expect(compositeClient).NotTo(BeNil())
			Expect(compositeClient).To(BeAssignableToTypeOf(&CompositeClient{}))

			// Verify RESTMapper is composite
			Expect(apiClient.RESTMapper).To(BeAssignableToTypeOf(&CompositeRESTMapper{}))
		})

		It("should handle creation with valid config", func() {
			// Create a minimal rest config (this would normally connect to a cluster)
			config := createLocalTestServerConfig()

			opts := Options{
				CacheOptions: CacheOptions{
					Logger: logger,
				},
				ClientOptions: client.Options{},
				Logger:        logger,
			}

			apiClient, err := NewCompositeAPIClient(config, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(apiClient).NotTo(BeNil())
			Expect(apiClient.Client).NotTo(BeNil())
			Expect(apiClient.Cache).NotTo(BeNil())
			Expect(apiClient.Discovery).NotTo(BeNil())
			Expect(apiClient.RESTMapper).NotTo(BeNil())
		})
	})

	Describe("Component integration", func() {
		It("should wire components together correctly", func() {
			opts := Options{
				CacheOptions: CacheOptions{
					Logger: logger,
				},
				ClientOptions: client.Options{},
				Logger:        logger,
			}

			apiClient, err := NewCompositeAPIClient(nil, opts)
			Expect(err).NotTo(HaveOccurred())

			// Test that discovery client is used by REST mapper
			discovery := apiClient.GetDiscovery()
			restMapper, ok := apiClient.RESTMapper.(*CompositeRESTMapper)
			Expect(ok).To(BeTrue())

			// The REST mapper should have a reference to the discovery client
			// (This is testing the internal wiring)
			Expect(restMapper).NotTo(BeNil())

			// Test view group recognition through the wired components
			viewGroup := "view.dcontroller.io"
			Expect(discovery.IsViewGroup(viewGroup)).To(BeTrue())
			Expect(discovery.IsViewGroup("apps")).To(BeFalse())

			// Test that cache and client can work together
			cache := apiClient.GetCache()
			client := apiClient.GetClient()
			Expect(cache).NotTo(BeNil())
			Expect(client).NotTo(BeNil())
		})
	})

	Describe("Getter methods", func() {
		var apiClient *APIClient

		BeforeEach(func() {
			opts := Options{
				CacheOptions: CacheOptions{
					Logger: logger,
				},
				ClientOptions: client.Options{},
				Logger:        logger,
			}

			var err error
			apiClient, err = NewCompositeAPIClient(nil, opts)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return correct typed components", func() {
			// GetDiscovery should return CompositeDiscoveryClient
			discovery := apiClient.GetDiscovery()
			Expect(discovery).NotTo(BeNil())
			Expect(discovery).To(BeAssignableToTypeOf(&CompositeDiscoveryClient{}))

			// Should also be accessible through the interface
			discoveryInterface := apiClient.Discovery
			Expect(discoveryInterface).To(Equal(discovery))

			// GetCache should return CompositeCache
			cache := apiClient.GetCache()
			Expect(cache).NotTo(BeNil())
			Expect(cache).To(BeAssignableToTypeOf(&CompositeCache{}))

			// Should also be accessible through the interface
			cacheInterface := apiClient.Cache
			Expect(cacheInterface).To(Equal(cache))

			// GetClient should return CompositeClient
			client := apiClient.GetClient()
			Expect(client).NotTo(BeNil())
			Expect(client).To(BeAssignableToTypeOf(&CompositeClient{}))

			// Should also be accessible through the interface
			clientInterface := apiClient.Client
			Expect(clientInterface).To(Equal(client))
		})

		It("should handle type assertion failures gracefully", func() {
			// Create an APIClient with non-composite components to test the getter edge cases
			nonCompositeClient := &APIClient{
				Discovery: &fakeDiscovery{},
				Cache:     &FakeRuntimeCache{},
				Client:    fake.NewClientBuilder().Build(),
			}

			// Getters should return nil when type assertions fail
			Expect(nonCompositeClient.GetDiscovery()).To(BeNil())
			Expect(nonCompositeClient.GetCache()).To(BeNil())
			Expect(nonCompositeClient.GetClient()).To(BeNil())
		})
	})
})

// Fake implementations for testing type assertion failures
type fakeDiscovery struct{}

func (f *fakeDiscovery) ServerResourcesForGroupVersion(string) (*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) { return nil, nil }
func (f *fakeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return nil, nil, nil
}
func (f *fakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerVersion() (*version.Info, error)       { return nil, nil }
func (f *fakeDiscovery) OpenAPISchema() (*openapiv2.Document, error) { return nil, nil }
func (f *fakeDiscovery) OpenAPIV3() openapi.Client                   { return nil }
func (f *fakeDiscovery) RESTClient() rest.Interface                  { return nil }
func (f *fakeDiscovery) WithLegacy() discovery.DiscoveryInterface    { return f }
