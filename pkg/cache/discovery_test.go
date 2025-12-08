package cache

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	fakeclient "k8s.io/client-go/testing"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

var _ = Describe("CompositeDiscoveryClient", func() {
	var (
		fakeNativeDiscovery *fakediscovery.FakeDiscovery
		compositeClient     *CompositeDiscoveryClient
		viewGroup           = viewv1a1.Group("test")
		testViewGVK         = schema.GroupVersionKind{
			Group:   viewGroup,
			Version: "v1alpha1",
			Kind:    "TestView",
		}
		nativeGVK = schema.GroupVersionKind{
			Group:   "apps",
			Version: "v1",
			Kind:    "Deployment",
		}
	)

	BeforeEach(func() {
		// Create fake native discovery client
		fakeClient := &fakeclient.Fake{}
		fakeNativeDiscovery = &fakediscovery.FakeDiscovery{Fake: fakeClient}

		// Set up fake discovery responses for native resources
		fakeNativeDiscovery.Resources = []*metav1.APIResourceList{
			{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{
						Name:         "deployments",
						SingularName: "deployment",
						Namespaced:   true,
						Kind:         "Deployment",
						Verbs:        []string{"create", "delete", "get", "list", "patch", "update", "watch"},
					},
				},
			},
		}

		// Create composite client
		compositeClient = NewCompositeDiscoveryClient(fakeNativeDiscovery)

		// Register test view GVK
		err := compositeClient.RegisterViewGVK(testViewGVK)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Basic functionality", func() {
		It("should create a composite discovery client", func() {
			Expect(compositeClient).NotTo(BeNil())
			Expect(compositeClient.nativeDiscovery).To(Equal(fakeNativeDiscovery))
			Expect(compositeClient.ViewDiscoveryInterface).NotTo(BeNil())
		})

		It("should support view groups detection", func() {
			Expect(compositeClient.IsViewGroup(viewGroup)).To(BeTrue())
			Expect(compositeClient.IsViewGroup("apps")).To(BeFalse())
			Expect(compositeClient.IsViewGroup("")).To(BeFalse())
		})

		It("should support view kind detection", func() {
			Expect(compositeClient.IsViewKind(testViewGVK)).To(BeTrue())
			Expect(compositeClient.IsViewKind(nativeGVK)).To(BeFalse())
		})
	})

	Describe("ServerResourcesForGroupVersion", func() {
		It("should handle view group versions", func() {
			resources, err := compositeClient.ServerResourcesForGroupVersion(viewv1a1.GroupVersion("test").String())
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
			Expect(resources.GroupVersion).To(Equal(viewv1a1.GroupVersion("test").String()))
			Expect(resources.APIResources).ToNot(BeEmpty())
		})

		It("should handle native group versions", func() {
			resources, err := compositeClient.ServerResourcesForGroupVersion("apps/v1")
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
			Expect(resources.GroupVersion).To(Equal("apps/v1"))
			Expect(resources.APIResources).To(HaveLen(1))
			Expect(resources.APIResources[0].Kind).To(Equal("Deployment"))
		})

		It("should handle invalid group versions", func() {
			_, err := compositeClient.ServerResourcesForGroupVersion("invalid-group-version")
			Expect(err).To(HaveOccurred())
		})

		It("should handle unknown native groups when native discovery is available", func() {
			_, err := compositeClient.ServerResourcesForGroupVersion("unknown/v1")
			Expect(err).To(HaveOccurred()) // Native discovery will reject unknown groups
		})
	})

	Describe("ServerGroups", func() {
		It("should return both view and native groups", func() {
			groups, err := compositeClient.ServerGroups()
			Expect(err).NotTo(HaveOccurred())
			Expect(groups).NotTo(BeNil())

			// Should contain view group
			var hasViewGroup bool
			for _, group := range groups.Groups {
				if group.Name == viewGroup {
					hasViewGroup = true
					break
				}
			}
			Expect(hasViewGroup).To(BeTrue())
		})
	})

	Describe("ServerPreferredResources", func() {
		It("should return both view and native preferred resources", func() {
			resources, err := compositeClient.ServerPreferredResources()
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
			Expect(resources).ToNot(BeEmpty())
		})
	})

	Describe("ServerPreferredNamespacedResources", func() {
		It("should return both view and native namespaced resources", func() {
			resources, err := compositeClient.ServerPreferredNamespacedResources()
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
			Expect(resources).ToNot(BeEmpty())
		})
	})

	Describe("ServerVersion", func() {
		It("should delegate to native discovery when available", func() {
			// Set up fake version response
			fakeNativeDiscovery.FakedServerVersion = &version.Info{
				Major:      "1",
				Minor:      "28",
				GitVersion: "v1.28.0",
			}

			ver, err := compositeClient.ServerVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(ver).NotTo(BeNil())
			Expect(ver.Major).To(Equal("1"))
			Expect(ver.Minor).To(Equal("28"))
		})

		It("should return default version when native discovery is nil", func() {
			compositeClient.nativeDiscovery = nil

			ver, err := compositeClient.ServerVersion()
			Expect(err).NotTo(HaveOccurred())
			Expect(ver).NotTo(BeNil())
			Expect(ver.Major).To(Equal("1"))
			Expect(ver.Minor).To(Equal("33"))
			Expect(ver.GitVersion).To(Equal("v1.33.0-dcontroller"))
		})
	})

	Describe("RESTClient", func() {
		It("should delegate to native discovery when available", func() {
			restClient := compositeClient.RESTClient()
			// Should delegate to native discovery, which returns nil for fake client
			Expect(restClient).To(BeNil())
		})

		It("should return nil when native discovery is nil", func() {
			compositeClient.nativeDiscovery = nil

			restClient := compositeClient.RESTClient()
			Expect(restClient).To(BeNil())
		})
	})

	Describe("WithLegacy", func() {
		It("should create new composite client with legacy native discovery", func() {
			Skip("Fake discovery client does not implement WithLegacy")

			legacyClient := compositeClient.WithLegacy()
			Expect(legacyClient).NotTo(BeNil())
			Expect(legacyClient).NotTo(Equal(compositeClient)) // Should be different instance

			// Should be a CompositeDiscoveryClient
			legacyComposite, ok := legacyClient.(*CompositeDiscoveryClient)
			Expect(ok).To(BeTrue())
			Expect(legacyComposite.ViewDiscoveryInterface).NotTo(BeNil())
		})

		It("should return self when native discovery is nil", func() {
			compositeClient.nativeDiscovery = nil

			legacyClient := compositeClient.WithLegacy()
			Expect(legacyClient).To(Equal(compositeClient)) // Should be same instance
		})
	})

	Describe("OpenAPI methods", func() {
		It("should delegate OpenAPISchema to native discovery", func() {
			schema, err := compositeClient.OpenAPISchema()
			Expect(err).NotTo(HaveOccurred())
			Expect(schema).NotTo(BeNil())
		})

		It("should delegate OpenAPIV3 to native discovery", func() {
			Skip("Fake discovery client does not implement OpenAPIV3")
			client := compositeClient.OpenAPIV3()
			Expect(client).NotTo(BeNil())
		})

		It("should handle nil native discovery for OpenAPI methods", func() {
			compositeClient.nativeDiscovery = nil

			schema, err := compositeClient.OpenAPISchema()
			Expect(err).To(HaveOccurred())
			Expect(schema).To(BeNil())

			client := compositeClient.OpenAPIV3()
			Expect(client).To(BeNil())
		})
	})

	Describe("View-specific extensions", func() {
		It("should support view GVK registration", func() {
			newGVK := schema.GroupVersionKind{
				Group:   viewGroup,
				Version: "v1alpha1",
				Kind:    "NewTestView",
			}

			err := compositeClient.RegisterViewGVK(newGVK)
			Expect(err).NotTo(HaveOccurred())

			Expect(compositeClient.IsViewKind(newGVK)).To(BeTrue())
		})
	})

	Describe("Error cases", func() {
		It("should handle nil native discovery gracefully", func() {
			compositeClient.nativeDiscovery = nil

			// These should not panic
			_, err := compositeClient.ServerResourcesForGroupVersion("apps/v1")
			Expect(err).To(HaveOccurred())

			groups, err := compositeClient.ServerGroups()
			Expect(err).NotTo(HaveOccurred())
			Expect(groups).NotTo(BeNil())

			resources, err := compositeClient.ServerPreferredResources()
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
		})
	})
})
