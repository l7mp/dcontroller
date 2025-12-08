package cache

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
)

var _ = Describe("ViewDiscovery", func() {
	var (
		viewDiscovery ViewDiscoveryInterface
		viewGroup     = viewv1a1.Group("test")
		testViewGVK   = schema.GroupVersionKind{
			Group:   viewGroup,
			Version: "v1alpha1",
			Kind:    "TestView",
		}
		testViewListGVK = schema.GroupVersionKind{
			Group:   viewGroup,
			Version: "v1alpha1",
			Kind:    "TestViewList",
		}
		nonViewGVK = schema.GroupVersionKind{
			Group:   "apps",
			Version: "v1",
			Kind:    "Deployment",
		}
	)

	BeforeEach(func() {
		viewDiscovery = NewViewDiscovery()
	})

	Describe("Basic functionality", func() {
		It("should create a view discovery instance", func() {
			Expect(viewDiscovery).NotTo(BeNil())
		})

		It("should recognize view groups", func() {
			Expect(viewDiscovery.IsViewGroup(viewGroup)).To(BeTrue())
			Expect(viewDiscovery.IsViewGroup("apps")).To(BeFalse())
			Expect(viewDiscovery.IsViewGroup("")).To(BeFalse())
			Expect(viewDiscovery.IsViewGroup("custom.example.com")).To(BeFalse())
		})

		It("should recognize view kinds", func() {
			// Register a test view GVK first
			err := viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())

			Expect(viewDiscovery.IsViewKind(testViewGVK)).To(BeTrue())
			Expect(viewDiscovery.IsViewKind(nonViewGVK)).To(BeFalse())
		})

		It("should recognize view list kinds", func() {
			// Register a test view GVK first
			err := viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())

			Expect(viewDiscovery.IsViewListKind(testViewListGVK)).To(BeTrue())
			Expect(viewDiscovery.IsViewListKind(testViewGVK)).To(BeFalse())
			Expect(viewDiscovery.IsViewListKind(nonViewGVK)).To(BeFalse())
		})
	})

	Describe("GVK transformations", func() {
		BeforeEach(func() {
			err := viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should convert object GVK to list GVK", func() {
			listGVK := viewDiscovery.ListGVKFromObjectGVK(testViewGVK)
			Expect(listGVK.Group).To(Equal(testViewGVK.Group))
			Expect(listGVK.Version).To(Equal(testViewGVK.Version))
			Expect(listGVK.Kind).To(Equal(testViewGVK.Kind + "List"))
		})

		It("should convert list GVK to object GVK", func() {
			objectGVK := viewDiscovery.ObjectGVKFromListGVK(testViewListGVK)
			Expect(objectGVK.Group).To(Equal(testViewListGVK.Group))
			Expect(objectGVK.Version).To(Equal(testViewListGVK.Version))
			Expect(objectGVK.Kind).To(Equal("TestView"))
		})

		It("should handle list GVK without List suffix", func() {
			// Edge case: what if someone passes a non-list GVK to ObjectGVKFromListGVK?
			objectGVK := viewDiscovery.ObjectGVKFromListGVK(testViewGVK)
			Expect(objectGVK.Kind).To(Equal(testViewGVK.Kind)) // Should return as-is
		})
	})

	Describe("Kind/Resource conversions", func() {
		It("should convert kind to resource", func() {
			resource := viewDiscovery.ResourceFromKind("TestView")
			Expect(resource).To(Equal("testview"))

			resource = viewDiscovery.ResourceFromKind("MyCustomView")
			Expect(resource).To(Equal("mycustomview"))

			resource = viewDiscovery.ResourceFromKind("API")
			Expect(resource).To(Equal("api"))
		})

		It("should convert resource to kind", func() {
			// This fails with "not registered"
			_, err := viewDiscovery.KindFromResource("testview")
			Expect(err).To(HaveOccurred())

			// After registering it should work
			err = viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())
			kind, err := viewDiscovery.KindFromResource("testview")
			Expect(err).NotTo(HaveOccurred())
			Expect(kind).To(Equal("TestView"))

			// Plural still fails
			_, err = viewDiscovery.KindFromResource("testviews")
			Expect(err).To(HaveOccurred())

			// This should still fail
			_, err = viewDiscovery.KindFromResource("mycustomviews")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Dynamic GVK registration", func() {
		It("should register new view GVKs", func() {
			newGVK := schema.GroupVersionKind{
				Group:   viewGroup,
				Version: "v1alpha1",
				Kind:    "NewTestView",
			}

			err := viewDiscovery.RegisterViewGVK(newGVK)
			Expect(err).NotTo(HaveOccurred())

			registeredGVKs := viewDiscovery.GetRegisteredViewGVKs()
			Expect(registeredGVKs).To(ContainElement(newGVK))
		})

		It("should unregister view GVKs", func() {
			// Register first
			err := viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())

			// Verify it's registered
			registeredGVKs := viewDiscovery.GetRegisteredViewGVKs()
			Expect(registeredGVKs).To(ContainElement(testViewGVK))

			// Unregister
			err = viewDiscovery.UnregisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())

			// Verify it's no longer registered
			registeredGVKs = viewDiscovery.GetRegisteredViewGVKs()
			Expect(registeredGVKs).NotTo(ContainElement(testViewGVK))
		})

		It("should handle registration of non-view groups", func() {
			err := viewDiscovery.RegisterViewGVK(nonViewGVK)
			// non-view gvks ignored
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle unregistration of non-existent GVKs", func() {
			nonExistentGVK := schema.GroupVersionKind{
				Group:   viewGroup,
				Version: "v1alpha1",
				Kind:    "NonExistentView",
			}

			// Silently ignore
			err := viewDiscovery.UnregisterViewGVK(nonExistentGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle duplicate registrations", func() {
			// Register once
			err := viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())

			// Register again - should not error but also not duplicate
			err = viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())

			// Should only appear once in registered GVKs
			registeredGVKs := viewDiscovery.GetRegisteredViewGVKs()
			count := 0
			for _, gvk := range registeredGVKs {
				if gvk == testViewGVK {
					count++
				}
			}
			Expect(count).To(Equal(1))
		})
	})

	Describe("Server discovery methods", func() {
		BeforeEach(func() {
			err := viewDiscovery.RegisterViewGVK(testViewGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return server resources for group version", func() {
			resources, err := viewDiscovery.ServerResourcesForGroupVersion(viewv1a1.GroupVersion("test").String())
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
			Expect(resources.GroupVersion).To(Equal(viewv1a1.GroupVersion("test").String()))
			Expect(resources.APIResources).ToNot(BeEmpty())

			// Should contain our registered view
			var foundTestView bool
			for _, resource := range resources.APIResources {
				if resource.Kind == "TestView" {
					foundTestView = true
					Expect(resource.Name).To(Equal("testview"))
					Expect(resource.Namespaced).To(BeTrue())
					Expect(resource.Verbs).To(ContainElements("get", "list", "create", "update", "patch", "delete", "watch"))
					break
				}
			}
			Expect(foundTestView).To(BeTrue())
		})

		It("should handle non-view group versions", func() {
			resources, err := viewDiscovery.ServerResourcesForGroupVersion("apps/v1")
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).NotTo(BeNil())
			Expect(resources.APIResources).To(BeEmpty())
		})

		It("should return server groups", func() {
			groups, err := viewDiscovery.ServerGroups()
			Expect(err).NotTo(HaveOccurred())
			Expect(groups.Groups).To(HaveLen(1))
			Expect(groups.Groups[0].Name).To(Equal(viewGroup))
			Expect(groups.Groups[0].Versions).To(HaveLen(1))
			Expect(groups.Groups[0].Versions[0].GroupVersion).To(Equal(viewv1a1.GroupVersion("test").String()))
		})

		It("should return server groups and resources", func() {
			groups, resources, err := viewDiscovery.ServerGroupsAndResources()
			Expect(err).NotTo(HaveOccurred())

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Name).To(Equal(viewGroup))

			Expect(resources).ToNot(BeEmpty())
			var foundViewGroupResources bool
			for _, resourceList := range resources {
				if resourceList.GroupVersion == viewv1a1.GroupVersion("test").String() {
					foundViewGroupResources = true
					Expect(resourceList.APIResources).ToNot(BeEmpty())
					break
				}
			}
			Expect(foundViewGroupResources).To(BeTrue())
		})

		It("should return server preferred resources", func() {
			resources, err := viewDiscovery.ServerPreferredResources()
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).ToNot(BeEmpty())

			var foundViewGroup bool
			for _, resourceList := range resources {
				if resourceList.GroupVersion == viewv1a1.GroupVersion("test").String() {
					foundViewGroup = true
					Expect(resourceList.APIResources).ToNot(BeEmpty())
					break
				}
			}
			Expect(foundViewGroup).To(BeTrue())
		})
	})

	Describe("Edge cases and error handling", func() {
		It("should handle empty view discovery", func() {
			emptyDiscovery := NewViewDiscovery()

			// Should still work but return empty results
			groups, err := emptyDiscovery.ServerGroups()
			Expect(err).NotTo(HaveOccurred())
			Expect(groups.Groups).To(BeEmpty())

			resources, err := emptyDiscovery.ServerPreferredResources()
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(BeEmpty())
		})

		It("should handle invalid group versions", func() {
			_, err := viewDiscovery.ServerResourcesForGroupVersion("invalid/group/version")
			Expect(err).To(HaveOccurred())
		})

		It("should be thread-safe for concurrent registration", func() {
			// This is a basic test - in a real scenario you'd want more sophisticated race testing
			gvk1 := schema.GroupVersionKind{Group: viewGroup, Version: "v1alpha1", Kind: "ConcurrentView1"}
			gvk2 := schema.GroupVersionKind{Group: viewGroup, Version: "v1alpha1", Kind: "ConcurrentView2"}

			done := make(chan bool, 2)

			go func() {
				defer GinkgoRecover()
				err := viewDiscovery.RegisterViewGVK(gvk1)
				Expect(err).NotTo(HaveOccurred())
				done <- true
			}()

			go func() {
				defer GinkgoRecover()
				err := viewDiscovery.RegisterViewGVK(gvk2)
				Expect(err).NotTo(HaveOccurred())
				done <- true
			}()

			// Wait for both goroutines
			<-done
			<-done

			// Both should be registered
			registeredGVKs := viewDiscovery.GetRegisteredViewGVKs()
			Expect(registeredGVKs).To(ContainElement(gvk1))
			Expect(registeredGVKs).To(ContainElement(gvk2))
		})
	})
})
