package apiserver

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/l7mp/dcontroller/internal/testutils"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/manager"
)

var _ = Describe("RegisterGVKs", func() {
	var (
		mgr        manager.Manager
		server     *APIServer
		serverCtx  context.Context
		cancel     context.CancelFunc
		port       int
		serverAddr string
	)

	BeforeEach(func() {
		var err error
		mgr, err = manager.NewFakeManager(manager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		serverAddr = "localhost"
		config, err := NewDefaultConfig(serverAddr, 0, mgr.GetClient(), true, false, logger)
		Expect(err).NotTo(HaveOccurred())
		server, err = NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())
		port = testutils.GetPort(server.GetInsecureServerAddress())

		// Start the server
		serverCtx, cancel = context.WithCancel(context.Background())
		go func() {
			defer GinkgoRecover()
			err := server.Start(serverCtx)
			Expect(err).NotTo(HaveOccurred())
		}()

		// Wait for server to start.
		Eventually(func() bool { return server.running }, timeout, interval).Should(BeTrue())
	})

	AfterEach(func() {
		cancel()
		time.Sleep(20 * time.Millisecond) // Give server time to shutdown
	})

	Context("Basic Registration", func() {
		It("should register a single GVK and make it discoverable", func() {
			testGroup := viewv1a1.Group("test-single")
			testGVK := viewv1a1.GroupVersionKind("test-single", "TestView1")

			// Register the GVK
			err := server.RegisterGVKs([]schema.GroupVersionKind{testGVK})
			Expect(err).NotTo(HaveOccurred())

			// Verify it's in the internal cache
			server.mu.RLock()
			groupGVKs, ok := server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testGVK))
			Expect(groupGVKs[testGVK]).To(BeTrue())

			// Verify discoverability via API server
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(&rest.Config{
				Host: fmt.Sprintf("http://%s:%d", serverAddr, port),
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				apiGroups, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
				if err != nil || len(apiGroups) == 0 {
					return false
				}

				// Find our group
				for _, group := range apiGroups {
					if group.Name == testGroup {
						// Find the resource list
						for _, resourceList := range apiResourceLists {
							if resourceList.GroupVersion == testGVK.GroupVersion().String() {
								// Find our resource
								for _, resource := range resourceList.APIResources {
									if resource.Kind == testGVK.Kind {
										return true
									}
								}
							}
						}
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})

		It("should register two GVKs in the same group and make them discoverable", func() {
			testGroup := viewv1a1.Group("test-dual")
			testGVK1 := viewv1a1.GroupVersionKind("test-dual", "TestView1")
			testGVK2 := viewv1a1.GroupVersionKind("test-dual", "TestView2")

			// Register both GVKs at once
			err := server.RegisterGVKs([]schema.GroupVersionKind{testGVK1, testGVK2})
			Expect(err).NotTo(HaveOccurred())

			// Verify both are in the internal cache
			server.mu.RLock()
			groupGVKs, ok := server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testGVK1))
			Expect(groupGVKs[testGVK1]).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testGVK2))
			Expect(groupGVKs[testGVK2]).To(BeTrue())

			// Verify both are discoverable via API server
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(&rest.Config{
				Host: fmt.Sprintf("http://%s:%d", serverAddr, port),
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				apiGroups, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
				if err != nil || len(apiGroups) == 0 {
					return false
				}

				foundKind1 := false
				foundKind2 := false

				// Find our group
				for _, group := range apiGroups {
					if group.Name == testGroup {
						// Find the resource list
						for _, resourceList := range apiResourceLists {
							if resourceList.GroupVersion == testGVK1.GroupVersion().String() {
								// Find our resources
								for _, resource := range resourceList.APIResources {
									if resource.Kind == testGVK1.Kind {
										foundKind1 = true
									}
									if resource.Kind == testGVK2.Kind {
										foundKind2 = true
									}
								}
							}
						}
					}
				}
				return foundKind1 && foundKind2
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Repeated Registration", func() {
		It("should allow re-registering the same group (idempotent behavior)", func() {
			testGroup := viewv1a1.Group("test-repeat")
			testGVK1 := viewv1a1.GroupVersionKind("test-repeat", "Kind1")

			// First registration
			err := server.RegisterGVKs([]schema.GroupVersionKind{testGVK1})
			Expect(err).NotTo(HaveOccurred())

			// Verify Kind1 is registered
			server.mu.RLock()
			groupGVKs, ok := server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testGVK1))

			// Second registration should succeed (idempotent) - this is the new behavior!
			testGVK2 := viewv1a1.GroupVersionKind("test-repeat", "Kind2")
			err = server.RegisterGVKs([]schema.GroupVersionKind{testGVK1, testGVK2})
			Expect(err).NotTo(HaveOccurred())

			// Verify both GVKs are registered
			server.mu.RLock()
			groupGVKs, ok = server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testGVK1))
			Expect(groupGVKs).To(HaveKey(testGVK2))
		})

		// This test documents the desired behavior: repeated registration should replace GVKs.
		// Currently this test will be skipped because the implementation doesn't support it yet.
		// Once we modify RegisterAPIGroup to allow re-registration by replacing the group,
		// we can unskip this test.
		It("should replace GVKs when registering the same group repeatedly (desired behavior)", func() {
			testGroup := viewv1a1.Group("test-replace")
			testGVK1 := viewv1a1.GroupVersionKind("test-replace", "Kind1")
			testGVK2 := viewv1a1.GroupVersionKind("test-replace", "Kind2")
			testGVK3 := viewv1a1.GroupVersionKind("test-replace", "Kind3")

			discoveryClient, err := discovery.NewDiscoveryClientForConfig(&rest.Config{
				Host: fmt.Sprintf("http://%s:%d", serverAddr, port),
			})
			Expect(err).NotTo(HaveOccurred())

			// Step 1: Register Kind1 only
			err = server.RegisterGVKs([]schema.GroupVersionKind{testGVK1})
			Expect(err).NotTo(HaveOccurred())

			// Verify Kind1 is registered and discoverable
			server.mu.RLock()
			groupGVKs := server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(groupGVKs).To(HaveKey(testGVK1))
			Expect(groupGVKs).NotTo(HaveKey(testGVK2))
			Expect(groupGVKs).NotTo(HaveKey(testGVK3))

			Eventually(func() bool {
				_, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
				if err != nil {
					return false
				}
				for _, resourceList := range apiResourceLists {
					if resourceList.GroupVersion == testGVK1.GroupVersion().String() {
						for _, resource := range resourceList.APIResources {
							if resource.Kind == testGVK1.Kind {
								return true
							}
						}
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())

			// Step 2: Register Kind1 and Kind2 (should replace the group)
			err = server.RegisterGVKs([]schema.GroupVersionKind{testGVK1, testGVK2})
			Expect(err).NotTo(HaveOccurred())

			// Verify both Kind1 and Kind2 are registered
			server.mu.RLock()
			groupGVKs = server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(groupGVKs).To(HaveKey(testGVK1))
			Expect(groupGVKs).To(HaveKey(testGVK2))
			Expect(groupGVKs).NotTo(HaveKey(testGVK3))

			Eventually(func() bool {
				_, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
				if err != nil {
					return false
				}
				foundKind1 := false
				foundKind2 := false
				for _, resourceList := range apiResourceLists {
					if resourceList.GroupVersion == testGVK1.GroupVersion().String() {
						for _, resource := range resourceList.APIResources {
							if resource.Kind == testGVK1.Kind {
								foundKind1 = true
							}
							if resource.Kind == testGVK2.Kind {
								foundKind2 = true
							}
						}
					}
				}
				return foundKind1 && foundKind2
			}, timeout, interval).Should(BeTrue())

			// Step 3: Register Kind2 and Kind3 (should replace again, Kind1 should be gone)
			err = server.RegisterGVKs([]schema.GroupVersionKind{testGVK2, testGVK3})
			Expect(err).NotTo(HaveOccurred())

			// Verify Kind1 is gone, Kind2 and Kind3 are present
			server.mu.RLock()
			groupGVKs = server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(groupGVKs).NotTo(HaveKey(testGVK1), "Kind1 should be removed")
			Expect(groupGVKs).To(HaveKey(testGVK2), "Kind2 should still be present")
			Expect(groupGVKs).To(HaveKey(testGVK3), "Kind3 should be added")

			Eventually(func() bool {
				_, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
				if err != nil {
					return false
				}
				foundKind1 := false
				foundKind2 := false
				foundKind3 := false
				for _, resourceList := range apiResourceLists {
					if resourceList.GroupVersion == testGVK1.GroupVersion().String() {
						for _, resource := range resourceList.APIResources {
							if resource.Kind == testGVK1.Kind {
								foundKind1 = true
							}
							if resource.Kind == testGVK2.Kind {
								foundKind2 = true
							}
							if resource.Kind == testGVK3.Kind {
								foundKind3 = true
							}
						}
					}
				}
				return !foundKind1 && foundKind2 && foundKind3
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Multiple Groups", func() {
		It("should handle registration of multiple groups independently", func() {
			testGroup1 := viewv1a1.Group("test-multi1")
			testGroup2 := viewv1a1.Group("test-multi2")
			testGVK1 := viewv1a1.GroupVersionKind("test-multi1", "View1")
			testGVK2 := viewv1a1.GroupVersionKind("test-multi2", "View2")

			// Register both groups
			err := server.RegisterGVKs([]schema.GroupVersionKind{testGVK1, testGVK2})
			Expect(err).NotTo(HaveOccurred())

			// Verify both groups are registered
			server.mu.RLock()
			groupGVKs1, ok1 := server.groupGVKs[testGroup1]
			groupGVKs2, ok2 := server.groupGVKs[testGroup2]
			server.mu.RUnlock()

			Expect(ok1).To(BeTrue())
			Expect(ok2).To(BeTrue())
			Expect(groupGVKs1).To(HaveKey(testGVK1))
			Expect(groupGVKs2).To(HaveKey(testGVK2))

			// Verify both are discoverable
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(&rest.Config{
				Host: fmt.Sprintf("http://%s:%d", serverAddr, port),
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				apiGroups, _, err := discoveryClient.ServerGroupsAndResources()
				if err != nil || len(apiGroups) < 2 {
					return false
				}

				foundGroup1 := false
				foundGroup2 := false
				for _, group := range apiGroups {
					if group.Name == testGroup1 {
						foundGroup1 = true
					}
					if group.Name == testGroup2 {
						foundGroup2 = true
					}
				}
				return foundGroup1 && foundGroup2
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("UnregisterGVKs", func() {
		It("should unregister GVKs and remove them from discovery", func() {
			testGroup := viewv1a1.Group("test-unregister")
			testGVK1 := viewv1a1.GroupVersionKind("test-unregister", "Kind1")
			testGVK2 := viewv1a1.GroupVersionKind("test-unregister", "Kind2")

			// Register two GVKs
			err := server.RegisterGVKs([]schema.GroupVersionKind{testGVK1, testGVK2})
			Expect(err).NotTo(HaveOccurred())

			// Verify registration
			server.mu.RLock()
			groupGVKs, ok := server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testGVK1))
			Expect(groupGVKs).To(HaveKey(testGVK2))

			// Unregister the group
			server.UnregisterGVKs([]schema.GroupVersionKind{testGVK1, testGVK2})

			// Verify unregistration
			server.mu.RLock()
			_, ok = server.groupGVKs[testGroup]
			server.mu.RUnlock()
			Expect(ok).To(BeFalse())
		})
	})
})
