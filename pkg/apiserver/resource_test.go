package apiserver

import (
	"context"
	"math/rand/v2"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/manager"
)

var _ = Describe("APIServerUnitTest", func() {
	var (
		mgr                       runtimeManager.Manager
		server                    *APIServer
		testGroup, testGroup2     string
		testViewGVK, test2ViewGVK schema.GroupVersionKind
		port                      int
	)

	BeforeEach(func() {
		testGroup = "test." + viewv1a1.GroupVersion.Group
		testViewGVK = schema.GroupVersionKind{
			Group:   testGroup,
			Version: viewv1a1.GroupVersion.Version,
			Kind:    "TestView",
		}
		testGroup2 = "test2." + viewv1a1.GroupVersion.Group
		test2ViewGVK = schema.GroupVersionKind{
			Group:   testGroup2,
			Version: viewv1a1.GroupVersion.Version,
			Kind:    "TestView",
		}

		var err error
		mgr, err = manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		port = rand.IntN(5000) + (32768)
		config, err := NewDefaultConfig("", port, true)
		Expect(err).NotTo(HaveOccurred())
		server, err = NewAPIServer(mgr, config)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("GVK Registration", func() {
		It("should register a view group", func() {
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			_, ok = server.groupGVKs[testGroup2]
			Expect(ok).To(BeFalse())
		})

		It("should register a native group", func() {
			pod := schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{pod})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(pod))
			Expect(groupGVKs[pod]).To(BeTrue())
		})

		It("should err for duplicate group-GVK registration", func() {
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			// Register again - should err
			err = server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).To(HaveOccurred())
		})

		It("should unregister a GVK", func() {
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			err = server.UnregisterAPIGroup(testGroup)
			Expect(err).NotTo(HaveOccurred())

			_, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeFalse())

			err = server.UnregisterAPIGroup(testGroup)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should silently handle unregistering non-existent group", func() {
			err := server.UnregisterAPIGroup("dummy")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle multiple GVK registrations", func() {
			view1 := schema.GroupVersionKind{Group: testViewGVK.Group, Version: testViewGVK.Version, Kind: "view1"}
			view2 := schema.GroupVersionKind{Group: testViewGVK.Group, Version: testViewGVK.Version, Kind: "view2"}
			view3 := schema.GroupVersionKind{Group: testViewGVK.Group, Version: testViewGVK.Version, Kind: "view3"}
			gvks := []schema.GroupVersionKind{view1, view2, view3}
			err := server.RegisterAPIGroup(testGroup, gvks)
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(view1))
			Expect(groupGVKs[view1]).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(view2))
			Expect(groupGVKs[view2]).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(view3))
			Expect(groupGVKs[view3]).To(BeTrue())

			// Unregister
			err = server.UnregisterAPIGroup(testGroup)
			Expect(err).NotTo(HaveOccurred())

			_, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeFalse())
		})
	})

	Describe("Lifecycle Management", func() {
		It("should start and shutdown gracefully", func() {
			ctx, cancel := context.WithCancel(context.Background())

			// Register a GVK first
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			// Start server in goroutine
			errChan := make(chan error, 1)
			go func() {
				defer close(errChan)
				errChan <- server.Start(ctx)
			}()

			// Give server a moment to start
			time.Sleep(20 * time.Millisecond)

			err = server.RegisterAPIGroup(testGroup2, []schema.GroupVersionKind{test2ViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			groupGVKs, ok = server.groupGVKs[testGroup2]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(test2ViewGVK))
			Expect(groupGVKs[test2ViewGVK]).To(BeTrue())

			// Shutdown
			cancel()

			// Wait for start to return
			Eventually(errChan).Should(Receive(BeNil()))
		})

		It("should handle context cancellation", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := server.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle restart when already running", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			// Start first time
			errChan1 := make(chan error, 1)
			go func() {
				errChan1 <- server.Start(ctx)
			}()

			time.Sleep(50 * time.Millisecond)

			// Start again (should shutdown first)
			errChan2 := make(chan error, 1)
			go func() {
				errChan2 <- server.Start(ctx)
			}()

			time.Sleep(50 * time.Millisecond)

			groupGVKs, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			// Cancel context to stop
			cancel()

			Eventually(errChan1).Should(Receive())
			Eventually(errChan2).Should(Receive())
		})
	})

	Describe("Server Configuration", func() {
		It("should handle empty GVK list", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			// Don't register any GVKs
			err := server.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle various address formats", func() {
			testCases := []struct {
				addr     string
				expected net.IP
			}{
				{"127.0.0.1", net.ParseIP("127.0.0.1")},
				{"localhost", net.ParseIP("127.0.0.1")}, // Should default
				{"", net.ParseIP("127.0.0.1")},          // Should default
				{"0.0.0.0", net.ParseIP("0.0.0.0")},
			}

			for _, tc := range testCases {
				config, err := NewDefaultConfig(tc.addr, 8080, true)
				Expect(err).NotTo(HaveOccurred())
				s, err := NewAPIServer(mgr, config)
				Expect(err).NotTo(HaveOccurred())
				Expect(s).NotTo(BeNil())
			}
		})
	})
})
