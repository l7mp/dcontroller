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
		mgr     runtimeManager.Manager
		server  *APIServer
		viewGVK schema.GroupVersionKind
		port    int
	)

	BeforeEach(func() {
		viewGVK = schema.GroupVersionKind{
			Group:   viewv1a1.GroupVersion.Group,
			Version: viewv1a1.GroupVersion.Version,
			Kind:    "TestView",
		}

		var err error
		mgr, err = manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		port = rand.IntN(5000) + (32768)
		server, err = NewAPIServer(mgr, "127.0.0.1", port)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("GVK Registration", func() {
		It("should register a new view GVK", func() {
			err := server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should faild for native GVK", func() {
			err := server.RegisterGVK(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})
			Expect(err).To(HaveOccurred())
		})

		It("should silently handle duplicate GVK registration", func() {
			err := server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())

			// Register again - should not error
			err = server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should unregister a GVK", func() {
			err := server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())

			err = server.UnregisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should silently handle unregistering non-existent GVK", func() {
			err := server.UnregisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle multiple GVK registrations", func() {
			gvks := []schema.GroupVersionKind{
				{Group: viewGVK.Group, Version: viewGVK.Version, Kind: "view1"},
				{Group: viewGVK.Group, Version: viewGVK.Version, Kind: "view2"},
				{Group: viewGVK.Group, Version: viewGVK.Version, Kind: "view3"},
			}

			for _, gvk := range gvks {
				err := server.RegisterGVK(gvk)
				Expect(err).NotTo(HaveOccurred())
			}

			// Unregister one
			err := server.UnregisterGVK(gvks[1])
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Lifecycle Management", func() {
		FIt("should start and shutdown gracefully", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Register a GVK first
			err := server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())

			// Start server in goroutine
			errChan := make(chan error, 1)
			go func() {
				defer close(errChan)
				errChan <- server.Start(ctx)
			}()

			// Give server a moment to start
			time.Sleep(20 * time.Millisecond)

			// Shutdown
			server.Shutdown()

			// Wait for start to return
			Eventually(errChan).Should(Receive(BeNil()))
		})

		It("should handle context cancellation", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())

			err = server.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle restart when already running", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := server.RegisterGVK(viewGVK)
			Expect(err).NotTo(HaveOccurred())

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
				s, err := NewAPIServer(mgr, tc.addr, 8080)
				Expect(err).NotTo(HaveOccurred())
				Expect(s).NotTo(BeNil())
			}
		})
	})
})
