package apiserver

import (
	"context"
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime/pkg/log"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/manager"
)

var _ = Describe("APIServerUnitTest", func() {
	var (
		mgr                       runtimeManager.Manager
		server                    *APIServer
		testGroup, test2Group     string
		testViewGVK, test2ViewGVK schema.GroupVersionKind
		serverCtx                 context.Context
		serverCancel              context.CancelFunc
		serverAddr                string
		port                      int
	)

	BeforeEach(func() {
		testGroup = viewv1a1.Group("test")
		testViewGVK = viewv1a1.GroupVersionKind("test", "TestView")
		test2Group = viewv1a1.Group("test2")
		test2ViewGVK = viewv1a1.GroupVersionKind("test2", "TestView")

		serverCtx, serverCancel = context.WithCancel(context.Background())

		// Create mock manager
		var err error
		mgr, err = manager.NewFakeManager(runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		// Set API server logger
		klog.SetLogger(logger.V(10).WithName("generic-apiserver"))
		// klog.V(10).Enabled() // This forces klog to think level 10 is enabled

		// Set the controller-runtime log for maximum verbosity
		ctrl.SetLogger(logger)

		// Create API server at random port
		serverAddr = "localhost"
		port = rand.IntN(15000) + 32768 //nolint:gosec
		config, err := NewDefaultConfig(serverAddr, port, true)
		Expect(err).NotTo(HaveOccurred())
		server, err = NewAPIServer(mgr, config)
		Expect(err).NotTo(HaveOccurred())

		// Start the server
		go func() {
			defer GinkgoRecover()
			err := server.Start(serverCtx)
			Expect(err).NotTo(HaveOccurred())
		}()

		// Wait for the API server to start
		Eventually(func() bool { return server.running }, timeout, interval).Should(BeTrue())
	})

	AfterEach(func() {
		serverCancel()
		time.Sleep(20 * time.Millisecond) // Give server time to shutdown
	})

	Describe("GVK Registration", func() {
		It("should register a view group", func() {
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			_, ok = server.groupGVKs[test2Group]
			Expect(ok).To(BeFalse())
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

			server.UnregisterAPIGroup(testGroup)
			_, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeFalse())

			server.UnregisterAPIGroup(testGroup)
			_, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeFalse())
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
			server.UnregisterAPIGroup(testGroup)
			_, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeFalse())
		})

		It("should register a multiple view groups from different operators", func() {
			err := server.RegisterAPIGroup(testGroup, []schema.GroupVersionKind{testViewGVK})
			Expect(err).NotTo(HaveOccurred())

			err = server.RegisterAPIGroup(test2Group, []schema.GroupVersionKind{test2ViewGVK})
			Expect(err).NotTo(HaveOccurred())

			groupGVKs, ok := server.groupGVKs[testGroup]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(testViewGVK))
			Expect(groupGVKs[testViewGVK]).To(BeTrue())

			groupGVKs, ok = server.groupGVKs[test2Group]
			Expect(ok).To(BeTrue())
			Expect(groupGVKs).To(HaveKey(test2ViewGVK))
			Expect(groupGVKs[test2ViewGVK]).To(BeTrue())

			server.UnregisterAPIGroup(testGroup)
			_, ok = server.groupGVKs[testGroup]
			Expect(ok).To(BeFalse())

			server.UnregisterAPIGroup(test2Group)
			_, ok = server.groupGVKs[test2Group]
			Expect(ok).To(BeFalse())
		})
	})
})
