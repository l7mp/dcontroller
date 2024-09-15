package view

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	viewapiv1 "hsnlab/dcontroller-runtime/pkg/api/view/v1"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/manager"
	"hsnlab/dcontroller-runtime/pkg/object"
)

const (
	timeout  = time.Second * 1
	interval = time.Millisecond * 50
)

var watcher chan Request

type TestReconciler struct{}

func (r *TestReconciler) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	watcher <- req
	return reconcile.Result{}, nil
}

var _ = Describe("Reconciler", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		view, pod object.Object
	)

	BeforeEach(func() {
		pod = &unstructured.Unstructured{}
		pod.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Pod",
		})
		object.SetName(pod, "default", "podname")

		view = object.NewViewObject("view")
		object.SetName(view, "default", "viewname")

		ctx, cancel = context.WithCancel(context.Background())
		watcher = make(chan Request, 10)
	})

	AfterEach(func() {
		cancel()
		close(watcher)
	})

	Describe("with plain sources", func() {
		It("should be able to create a watch and emit events for view objects", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			s := Source{Kind: "view"}

			// Create controller
			c, err := controller.NewTyped("test-controller", mgr, controller.TypedOptions[Request]{
				Reconciler: &TestReconciler{},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := s.GetSource(mgr)
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			err = mgr.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Push a view object
			err = mgr.GetCompositeCache().GetViewCache().Add(view)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: cache.Added,
				GVK: schema.GroupVersionKind{
					Group:   viewapiv1.GroupVersion.Group,
					Version: viewapiv1.GroupVersion.Version,
					Kind:    "view",
				},
			}))
		})

		FIt("should be able to create a watch and emit events for native objects", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(ctx, logger, pod)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			group, version := "", "v1"
			s := Source{
				Group:   &group,
				Version: &version,
				Kind:    "view",
			}

			// Create controller
			c, err := controller.NewTyped("test-controller", mgr, controller.TypedOptions[Request]{
				Reconciler: &TestReconciler{},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := s.GetSource(mgr)
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			err = mgr.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: cache.Added,
				GVK: schema.GroupVersionKind{
					Group:   viewapiv1.GroupVersion.Group,
					Version: viewapiv1.GroupVersion.Version,
					Kind:    "view",
				},
			}))
		})
	})
})

func tryWatch(watcher chan Request, d time.Duration) (Request, bool) {
	select {
	case req := <-watcher:
		return req, true
	case <-time.After(d):
		return Request{}, false
	}
}
