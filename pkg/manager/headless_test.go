package manager

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("Headless Mode", func() {
	var (
		pod    *unstructured.Unstructured
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		pod = &unstructured.Unstructured{}
		content, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
		pod.SetUnstructuredContent(content)
		pod.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Pod",
		})
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
	})

	It("should create a Manager", func() {
		mgr, err := New(nil, "test", Options{
			Options: manager.Options{Logger: logger},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go mgr.Start(ctx)

		cache := mgr.GetCache()
		Expect(cache).Should(BeAssignableToTypeOf(&composite.ViewCache{}))
		cc := cache.(*composite.ViewCache)
		Expect(cc).NotTo(BeNil())

		c := mgr.GetClient()
		Expect(c).NotTo(BeNil())
	})

	It("should add and retrieve a view object", func() {
		mgr, err := New(nil, "test", Options{
			Options: manager.Options{Logger: logger},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go mgr.Start(ctx)

		obj := object.NewViewObject("test", "view")
		object.SetName(obj, "test-ns", "test-obj")
		object.SetContent(obj, map[string]any{"x": "y"})
		object.WithUID(obj)

		// client should allow the viewcache to be directly updated
		c := mgr.GetClient()
		Expect(c).NotTo(BeNil())
		err = c.Create(ctx, obj)
		Expect(err).NotTo(HaveOccurred())

		retrieved := object.DeepCopy(obj)
		err = c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved).To(Equal(obj))
		Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
	})

	It("should patch a view object", func() {
		mgr, err := New(nil, "test", Options{
			Options: manager.Options{Logger: logger},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go mgr.Start(ctx)

		obj := object.NewViewObject("test", "view")
		object.SetContent(obj, map[string]any{"a": int64(1)})
		object.SetName(obj, "ns", "test-1")

		c := mgr.GetClient()
		Expect(c).NotTo(BeNil())
		err = c.Create(ctx, obj)
		Expect(err).NotTo(HaveOccurred())

		retrieved := object.NewViewObject("test", "view")
		object.SetContent(retrieved, map[string]any{"a": int64(2)})
		object.SetName(retrieved, "ns", "test-1")
		object.WithUID(retrieved)

		patch, err := json.Marshal(object.DeepCopy(retrieved).UnstructuredContent())
		Expect(err).NotTo(HaveOccurred())

		// reset obj
		obj = object.NewViewObject("test", "view")
		object.SetName(obj, "ns", "test-1")
		err = c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
		Expect(err).NotTo(HaveOccurred())

		// must Get the new new content (Patch does not update object)
		err = c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj).To(Equal(retrieved))
	})

	It("should watch a view object", func() {
		mgr, err := New(nil, "test", Options{
			Options: manager.Options{Logger: logger},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr).NotTo(BeNil())

		go mgr.Start(ctx)

		gvk := viewv1a1.GroupVersionKind("test", "view")
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)

		c, ok := mgr.GetClient().(client.WithWatch)
		Expect(ok).To(BeTrue())
		Expect(c).NotTo(BeNil())

		watcher, err := c.Watch(ctx, list)
		Expect(err).NotTo(HaveOccurred())

		obj := object.NewViewObject("test", "view")
		object.SetName(obj, "test-ns", "test-obj")
		object.SetContent(obj, map[string]any{"x": "y"})
		object.WithUID(obj)

		err = c.Create(ctx, obj)
		Expect(err).NotTo(HaveOccurred())

		event, ok := tryWatch(watcher, interval)
		Expect(ok).To(BeTrue())
		Expect(event.Type).To(Equal(watch.Added))
		Expect(event.Object.GetObjectKind().GroupVersionKind()).To(Equal(gvk))
		Expect(event.Object).To(Equal(obj))
	})
})
