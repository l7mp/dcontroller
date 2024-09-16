package view

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	viewv1a1 "hsnlab/dcontroller-runtime/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/manager"
	"hsnlab/dcontroller-runtime/pkg/object"
)

var watcher chan Request

type TestReconciler struct{}

func (r *TestReconciler) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	logger.V(4).Info("reconcile", "request", req)
	watcher <- req
	return reconcile.Result{}, nil
}

var _ = Describe("Reconciler", func() {
	var (
		ctx                     context.Context
		cancel                  context.CancelFunc
		view, oldObj, pod, pod2 object.Object
	)

	BeforeEach(func() {
		pod = &unstructured.Unstructured{}
		pod.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Pod",
		})
		object.SetName(pod, "default", "podname")

		oldObj = object.NewViewObject("view")
		object.SetName(oldObj, "default", "viewname")

		ctx, cancel = context.WithCancel(context.Background())
		watcher = make(chan Request, 10)

		view = object.NewViewObject("view")
		object.SetName(view, "default", "viewname")
		object.SetContent(view, map[string]any{"a": int64(1)})

		pod2 = &unstructured.Unstructured{}
		content, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
		pod2.SetUnstructuredContent(content)
		pod2.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Pod",
		})
	})

	AfterEach(func() {
		cancel()
		close(watcher)
	})

	Describe("with sources", func() {
		It("should be able to create a watch and emit events for view objects", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			s := Source{Resource: Resource{Kind: "view"}}

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Create controller
			on := true
			c, err := controller.NewTyped("test-controller", mgr, controller.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &TestReconciler{},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := s.GetSource(mgr)
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			// go mgr.Start(ctx) // will stop with a context cancelled erro
			go func() { mgr.Start(ctx) }() // will stop with a context cancelled erro

			// Push a view object
			err = vcache.Add(oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok := tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: cache.Added,
				GVK: schema.GroupVersionKind{
					Group:   viewv1a1.GroupVersion.Group,
					Version: viewv1a1.GroupVersion.Version,
					Kind:    "view",
				},
			}))

			// Modify the view object
			newObj := object.DeepCopy(oldObj)
			object.SetContent(newObj, map[string]any{"a": "b"})
			err = vcache.Update(oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok = tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: cache.Updated,
				GVK: schema.GroupVersionKind{
					Group:   viewv1a1.GroupVersion.Group,
					Version: viewv1a1.GroupVersion.Version,
					Kind:    "view",
				},
			}))

			// Delete the view object
			err = vcache.Delete(oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok = tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: cache.Deleted,
				GVK: schema.GroupVersionKind{
					Group:   viewv1a1.GroupVersion.Group,
					Version: viewv1a1.GroupVersion.Version,
					Kind:    "view",
				},
			}))
		})

		It("should be able to create a watch and emit events for native objects", func() {
			// Cannot add/remove native objects due to the limitations of the fake
			// client: use an initial object list only
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger}, pod)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Get initial object: just to make sure
			getObj := &unstructured.Unstructured{}
			getObj.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})

			// From the fake runtime client
			Expect(mgr.GetRuntimeClient().Get(ctx, types.NamespacedName{
				Namespace: "default", Name: "podname"}, getObj)).NotTo(HaveOccurred())
			Expect(getObj.UnstructuredContent()).To(Equal(map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"namespace":         "default",
					"name":              "podname",
					"resourceVersion":   "999",
					"creationTimestamp": nil,
				},
				"spec":   map[string]any{"containers": nil},
				"status": map[string]any{},
			}))

			// from the default cache
			Expect(mgr.GetClient().Get(ctx, types.NamespacedName{
				Namespace: "default", Name: "podname"}, getObj)).NotTo(HaveOccurred())
			// content is different this time: the object never goes through the fake
			// runtime cache so it does not get the defaults added
			Expect(getObj.UnstructuredContent()).To(Equal(map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"namespace": "default",
					"name":      "podname",
				},
			}))

			// Register source
			group, version := "", "v1"
			s := Source{Resource: Resource{
				Group:   &group,
				Version: &version,
				Kind:    "Pod",
			}}

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Create controller
			on := true
			c, err := controller.NewTyped("test-controller", mgr, controller.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &TestReconciler{},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := s.GetSource(mgr)
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			go func() { mgr.Start(ctx) }() // will stop with a context cancelled erro

			// Try to obtain the view from the watcher
			req, ok := tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "podname",
				EventType: cache.Added,
				GVK: schema.GroupVersionKind{
					Group:   "",
					Version: "v1",
					Kind:    "Pod",
				},
			}))
		})
	})

	Describe("with targets", func() {
		It("should be able to write view objects to Updater targets", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			target := Target{Resource: Resource{Kind: "view"}}

			// Start the manager
			// go mgr.Start(ctx) // will stop with a context cancelled erro
			go func() { mgr.Start(ctx) }() // will stop with a context cancelled erro

			// Push a view object to the target
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Added, Object: view})
			Expect(err).NotTo(HaveOccurred())

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			watcher, err := vcache.Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(view, event.Object.(object.Object))).To(BeTrue())

			// Push an update to the target
			view2 := object.DeepCopy(view)
			object.SetContent(view2, map[string]any{"b": int64(2)})

			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Updated, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.EventType("MODIFIED")))
			res := view.DeepCopy()
			// Updater rewrites, not patches!
			// object.SetContent(view2, map[string]any{"a": int64(1), "b": int64(2)})
			object.SetContent(res, map[string]any{"b": int64(2)})
			Expect(object.DeepEqual(res, event.Object.(object.Object))).To(BeTrue())

			// Push a delete to the target
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Deleted, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Deleted))
			Expect(object.DeepEqual(res, event.Object.(object.Object))).To(BeTrue())

			// Get should fail now
			res = object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(view), res)).To(HaveOccurred())
		})

		It("should be able to write native objects to Updater targets", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register target
			group, version := "", "v1"
			target := Target{Resource: Resource{
				Group:   &group,
				Version: &version,
				Kind:    "Pod",
			}}

			// Push a view object to the target
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Added, Object: pod2})
			Expect(err).NotTo(HaveOccurred())

			// Get from the object tracker: a normal get would go through the cache
			// first get should fail
			tracker := mgr.GetObjectTracker()
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods", // Resource does not equal Kind!
			}
			getFromTracker, err := tracker.Get(gvr, "testns", "testpod")
			Expect(err).NotTo(HaveOccurred())
			// no way to deep-equal: the tracker returns a native Pod object (not unstructured)
			Expect(getFromTracker.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			// make a Get so that we get a full object (tracker returns only a runtime.Object)

			// getFromClient := &corev1.Pod{}
			// Expect(c.Get(ctx, client.ObjectKeyFromObject(pod), getFromClient)).NotTo(HaveOccurred())
			getFromClient, err := object.ConvertRuntimeObjectToClientObject(getFromTracker)
			Expect(err).NotTo(HaveOccurred())
			Expect(getFromClient.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			p := getFromClient.(*corev1.Pod)
			Expect(p.GetName()).To(Equal("testpod"))
			Expect(p.GetNamespace()).To(Equal("testns"))
			Expect(p.Spec.Containers).To(HaveLen(1))
			Expect(p.Spec.Containers[0].Name).To(Equal("nginx"))
			Expect(p.Spec.Containers[0].Image).To(Equal("nginx"))
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("OnFailure")))

			// Push update to the target
			newPod := object.DeepCopy(pod2)
			unstructured.RemoveNestedField(newPod.UnstructuredContent(), "spec", "containers")
			unstructured.SetNestedField(newPod.UnstructuredContent(), "Always", "spec", "restartPolicy")
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Updated, Object: newPod})
			Expect(err).NotTo(HaveOccurred())

			getFromTracker, err = tracker.Get(gvr, "testns", "testpod")
			Expect(err).NotTo(HaveOccurred())
			// no way to deep-equal: the tracker returns a native Pod object (not unstructured)
			Expect(getFromTracker.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			getFromClient, err = object.ConvertRuntimeObjectToClientObject(getFromTracker)
			Expect(err).NotTo(HaveOccurred())
			Expect(getFromClient.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			p = getFromClient.(*corev1.Pod)
			Expect(p.GetName()).To(Equal("testpod"))
			Expect(p.GetNamespace()).To(Equal("testns"))
			Expect(p.Spec.Containers).To(HaveLen(0))
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("Always")))

			// Delete from the target
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Deleted, Object: newPod})
			Expect(err).NotTo(HaveOccurred())
			getFromTracker, err = tracker.Get(gvr, "testns", "testpod")
			Expect(err).To(HaveOccurred())
		})

		It("should be able to write view objects to Patch targets", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			target := Target{Resource: Resource{Kind: "view"}, Type: "Patcher"}

			// Start the manager
			// go mgr.Start(ctx) // will stop with a context cancelled erro
			go func() { mgr.Start(ctx) }() // will stop with a context cancelled erro

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Write object into the cache (otherwise we cannot patch it later)
			err = vcache.Add(view)
			Expect(err).NotTo(HaveOccurred())

			watcher, err := vcache.Watch(ctx, object.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(view, event.Object.(object.Object))).To(BeTrue())

			// Push an update to the target
			view2 := object.DeepCopy(view)
			object.SetContent(view2, map[string]any{"b": int64(2)})

			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Updated, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.EventType("MODIFIED")))
			res := view.DeepCopy()
			// Updater patches!
			object.SetContent(res, map[string]any{"a": int64(1), "b": int64(2)})
			// object.SetContent(res, map[string]any{"b": int64(2)})
			Expect(event.Object.(object.Object)).To(Equal(res))

			// Push a delete to the target
			view2 = object.DeepCopy(view)
			object.SetContent(view2, map[string]any{"a": int64(1)})
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Deleted, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.EventType("MODIFIED")))
			res = view.DeepCopy()
			object.SetName(res, "default", "viewname")
			object.SetContent(res, map[string]any{"b": int64(2)})
			Expect(event.Object).To(Equal(res))

			// Get should not fail now
			res2 := object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(view), res2)).NotTo(HaveOccurred())
			Expect(res).To(Equal(res2))
		})

		It("should be able to write native objects to Patcher targets", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger}, pod2)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register target
			group, version := "", "v1"
			target := Target{Resource: Resource{
				Group:   &group,
				Version: &version,
				Kind:    "Pod",
			}, Type: "Patcher"}

			// Object should already be registered in the runtime cache client
			c := mgr.GetClient()
			get := object.DeepCopy(pod2)
			err = c.Get(ctx, client.ObjectKeyFromObject(pod2), get)
			Expect(err).NotTo(HaveOccurred())

			// Patch to the target
			newPod := object.DeepCopy(pod2)
			unstructured.RemoveNestedField(newPod.UnstructuredContent(), "spec", "containers")
			unstructured.SetNestedField(newPod.UnstructuredContent(), "Always", "spec", "restartPolicy")
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Updated, Object: newPod})
			Expect(err).NotTo(HaveOccurred())

			tracker := mgr.GetObjectTracker()
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods", // Resource does not equal Kind!
			}
			getFromTracker, err := tracker.Get(gvr, "testns", "testpod")
			Expect(err).NotTo(HaveOccurred())
			// no way to deep-equal: the tracker returns a native Pod object (not unstructured)
			Expect(getFromTracker.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			getFromClient, err := object.ConvertRuntimeObjectToClientObject(getFromTracker)
			Expect(err).NotTo(HaveOccurred())
			Expect(getFromClient.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			p := getFromClient.(*corev1.Pod)
			Expect(p.GetName()).To(Equal("testpod"))
			Expect(p.GetNamespace()).To(Equal("testns"))
			// updates leaves existing fields around
			Expect(p.Spec.Containers).To(HaveLen(1))
			Expect(p.Spec.Containers[0].Name).To(Equal("nginx"))
			Expect(p.Spec.Containers[0].Image).To(Equal("nginx"))
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("Always")))

			// Delete patch to the target
			newPod = object.DeepCopy(pod2)
			unstructured.SetNestedField(newPod.UnstructuredContent(), nil, "spec", "restartPolicy")
			fmt.Println(newPod)
			err = target.Write(ctx, mgr, cache.Delta{Type: cache.Deleted, Object: newPod})
			Expect(err).NotTo(HaveOccurred())
			getFromTracker, err = tracker.Get(gvr, "testns", "testpod")
			Expect(err).NotTo(HaveOccurred())
			// no way to deep-equal: the tracker returns a native Pod object (not unstructured)
			Expect(getFromTracker.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			getFromClient, err = object.ConvertRuntimeObjectToClientObject(getFromTracker)
			Expect(err).NotTo(HaveOccurred())
			Expect(getFromClient.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			p = getFromClient.(*corev1.Pod)
			Expect(p.GetName()).To(Equal("testpod"))
			Expect(p.GetNamespace()).To(Equal("testns"))
			// updates leaves existing fields around
			Expect(p.Spec.Containers).To(HaveLen(1))
			Expect(p.Spec.Containers[0].Name).To(Equal("nginx"))
			Expect(p.Spec.Containers[0].Image).To(Equal("nginx"))
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("")))
		})
	})
})
