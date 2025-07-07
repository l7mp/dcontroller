package reconciler

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeCtrl "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

const (
	timeout       = time.Second * 1
	interval      = time.Millisecond * 50
	retryInterval = time.Millisecond * 100
)

var (
	loglevel = -10
	// loglevel = -3
	logger = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(10),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
	log  = logger.WithName("test")
	podn = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod",
			Namespace: "testns",
		},
		Spec: corev1.PodSpec{
			Containers:    []corev1.Container{{Name: "nginx", Image: "nginx"}},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}
	watcher chan Request
)

func TestReconciler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller")
}

type testReconciler struct{ watcher chan Request }

func (r *testReconciler) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	log.V(4).Info("reconcile", "request", req)
	r.watcher <- req
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

		ctx, cancel = context.WithCancel(context.Background())
		watcher = make(chan Request, 10)
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
			s := opv1a1.Source{Resource: opv1a1.Resource{Kind: "view"}}

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Create controller
			on := true
			c, err := runtimeCtrl.NewTyped("test-controller", mgr, runtimeCtrl.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &testReconciler{watcher: watcher},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := NewSource(mgr, s).GetSource()
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Push a view object
			err = vcache.Add(oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok := tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: object.Added,
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
				EventType: object.Updated,
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
				EventType: object.Deleted,
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
			s := opv1a1.Source{
				Resource: opv1a1.Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
			}

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Create runtime controller
			on := true
			c, err := runtimeCtrl.NewTyped("test-controller", mgr, runtimeCtrl.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &testReconciler{watcher: watcher},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := NewSource(mgr, s).GetSource()
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Try to obtain the view from the watcher
			req, ok := tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "podname",
				EventType: object.Added,
				GVK: schema.GroupVersionKind{
					Group:   "",
					Version: "v1",
					Kind:    "Pod",
				},
			}))
		})

		It("should be able to watch and filter views by a predicate", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			s := opv1a1.Source{
				Resource: opv1a1.Resource{Kind: "view"},
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "test"},
				},
			}
			on := true
			c, err := runtimeCtrl.NewTyped("test-controller-labeled", mgr, runtimeCtrl.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &testReconciler{watcher: watcher},
			})

			Expect(err).NotTo(HaveOccurred())
			src, err := NewSource(mgr, s).GetSource()
			Expect(err).NotTo(HaveOccurred())
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Push an unlabeled view object
			err = vcache.Add(oldObj)
			Expect(err).NotTo(HaveOccurred())

			_, ok := tryWatchReq(watcher, 5*interval)
			Expect(ok).To(BeFalse())

			// Get the object and check
			res := object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(oldObj), res)).NotTo(HaveOccurred())
			Expect(res.GetLabels()).To(BeNil())

			// Add the label and update
			newObj := object.DeepCopy(oldObj)
			newObj.SetLabels(map[string]string{"app": "test"})
			err = vcache.Update(oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())

			req, ok := tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: object.Updated,
				GVK: schema.GroupVersionKind{
					Group:   viewv1a1.GroupVersion.Group,
					Version: viewv1a1.GroupVersion.Version,
					Kind:    "view",
				},
			}))

			// Get the object and check
			res = object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(oldObj), res)).NotTo(HaveOccurred())
			Expect(res.GetLabels()).To(Equal(map[string]string{"app": "test"}))

			// Remove the label from the view object
			newObj = object.DeepCopy(oldObj)
			newObj.SetLabels(map[string]string{})
			err = vcache.Update(oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			_, ok = tryWatchReq(watcher, interval)
			Expect(ok).To(BeFalse())

			// Get the object and check
			res = object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(oldObj), res)).NotTo(HaveOccurred())
			Expect(res.GetLabels()).To(Equal(map[string]string{})) // we have just added a zero value

			// Restore the label and change the content
			newObj = object.DeepCopy(oldObj)
			newObj.SetLabels(map[string]string{"app": "test"})
			object.SetContent(view, map[string]any{"a": int64(1)})
			err = vcache.Update(oldObj, newObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok = tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: object.Updated,
				GVK: schema.GroupVersionKind{
					Group:   viewv1a1.GroupVersion.Group,
					Version: viewv1a1.GroupVersion.Version,
					Kind:    "view",
				},
			}))

			// Get the object and check
			res = object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(oldObj), res)).NotTo(HaveOccurred())
			Expect(res.GetLabels()).To(Equal(map[string]string{"app": "test"}))

			// Delete the view object
			err = vcache.Delete(oldObj)
			Expect(err).NotTo(HaveOccurred())

			// Try to obtain the view from the watcher
			req, ok = tryWatchReq(watcher, 50*interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "viewname",
				EventType: object.Deleted,
				GVK: schema.GroupVersionKind{
					Group:   viewv1a1.GroupVersion.Group,
					Version: viewv1a1.GroupVersion.Version,
					Kind:    "view",
				},
			}))
		})

		It("should get a watch event on a controller using a labeled watch for a labeled native object", func() {
			podL := pod.DeepCopy()
			podL.SetLabels(map[string]string{"app": "test"})
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger}, podL)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			group, version := "", "v1"
			s := opv1a1.Source{
				Resource: opv1a1.Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "test"},
				},
			}

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Create runtime controller
			on := true
			c, err := runtimeCtrl.NewTyped("test-controller", mgr, runtimeCtrl.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &testReconciler{watcher: watcher},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := NewSource(mgr, s).GetSource()
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Try to obtain the view from the watcher
			req, ok := tryWatchReq(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(req).To(Equal(Request{
				Namespace: "default",
				Name:      "podname",
				EventType: object.Added,
				GVK: schema.GroupVersionKind{
					Group:   "",
					Version: "v1",
					Kind:    "Pod",
				},
			}))
		})

		It("should suppress watch events on a controller using a labeled watch for an un labeled native object", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger}, pod)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			group, version := "", "v1"
			s := opv1a1.Source{
				Resource: opv1a1.Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "test"},
				},
			}

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Create runtime controller
			on := true
			c, err := runtimeCtrl.NewTyped("test-controller", mgr, runtimeCtrl.TypedOptions[Request]{
				SkipNameValidation: &on,
				Reconciler:         &testReconciler{watcher: watcher},
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a source
			src, err := NewSource(mgr, s).GetSource()
			Expect(err).NotTo(HaveOccurred())

			// Watch the source
			err = c.Watch(src)
			Expect(err).NotTo(HaveOccurred())

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Try to obtain the view from the watcher
			_, ok := tryWatchReq(watcher, 10*interval)
			Expect(ok).To(BeFalse())
		})
	})

	Describe("with targets", func() {
		It("should be able to write view objects to Updater targets", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register source
			target := NewTarget(mgr, opv1a1.Target{Resource: opv1a1.Resource{Kind: "view"}})

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Push a view object to the target
			err = target.Write(ctx, object.Delta{Type: object.Added, Object: view})
			Expect(err).NotTo(HaveOccurred())

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(view, event.Object.(object.Object))).To(BeTrue())

			// Push an update to the target
			view2 := object.DeepCopy(view)
			object.SetContent(view2, map[string]any{"b": int64(2)})

			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			res := view.DeepCopy()
			object.SetContent(res, map[string]any{"b": int64(2)})
			Expect(object.DeepEqual(res, event.Object.(object.Object))).To(BeTrue())
			// Expect(res).To(Equal(event.Object.(object.Object)))

			// Push a delete to the target
			err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: view2})
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

			// Register an updater target
			group, version := "", "v1"
			target := NewTarget(mgr, opv1a1.Target{
				Resource: opv1a1.Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
			})

			// Push a view object to the target
			err = target.Write(ctx, object.Delta{Type: object.Added, Object: pod2})
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

			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: newPod})
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
			Expect(p.Spec.Containers).To(BeEmpty())
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("Always")))

			// Set the status
			unstructured.SetNestedField(newPod.UnstructuredContent(), map[string]any{
				"message": "testmessage",
				"reason":  "testreason",
			}, "status")

			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: newPod})
			Expect(err).NotTo(HaveOccurred())

			getFromTracker, err = tracker.Get(gvr, "testns", "testpod")
			Expect(err).NotTo(HaveOccurred())
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
			Expect(p.Spec.Containers).To(BeEmpty())
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("Always")))
			Expect(p.Status).To(Equal(corev1.PodStatus{
				Message: "testmessage",
				Reason:  "testreason",
			}))

			// Delete from the target
			err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: newPod})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should be able to write view objects to Patch targets", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register target
			target := NewTarget(mgr, opv1a1.Target{Resource: opv1a1.Resource{Kind: "view"}, Type: "Patcher"})

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Write object into the cache (otherwise we cannot patch it later)
			err = vcache.Add(view)
			Expect(err).NotTo(HaveOccurred())

			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(view, event.Object.(object.Object))).To(BeTrue())

			// Push an update to the target
			view2 := object.DeepCopy(view)
			object.SetContent(view2, map[string]any{"b": int64(2)})

			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			res := view.DeepCopy()
			object.SetContent(res, map[string]any{"a": int64(1), "b": int64(2)})
			Expect(event.Object.(object.Object)).To(Equal(res))

			// TODO this fails since status updates need a working client with a functional Get...
			// // Update the status
			// view3 := object.DeepCopy(view2)
			// Expect(unstructured.SetNestedField(view3.UnstructuredContent(),
			// 	map[string]any{"b": int64(2)}, "status")).NotTo(HaveOccurred())
			// err = target.Write(ctx, object.Delta{Type: object.Updated, Object: view3})
			// Expect(err).NotTo(HaveOccurred())

			// event, ok = tryWatchWatcher(watcher, interval)
			// Expect(ok).To(BeTrue())
			// Expect(event.Type).To(Equal(watch.Modified))

			// // Updater patches!
			// res = view.DeepCopy()
			// object.SetContent(res, map[string]any{"a": int64(1), "b": int64(2)})
			// Expect(unstructured.SetNestedField(res.UnstructuredContent(),
			// 	map[string]any{"b": int64(2)}, "status")).NotTo(HaveOccurred())
			// Expect(event.Object.(object.Object)).To(Equal(res))

			// Push a delete to the target
			view4 := object.DeepCopy(view)
			object.SetContent(view4, map[string]any{"a": int64(1)})
			// TODO: this will work in a patch but how do do this from a pipeline???
			Expect(unstructured.SetNestedField(view4.UnstructuredContent(), nil, "status")).NotTo(HaveOccurred())
			err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: view4})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			res = view.DeepCopy()
			object.SetName(res, "default", "viewname")
			object.SetContent(res, map[string]any{"b": int64(2)})
			Expect(event.Object).To(Equal(res))

			// Get should not fail now
			res2 := object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(view), res2)).NotTo(HaveOccurred())
			Expect(*res).To(Equal(*res2))
		})

		It("should be able to write a new status into view objects via Patch targets", func() {
			// Start manager and push a native object into the runtime client fake
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register target
			target := NewTarget(mgr, opv1a1.Target{Resource: opv1a1.Resource{Kind: "view"}, Type: "Patcher"})

			// Start the manager
			go func() { mgr.Start(ctx) }()

			// Get view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			Expect(vcache).NotTo(BeNil())

			// Write object into the cache (otherwise we cannot patch it later)
			err = vcache.Add(view)
			Expect(err).NotTo(HaveOccurred())

			watcher, err := vcache.Watch(ctx, composite.NewViewObjectList("view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			// Expect(object.DeepEqual(view, event.Object.(object.Object))).To(BeTrue())
			Expect(event.Object).To(Equal(view))

			// Update the status
			view2 := object.DeepCopy(view)
			Expect(unstructured.SetNestedField(view2.UnstructuredContent(),
				map[string]any{"ready": "true"}, "status")).NotTo(HaveOccurred())
			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: view2})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))

			retrieved := view.DeepCopy()
			object.SetContent(retrieved, map[string]any{"a": int64(1)})
			Expect(unstructured.SetNestedField(retrieved.UnstructuredContent(),
				map[string]any{"ready": "true"}, "status")).NotTo(HaveOccurred())
			Expect(event.Object).To(Equal(retrieved))

			// Push a delete to the target
			view3 := object.NewViewObject("view")
			Expect(unstructured.SetNestedField(view3.UnstructuredContent(), "", "status")).NotTo(HaveOccurred())
			object.SetName(view3, "default", "viewname")
			err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: view3})
			Expect(err).NotTo(HaveOccurred())

			event, ok = tryWatchWatcher(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			retrieved = view.DeepCopy()
			object.SetName(retrieved, "default", "viewname")
			object.SetContent(retrieved, map[string]any{"a": int64(1)})
			Expect(event.Object).To(Equal(retrieved))

			// Get should not fail now
			res2 := object.NewViewObject("view")
			Expect(vcache.Get(ctx, client.ObjectKeyFromObject(view), res2)).NotTo(HaveOccurred())
			Expect(*retrieved).To(Equal(*res2))
		})

		It("should be able to write native objects to Patcher targets", func() {
			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger}, pod2)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			// Register target
			group, version := "", "v1"
			target := NewTarget(mgr, opv1a1.Target{
				Resource: opv1a1.Resource{
					Group:   &group,
					Version: &version,
					Kind:    "Pod",
				},
				Type: "Patcher",
			})

			// Object should already be registered in the runtime cache client
			c := mgr.GetClient()
			get := object.DeepCopy(pod2)
			err = c.Get(ctx, client.ObjectKeyFromObject(pod2), get)
			Expect(err).NotTo(HaveOccurred())

			// Patch to the target
			newPod := object.DeepCopy(pod2)
			unstructured.RemoveNestedField(newPod.UnstructuredContent(), "spec", "containers")
			unstructured.SetNestedField(newPod.UnstructuredContent(), "Always", "spec", "restartPolicy")
			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: newPod})
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

			// TODO Delete patch needs more work!
			// // Delete patch to the target
			// newPod = object.DeepCopy(pod2)
			// // remove content and restore namespace/name
			// object.SetContent(newPod, map[string]any{})
			// newPod.SetName("testpod")
			// newPod.SetNamespace("testns")
			// unstructured.SetNestedField(newPod.UnstructuredContent(), nil, "spec", "containers")
			// err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: newPod})
			// Expect(err).NotTo(HaveOccurred())
			// getFromTracker, err = tracker.Get(gvr, "testns", "testpod")
			// Expect(err).NotTo(HaveOccurred())
			// // no way to deep-equal: the tracker returns a native Pod object (not unstructured)
			// Expect(getFromTracker.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
			// 	Group:   "",
			// 	Version: "v1",
			// 	Kind:    "Pod",
			// }))
			// getFromClient, err = object.ConvertRuntimeObjectToClientObject(getFromTracker)
			// Expect(err).NotTo(HaveOccurred())
			// Expect(getFromClient.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
			// 	Group:   "",
			// 	Version: "v1",
			// 	Kind:    "Pod",
			// }))
			// p = getFromClient.(*corev1.Pod)
			// Expect(p.GetName()).To(Equal("testpod"))
			// Expect(p.GetNamespace()).To(Equal("testns"))
			// // updates leaves existing fields around
			// Expect(p.Spec.Containers).To(HaveLen(1))
			// Expect(p.Spec.Containers[0].Name).To(Equal("nginx"))
			// Expect(p.Spec.Containers[0].Image).To(Equal("nginx"))
			// Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicy("")))
		})
	})
})

func tryWatchReq(watcher chan Request, d time.Duration) (Request, bool) {
	select {
	case req := <-watcher:
		return req, true
	case <-time.After(d):
		return Request{}, false
	}
}

func tryWatchWatcher(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}
