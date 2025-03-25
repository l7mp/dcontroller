package manager

import (
	"context"
	"encoding/json"
	"reflect"
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
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	ccache "github.com/hsnlab/dcontroller/pkg/cache"
	"github.com/hsnlab/dcontroller/pkg/object"
)

const (
	timeout  = time.Second * 1
	interval = time.Millisecond * 50
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
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
)

func TestManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Manager")
}

var _ = Describe("Startup", func() {
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

	Describe("Basics", func() {
		It("Manager created", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go mgr.Start(ctx)

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			cc := cache.(*ccache.CompositeCache)
			Expect(cc).NotTo(BeNil())
			Expect(cc.GetViewCache()).Should(BeAssignableToTypeOf(&ccache.ViewCache{}))
			Expect(cc.GetViewCache()).NotTo(BeNil())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())
			Expect(c).Should(BeAssignableToTypeOf(&compositeClient{}))

		})
	})

	Describe("Cache operation", func() {
		It("should retrieve a native object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			// pod added
			Expect(mgr.GetRuntimeCache().Add(pod)).NotTo(HaveOccurred())

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))

			obj := &unstructured.Unstructured{}
			obj.GetObjectKind().SetGroupVersionKind(pod.GetObjectKind().GroupVersionKind())
			err = cache.Get(ctx, client.ObjectKeyFromObject(pod), obj)
			Expect(err).NotTo(HaveOccurred())

			content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
			Expect(err).NotTo(HaveOccurred())
			podu := &unstructured.Unstructured{}
			podu.SetUnstructuredContent(content)
			// for some reason, gvk is not copied
			podu.GetObjectKind().SetGroupVersionKind(pod.GetObjectKind().GroupVersionKind())
			Expect(reflect.DeepEqual(podu, obj)).To(BeTrue())
		})

		It("should retrieve an added view object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			obj := object.NewViewObject("view")
			object.SetName(obj, "test-ns", "test-obj")
			object.SetContent(obj, map[string]any{"x": "y"})

			// must be added via the view-cache: the default client.Add would go to the fake client
			err = ccache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(obj)
			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved).To(Equal(obj))
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})
	})

	Describe("Client operation", func() {
		It("should retrieve a native object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			// pod added
			Expect(mgr.GetRuntimeCache().Add(pod)).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})
			err = c.Get(ctx, client.ObjectKeyFromObject(pod), obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj).To(Equal(pod))
			Expect(reflect.DeepEqual(pod, obj)).To(BeTrue())
		})

		It("should retrieve an added view object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			obj := object.NewViewObject("view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-1")

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			err = ccache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			retrieved := object.NewViewObject("view")
			object.SetName(retrieved, "ns", "test-1")

			err = c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should patch a view object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			obj := object.NewViewObject("view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-1")

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			err = ccache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			retrieved := object.NewViewObject("view")
			object.SetContent(retrieved, map[string]any{"a": int64(2)})
			object.SetName(retrieved, "ns", "test-1")

			patch, err := json.Marshal(object.DeepCopy(retrieved).UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())

			// reset obj
			obj = object.NewViewObject("view")
			object.SetName(obj, "ns", "test-1")
			err = c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
			Expect(err).NotTo(HaveOccurred())

			Expect(obj).To(Equal(retrieved))
		})

		It("should write and retrieve a native object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			obj := &unstructured.UnstructuredList{}
			obj.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			// get from the object tracker: a normal get would go through the cache
			// first get should fail
			tracker := mgr.GetObjectTracker()
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods", // Resource does not equal Kind!
			}
			// create
			Expect(c.Create(ctx, pod)).NotTo(HaveOccurred())

			// second get should succeed
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
		})

		It("should patch a native object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			// create
			Expect(c.Create(ctx, pod)).NotTo(HaveOccurred())

			newPod := pod.DeepCopy()
			err = unstructured.SetNestedField(newPod.UnstructuredContent(), "OnFailure", "Spec", "restartPolicy")
			Expect(err).NotTo(HaveOccurred())

			// patch
			patch, err := json.Marshal(object.DeepCopy(newPod).UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())

			// reset obj
			newPod = pod.DeepCopy()
			err = c.Patch(ctx, newPod, client.RawPatch(types.MergePatchType, patch))
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
			Expect(p.Spec.Containers).To(HaveLen(1))
			Expect(p.Spec.Containers[0].Name).To(Equal("nginx"))
			Expect(p.Spec.Containers[0].Image).To(Equal("nginx"))
			Expect(p.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyOnFailure))
		})

		It("should write and watch a native object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})

			// must watch: get would go through the cache
			watcher, err := mgr.GetRuntimeClient().Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			Expect(c.Create(ctx, pod)).NotTo(HaveOccurred())

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			// no way to deep-equal: fakeclient adds lots of defaults
			Expect(event.Object.GetObjectKind().GroupVersionKind()).To(Equal(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}))
			// make a Get so that we get a full object (event.Object is only a runtime.Object)
			u, ok := event.Object.(*corev1.Pod)
			Expect(ok).To(BeTrue())
			Expect(u).NotTo(BeNil())
			Expect(u.GetName()).To(Equal("testpod"))
			Expect(u.GetNamespace()).To(Equal("testns"))
		})
	})

	Describe("StatusClient operation", func() {
		It("should create status on a view object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			obj := object.NewViewObject("view")
			object.SetContent(obj, map[string]any{"a": int64(1), "b": int64(2)})
			object.SetName(obj, "ns", "test-1")

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			err = ccache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			retrieved := object.DeepCopy(obj)
			Expect(unstructured.SetNestedMap(retrieved.UnstructuredContent(),
				map[string]any{"ready": "true"}, "status")).NotTo(HaveOccurred())

			// reset obj
			obj = object.NewViewObject("view")
			object.SetName(obj, "ns", "test-1")

			// create sub-resource obj
			sObj := object.DeepCopy(obj)
			Expect(unstructured.SetNestedMap(sObj.UnstructuredContent(),
				map[string]any{"ready": "true"}, "status")).NotTo(HaveOccurred())

			err = c.Status().Create(ctx, obj, sObj)
			Expect(err).NotTo(HaveOccurred())

			Expect(obj).To(Equal(retrieved))
		})

		It("should update status on a view object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			obj := object.NewViewObject("view")
			object.SetContent(obj, map[string]any{"a": int64(1), "b": int64(2)})
			object.SetName(obj, "ns", "test-1")

			retrieved := object.DeepCopy(obj)
			Expect(unstructured.SetNestedMap(retrieved.UnstructuredContent(),
				map[string]any{"ready": "true"}, "status")).NotTo(HaveOccurred())

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			err = ccache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			Expect(unstructured.SetNestedMap(obj.UnstructuredContent(),
				map[string]any{"ready": "true"}, "status")).NotTo(HaveOccurred())

			err = c.Status().Update(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj).To(Equal(retrieved))
		})

		It("should patch status on a view object", func() {
			mgr, err := NewFakeManager(manager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())

			go mgr.Start(ctx)

			obj := object.NewViewObject("view")
			object.SetContent(obj, map[string]any{"a": int64(1), "b": int64(2),
				"status": map[string]any{
					"a": map[string]any{
						"b": "c",
					},
				},
			})
			object.SetName(obj, "ns", "test-1")

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			err = ccache.GetViewCache().Add(obj) // this adds the status but anyway
			Expect(err).NotTo(HaveOccurred())

			Expect(unstructured.SetNestedField(obj.UnstructuredContent(),
				"d", "status", "a", "b")).NotTo(HaveOccurred())
			retrieved := object.DeepCopy(obj)

			patch, err := json.Marshal(object.DeepCopy(obj).UnstructuredContent())
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			// reset obj
			obj = object.NewViewObject("view")
			object.SetName(obj, "ns", "test-1")
			err = c.Status().Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
			Expect(err).NotTo(HaveOccurred())

			Expect(obj).To(Equal(retrieved))
		})
	})
})

func tryWatch(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}
