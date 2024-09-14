package manager

import (
	"context"
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
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	ccache "hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
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
			mgr, err := NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

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
			mgr, err := NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())

			// pod added
			Expect(mgr.GetRuntimeCache().Upsert(pod)).NotTo(HaveOccurred())

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
			mgr, err := NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())

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
			mgr, err := NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())

			// pod added
			Expect(mgr.GetRuntimeCache().Upsert(pod)).NotTo(HaveOccurred())

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
			// the returned object somehow obtains a resource-version: remove it
			//unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
			Expect(obj).To(Equal(pod))
			Expect(reflect.DeepEqual(pod, obj)).To(BeTrue())
		})

		It("should retrieve an added view object", func() {
			mgr, err := NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())

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

			retrieved := object.DeepCopy(obj)
			err = c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should write and retrieve a native object", func() {
			mgr, err := NewFakeManager(ctx, logger)
			Expect(err).NotTo(HaveOccurred())

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
})

func tryWatch(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}
