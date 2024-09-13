package manager

import (
	"context"
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	ccache "hsnlab/dcontroller-runtime/pkg/cache"
	"hsnlab/dcontroller-runtime/pkg/object"
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

func newFakeManager(ctx context.Context) (manager.Manager, error) {
	fakeRuntimeClient := fake.NewClientBuilder().WithRuntimeObjects(podn).Build()
	fakeRuntimeCache := ccache.NewFakeRuntimeCache(nil)

	cache, err := ccache.NewCompositeCache(nil, ccache.Options{
		DefaultCache: fakeRuntimeCache,
		Logger:       &logger,
	})
	if err != nil {
		return nil, err
	}

	fakeRuntimeManager := NewFakeManager(cache, &compositeClient{
		Client:         fakeRuntimeClient,
		compositeCache: cache,
	})

	// mgr, err := New(&rest.Config{Host: "https://fake.example.com"}, Options{
	mgr, err := New(nil, Options{
		Manager: fakeRuntimeManager,
	})
	if err != nil {
		return nil, err
	}

	go mgr.Start(ctx)
	return mgr, nil
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
			mgr, err := newFakeManager(ctx)
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
			mgr, err := newFakeManager(ctx)
			Expect(err).NotTo(HaveOccurred())

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))

			// pod is already added
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
			mgr, err := newFakeManager(ctx)
			Expect(err).NotTo(HaveOccurred())

			cache := mgr.GetCache()
			Expect(cache).Should(BeAssignableToTypeOf(&ccache.CompositeCache{}))
			ccache := cache.(*ccache.CompositeCache)

			obj := object.NewViewObject("view")
			// must be added via the view-cache: the default clientAdd would go to the fake client
			err = ccache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(obj)
			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})
	})

	Describe("Client operation", func() {
		FIt("should retrieve a native object", func() {
			mgr, err := newFakeManager(ctx)
			Expect(err).NotTo(HaveOccurred())

			c := mgr.GetClient()
			Expect(c).NotTo(BeNil())

			// pod is already added
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})
			err = c.Get(ctx, client.ObjectKeyFromObject(pod), obj)
			Expect(err).NotTo(HaveOccurred())
			// the returned object somehow obtains a resource-version: remove it
			unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
			Expect(obj).To(Equal(pod))
			Expect(reflect.DeepEqual(pod, obj)).To(BeTrue())
		})

		It("should retrieve an added view object", func() {
			mgr, err := newFakeManager(ctx)
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("view").
				WithContent(map[string]any{"a": int64(1)}).
				WithName("ns", "test-1")

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
	})
})
