package cache

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/hsnlab/dcontroller/pkg/object"
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
	pod  = &unstructured.Unstructured{}
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

func TestCache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cache")
}

var _ = Describe("CompositeCache", func() {
	var (
		cache     *CompositeCache
		fakeCache *FakeRuntimeCache
		ctx       context.Context
		cancel    context.CancelFunc
	)

	BeforeEach(func() {
		content, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(podn)
		pod.SetUnstructuredContent(content)
		// this is needed: for some unknown reason the converter does not work on the GVK
		pod.GetObjectKind().SetGroupVersionKind(podn.GetObjectKind().GroupVersionKind())
		fakeCache = NewFakeRuntimeCache(scheme.Scheme)
		cache, _ = NewCompositeCache(nil, Options{
			DefaultCache: fakeCache,
			Logger:       logger,
		})
		ctx, cancel = context.WithCancel(context.Background())
		go cache.Start(ctx)
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Basics", func() {
		It("cache created", func() {
			Expect(cache).NotTo(BeNil())
			Expect(cache.GetDefaultCache).NotTo(BeNil())
			Expect(cache.GetViewCache).NotTo(BeNil())
		})
	})

	Describe("Get operation", func() {
		It("should retrieve an added view object", func() {
			obj := object.NewViewObject("view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-1")

			err := cache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(obj)
			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should retrieve an added view object that is created from a real resource", func() {
			obj, err := object.NewViewObjectFromNativeObject("view", pod)
			Expect(err).NotTo(HaveOccurred())

			err = cache.GetViewCache().Add(obj)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(obj)
			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should retrieve an added native object", func() {
			err := fakeCache.Add(pod)
			Expect(err).NotTo(HaveOccurred())

			retrieved := object.DeepCopy(pod)
			Expect(retrieved).To(Equal(pod))

			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved).To(Equal(pod))
			Expect(object.DeepEqual(retrieved, pod)).To(BeTrue())
		})

		It("should return an error for non-existent object", func() {
			obj := object.NewViewObject("view")
			object.SetName(obj, "", "non-existent")
			err := cache.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(err).To(HaveOccurred())
			err = cache.Get(ctx, client.ObjectKeyFromObject(pod), pod)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List operation", func() {
		It("should list all added view objects", func() {
			objects := []object.Object{object.NewViewObject("view"), object.NewViewObject("view"), object.NewViewObject("view")}
			object.SetName(objects[0], "ns1", "test-1")
			object.SetName(objects[1], "ns2", "test-2")
			object.SetName(objects[2], "ns3", "test-3")
			object.SetContent(objects[0], map[string]any{"a": int64(1)})
			object.SetContent(objects[1], map[string]any{"b": int64(2)})
			object.SetContent(objects[2], map[string]any{"c": int64(3)})

			for _, obj := range objects {
				err := cache.GetViewCache().Add(obj)
				Expect(err).NotTo(HaveOccurred())
			}

			list := object.NewViewObjectList("view")
			err := cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))
			Expect(list.Items).To(ContainElement(*objects[0]))
			Expect(list.Items).To(ContainElement(*objects[1]))
			Expect(list.Items).To(ContainElement(*objects[2]))
		})

		It("should list all added native objects", func() {
			err := fakeCache.Add(pod)
			Expect(err).NotTo(HaveOccurred())

			list := &unstructured.UnstructuredList{}
			err = cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1))
			Expect(object.DeepEqual(&list.Items[0], pod)).To(BeTrue())
		})

		It("should return an empty list when cache is empty", func() {
			list := &unstructured.UnstructuredList{}
			err := cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})
	})

})
