package operator

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
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
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

func TestManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Operator")
}

var _ = Describe("Headless Operator", func() {
	var (
		obj    object.Object
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		obj = object.NewViewObject("test", "view")
		object.SetName(obj, "test-ns", "test-obj")
		object.SetContent(obj, map[string]any{"x": "y"})
		object.WithUID(obj)
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
	})

	It("should create an empty Operator", func() {
		mgr, err := manager.NewHeadless(manager.Options{
			Metrics: metrics.Options{
				BindAddress: ":54322",
			},
			HealthProbeBindAddress: "",
			Logger:                 logger,
		})
		Expect(err).NotTo(HaveOccurred())

		// closed by the operator
		errorChan := make(chan error, 16)
		opts := Options{
			ErrorChannel: errorChan,
			Logger:       logger,
		}

		op := New("test", mgr, opts)
		Expect(op).NotTo(BeNil())

		go func() {
			defer GinkgoRecover()
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-errorChan:
					Expect(err).NotTo(HaveOccurred())
				}
			}
		}()

		go func() {
			defer GinkgoRecover()
			err := mgr.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		}()

		c := mgr.GetClient()
		Expect(c).NotTo(BeNil())
		err = c.Create(ctx, obj)
		Expect(err).NotTo(HaveOccurred())

		cc := mgr.GetCache()
		Expect(cc).NotTo(BeNil())

		// get from cache
		retrieved := object.DeepCopy(obj)
		err = cc.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved).To(Equal(obj))

		// get from client
		retrieved = object.DeepCopy(obj)
		err = c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved).To(Equal(obj))
	})

	It("should load an Operator", func() {
		mgr, err := manager.NewHeadless(manager.Options{
			Metrics: metrics.Options{
				BindAddress: ":54321",
			},
			HealthProbeBindAddress: "",
			Logger:                 logger,
		})
		Expect(err).NotTo(HaveOccurred())

		errorChan := make(chan error, 16)
		opts := Options{
			ErrorChannel: errorChan,
			Logger:       logger,
		}

		jsonData := `
- '@project':
    metadata: $.metadata
    x: z
    a: b`
		var p opv1a1.Pipeline
		err = yaml.Unmarshal([]byte(jsonData), &p)
		Expect(err).NotTo(HaveOccurred())
		operatorSpec := &opv1a1.OperatorSpec{
			Controllers: []opv1a1.Controller{{
				Name: "test-controller",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{Kind: "view"},
				}},
				Pipeline: p,
				Target: opv1a1.Target{
					Resource: opv1a1.Resource{Kind: "viewres"},
				},
			}},
		}

		op := New("test", mgr, opts)
		Expect(op).NotTo(BeNil())
		op.AddSpec(operatorSpec)

		go func() {
			defer GinkgoRecover()
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-errorChan:
					Expect(err).NotTo(HaveOccurred())
				}
			}
		}()

		go func() {
			defer GinkgoRecover()
			err := mgr.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		}()

		Expect(op.ListControllers()).To(HaveLen(1))
		ctrl := op.GetController("test-controller")
		Expect(ctrl).NotTo(BeNil())
		Expect(ctrl.GetName()).To(Equal("test-controller"))

		c, ok := mgr.GetClient().(client.WithWatch)
		Expect(ok).To(BeTrue())
		Expect(c).NotTo(BeNil())

		gvk := viewv1a1.GroupVersionKind("test", "viewres")
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		watcher, err := c.Watch(ctx, list)
		Expect(err).NotTo(HaveOccurred())

		err = c.Create(ctx, obj)
		Expect(err).NotTo(HaveOccurred())

		// watch from client
		expected := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "test.view.dcontroller.io/v1alpha1",
				"kind":       "viewres",
				"metadata": map[string]any{
					"name":      "test-obj",
					"namespace": "test-ns",
				},
				"x": "z",
				"a": "b",
			},
		}
		object.WithUID(expected)

		event, ok := tryWatch(watcher, interval)
		Expect(ok).To(BeTrue())
		Expect(event.Type).To(Equal(watch.Added))
		Expect(event.Object.GetObjectKind().GroupVersionKind()).To(Equal(gvk))
		Expect(event.Object).To(Equal(expected))

		// get from client
		retrieved := object.NewViewObject("test", "viewres")
		object.SetName(retrieved, "test-ns", "test-obj")
		err = c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved).To(Equal(expected))

		// get from cache
		retrieved = object.NewViewObject("test", "viewres")
		object.SetName(retrieved, "test-ns", "test-obj")
		cc := mgr.GetCache()
		Expect(cc).NotTo(BeNil())
		retrieved = object.NewViewObject("test", "viewres")
		object.SetName(retrieved, "test-ns", "test-obj")
		err = cc.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved).To(Equal(expected))
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
