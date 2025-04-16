// KUBEBUILDER_ASSETS="/export/l7mp/dcontroller/bin/k8s/1.30.0-linux-amd64" go test ./... -v #-ginkgo.v -ginkgo.trace

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/hsnlab/dcontroller/internal/testutils"
	opv1a1 "github.com/hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/hsnlab/dcontroller/pkg/cache"
	dmanager "github.com/hsnlab/dcontroller/pkg/manager"
	"github.com/hsnlab/dcontroller/pkg/object"
	doperator "github.com/hsnlab/dcontroller/pkg/operator"
	dreconciler "github.com/hsnlab/dcontroller/pkg/reconciler"
)

var (
	suite    *testutils.SuiteContext
	loglevel = 1
	// loglevel | -10
	// loglevel = -6
	epCtrl  *testEpCtrl
	errorCh chan error
	eventCh chan dreconciler.Request
)

var _ = BeforeSuite(func() {
	var err error
	suite, err = testutils.NewSuite(loglevel)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() { suite.Close() })

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndpointSlice controller test")
}

// testEpCtrl implements the endpointSlice controller
type testEpCtrl struct {
	client.Client
	log logr.Logger
}

func (r *testEpCtrl) Reconcile(ctx context.Context, req dreconciler.Request) (reconcile.Result, error) {
	suite.Log.Info("Reconciling", "request", req.String())
	eventCh <- req
	return reconcile.Result{}, nil
}

var _ = Describe("EndpointSlice controller test:", Ordered, func() {
	Context("When creating an endpointslice controller w/o gather", Ordered, Label("operator"), func() {
		var svc1, es1, es2 object.Object
		var specs []map[string]any
		var ctx context.Context // context for the endpointslice controller
		var cancel context.CancelFunc

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(suite.Ctx)
			svc1 = testutils.TestSvc.DeepCopy()
			svc1.SetName("test-service-1")
			svc1.SetNamespace("testnamespace")
			svc1.SetAnnotations(map[string]string{EndpointSliceCtrlAnnotationName: "true"})
			Expect(unstructured.SetNestedSlice(svc1.Object, []any{
				map[string]any{
					"name":       "tcp-port",
					"protocol":   "TCP",
					"port":       int64(80),
					"targetPort": int64(8080),
				},
				map[string]any{
					"name":       "udp-port",
					"protocol":   "UDP",
					"port":       int64(3478),
					"targetPort": int64(33478),
				},
			}, "spec", "ports")).NotTo(HaveOccurred())

			es1 = testutils.TestEndpointSlice.DeepCopy()
			es1.SetName("test-endpointslice-1")
			es1.SetNamespace("testnamespace")
			es1.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})

			es2 = testutils.TestEndpointSlice.DeepCopy()
			es2.SetName("test-endpointslice-2")
			es2.SetNamespace("testnamespace")
			es2.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})
		})

		AfterAll(func() { cancel() })

		It("should create and start the controller", func() {
			// Create a dmanager
			mgr, err := dmanager.New(suite.Cfg, dmanager.Options{
				Options: ctrl.Options{Scheme: scheme},
			})
			Expect(err).NotTo(HaveOccurred())

			// Load the operator from file
			eventCh = make(chan dreconciler.Request, 16)
			errorCh = make(chan error, 16)
			opts := doperator.Options{
				ErrorChannel: errorCh,
				Logger:       suite.Log,
			}
			specFile := OperatorSpec
			if _, err := os.Stat(specFile); errors.Is(err, os.ErrNotExist) {
				specFile = filepath.Base(specFile)
			}
			_, err = doperator.NewFromFile("test-ep-operator", mgr, specFile, opts)
			Expect(err).NotTo(HaveOccurred())

			// Create the endpointslice controller
			epCtrl = &testEpCtrl{Client: mgr.GetClient(), log: suite.Log.WithName("test-endpointslice-ctrl")}
			on := true
			c, err := controller.NewTyped("test-ep-controller", mgr, controller.TypedOptions[dreconciler.Request]{
				SkipNameValidation: &on,
				Reconciler:         epCtrl,
			})
			Expect(err).NotTo(HaveOccurred())

			src, err := dreconciler.NewSource(mgr, opv1a1.Source{
				Resource: opv1a1.Resource{
					Kind: "EndpointView",
				},
			}).GetSource()
			Expect(err).NotTo(HaveOccurred())

			Expect(c.Watch(src)).NotTo(HaveOccurred())

			suite.Log.Info("starting error channel watcher")
			go func() {
				defer GinkgoRecover()
				for {
					select {
					case <-ctx.Done():
						return
					case err := <-errorCh:
						Fail(fmt.Sprintf("async error caught: %s", err.Error()))
					}
				}
			}()

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := mgr.Start(ctx)
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should generate 4 EndpointView events", func() {
			ctrl.Log.Info("loading service")
			Expect(suite.K8sClient.Create(ctx, svc1)).Should(Succeed())

			ctrl.Log.Info("loading endpointslice")
			Expect(suite.K8sClient.Create(ctx, es1)).Should(Succeed())
			Expect(suite.K8sClient.Create(ctx, es2)).Should(Succeed())

			specs = []map[string]any{}
			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj := object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj = object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj = object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj = object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
		})

		It("should match the expected EndpointView objects", func() {
			Expect(specs).To(HaveLen(4))
			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"address":     "192.0.2.1",
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"address":     "192.0.2.2",
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(3478),
				"targetPort":  int64(33478),
				"protocol":    "UDP",
				"address":     "192.0.2.1",
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(3478),
				"targetPort":  int64(33478),
				"protocol":    "UDP",
				"address":     "192.0.2.2",
			}))
		})

		It("should adjust 2 EndpointView objects when an address changes", func() {
			ctrl.Log.Info("updating service")
			es1 = testutils.TestEndpointSlice.DeepCopy()
			es1.SetName("test-endpointslice-1")
			es1.SetNamespace("testnamespace")
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, es1, func() error {
				es1.Object["endpoints"].([]any)[0].(map[string]any)["addresses"] = []any{"192.0.2.3"}
				return nil
			})
			Expect(err).Should(Succeed())

			// 2 deletes
			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			// 2 adds
			specs = []map[string]any{}
			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj := object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj = object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
		})

		It("should match the expected EndpointView objects", func() {
			Expect(specs).To(HaveLen(2))
			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"address":     "192.0.2.3",
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(3478),
				"targetPort":  int64(33478),
				"protocol":    "UDP",
				"address":     "192.0.2.3",
			}))

		})

		It("should remove EndpointView objects when the annotation is deleted from the Service", func() {
			obj := svc1.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, obj, func() error {
				obj.SetAnnotations(map[string]string{})
				return nil
			})
			Expect(err).ToNot(HaveOccurred())

			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))
		})

		It("should delete the objects added", func() {
			ctrl.Log.Info("deleting objects")
			Expect(suite.K8sClient.Delete(ctx, svc1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, es1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, es2)).Should(Succeed())
		})
	})

	Context("When creating an endpointslice controller w/ gather", Ordered, Label("operator"), func() {
		var svc1, es1, es2 object.Object
		var specs []map[string]any
		var ctx context.Context // context for the endpointslice controller
		var cancel context.CancelFunc

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(suite.Ctx)
			svc1 = testutils.TestSvc.DeepCopy()
			svc1.SetName("test-service-1")
			svc1.SetNamespace("testnamespace")
			svc1.SetAnnotations(map[string]string{EndpointSliceCtrlAnnotationName: "true"})
			Expect(unstructured.SetNestedSlice(svc1.Object, []any{
				map[string]any{
					"name":       "tcp-port",
					"protocol":   "TCP",
					"port":       int64(80),
					"targetPort": int64(8080),
				},
				map[string]any{
					"name":       "udp-port",
					"protocol":   "UDP",
					"port":       int64(3478),
					"targetPort": int64(33478),
				},
			}, "spec", "ports")).NotTo(HaveOccurred())

			es1 = testutils.TestEndpointSlice.DeepCopy()
			es1.SetName("test-endpointslice-1")
			es1.SetNamespace("testnamespace")
			es1.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})

			es2 = testutils.TestEndpointSlice.DeepCopy()
			es2.SetName("test-endpointslice-2")
			es2.SetNamespace("testnamespace")
			es2.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})
		})

		AfterAll(func() { cancel() })

		It("should create and start the controller", func() {
			// Create a dmanager
			mgr, err := dmanager.New(suite.Cfg, dmanager.Options{
				Options: ctrl.Options{Scheme: scheme},
			})
			Expect(err).NotTo(HaveOccurred())

			// Load the operator from file
			eventCh = make(chan dreconciler.Request, 16)
			errorCh = make(chan error, 16)
			opts := doperator.Options{
				ErrorChannel: errorCh,
				Logger:       suite.Log,
			}
			specFile := OperatorGatherSpec
			if _, err := os.Stat(specFile); errors.Is(err, os.ErrNotExist) {
				specFile = filepath.Base(specFile)
			}
			_, err = doperator.NewFromFile("test-ep-operator", mgr, specFile, opts)
			Expect(err).NotTo(HaveOccurred())

			// Create the endpointslice controller
			epCtrl = &testEpCtrl{Client: mgr.GetClient(), log: suite.Log.WithName("test-endpointslice-ctrl")}
			on := true
			c, err := controller.NewTyped("test-ep-controller", mgr, controller.TypedOptions[dreconciler.Request]{
				SkipNameValidation: &on,
				Reconciler:         epCtrl,
			})
			Expect(err).NotTo(HaveOccurred())

			src, err := dreconciler.NewSource(mgr, opv1a1.Source{
				Resource: opv1a1.Resource{
					Kind: "EndpointView",
				},
			}).GetSource()
			Expect(err).NotTo(HaveOccurred())

			Expect(c.Watch(src)).NotTo(HaveOccurred())

			suite.Log.Info("starting error channel watcher")
			go func() {
				defer GinkgoRecover()
				for {
					select {
					case <-ctx.Done():
						return
					case err := <-errorCh:
						Fail(fmt.Sprintf("async error caught: %s", err.Error()))
					}
				}
			}()

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := mgr.Start(ctx)
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should generate 4 EndpointView events", func() {
			ctrl.Log.Info("loading service")
			Expect(suite.K8sClient.Create(ctx, svc1)).Should(Succeed())

			ctrl.Log.Info("loading endpointslice")
			Expect(suite.K8sClient.Create(ctx, es1)).Should(Succeed())
			Expect(suite.K8sClient.Create(ctx, es2)).Should(Succeed())

			specs = []map[string]any{}
			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj := object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Added))
			obj = object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
		})

		It("should match the expected EndpointView objects", func() {
			Expect(specs).To(HaveLen(2))
			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"addresses":   []any{"192.0.2.1", "192.0.2.2"},
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"addresses":   []any{"192.0.2.1", "192.0.2.2"},
			}))
		})

		It("should adjust 2 EndpointView objects when an address changes", func() {
			ctrl.Log.Info("updating service")
			es1 = testutils.TestEndpointSlice.DeepCopy()
			es1.SetName("test-endpointslice-1")
			es1.SetNamespace("testnamespace")
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, es1, func() error {
				es1.Object["endpoints"].([]any)[0].(map[string]any)["addresses"] = []any{"192.0.2.3"}
				return nil
			})
			Expect(err).Should(Succeed())

			specs = []map[string]any{}
			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Updated))
			obj := object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Updated))
			obj = object.NewViewObject(req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
		})

		It("should match the expected EndpointView objects", func() {
			Expect(specs).To(HaveLen(2))
			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"addresses":   []any{"192.0.2.3", "192.0.2.2"},
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(3478),
				"targetPort":  int64(33478),
				"protocol":    "UDP",
				"addresses":   []any{"192.0.2.3", "192.0.2.2"},
			}))

		})

		It("should remove EndpointView objects when the annotation is deleted from the Service", func() {
			obj := svc1.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, obj, func() error {
				obj.SetAnnotations(map[string]string{})
				return nil
			})
			Expect(err).ToNot(HaveOccurred())

			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(cache.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))
		})

		It("should delete the objects added", func() {
			ctrl.Log.Info("deleting objects")
			Expect(suite.K8sClient.Delete(ctx, svc1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, es1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, es2)).Should(Succeed())
		})
	})
})

// wait for some configurable time for a watch event
func watchEvent(d time.Duration) (dreconciler.Request, error) {
	select {
	case e := <-eventCh:
		return e, nil
	case <-time.After(d):
		return dreconciler.Request{}, errors.New("timeout")
	}
}
