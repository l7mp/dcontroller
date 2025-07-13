// KUBEBUILDER_ASSETS="/export/l7mp/dcontroller/bin/k8s/1.30.0-linux-amd64" go test ./... -v #-ginkgo.v -ginkgo.trace

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/apiserver"
	dmanager "github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
	doperator "github.com/l7mp/dcontroller/pkg/operator"
	dreconciler "github.com/l7mp/dcontroller/pkg/reconciler"
)

var (
	suite *testutils.SuiteContext
	// loglevel = 1
	// loglevel = -10
	loglevel      = -5
	port          int
	epCtrl        *testEpCtrl
	errorCh       chan error
	eventCh       chan dreconciler.Request
	server        *apiserver.APIServer
	dynamicClient dynamic.Interface
	watcher       watch.Interface
	watchCh       <-chan watch.Event
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
	suite.Log.Info("reconciling", "request", req.String())
	eventCh <- req
	return reconcile.Result{}, nil
}

var _ = Describe("EndpointSlice controller test:", Ordered, func() {
	Context("When creating an endpointslice controller w/o gather", Ordered, Label("operator"), func() {
		var mgr manager.Manager
		var svc1, es1 object.Object
		var specs []map[string]any
		var epNames []string
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
		})

		AfterAll(func() { cancel() })

		It("should create and start the API server", func() {
			suite.Log.Info("creating a dmanager")
			var err error
			mgr, err = dmanager.New(suite.Cfg, dmanager.Options{
				Options: ctrl.Options{Scheme: scheme, Logger: suite.Log},
			})
			Expect(err).NotTo(HaveOccurred())

			suite.Log.Info("creating the API server")
			port = rand.IntN(5000) + (32768)
			config, err := apiserver.NewDefaultConfig("", port, true)
			Expect(err).NotTo(HaveOccurred())
			server, err = apiserver.NewAPIServer(mgr, config)
			Expect(err).NotTo(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				err := server.Start(ctx)
				Expect(err).NotTo(HaveOccurred())
			}()

			// Give server a moment to start
			time.Sleep(20 * time.Millisecond)
		})

		It("should create and start the controller", func() {
			suite.Log.Info("loading the operator from file")
			eventCh = make(chan dreconciler.Request, 16)
			errorCh = make(chan error, 16)
			opts := doperator.Options{
				ErrorChannel: errorCh,
				APIServer:    server,
				Logger:       suite.Log,
			}
			specFile := OperatorSpec
			if _, err := os.Stat(specFile); errors.Is(err, os.ErrNotExist) {
				specFile = filepath.Base(specFile)
			}
			_, err := doperator.NewFromFile(OperatorName, mgr, specFile, opts)
			Expect(err).NotTo(HaveOccurred())

			suite.Log.Info("creating the endpointslice controller")
			epCtrl = &testEpCtrl{Client: mgr.GetClient(), log: suite.Log.WithName("test-ep-ctrl")}
			on := true
			c, err := controller.NewTyped("test-ep-ctrl", mgr, controller.TypedOptions[dreconciler.Request]{
				SkipNameValidation: &on,
				Reconciler:         epCtrl,
			})
			Expect(err).NotTo(HaveOccurred())

			suite.Log.Info("creating an endpoint-view watcher")
			src, err := dreconciler.NewSource(mgr, OperatorName, opv1a1.Source{
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
						// if apierrors.IsNotFound(err) {
						// 	ctrl.Log.Info("ignoring notfound error")
						// 	continue
						// }
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

		It("should allow an API server discovery client to be created", func() {
			// Create a discovery client
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(&rest.Config{
				Host: fmt.Sprintf("http://localhost:%d", port),
			})
			Expect(err).NotTo(HaveOccurred())

			// Get OpenAPI V2 spec: wait until the server starts
			Eventually(func() bool {
				openAPISchema, err := discoveryClient.OpenAPISchema()
				return err == nil && openAPISchema != nil
			}, time.Second).Should(BeTrue())

			// Get all API groups and resources
			apiGroups, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
			Expect(err).NotTo(HaveOccurred())
			Expect(apiGroups).To(HaveLen(1))
			Expect(apiGroups[0].Name).To(Equal(viewv1a1.Group(OperatorName)))
			Expect(apiGroups[0].PreferredVersion.Version).To(Equal("v1alpha1"))
			Expect(apiGroups[0].Versions).To(HaveLen(1))

			Expect(apiResourceLists).To(HaveLen(1))
			resourceList := apiResourceLists[0]
			Expect(resourceList.GroupVersion).To(Equal(schema.GroupVersion{
				Group:   viewv1a1.Group(OperatorName),
				Version: viewv1a1.Version,
			}.String()))
			Expect(resourceList.APIResources).To(HaveLen(2))
		})

		It("should generate 4 EndpointView events", func() {
			ctrl.Log.Info("loading service")
			Expect(suite.K8sClient.Create(ctx, svc1)).Should(Succeed())

			ctrl.Log.Info("loading endpointslice")
			Expect(suite.K8sClient.Create(ctx, es1)).Should(Succeed())

			specs = []map[string]any{}
			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Added))
			obj := object.NewViewObject(OperatorName, req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
			epNames = append(epNames, req.Name)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Added))
			obj = object.NewViewObject(OperatorName, req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
			epNames = append(epNames, req.Name)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Added))
			obj = object.NewViewObject(OperatorName, req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
			epNames = append(epNames, req.Name)

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Added))
			obj = object.NewViewObject(OperatorName, req.GVK.Kind)
			err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
			Expect(err).Should(Succeed())
			Expect(obj.GetName()).To(HavePrefix("test-service"))
			Expect(obj.GetNamespace()).To(Equal("testnamespace"))
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)
			epNames = append(epNames, req.Name)

			Expect(epNames).To(HaveLen(4))
			checkSpecs(specs, []string{"192.0.2.1", "192.0.2.2"})
		})

		It("should allow an API server client to be created", func() {
			suite.Log.Info("creating dynamic client")
			var err error
			dynamicClient, err = dynamic.NewForConfig(&rest.Config{
				Host: fmt.Sprintf("http://%s:%d", "localhost", port),
			})
			Expect(err).NotTo(HaveOccurred())

			list, err := dynamicClient.Resource(schema.GroupVersionResource{
				Group:    viewv1a1.Group(OperatorName),
				Version:  viewv1a1.Version,
				Resource: "endpointview",
			}).Namespace("default").List(context.TODO(), metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})

		It("should allow a watcher to be created", func() {
			suite.Log.Info("creating a watch")

			var err error
			watcher, err = dynamicClient.Resource(schema.GroupVersionResource{
				Group:    viewv1a1.Group(OperatorName),
				Version:  viewv1a1.Version,
				Resource: "endpointview",
			}).Namespace("testnamespace").Watch(ctx, metav1.ListOptions{})
			Expect(err).NotTo(HaveOccurred())
			watchCh = watcher.ResultChan()
		})

		It("should let the results to be loaded from the API server", func() {
			gvr := schema.GroupVersionResource{
				Group:    viewv1a1.Group(OperatorName),
				Version:  viewv1a1.Version,
				Resource: "endpointview",
			}
			specs = []map[string]any{}

			for i := 0; i < 4; i++ {
				obj, err := dynamicClient.Resource(gvr).Namespace("testnamespace").
					Get(ctx, epNames[i], metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
				Expect(err).Should(Succeed())
				Expect(ok).To(BeTrue())
				specs = append(specs, spec)
			}

			checkSpecs(specs, []string{"192.0.2.1", "192.0.2.2"})
		})

		It("should generate 4 EndpointView watch events from the API server", func() {
			specs = []map[string]any{}

			for i := 0; i < 4; i++ {
				req, err := apiServerWatchEvent(suite.Timeout)
				Expect(err).Should(Succeed())
				Expect(req.Type).To(Equal(watch.Added))
				gvk := req.Object.GetObjectKind().GroupVersionKind()
				Expect(gvk).To(Equal(schema.GroupVersionKind{
					Group:   viewv1a1.Group(OperatorName),
					Version: viewv1a1.Version,
					Kind:    "EndpointView",
				}))
				reqObj, err := meta.Accessor(req.Object)
				Expect(err).Should(Succeed())
				obj := object.NewViewObject(OperatorName, gvk.Kind)
				err = epCtrl.Get(ctx, types.NamespacedName{
					Name:      reqObj.GetName(),
					Namespace: reqObj.GetNamespace()}, obj)
				Expect(err).Should(Succeed())
				Expect(obj.GetName()).To(HavePrefix("test-service"))
				Expect(obj.GetNamespace()).To(Equal("testnamespace"))
				spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
				Expect(err).Should(Succeed())
				Expect(ok).To(BeTrue())
				specs = append(specs, spec)
			}

			checkSpecs(specs, []string{"192.0.2.1", "192.0.2.2"})
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

			// Since EndpointView have different names (the name contains the hash of
			// the spec) there is no guarantee that deletes come before upserts (this
			// guarantee exists only for identically named objects)
			epNames = []string{}
			specs = []map[string]any{}
			for i := 0; i < 4; i++ {
				req, err := watchEvent(suite.Timeout)
				Expect(err).Should(Succeed())
				switch req.EventType {
				case object.Deleted:
					Expect(req.Name).To(HavePrefix("test-service"))
					Expect(req.Namespace).To(Equal("testnamespace"))
				case object.Upserted, object.Added:
					obj := object.NewViewObject(OperatorName, req.GVK.Kind)
					err = epCtrl.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj)
					Expect(err).Should(Succeed())
					Expect(obj.GetName()).To(HavePrefix("test-service"))
					Expect(obj.GetNamespace()).To(Equal("testnamespace"))
					spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
					Expect(err).Should(Succeed())
					Expect(ok).To(BeTrue())
					specs = append(specs, spec)
					epNames = append(epNames, req.Name)
				default:
					Fail("unexpected delta type")
				}
			}

			checkSpecs(specs, []string{"192.0.2.3"})
		})

		It("should let the 2 EndpointView objects be re-loaded from the API server", func() {
			gvr := schema.GroupVersionResource{
				Group:    viewv1a1.Group(OperatorName),
				Version:  viewv1a1.Version,
				Resource: "endpointview",
			}
			specs = []map[string]any{}

			obj, err := dynamicClient.Resource(gvr).Namespace("testnamespace").
				Get(ctx, epNames[0], metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			obj, err = dynamicClient.Resource(gvr).Namespace("testnamespace").
				Get(ctx, epNames[1], metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			spec, ok, err = unstructured.NestedMap(obj.Object, "spec")
			Expect(err).Should(Succeed())
			Expect(ok).To(BeTrue())
			specs = append(specs, spec)

			checkSpecs(specs, []string{"192.0.2.3"})
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
			Expect(req.EventType).To(Equal(object.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))
		})

		It("should delete the objects added", func() {
			ctrl.Log.Info("deleting objects")
			Expect(suite.K8sClient.Delete(ctx, svc1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, es1)).Should(Succeed())
		})
	})

	Context("When creating an endpointslice controller w/ gather", Ordered, Label("operator"), func() {
		var svc1, es1 object.Object
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
			_, err = doperator.NewFromFile(OperatorName, mgr, specFile, opts)
			Expect(err).NotTo(HaveOccurred())

			// Create the endpointslice controller
			epCtrl = &testEpCtrl{Client: mgr.GetClient(), log: suite.Log.WithName("test-endpointslice-ctrl")}
			on := true
			c, err := controller.NewTyped("test-ep-controller", mgr, controller.TypedOptions[dreconciler.Request]{
				SkipNameValidation: &on,
				Reconciler:         epCtrl,
			})
			Expect(err).NotTo(HaveOccurred())

			src, err := dreconciler.NewSource(mgr, OperatorName, opv1a1.Source{
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

		It("should generate 2 EndpointView events", func() {
			ctrl.Log.Info("loading service")
			Expect(suite.K8sClient.Create(ctx, svc1)).Should(Succeed())

			ctrl.Log.Info("loading endpointslice")
			Expect(suite.K8sClient.Create(ctx, es1)).Should(Succeed())

			specs = []map[string]any{}
			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Added))
			obj := object.NewViewObject(OperatorName, req.GVK.Kind)
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
			Expect(req.EventType).To(Equal(object.Added))
			obj = object.NewViewObject(OperatorName, req.GVK.Kind)
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
			// addresses is in random order
			sortAtField(specs, "addresses")
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
				"port":        int64(3478),
				"targetPort":  int64(33478),
				"protocol":    "UDP",
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
			Expect(req.EventType).To(Equal(object.Updated))
			obj := object.NewViewObject(OperatorName, req.GVK.Kind)
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
			Expect(req.EventType).To(Equal(object.Updated))
			obj = object.NewViewObject(OperatorName, req.GVK.Kind)
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
			sortAtField(specs, "addresses")
			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(80),
				"targetPort":  int64(8080),
				"protocol":    "TCP",
				"addresses":   []any{"192.0.2.2", "192.0.2.3"},
			}))

			Expect(specs).To(ContainElement(map[string]any{
				"serviceName": "test-service-1",
				"type":        "ClusterIP",
				"port":        int64(3478),
				"targetPort":  int64(33478),
				"protocol":    "UDP",
				"addresses":   []any{"192.0.2.2", "192.0.2.3"},
			}))

		})

		It("should remove EndpointView objects when the annotation is deleted from the Service", func() {
			obj := svc1.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, obj, func() error {
				obj.SetAnnotations(map[string]string{})
				return nil
			})
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(5 * time.Second)

			req, err := watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))

			req, err = watchEvent(suite.Timeout)
			Expect(err).Should(Succeed())
			Expect(req.EventType).To(Equal(object.Deleted))
			Expect(req.Name).To(HavePrefix("test-service"))
			Expect(req.Namespace).To(Equal("testnamespace"))
		})

		It("should delete the objects added", func() {
			ctrl.Log.Info("deleting objects")
			Expect(suite.K8sClient.Delete(ctx, svc1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, es1)).Should(Succeed())
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

// wait for some configurable time for a watch event
func apiServerWatchEvent(d time.Duration) (watch.Event, error) {
	select {
	case e := <-watchCh:
		return e, nil
	case <-time.After(d):
		return watch.Event{}, errors.New("timeout")
	}
}

func sortAtField(specs []map[string]any, fields ...string) {
	for _, spec := range specs {
		list, ok, err := unstructured.NestedSlice(spec, fields...)
		if err != nil || !ok {
			continue
		}

		sortAny(list)
		err = unstructured.SetNestedSlice(spec, list, fields...)
		if err != nil {
			continue
		}
	}
}

func sortAny(slice []any) {
	sort.Slice(slice, func(i, j int) bool {
		// Convert both to JSON for consistent comparison
		jsonI, _ := json.Marshal(slice[i])
		jsonJ, _ := json.Marshal(slice[j])
		return string(jsonI) < string(jsonJ)
	})
}

func checkSpecs(specs []map[string]any, addrs []string) {
	Expect(specs).To(HaveLen(len(addrs) * 2))

	for _, addr := range addrs {
		Expect(specs).To(ContainElement(map[string]any{
			"serviceName": "test-service-1",
			"type":        "ClusterIP",
			"port":        int64(80),
			"targetPort":  int64(8080),
			"protocol":    "TCP",
			"address":     addr,
		}))

		Expect(specs).To(ContainElement(map[string]any{
			"serviceName": "test-service-1",
			"type":        "ClusterIP",
			"port":        int64(3478),
			"targetPort":  int64(33478),
			"protocol":    "UDP",
			"address":     addr,
		}))
	}
}
