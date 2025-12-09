package integration

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/controller"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/testsuite"
)

var _ = Describe("Controller test:", Ordered, func() {
	// write service type into an annotation for services running in the default namespace
	Context("When applying a self-referencial controller", Ordered, Label("controller"), func() {
		const annotationName = "service-type"
		var (
			suite      *testsuite.Suite
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
			svc        object.Object
			gvk        schema.GroupVersionKind
			mgr        manager.Manager
		)

		BeforeAll(func() {
			var err error
			suite, err = testsuite.New(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctrlCtx, ctrlCancel = context.WithCancel(suite.Ctx)
			svc = testutils.TestSvc.DeepCopy()
			gvk = schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Service",
			}
			svc.SetGroupVersionKind(gvk)
		})

		AfterAll(func() {
			ctrlCancel()
			suite.Close()
		})

		It("should create and start a manager succcessfully", func() {
			suite.Log.Info("setting up controller manager")
			off := true
			m, err := manager.New(suite.Cfg, manager.Options{
				LeaderElection:         false, // disable leader-election
				HealthProbeBindAddress: "0",   // disable health-check
				Metrics: metricsserver.Options{
					BindAddress: "0", // disable the metrics server
				},
				Controller: config.Controller{
					SkipNameValidation: &off,
				},
				Logger: suite.Log,
			})
			Expect(err).NotTo(HaveOccurred())
			mgr = m
			Expect(mgr).NotTo(BeNil())

			suite.Log.Info("starting manager")
			go func() {
				defer GinkgoRecover()
				err := mgr.Start(ctrlCtx)
				Expect(err).NotTo(HaveOccurred(), "failed to run manager")
			}()
		})

		It("should let a controller to be attached to the manager", func() {
			yamlData := `
name: svc-annotator
sources:
  - apiGroup: ""
    kind: Service
pipeline:
  - "@project":
      metadata:
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
        annotations:
          "service-type": "$.spec.type"
target:
  apiGroup: ""
  kind: Service
  type: Patcher
`

			var config opv1a1.Controller
			Expect(yaml.Unmarshal([]byte(yamlData), &config)).NotTo(HaveOccurred())

			c, err := controller.NewDeclarative(mgr, "service-type-op", config, controller.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("svc-annotator"))
		})

		It("should add the clusterIP annotation to a service", func() {
			suite.Log.Info("loading service")
			Expect(suite.K8sClient.Create(ctrlCtx, svc)).Should(Succeed())

			get := object.New()
			get.SetGroupVersionKind(gvk)
			key := client.ObjectKeyFromObject(svc)
			Eventually(func() bool {
				// if err := mgr.GetClient().Get(suite.Ctx, key, get); err != nil && apierrors.IsNotFound(err) {
				if err := suite.K8sClient.Get(suite.Ctx, key, get); err != nil && apierrors.IsNotFound(err) {
					suite.Log.Info("could not query starting manager")
					return false
				}

				if get.GetName() != key.Name || get.GetNamespace() != key.Namespace {
					return false
				}

				serviceType, ok, err := unstructured.NestedString(get.Object, "spec", "type")
				if err != nil || !ok {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == serviceType
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should adjust the annotation when the service type is manually updated", func() {
			suite.Log.Info("updating service service")

			_, err := ctrlutil.CreateOrUpdate(ctrlCtx, suite.K8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.UnstructuredContent(), "NodePort", "spec", "type")
			})
			Expect(err).Should(Succeed())

			get := object.New()
			get.SetGroupVersionKind(gvk)
			key := client.ObjectKeyFromObject(svc)
			Eventually(func() bool {
				if err := suite.K8sClient.Get(suite.Ctx, key, get); err != nil && apierrors.IsNotFound(err) {
					suite.Log.Info("could not query starting manager")
					return false
				}

				if get.GetName() != key.Name || get.GetNamespace() != key.Namespace {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == "NodePort"
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should survive deleting the service", func() {
			suite.Log.Info("deleting service")
			Expect(suite.K8sClient.Delete(ctrlCtx, svc)).Should(Succeed())

			// removed from the cache?
			Eventually(func() bool {
				return apierrors.IsNotFound(mgr.GetCache().Get(ctrlCtx,
					client.ObjectKeyFromObject(svc), svc))
			}, suite.Timeout, suite.Interval).Should(BeTrue())

		})
	})
})
