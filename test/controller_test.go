package integration

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	// viewv1a1 "hsnlab/dcontroller-runtime/pkg/api/view/v1alpha1"
	"hsnlab/dcontroller-runtime/internal/testutils"
	opv1a1 "hsnlab/dcontroller-runtime/pkg/api/operator/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/manager"

	"hsnlab/dcontroller-runtime/pkg/controller"
	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Controller test:", Ordered, func() {
	// write service type into an annotation for services running in the default namespace
	Context("When applying a self-referencial controller", Ordered, Label("controller"), func() {
		const annotationName = "service-type"
		var (
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
			svc        object.Object
			gvk        schema.GroupVersionKind
			mgr        runtimeManager.Manager
		)

		BeforeAll(func() {
			ctrlCtx, ctrlCancel = context.WithCancel(context.Background())
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
		})

		It("should create and start a manager succcessfully", func() {
			setupLog.Info("setting up controller manager")
			m, err := manager.New(nil, cfg, runtimeManager.Options{
				LeaderElection:         false, // disable leader-election
				HealthProbeBindAddress: "0",   // disable health-check
				Metrics: metricsserver.Options{
					BindAddress: "0", // disable the metrics server
				},
				Logger: logger,
			})
			Expect(err).NotTo(HaveOccurred())
			mgr = m
			Expect(mgr).NotTo(BeNil())

			setupLog.Info("starting manager")
			go func() {
				defer GinkgoRecover()
				err := mgr.Start(ctrlCtx)
				Expect(err).ToNot(HaveOccurred(), "failed to run manager")
			}()
		})

		It("should let a controller to be attached to the manager", func() {
			yamlData := `
name: svc-annotator
sources:
  - apiGroup: ""
    kind: Service
pipeline:
  "@aggregate":
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

			c, err := controller.New(mgr, config, controller.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("svc-annotator"))
		})

		It("should add the clusterIP annotation to a service", func() {
			ctrl.Log.Info("loading service")
			Expect(k8sClient.Create(ctrlCtx, svc)).Should(Succeed())

			get := object.New()
			get.SetGroupVersionKind(gvk)
			key := client.ObjectKeyFromObject(svc)
			Eventually(func() bool {
				// if err := mgr.GetClient().Get(ctx, key, get); err != nil && apierrors.IsNotFound(err) {
				if err := k8sClient.Get(ctx, key, get); err != nil && apierrors.IsNotFound(err) {
					setupLog.Info("could not query starting manager")
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
			}, timeout, interval).Should(BeTrue())
		})

		It("should adjust the annotation when the service type is manually updated", func() {
			ctrl.Log.Info("updating service service")

			_, err := ctrlutil.CreateOrUpdate(ctrlCtx, k8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.UnstructuredContent(), "NodePort", "spec", "type")
			})
			Expect(err).Should(Succeed())

			get := object.New()
			get.SetGroupVersionKind(gvk)
			key := client.ObjectKeyFromObject(svc)
			Eventually(func() bool {
				// if err := mgr.GetClient().Get(ctx, key, get); err != nil && apierrors.IsNotFound(err) {
				if err := k8sClient.Get(ctx, key, get); err != nil && apierrors.IsNotFound(err) {
					setupLog.Info("could not query starting manager")
					return false
				}

				if get.GetName() != key.Name || get.GetNamespace() != key.Namespace {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == "NodePort"
			}, timeout, interval).Should(BeTrue())
		})
	})
})
