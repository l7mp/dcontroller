package integration

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	"hsnlab/dcontroller-runtime/internal/testutils"
	opv1a1 "hsnlab/dcontroller-runtime/pkg/api/operator/v1alpha1"
	"hsnlab/dcontroller-runtime/pkg/object"
	"hsnlab/dcontroller-runtime/pkg/operator"
)

var _ = Describe("Operator test:", Ordered, func() {
	// annotate EndpointSlices with the type of the corresponding Service
	Context("When creating an endpoint annotator operator", Ordered, Label("operator"), func() {
		const annotationName = "dcontroller.io/service-type"
		var (
			ctx                             context.Context
			cancel                          context.CancelFunc
			svc1, svc2, svc3, es1, es2, es3 object.Object
		)

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(context.Background())

			svc1 = testutils.TestSvc.DeepCopy()
			svc1.SetName("test-service-1")
			svc1.SetNamespace("default")

			svc2 = testutils.TestSvc.DeepCopy()
			svc2.SetName("test-service-2")
			svc2.SetNamespace("other")

			svc3 = testutils.TestSvc.DeepCopy()
			svc3.SetName("test-service-3")
			svc3.SetNamespace("default")

			es1 = testutils.TestEndpointSlice.DeepCopy()
			es1.SetName("test-endpointslice-1")
			es1.SetNamespace("default")
			es1.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})

			es2 = testutils.TestEndpointSlice.DeepCopy()
			es2.SetName("test-endpointslice-2")
			es2.SetNamespace("other")
			es2.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-2"})

			es3 = testutils.TestEndpointSlice.DeepCopy()
			es3.SetName("test-endpointslice-3")
			es3.SetNamespace("default")
			es3.SetLabels(map[string]string{"kubernetes.io/service-name": "dummy"})

		})

		AfterAll(func() {
			cancel()
		})

		It("should create and start the operator controller", func() {
			setupLog.Info("setting up operator controller")
			c, err := operator.NewController(cfg, ctrl.Options{
				Scheme:                 scheme,
				LeaderElection:         false, // disable leader-election
				HealthProbeBindAddress: "0",   // disable health-check
				Metrics: metricsserver.Options{
					BindAddress: "0", // disable the metrics server
				},
				Logger: logger,
			})
			Expect(err).NotTo(HaveOccurred())

			setupLog.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := c.Start(ctx)
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should let an operator to be attached to the manager", func() {
			yamlData := `
name: svc-endpointslice-annotator
controllers:
  - name: svc-endpointslice-annotator
    sources:
      - apiGroup: ""
        kind: Service
      - apiGroup: "discovery.k8s.io"
        kind: EndpointSlice
    pipeline:
      "@join":
        "@and":
          - '@eq':
              - $.Service.metadata.name
              - '$["EndpointSlice"]["metadata"]["labels"]["kubernetes.io/service-name"]'
          - '@eq':
              - $.Service.metadata.namespace
              - $.EndpointSlice.metadata.namespace
      "@aggregate":
        - "@project":
            metadata:
              name: "$.EndpointSlice.metadata.name"
              namespace: "$.EndpointSlice.metadata.namespace"
              annotations:
                "dcontroller.io/service-type": "$.Service.spec.type"
    target:
      apiGroup: "discovery.k8s.io"
      kind: EndpointSlice
      type: Patcher
`
			var spec opv1a1.OperatorSpec
			Expect(yaml.Unmarshal([]byte(yamlData), &spec)).NotTo(HaveOccurred())

			setupLog.Info("adding new operator")
			op := &opv1a1.Operator{}
			op.SetName("svc-endpointslice-annotator")
			op.Spec = spec
			Expect(k8sClient.Create(ctx, op)).Should(Succeed())

			key := client.ObjectKeyFromObject(op)
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := k8sClient.Get(ctx, key, get)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})

		It("should add an annotation", func() {
			ctrl.Log.Info("loading service")
			Expect(k8sClient.Create(ctx, svc1)).Should(Succeed())

			ctrl.Log.Info("loading endpointslice")
			Expect(k8sClient.Create(ctx, es1)).Should(Succeed())

			// time.Sleep(6 * time.Hour)

			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es1)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == "ClusterIP"
			}, timeout, interval).Should(BeTrue())
		})

		It("should adjust the annotation when the service type is manually updated", func() {
			ctrl.Log.Info("updating service service")
			svc := svc1.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.UnstructuredContent(), "NodePort", "spec", "type")
			})
			Expect(err).Should(Succeed())

			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es1)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == "NodePort"
			}, timeout, interval).Should(BeTrue())
		})
	})
})
