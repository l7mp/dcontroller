package integration

import (
	"context"
	"os"
	"time"

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

const serviceTypeAnnotationName = "dcontroller.io/service-type"

var _ = Describe("EndpointSlice annotator operator test:", Ordered, func() {
	// annotate EndpointSlices with the type of the corresponding Service
	Context("When creating an endpoint annotator operator", Ordered, Label("operator"), func() {
		var (
			ctx                            context.Context
			cancel                         context.CancelFunc
			svc1, svc2, es1, es2, es3, es4 object.Object
		)

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(context.Background())

			svc1 = testutils.TestSvc.DeepCopy()
			svc1.SetName("test-service-1")
			svc1.SetNamespace("default")

			svc2 = testutils.TestSvc.DeepCopy()
			svc2.SetName("test-service-2")
			svc2.SetNamespace("other")

			es1 = testutils.TestEndpointSlice.DeepCopy()
			es1.SetName("test-endpointslice-1")
			es1.SetNamespace("default")
			es1.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})

			es2 = testutils.TestEndpointSlice.DeepCopy()
			es2.SetName("test-endpointslice-2")
			es2.SetNamespace("default")
			es2.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})

			es3 = testutils.TestEndpointSlice.DeepCopy()
			es3.SetName("test-endpointslice-3")
			es3.SetNamespace("other")
			es3.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})

			es4 = testutils.TestEndpointSlice.DeepCopy()
			es4.SetName("test-endpointslice-4")
			es4.SetNamespace("other")
			es4.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-2"})
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
			setupLog.Info("reading YAML file")
			yamlData, err := os.ReadFile("endpointslice_annotator.yaml")
			Expect(err).NotTo(HaveOccurred())
			var op opv1a1.Operator
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			setupLog.Info("adding new operator")
			Expect(k8sClient.Create(ctx, &op)).Should(Succeed())

			key := client.ObjectKeyFromObject(&op)
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

			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es1)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[serviceTypeAnnotationName] == "ClusterIP"
			}, timeout, interval).Should(BeTrue())
		})

		It("should adjust the annotation when the service type is manually updated", func() {
			ctrl.Log.Info("updating service")
			svc := svc1.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.UnstructuredContent(), "NodePort", "spec", "type")
			})
			Expect(err).Should(Succeed())

			var anns map[string]string
			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es1)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}

				anns = get.GetAnnotations()
				return len(anns) > 0 && anns[serviceTypeAnnotationName] == "NodePort"
			}, timeout, interval).Should(BeTrue())

			// just to make sure
			Expect(anns[serviceTypeAnnotationName]).To(Equal("NodePort"))
		})

		It("should handle the addition of another EndpointSlice referring to the same Service", func() {
			// can this happen in real K8s?
			ctrl.Log.Info("adding new referring EndpointSlice")
			Expect(k8sClient.Create(ctx, es2)).Should(Succeed())

			var anns map[string]string
			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es2)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}

				anns = get.GetAnnotations()
				return len(anns) > 0 && anns[serviceTypeAnnotationName] == "NodePort"
			}, timeout, interval).Should(BeTrue())

			// just to make sure
			Expect(anns[serviceTypeAnnotationName]).To(Equal("NodePort"))
		})

		It("should update all referring EndpointSlices when the Service changes", func() {
			ctrl.Log.Info("updating service")
			svc := svc1.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.UnstructuredContent(), "LoadBalancer", "spec", "type")
			})
			Expect(err).Should(Succeed())

			Eventually(func() bool {
				key1 := client.ObjectKeyFromObject(es1)
				get1 := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key1, get1); err != nil {
					return false
				}
				anns1 := get1.GetAnnotations()

				key2 := client.ObjectKeyFromObject(es2)
				get2 := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key2, get2); err != nil {
					return false
				}
				anns2 := get2.GetAnnotations()

				return len(anns1) > 0 && anns1[serviceTypeAnnotationName] == "LoadBalancer" &&
					len(anns2) > 0 && anns2[serviceTypeAnnotationName] == "LoadBalancer"
			}, timeout, interval).Should(BeTrue())
		})

		It("should not allow EndpointSlices in other namespaces to attach to the Service", func() {
			// can this happen in real K8s?
			ctrl.Log.Info("adding referring EndpointSlice from a different namespace")
			Expect(k8sClient.Create(ctx, es3)).Should(Succeed())

			// makes sure the es is written
			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es3)
				get := &discoveryv1.EndpointSlice{}
				err := k8sClient.Get(ctx, key, get)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// this is ugly and prone to errors
			time.Sleep(250 * time.Millisecond)

			key := client.ObjectKeyFromObject(es3)
			get := &discoveryv1.EndpointSlice{}
			Expect(k8sClient.Get(ctx, key, get)).To(Succeed())
			Expect(get.GetAnnotations()).To(BeEmpty())
		})

		It("should allow EndpointSlices in other namespaces to attach to Services in the same namespace", func() {
			ctrl.Log.Info("loading endpointslice in the other namespace")
			Expect(k8sClient.Create(ctx, es4)).Should(Succeed())

			ctrl.Log.Info("loading service in the other namespace")
			Expect(k8sClient.Create(ctx, svc2)).Should(Succeed())

			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es4)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[serviceTypeAnnotationName] == "ClusterIP"
			}, timeout, interval).Should(BeTrue())
		})

		It("should remove annotations from all referring EndpointSlices when Service is deleted", func() {
			ctrl.Log.Info("deleting service-1")
			Expect(k8sClient.Delete(ctx, svc1)).Should(Succeed())

			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es1)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}
				return len(get.GetAnnotations()) == 0
			}, timeout, interval).Should(BeTrue())

			Eventually(func() bool {
				key := client.ObjectKeyFromObject(es2)
				get := &discoveryv1.EndpointSlice{}
				if err := k8sClient.Get(ctx, key, get); err != nil {
					return false
				}
				return len(get.GetAnnotations()) == 0
			}, timeout, interval).Should(BeTrue())
		})

		It("should delete the objects added", func() {
			ctrl.Log.Info("deleting objects")
			Expect(k8sClient.Delete(ctx, svc2)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, es1)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, es2)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, es3)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, es4)).Should(Succeed())
		})
	})
})
