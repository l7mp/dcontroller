package integration

import (
	"context"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	"hsnlab/dcontroller/internal/testutils"
	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
	"hsnlab/dcontroller/pkg/object"
	"hsnlab/dcontroller/pkg/operator"
)

const (
	relatedConfigMapAnnotationName = "dcontroller.io/related-configmap"
	deploymentMarkerAnnotationName = "dcontroller.io/configmap-version"
)

var _ = Describe("Deployment handler operator test:", Ordered, func() {
	// restart Deployments whenever the related ConfigMap changes
	Context("When creating a Deployment handler operator", Ordered, Label("operator"), func() {
		var (
			ctx          context.Context
			cancel       context.CancelFunc
			dp1, dp2, cm object.Object
		)

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(context.Background())

			// each dp relate to the same configmapach
			dp1 = testutils.TestDeployment.DeepCopy()
			dp1.SetName("test-deploy-1")
			dp1.SetNamespace("default")

			dp2 = testutils.TestDeployment.DeepCopy()
			dp2.SetName("test-deploy-2")
			dp2.SetNamespace("default")

			cm = testutils.TestConfigMap.DeepCopy()
			dp1.SetNamespace("default")
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
			yamlData, err := os.ReadFile("deployment_configmap_handler.yaml")
			Expect(err).NotTo(HaveOccurred())
			var op opv1a1.Operator
			Expect(yaml.Unmarshal(yamlData, &op)).NotTo(HaveOccurred())

			setupLog.Info("adding new operator")
			Expect(k8sClient.Create(ctx, &op)).Should(Succeed())

			key := client.ObjectKeyFromObject(&op)
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := k8sClient.Get(ctx, key, get)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})

		It("should add an annotation to a Deployment we start", func() {
			ctrl.Log.Info("loading deployment")
			Expect(k8sClient.Create(ctx, dp1)).Should(Succeed())

			ctrl.Log.Info("loading configmap")
			Expect(k8sClient.Create(ctx, cm)).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := k8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp1)
				dpget := &appsv1.Deployment{}
				if err := k8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				return ok && rv == cmget.GetResourceVersion()
			}, timeout, interval).Should(BeTrue())
		})

		It("should adjust the annotation when the configmap is updated", func() {
			ctrl.Log.Info("updating configmap")
			cmup := testutils.TestConfigMap.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, cmup, func() error {
				return unstructured.SetNestedField(cmup.UnstructuredContent(), "value3", "data", "key3")
			})
			Expect(err).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := k8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp1)
				dpget := &appsv1.Deployment{}
				if err := k8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				return ok && rv == cmget.GetResourceVersion()
			}, timeout, interval).Should(BeTrue())
		})

		It("should handle the addition of another Deployment referring to the same ConfigMap", func() {
			ctrl.Log.Info("adding new referring Deployment")
			Expect(k8sClient.Create(ctx, dp2)).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := k8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp2)
				dpget := &appsv1.Deployment{}
				if err := k8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				return ok && rv == cmget.GetResourceVersion()
			}, timeout, interval).Should(BeTrue())
		})

		It("should update both Deployments when the Service changes", func() {
			ctrl.Log.Info("updating ConfigMap")
			cmup := testutils.TestConfigMap.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, cmup, func() error {
				return unstructured.SetNestedField(cmup.UnstructuredContent(), "value4", "data", "key3")
			})
			Expect(err).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := k8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp1)
				dpget := &appsv1.Deployment{}
				if err := k8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				if !ok || rv != cmget.GetResourceVersion() {
					return false
				}

				dpkey = client.ObjectKeyFromObject(dp2)
				dpget = &appsv1.Deployment{}
				if err := k8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns = dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok = anns[deploymentMarkerAnnotationName]
				if !ok || rv != cmget.GetResourceVersion() {
					return false
				}

				return true
			}, timeout, interval).Should(BeTrue())
		})

		It("should delete the objects added", func() {
			ctrl.Log.Info("deleting objects")
			Expect(k8sClient.Delete(ctx, dp1)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, dp2)).Should(Succeed())
			Expect(k8sClient.Delete(ctx, cm)).Should(Succeed())
		})
	})
})
