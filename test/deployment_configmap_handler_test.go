// A simpler configmap-deployment operator that does not use CRDs. For a complete example, see
// examples/configmap-deployment-controller.

package integration

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/kubernetes/controllers"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/testsuite"
)

const (
	// relatedConfigMapAnnotationName = "dcontroller.io/related-configmap"
	deploymentMarkerAnnotationName = "dcontroller.io/configmap-version"
)

var _ = Describe("Deployment handler operator test:", Ordered, func() {
	// restart Deployments whenever the related ConfigMap changes
	Context("When creating a Deployment handler operator", Ordered, Label("operator"), func() {
		var (
			suite        *testsuite.Suite
			ctx          context.Context
			cancel       context.CancelFunc
			api          *cache.API
			dp1, dp2, cm object.Object
			off          = true
		)

		BeforeAll(func() {
			var err error
			suite, err = testsuite.New(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel = context.WithCancel(suite.Ctx)

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
			suite.Close()
		})

		It("should create and start the operator controller", func() {
			suite.Log.Info("setting up operator controller")
			var err error
			api, err = cache.NewAPI(suite.Cfg, cache.APIOptions{
				CacheOptions: cache.CacheOptions{Logger: suite.Log},
			})
			Expect(err).NotTo(HaveOccurred())

			c, err := controllers.NewOpController(suite.Cfg, api.Cache, ctrl.Options{
				Scheme:                 suite.Scheme,
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

			suite.Log.Info("starting shared view storage")
			go func() {
				defer GinkgoRecover()
				err := api.Cache.Start(ctx)
				Expect(err).NotTo(HaveOccurred(), "failed to shared cache")
			}()

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := c.Start(ctx)
				Expect(err).ToNot(HaveOccurred(), "failed to run operator controller")
			}()
		})

		It("should let an operator to be attached to the manager", func() {
			suite.Log.Info("reading YAML file")
			yamlData, err := os.ReadFile("deployment_configmap_handler.yaml")
			Expect(err).NotTo(HaveOccurred())
			var op opv1a1.Operator
			Expect(yaml.Unmarshal(yamlData, &op)).NotTo(HaveOccurred())

			suite.Log.Info("adding new operator")
			Expect(suite.K8sClient.Create(ctx, &op)).Should(Succeed())

			key := client.ObjectKeyFromObject(&op)
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctx, key, get)
				return err == nil
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should add an annotation to a Deployment we start", func() {
			suite.Log.Info("loading deployment")
			Expect(suite.K8sClient.Create(ctx, dp1)).Should(Succeed())

			suite.Log.Info("loading configmap")
			Expect(suite.K8sClient.Create(ctx, cm)).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := suite.K8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp1)
				dpget := &appsv1.Deployment{}
				if err := suite.K8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				return ok && rv == cmget.GetResourceVersion()
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should adjust the annotation when the configmap is updated", func() {
			suite.Log.Info("updating configmap")
			cmup := testutils.TestConfigMap.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, cmup, func() error {
				return unstructured.SetNestedField(cmup.UnstructuredContent(), "value3", "data", "key3")
			})
			Expect(err).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := suite.K8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp1)
				dpget := &appsv1.Deployment{}
				if err := suite.K8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				return ok && rv == cmget.GetResourceVersion()
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should handle the addition of another Deployment referring to the same ConfigMap", func() {
			suite.Log.Info("adding new referring Deployment")
			Expect(suite.K8sClient.Create(ctx, dp2)).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cm)
				cmget := &corev1.ConfigMap{}
				if err := suite.K8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp2)
				dpget := &appsv1.Deployment{}
				if err := suite.K8sClient.Get(ctx, dpkey, dpget); err != nil {
					return false
				}

				anns := dpget.Spec.Template.GetAnnotations()
				if len(anns) == 0 {
					return false
				}

				rv, ok := anns[deploymentMarkerAnnotationName]
				return ok && rv == cmget.GetResourceVersion()
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should update both Deployments when the Service changes", func() {
			suite.Log.Info("updating ConfigMap")
			cmup := testutils.TestConfigMap.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, cmup, func() error {
				return unstructured.SetNestedField(cmup.UnstructuredContent(), "value4", "data", "key3")
			})
			Expect(err).Should(Succeed())

			Eventually(func() bool {
				cmkey := client.ObjectKeyFromObject(cmup)
				cmget := &corev1.ConfigMap{}
				if err := suite.K8sClient.Get(ctx, cmkey, cmget); err != nil {
					return false
				}

				dpkey := client.ObjectKeyFromObject(dp1)
				dpget := &appsv1.Deployment{}
				if err := suite.K8sClient.Get(ctx, dpkey, dpget); err != nil {
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
				if err := suite.K8sClient.Get(ctx, dpkey, dpget); err != nil {
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
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should delete the operator", func() {
			suite.Log.Info("deleting operator")
			yamlData, err := os.ReadFile("deployment_configmap_handler.yaml")
			Expect(err).NotTo(HaveOccurred())
			var op opv1a1.Operator
			Expect(yaml.Unmarshal(yamlData, &op)).NotTo(HaveOccurred())
			Expect(suite.K8sClient.Delete(ctx, &op)).Should(Succeed())
		})

		It("should delete the objects added", func() {
			suite.Log.Info("deleting objects")
			Expect(suite.K8sClient.Delete(ctx, dp1)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, dp2)).Should(Succeed())
			Expect(suite.K8sClient.Delete(ctx, cm)).Should(Succeed())
		})
	})
})
