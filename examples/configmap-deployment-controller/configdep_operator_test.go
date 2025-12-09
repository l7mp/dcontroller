// KUBEBUILDER_ASSETS="/export/l7mp/dcontroller/bin/k8s/1.30.0-linux-amd64" go test ./... -v #-ginkgo.v -ginkgo.trace

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/kubernetes/controllers"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/testsuite"
)

const (
	OperatorSpecFile               = "configdeployment-operator.yaml"
	deploymentMarkerAnnotationName = "dcontroller.io/configmap-version"

	timeout  = time.Second * 10
	interval = time.Millisecond * 250
	// loglevel = 1
	// loglevel = -10
	loglevel int8 = -5
)

var (
	suite            *testsuite.Suite
	cfg              *rest.Config
	scheme           = runtime.NewScheme()
	k8sClient        client.Client
	logger, setupLog logr.Logger
)

var _ = BeforeSuite(func() {
	opts := zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(4),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger = ctrl.Log
	setupLog = logger.WithName("setup")

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	opv1a1.AddToScheme(scheme)

	var err error
	suite, err = testsuite.New(loglevel, filepath.Join("..", "..", "config", "crd", "resources"), "./")
	Expect(err).NotTo(HaveOccurred())
	k8sClient = suite.K8sClient
	cfg = suite.Cfg
})

var _ = AfterSuite(func() { suite.Close() })

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Configmap-deployment operator test")
}

var _ = Describe("Configmap-deployment controller test:", Ordered, func() {
	var dp1, dp2, dp3, cm1, cm2, cd1, cd2, cd3 object.Object
	var ctx context.Context // context for the endpointslice controller
	var cancel context.CancelFunc

	BeforeAll(func() {
		ctx, cancel = context.WithCancel(context.Background())

		// each dp relate to the same configmapach
		dp1 = testutils.TestDeployment.DeepCopy()
		dp1.SetName("test-deployment-1")
		dp1.SetNamespace("default")
		dp2 = testutils.TestDeployment.DeepCopy()
		dp2.SetName("test-deployment-2")
		dp2.SetNamespace("default")

		cm1 = testutils.TestConfigMap.DeepCopy()
		cm1.SetName("test-configmap-1")
		cm1.SetNamespace("default")
		cm2 = testutils.TestConfigMap.DeepCopy()
		cm2.SetName("test-configmap-2")
		cm2.SetNamespace("default")

		cd1 = testutils.TestConfigMapDeployment.DeepCopy()
		cd1.SetName("test-dep-config-1")
		cd1.SetNamespace("default")
		err := unstructured.SetNestedMap(cd1.UnstructuredContent(),
			map[string]any{
				"configMap":  "test-configmap-1",
				"deployment": "test-deployment-1",
			}, "spec")
		Expect(err).NotTo(HaveOccurred())
		cd2 = testutils.TestConfigMapDeployment.DeepCopy()
		cd2.SetName("test-dep-config-2")
		cd2.SetNamespace("default")
		err = unstructured.SetNestedMap(cd2.UnstructuredContent(),
			map[string]any{
				"configMap":  "test-configmap-2",
				"deployment": "test-deployment-2",
			}, "spec")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		cancel()
	})

	It("should create and start the operator controller", func() {
		setupLog.Info("setting up operator controller")
		c, err := controllers.NewOpController(cfg, nil, ctrl.Options{
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
		yamlData, err := os.ReadFile(OperatorSpecFile)
		Expect(err).NotTo(HaveOccurred())
		var op opv1a1.Operator
		Expect(yaml.Unmarshal(yamlData, &op)).NotTo(HaveOccurred())

		setupLog.Info("adding new operator")
		Expect(k8sClient.Create(ctx, &op)).Should(Succeed())

		key := client.ObjectKeyFromObject(&op)
		Eventually(func() bool {
			get := &opv1a1.Operator{}
			err := k8sClient.Get(ctx, key, get)
			return err == nil && len(get.Status.Controllers) != 0
		}, timeout, interval).Should(BeTrue())
	})

	It("should complete the operator status", func() {
		key := types.NamespacedName{Namespace: "default", Name: "configdep-operator"}
		opget := &opv1a1.Operator{}
		err := k8sClient.Get(ctx, key, opget)
		Expect(err).NotTo(HaveOccurred())

		ctrlStatuses := opget.Status.Controllers
		Expect(ctrlStatuses).NotTo(BeEmpty())

		ctrlStatus := ctrlStatuses[0]
		Expect(ctrlStatus.Name).To(Equal("configmap-controller"))
		Expect(ctrlStatus.LastErrors).To(BeEmpty())
		Expect(ctrlStatus.Conditions).NotTo(BeEmpty())
		cond := ctrlStatus.Conditions[0]
		Expect(cond.Type).To(Equal(string(opv1a1.ControllerConditionReady)))
		Expect(cond.Reason).To(Equal(string(opv1a1.ControllerReasonReady)))
		Expect(cond.Message).NotTo(BeEmpty())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
	})

	It("should add an annotation to a Deployment", func() {
		ctrl.Log.Info("loading deployments")
		Expect(k8sClient.Create(ctx, dp1)).Should(Succeed())
		Expect(k8sClient.Create(ctx, dp2)).Should(Succeed())

		ctrl.Log.Info("loading configmaps")
		Expect(k8sClient.Create(ctx, cm1)).Should(Succeed())
		Expect(k8sClient.Create(ctx, cm2)).Should(Succeed())

		ctrl.Log.Info("loading the first config-dep")
		Expect(k8sClient.Create(ctx, cd1)).Should(Succeed())

		Eventually(func() bool {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cm1), cm); err != nil {
				return false
			}
			cm = &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cm2), cm); err != nil {
				return false
			}
			dp := &appsv1.Deployment{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(dp1), dp); err != nil {
				return false
			}
			dp = &appsv1.Deployment{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(dp2), dp); err != nil {
				return false
			}
			cd := object.New()
			cd.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "dcontroller.io",
				Version: "v1alpha1",
				Kind:    "ConfigDeployment",
			})
			cd.SetName("test-dep-config-1")
			cd.SetNamespace("default")
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cd), cd); err != nil {
				return false
			}

			return true
		}, timeout, interval).Should(BeTrue())
	})

	It("should adjust the first deployment's annotation to the configmap's resource-version", func() {
		Eventually(func() bool { return checkDepCM(ctx, dp1, cm1) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepNoAnns(ctx, dp2) }, timeout, interval).Should(BeTrue())
	})

	It("should handle the second deployment when a new config-dep is injected", func() {
		ctrl.Log.Info("loading the first config-dep")
		Expect(k8sClient.Create(ctx, cd2)).Should(Succeed())
		Eventually(func() bool { return checkDepCM(ctx, dp2, cm2) }, timeout, interval).Should(BeTrue())
	})

	It("should handle the addition of another Deployment referring to the same ConfigMap", func() {
		ctrl.Log.Info("adding new referring Deployment")
		dp3 = testutils.TestDeployment.DeepCopy()
		dp3.SetName("test-deployment-3")
		dp3.SetNamespace("default")
		Expect(k8sClient.Create(ctx, dp3)).Should(Succeed())

		cd3 = testutils.TestConfigMapDeployment.DeepCopy()
		cd3.SetName("test-dep-config-3")
		cd3.SetNamespace("default")
		err := unstructured.SetNestedMap(cd3.UnstructuredContent(),
			map[string]any{
				"configMap":  "test-configmap-1",
				"deployment": "test-deployment-3",
			}, "spec")
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, cd3)).Should(Succeed())

		Eventually(func() bool { return checkDepCM(ctx, dp1, cm1) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepCM(ctx, dp2, cm2) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepCM(ctx, dp3, cm1) }, timeout, interval).Should(BeTrue())
	})

	It("should adjust the annotations when the configmap is updated", func() {
		ctrl.Log.Info("updating configmap")
		cm := cm1.DeepCopy()
		_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, cm, func() error {
			return unstructured.SetNestedField(cm.UnstructuredContent(), "value3", "data", "key3")
		})
		Expect(err).Should(Succeed())

		Eventually(func() bool { return checkDepCM(ctx, dp1, cm1) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepCM(ctx, dp2, cm2) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepCM(ctx, dp3, cm1) }, timeout, interval).Should(BeTrue())
	})

	It("should remove the annotations when the configmap is deleted", func() {
		ctrl.Log.Info("deleting configmap")
		Expect(k8sClient.Delete(ctx, cm1)).Should(Succeed())

		Eventually(func() bool { return checkDepNoAnns(ctx, dp1) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepCM(ctx, dp2, cm2) }, timeout, interval).Should(BeTrue())
		Eventually(func() bool { return checkDepNoAnns(ctx, dp3) }, timeout, interval).Should(BeTrue())
	})

	It("should delete the objects added", func() {
		ctrl.Log.Info("deleting objects")
		Expect(k8sClient.Delete(ctx, dp1)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, dp2)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, dp3)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, cm2)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, cd1)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, cd2)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, cd3)).Should(Succeed())
	})
})

func checkDepCM(ctx context.Context, dp, cm object.Object) bool {
	cmget := &corev1.ConfigMap{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cm), cmget); err != nil {
		return false
	}

	// ctrl.Log.Info("cm", "resource", util.Stringify(cmget))

	dpget := &appsv1.Deployment{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(dp), dpget); err != nil {
		return false
	}

	// ctrl.Log.Info("dp", "resource", util.Stringify(dpget))

	anns := dpget.Spec.Template.GetAnnotations()
	if len(anns) == 0 {
		return false
	}

	rv, ok := anns[deploymentMarkerAnnotationName]
	return ok && rv == cmget.GetResourceVersion()
}

func checkDepNoAnns(ctx context.Context, dp object.Object) bool {
	dpget := &appsv1.Deployment{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(dp), dpget); err != nil {
		return false
	}
	_, ok := dpget.Spec.Template.GetAnnotations()[deploymentMarkerAnnotationName]
	return !ok
}
