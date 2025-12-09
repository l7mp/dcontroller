// KUBEBUILDER_ASSETS="/export/l7mp/dcontroller/bin/k8s/1.30.0-linux-amd64" go test ./... -v #-ginkgo.v -ginkgo.trace

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
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
	OperatorSpecFile     = "svc-health-operator-test.yaml"
	healthAnnotationName = "dcontroller.io/pod-ready"

	timeout  = time.Second * 10
	interval = time.Millisecond * 250
	// loglevel = 1
	// loglevel = -10
	loglevel int8 = -5
)

var (
	suite               *testsuite.Suite
	cfg                 *rest.Config
	scheme              = runtime.NewScheme()
	k8sClient, opClient client.Client
	logger, setupLog    logr.Logger
	api                 *cache.API
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
	RunSpecs(t, "Service health monitor operator test")
}

var _ = Describe("Service health monitor controller test:", Ordered, func() {
	var podView1, podView2, podView3, svc1, svc2 object.Object
	var ctx context.Context // context for the service health monitor controller
	var cancel context.CancelFunc

	BeforeAll(func() {
		ctx, cancel = context.WithCancel(context.Background())

		// Create test pods as view objects (these will go into PodView)
		podView1 = object.NewViewObject("svc-health-operator", "PodView")
		object.SetName(podView1, "default", "web-app-pod-1")
		podView1.SetLabels(map[string]string{"app": "web-app"})
		unstructured.SetNestedSlice(podView1.UnstructuredContent(), []any{
			map[string]any{
				"type":   "Ready",
				"status": "True",
			},
		}, "status", "conditions")

		podView2 = object.NewViewObject("svc-health-operator", "PodView")
		object.SetName(podView2, "default", "web-app-pod-2")
		podView2.SetLabels(map[string]string{"app": "web-app"})
		unstructured.SetNestedSlice(podView2.UnstructuredContent(), []any{
			map[string]any{
				"type":   "Ready",
				"status": "True",
			},
		}, "status", "conditions")

		podView3 = object.NewViewObject("svc-health-operator", "PodView")
		object.SetName(podView3, "default", "web-app-pod-3")
		podView3.SetLabels(map[string]string{"app": "web-app"})
		unstructured.SetNestedSlice(podView3.UnstructuredContent(), []any{
			map[string]any{
				"type":   "Ready",
				"status": "False",
			},
		}, "status", "conditions")

		// Create test services (these will go to envtest)
		svc1 = testutils.TestSvc.DeepCopy()
		svc1.SetName("web-app")
		svc1.SetNamespace("default")

		svc2 = testutils.TestSvc.DeepCopy()
		svc2.SetName("other-app")
		svc2.SetNamespace("default")
	})

	AfterAll(func() {
		cancel()
	})

	It("should create and start the operator controller", func() {
		setupLog.Info("setting up operator controller")
		var err error
		api, err = cache.NewAPI(suite.Cfg, cache.APIOptions{
			CacheOptions: cache.CacheOptions{Logger: suite.Log},
		})
		Expect(err).NotTo(HaveOccurred())
		c, err := controllers.NewOpController(cfg, api.Cache, ctrl.Options{
			Scheme:                 scheme,
			LeaderElection:         false, // disable leader-election
			HealthProbeBindAddress: "0",   // disable health-check
			Metrics: metricsserver.Options{
				BindAddress: "0", // disable the metrics server
			},
			Logger: logger,
		})
		Expect(err).NotTo(HaveOccurred())

		setupLog.Info("obtaining operator client")
		opClient = c.GetClient()
		Expect(opClient).NotTo(BeNil())

		suite.Log.Info("starting shared view storage")
		go func() {
			defer GinkgoRecover()
			err := api.Cache.Start(ctx)
			Expect(err).NotTo(HaveOccurred(), "failed to start API server cache")
		}()

		setupLog.Info("starting operator controller")
		go func() {
			defer GinkgoRecover()
			err := c.Start(ctx)
			Expect(err).ToNot(HaveOccurred(), "failed to run controller")
		}()
	})

	It("should let an operator to be loaded", func() {
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
		key := types.NamespacedName{Name: "svc-health-operator"}
		opget := &opv1a1.Operator{}
		err := k8sClient.Get(ctx, key, opget)
		Expect(err).NotTo(HaveOccurred())

		ctrlStatuses := opget.Status.Controllers
		Expect(ctrlStatuses).To(HaveLen(2))

		for _, ctrlStatus := range ctrlStatuses {
			Expect(ctrlStatus.LastErrors).To(BeEmpty())
			Expect(ctrlStatus.Conditions).NotTo(BeEmpty())
			cond := ctrlStatus.Conditions[0]
			Expect(cond.Type).To(Equal(string(opv1a1.ControllerConditionReady)))
			Expect(cond.Reason).To(Equal(string(opv1a1.ControllerReasonReady)))
			Expect(cond.Message).NotTo(BeEmpty())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}
	})

	It("should add health annotation to Service when pods are ready", func() {
		ctrl.Log.Info("loading service")
		Expect(k8sClient.Create(ctx, svc1)).Should(Succeed())

		ctrl.Log.Info("injecting pod views")
		Expect(opClient.Create(ctx, podView1)).Should(Succeed())
		Expect(opClient.Create(ctx, podView2)).Should(Succeed())

		Eventually(func() bool {
			return checkServiceHealthAnnotation(ctx, svc1, "2/2")
		}, timeout, interval).Should(BeTrue())
	})

	It("should update health annotation when pod becomes unhealthy", func() {
		ctrl.Log.Info("adding unhealthy pod view")
		Expect(opClient.Create(ctx, podView3)).Should(Succeed())

		Eventually(func() bool {
			return checkServiceHealthAnnotation(ctx, svc1, "2/3")
		}, timeout, interval).Should(BeTrue())
	})

	It("should update health annotation when unhealthy pod becomes ready", func() {
		ctrl.Log.Info("updating podView3 to ready")
		podView3get := podView3.DeepCopy()
		unstructured.SetNestedSlice(podView3get.UnstructuredContent(), []any{
			map[string]any{
				"type":   "Ready",
				"status": "True",
			},
		}, "status", "conditions")

		Expect(opClient.Update(ctx, podView3get)).Should(Succeed())

		Eventually(func() bool {
			return checkServiceHealthAnnotation(ctx, svc1, "3/3")
		}, timeout, interval).Should(BeTrue())
	})

	It("should remove health annotation when pods are deleted", func() {
		ctrl.Log.Info("deleting pod views")
		Expect(opClient.Delete(ctx, podView1)).Should(Succeed())
		Expect(opClient.Delete(ctx, podView2)).Should(Succeed())
		Expect(opClient.Delete(ctx, podView3)).Should(Succeed())

		// We no longer run target.delete for patcher targets
		Eventually(func() bool {
			return checkServiceNoHealthAnnotation(ctx, svc1)
		}, timeout, interval).Should(BeTrue())
	})

	It("should not affect services without matching pods", func() {
		ctrl.Log.Info("loading service without matching pods")
		Expect(k8sClient.Create(ctx, svc2)).Should(Succeed())

		// Give some time for any potential processing
		time.Sleep(250 * time.Millisecond)

		Consistently(func() bool {
			return checkServiceNoHealthAnnotation(ctx, svc2)
		}, time.Second, interval).Should(BeTrue())
	})

	It("should delete the objects added", func() {
		ctrl.Log.Info("deleting objects")
		Expect(k8sClient.Delete(ctx, svc1)).Should(Succeed())
		Expect(k8sClient.Delete(ctx, svc2)).Should(Succeed())
	})
})

func checkServiceHealthAnnotation(ctx context.Context, svc object.Object, expectedHealth string) bool {
	svcget := &corev1.Service{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(svc), svcget); err != nil {
		return false
	}

	anns := svcget.GetAnnotations()
	if len(anns) == 0 {
		return false
	}

	health, ok := anns[healthAnnotationName]
	return ok && health == expectedHealth
}

func checkServiceNoHealthAnnotation(ctx context.Context, svc object.Object) bool {
	svcget := &corev1.Service{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(svc), svcget); err != nil {
		return false
	}

	anns := svcget.GetAnnotations()
	if len(anns) == 0 {
		return true
	}

	_, ok := anns[healthAnnotationName]
	return !ok
}
