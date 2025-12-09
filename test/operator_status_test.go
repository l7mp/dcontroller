package integration

import (
	"context"
	"path/filepath"
	"runtime/debug"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
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

var _ = Describe("Operator status report test:", Ordered, func() {
	Context("When creating a controller with an invalid config", Ordered, Label("controller"), func() {
		var (
			suite      *testsuite.Suite
			off        = true
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
		)

		BeforeAll(func() {
			var err error
			suite, err = testsuite.New(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctrlCtx, ctrlCancel = context.WithCancel(suite.Ctx)
		})

		AfterAll(func() {
			ctrlCancel()
			suite.Close()
		})

		It("should create and start the operator controller", func() {
			suite.Log.Info("setting up operator controller")
			var err error
			c, err := controllers.NewOpController(suite.Cfg, nil, ctrl.Options{
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

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := c.Start(ctrlCtx)
				if err != nil {
					debug.PrintStack()
				}
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should let an operator to be attached to the manager", func() {
			yamlData := `
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: bogus-operator
spec:
  controllers:
    - name: bogus-controller
      sources: []
      pipeline:
        - '@select': whatever
      target:
        kind: myview`

			var op opv1a1.Operator
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			suite.Log.Info("adding new operator")
			Expect(suite.K8sClient.Create(ctrlCtx, &op)).Should(Succeed())
		})

		It("should set the accepted operator status to False", func() {
			key := types.NamespacedName{Name: "bogus-operator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil {
					return false
				}
				if len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				return status.Conditions[0].Status == metav1.ConditionFalse
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Type).Should(
				Equal(string(opv1a1.ControllerConditionReady)))
			Expect(cond.Status).Should(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonNotReady)))
		})

		It("should survive deleting the operator", func() {
			suite.Log.Info("deleting op")
			op := opv1a1.Operator{}
			op.SetName("bogus-operator")
			Expect(suite.K8sClient.Delete(ctrlCtx, &op)).Should(Succeed())
		})
	})

	// write container-num into pods
	Context("When applying a controller with a pipeline that makes a runtime error", Ordered, Label("controller"), func() {
		var (
			suite      *testsuite.Suite
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
			api        *cache.API
			pod        object.Object
			op         opv1a1.Operator
		)

		BeforeAll(func() {
			var err error
			suite, err = testsuite.New(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctrlCtx, ctrlCancel = context.WithCancel(suite.Ctx)
			pod = testutils.TestPod.DeepCopy()
		})

		AfterAll(func() {
			ctrlCancel()
			suite.Close()
		})

		It("should create and start the operator controller", func() {
			suite.Log.Info("setting up operator controller")
			off := true
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
				err := api.Cache.Start(ctrlCtx)
				Expect(err).NotTo(HaveOccurred(), "failed to shared cache")
			}()

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := c.Start(ctrlCtx)
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should let an operator to be attached to the manager", func() {
			yamlData := `
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-container-num-annotator
spec:
  controllers:
    - name: pod-container-num-annotator
      sources:
        - apiGroup: ""
          kind: Pod
      pipeline:
        - "@project":                                
            metadata:                                
              name: "$.metadata.name"                # copy metadata.namespace and metadata.name
              namespace: "$.metadata.namespace"      
              annotations:                           
                "dcontroller.io/container-num":      # add a new annotation indicating the number of containers
                  '@len': ["$.spec.containers"]      # this is deliberately wrong (needs string coercion)
      target:
        apiGroup: ""
        kind: Pod
        type: Patcher`
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			suite.Log.Info("adding new operator")
			Expect(suite.K8sClient.Create(ctrlCtx, &op)).Should(Succeed())
		})

		It("should create a nonempty status on the operator resource", func() {
			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil {
					return false
				}
				if len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				return true
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Type).Should(
				Equal(string(opv1a1.ControllerConditionReady)))
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonReady)))
		})

		It("should report a failure in the operator ready status conditions when inserting a pod", func() {
			suite.Log.Info("adding pod")
			Expect(suite.K8sClient.Create(ctrlCtx, pod)).Should(Succeed())

			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil || len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
				if cond == nil {
					return false
				}
				return cond.Status == metav1.ConditionUnknown
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).Should(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonReconciliationFailed)))

			Expect(status.LastErrors).NotTo(BeEmpty())
		})

		It("should report a true ready status for the correct operator spec", func() {
			yamlData := `
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-container-num-annotator
spec:
  controllers:
    - name: pod-container-num-annotator
      sources:
        - apiGroup: ""
          kind: Pod
      pipeline:
        - "@project":                                
            metadata:                                
              name: "$.metadata.name"                # copy metadata.namespace and metadata.name
              namespace: "$.metadata.namespace"      
              annotations:                           
                "dcontroller.io/container-num":      # add a new annotation indicating the number of containers
                  '@string':                         # explicitly force string conversion
                    '@len': ["$.spec.containers"]
      target:
        apiGroup: ""
        kind: Pod
        type: Patcher`
			newOp := opv1a1.Operator{}
			Expect(yaml.Unmarshal([]byte(yamlData), &newOp)).NotTo(HaveOccurred())

			suite.Log.Info("updating operator")
			_, err := ctrlutil.CreateOrUpdate(ctrlCtx, suite.K8sClient, &op, func() error {
				newOp.Spec.DeepCopyInto(&op.Spec)
				return nil
			})
			Expect(err).Should(Succeed())

			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil || len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
				if cond == nil {
					return false
				}
				return cond.Status == metav1.ConditionTrue
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonReady)))
		})

		It("should survive deleting the pod", func() {
			suite.Log.Info("deleting pod")
			Expect(suite.K8sClient.Delete(ctrlCtx, pod)).Should(Succeed())
		})

		It("should survive deleting the operator", func() {
			suite.Log.Info("deleting op")
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				return suite.K8sClient.Delete(ctrlCtx, &op)
			})
			Expect(err).Should(Succeed())
		})
	})
})
