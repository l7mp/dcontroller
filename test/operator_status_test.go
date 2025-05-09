package integration

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/operator"
	"github.com/l7mp/dcontroller/pkg/util"
)

var _ = Describe("Operator status report test:", Ordered, func() {
	Context("When creating a controller with an invalid config", Ordered, Label("controller"), func() {
		var (
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
		)

		BeforeAll(func() {
			ctrlCtx, ctrlCancel = context.WithCancel(context.Background())
		})

		AfterAll(func() {
			ctrlCancel()
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
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: bogus-operator
spec:
  controllers:
    - name: bogus-controller
      sources: []
      pipeline:
        '@aggregate':
          - '@select': whatever
      target:
        kind: myview`

			var op opv1a1.Operator
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			setupLog.Info("adding new operator")
			Expect(k8sClient.Create(ctx, &op)).Should(Succeed())
		})

		It("should set the accepted operator status to False", func() {
			key := types.NamespacedName{Name: "bogus-operator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := k8sClient.Get(ctx, key, get)
				if err != nil {
					fmt.Println(util.Stringify(err))
					return false
				}
				if len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				return status.Conditions[0].Status == metav1.ConditionFalse
			}, timeout, interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Type).Should(
				Equal(string(opv1a1.ControllerConditionReady)))
			Expect(cond.Status).Should(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonNotReady)))
		})

		It("should survive deleting the operator", func() {
			ctrl.Log.Info("deleting op")
			op := opv1a1.Operator{}
			op.SetName("bogus-operator")
			Expect(k8sClient.Delete(ctrlCtx, &op)).Should(Succeed())
		})
	})

	// write container-num into pods
	Context("When applying a controller with a pipeline that makes a runtime error", Ordered, Label("controller"), func() {
		var (
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
			pod        object.Object
			op         opv1a1.Operator
		)

		BeforeAll(func() {
			ctrlCtx, ctrlCancel = context.WithCancel(context.Background())
			pod = testutils.TestPod.DeepCopy()
		})

		AfterAll(func() {
			ctrlCancel()
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
        "@aggregate":
          - "@project":                                
              metadata:                                
                name: "$.metadata.name"                # copy metadata.namespace and metadata.name
                namespace: "$.metadata.namespace"      
                annotations:                           
                  "dcontroller.io/container-num":      # add a new annotation indicating the number of containers
                    '@len': ["$.spec.containers"]      # value is the length of .spec.containers
        #            '@string':                        # should explicitly force string conversion
        #              '@len': ["$.spec.containers"]
      target:
        apiGroup: ""
        kind: Pod
        type: Patcher`
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			setupLog.Info("adding new operator")
			Expect(k8sClient.Create(ctx, &op)).Should(Succeed())
		})

		It("should create a nonempty status on the operator resource", func() {
			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := k8sClient.Get(ctx, key, get)
				if err != nil {
					return false
				}
				if len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				return true
			}, timeout, interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Type).Should(
				Equal(string(opv1a1.ControllerConditionReady)))
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonReady)))
		})

		It("should report a failure in the operator ready status conditions when inserting a pod", func() {
			setupLog.Info("adding pod")
			Expect(k8sClient.Create(ctx, pod)).Should(Succeed())

			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := k8sClient.Get(ctx, key, get)
				if err != nil || len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
				if cond == nil {
					return false
				}
				return cond.Status == metav1.ConditionUnknown
			}, timeout, interval).Should(BeTrue())

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
        "@aggregate":
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

			setupLog.Info("updating operator")
			_, err := ctrlutil.CreateOrUpdate(ctx, k8sClient, &op, func() error {
				newOp.Spec.DeepCopyInto(&op.Spec)
				return nil
			})
			Expect(err).Should(Succeed())

			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.ControllerStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := k8sClient.Get(ctx, key, get)
				if err != nil || len(get.Status.Controllers) != 1 {
					return false
				}
				status = get.Status.Controllers[0]
				cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
				if cond == nil {
					return false
				}
				return cond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.ControllerConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.ControllerReasonReady)))
		})

		It("should survive deleting the pod", func() {
			ctrl.Log.Info("deleting pod")
			Expect(k8sClient.Delete(ctrlCtx, pod)).Should(Succeed())
		})

		It("should survive deleting the operator", func() {
			ctrl.Log.Info("deleting op")
			Expect(k8sClient.Delete(ctrlCtx, &op)).Should(Succeed())
		})
	})
})
