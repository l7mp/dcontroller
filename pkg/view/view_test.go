package view

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"hsnlab/dcontroller-runtime/pkg/manager"
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

// func init() {
// 	corev1.AddToScheme(scheme)
// }

func TestView(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "View")
}

var _ = Describe("Cache", func() {
	var (
		mgr     *manager.FakeManager
		testObj client.Object
		ctx     context.Context
		cancel  context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(logr.NewContext(context.Background(), logger))
		testObj = &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"test-label-key": "test-label-value"},
				Ports: []corev1.ServicePort{{
					Name: "http",
					Port: 80,
				}},
				ClusterIP:             "1.2.3.4",
				Type:                  corev1.ServiceTypeNodePort,
				ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			},
		}
		mgr, _ = manager.NewFakeManager(ctx, logger)
		fmt.Println(mgr)
		fmt.Println(testObj)
	})

	AfterEach(func() {
		cancel()
	})

})

// 	Describe("Reconciling views", func() {
// 		It("should allow a view with empty pipeline to be reconciled", func() {
// 			// jsonData := `{"@aggregate":[{"@select":{"@eq":["$.metadata.name","name"]}}]}`
// 			// var ag Aggregation
// 			// err := json.Unmarshal([]byte(jsonData), &ag)
// 			// Expect(err).NotTo(HaveOccurred())

// 			// testView, err := NewBaseView("view", mgr, pipeline.NewEmptyPipeline(), testObj)
// 			// Expect(err).NotTo(HaveOccurred())

// 			// req := reconcile.Request{
// 			// 	NamespacedName: types.NamespacedName{
// 			// 		Name:      "test-service",
// 			// 		Namespace: "default",
// 			// 	},
// 			// }

// 			// // Call Reconcile

// 			// result, err := testView.Reconcile(ctx, req)
// 			// Expect(err).NotTo(HaveOccurred())
// 			// Expect(result).To(Equal(reconcile.Result{}))

// 			// // Fetch the updated object
// 			// updatedObj := object.New("view").WithName("default", "test-svc")
// 			// err = testView.GetCache().NewClient().Get(context.TODO(), req.NamespacedName, updatedObj)
// 			// Expect(err).NotTo(HaveOccurred())

// 			// // The view should contain the testObj packaged up into an object
// 			// obj, err := object.FromNativeObject("view", testObj)
// 			// Expect(err).NotTo(HaveOccurred())
// 			// Expect(updatedObj.DeepEqual(obj)).To(BeTrue())
// 		})
// 	})
// })
