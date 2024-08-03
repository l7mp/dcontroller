package view

import (
	"context"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"hsnlab/dcontroller-runtime/pkg/object"
)

var _ = Describe("Cache", func() {
	var (
		testObj client.Object
		ctx     = logr.NewContext(context.Background(), logger)
	)

	BeforeEach(func() {
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
	})

	Describe("Reconciling views", func() {
		It("should allow a view with empty pipeline to be reconciled", func() {
			testView, err := NewFakeBaseView("view", scheme, pipeline.NewEmptyPipeline(), testObj)
			Expect(err).NotTo(HaveOccurred())

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-service",
					Namespace: "default",
				},
			}

			// Call Reconcile

			result, err := testView.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			// Fetch the updated object
			updatedObj := object.New("view").WithName("default", "test-svc")
			err = testView.GetCache().NewClient().Get(context.TODO(), req.NamespacedName, updatedObj)
			Expect(err).NotTo(HaveOccurred())

			// The view should contain the testObj packaged up into an object
			obj, err := object.FromNativeObject("view", testObj)
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedObj.DeepEqual(obj)).To(BeTrue())
		})
	})
})
