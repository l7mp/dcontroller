package reconciler

import (
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	// "k8s.io/client-go/util/workqueue"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/composite"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("Virtual Sources", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		queue  workqueue.TypedRateLimitingInterface[Request]
		mgr    runtimeManager.Manager
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		var err error
		mgr, err = manager.NewFakeManager("test", runtimeManager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		queue = workqueue.NewTypedRateLimitingQueue[Request](workqueue.DefaultTypedControllerRateLimiter[Request]())
	})

	AfterEach(func() {
		cancel()
		queue.ShutDown()
	})

	It("should unmarshal a OneShot source from YAML", func() {
		sourceYAML := `
apiGroup: oneshot.virtual-source.dcontroller.io
kind: TestOneShotTrigger
`
		var source opv1a1.Source
		err := yaml.Unmarshal([]byte(sourceYAML), &source)
		Expect(err).ToNot(HaveOccurred())

		Expect(source.Group).ToNot(BeNil())
		Expect(*source.Group).To(Equal(opv1a1.OneShotSourceGroupVersion.Group))
		Expect(source.Kind).To(Equal("TestOneShotTrigger"))
	})

	It("OneShot Source should trigger exactly once", func() {
		group := opv1a1.OneShotSourceGroupVersion.Group
		s := newOneShotSource(mgr, "test", opv1a1.Source{
			Resource: opv1a1.Resource{
				Group: &group,
				Kind:  "TestOneShotTrigger",
			},
		})
		src, err := s.GetSource()
		Expect(err).NotTo(HaveOccurred())

		err = src.Start(ctx, queue)
		Expect(err).ToNot(HaveOccurred())

		// Should receive one event
		Eventually(func() int {
			return queue.Len()
		}, time.Second, 50*time.Millisecond).Should(Equal(1))

		// Wait a bit more and verify no additional events
		Consistently(func() int {
			return queue.Len()
		}, 100*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

		// Verify we can dequeue the event
		item, shutdown := queue.Get()
		Expect(shutdown).To(BeFalse())
		Expect(item).ToNot(BeNil())
		gvk := item.GVK
		Expect(gvk.Group).To(Equal(viewv1a1.Group("test")))
		Expect(gvk.Kind).To(Equal("TestOneShotTrigger"))
		Expect(item.Name).To(Equal(OneShotSourceObjectName))
		Expect(item.Namespace).To(Equal(""))
		Expect(queue.Len()).To(Equal(0))

		// Verify that the object can be listed from the cache
		list := composite.NewViewObjectList("test", "TestOneShotTrigger")
		Eventually(func() bool {
			err := mgr.GetClient().List(ctx, list)
			return err == nil && len(list.Items) == 1
		}, time.Second, 50*time.Millisecond).Should(BeTrue())
		listObj := list.Items[0]
		Expect(listObj.GetName()).To(Equal(OneShotSourceObjectName))
		Expect(listObj.GetLabels()).To(HaveKey(VirtualSourceTriggeredLabel))
		queue.Done(item)

		// Verify that the object can be fetched from the cache
		obj := object.NewViewObject("test", "TestOneShotTrigger")
		Eventually(func() bool {
			err := mgr.GetClient().Get(ctx, client.ObjectKey{Name: OneShotSourceObjectName}, obj)
			return err == nil
		}, time.Second, 50*time.Millisecond).Should(BeTrue())
		Expect(obj.GetName()).To(Equal(OneShotSourceObjectName))
		Expect(obj.GetLabels()).To(HaveKey(VirtualSourceTriggeredLabel))

		queue.Done(item)
	})

	It("should unmarshal a complete Periodic source from YAML", func() {
		sourceYAML := `
apiGroup: periodic.virtual-source.dcontroller.io
kind: TestPeriodicTrigger
parameters:
  period: "10ms"`
		var source opv1a1.Source
		err := yaml.Unmarshal([]byte(sourceYAML), &source)
		Expect(err).ToNot(HaveOccurred())

		// Verify fields
		Expect(source.Group).ToNot(BeNil())
		Expect(*source.Group).To(Equal(opv1a1.PeriodicSourceGroupVersion.Group))
		Expect(source.Kind).To(Equal("TestPeriodicTrigger"))
		Expect(source.Parameters).ToNot(BeNil())

		// Verify parameter parsing
		var params map[string]interface{}
		err = json.Unmarshal(source.Parameters.Raw, &params)
		Expect(err).ToNot(HaveOccurred())
		Expect(params["period"]).To(Equal("10ms"))
	})

	It("Periodic Source should trigger periodically", func() {
		group := opv1a1.PeriodicSourceGroupVersion.Group
		params := apiextensionsv1.JSON{
			Raw: []byte(`{"period": "5ms"}`),
		}
		s := newPeriodicSource(mgr, "test", opv1a1.Source{
			Resource: opv1a1.Resource{
				Group: &group,
				Kind:  "TestPeriodicTrigger",
			},
			Parameters: &params,
		})

		src, err := s.GetSource()
		Expect(err).NotTo(HaveOccurred())

		err = src.Start(ctx, queue)
		Expect(err).ToNot(HaveOccurred())

		// Count events over a period
		eventCount := 0
		deadline := time.Now().Add(20 * time.Millisecond)

		for time.Now().Before(deadline) {
			if queue.Len() > 0 {
				item, shutdown := queue.Get()
				if shutdown {
					break
				}
				eventCount++
				queue.Done(item)
			}
			time.Sleep(1 * time.Millisecond)
		}

		// Should have received at least 2-3 events in 20ms with 5ms period
		Expect(eventCount).To(BeNumerically(">=", 2))
		Expect(eventCount).To(BeNumerically("<=", 5))

		// Verify that the object can be listed from the cache
		list := composite.NewViewObjectList("test", "TestPeriodicTrigger")
		Eventually(func() bool {
			err := mgr.GetClient().List(ctx, list)
			return err == nil && len(list.Items) == 1
		}, time.Second, 50*time.Millisecond).Should(BeTrue())
		listObj := list.Items[0]
		Expect(listObj.GetName()).To(Equal(PeriodicSourceObjectName))
		Expect(listObj.GetNamespace()).To(Equal(""))
		Expect(listObj.GetLabels()).To(HaveKey(VirtualSourceTriggeredLabel))

		// Verify that the object can be fetched from the cache
		obj := object.NewViewObject("test", "TestPeriodicTrigger")
		Eventually(func() bool {
			err := mgr.GetClient().Get(ctx, client.ObjectKey{Name: PeriodicSourceObjectName}, obj)
			return err == nil
		}, time.Second, 50*time.Millisecond).Should(BeTrue())
		Expect(obj.GetName()).To(Equal(PeriodicSourceObjectName))
		Expect(obj.GetLabels()).To(HaveKey(VirtualSourceTriggeredLabel))
	})
})
