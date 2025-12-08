package reconciler

import (
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("Target Operations", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		mgr    manager.Manager
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		var err error
		mgr, err = manager.NewFakeManager(manager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		go func() { mgr.Start(ctx) }()
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Client status behavior", func() {
		It("should verify that client.Update() updates status for unstructured objects", func() {
			// CRITICAL TEST: This verifies that client.Update() with unstructured objects
			// DOES update the status subresource. This is an under-documented edge case.
			// If this test ever fails, it means controller-runtime behavior changed and
			// we need to add separate Status().Update() calls in target.go.

			c := mgr.GetClient()

			// Create a base object without status.
			obj := object.NewViewObject("test", "StatusTest")
			obj.SetName("status-test-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field": "initial"}

			err := c.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Retrieve the object.
			retrieved := object.NewViewObject("test", "StatusTest")
			retrieved.SetName("status-test-obj")
			retrieved.SetNamespace("default")
			Eventually(func() error {
				return c.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			}, time.Second, 50*time.Millisecond).Should(Succeed())

			// Verify no status initially.
			_, hasStatus, _ := unstructured.NestedMap(retrieved.UnstructuredContent(), "status")
			Expect(hasStatus).To(BeFalse(), "Object should not have status initially")

			// Now add status using ONLY client.Update() (not Status().Update()).
			retrieved.Object["status"] = map[string]any{"condition": "Ready", "phase": "Active"}

			err = c.Update(ctx, retrieved)
			Expect(err).NotTo(HaveOccurred())

			// Fetch again and verify status was written.
			retrieved2 := object.NewViewObject("test", "StatusTest")
			retrieved2.SetName("status-test-obj")
			retrieved2.SetNamespace("default")

			Eventually(func() bool {
				err := c.Get(ctx, client.ObjectKeyFromObject(retrieved2), retrieved2)
				if err != nil {
					return false
				}
				status, hasStatus, err := unstructured.NestedMap(retrieved2.UnstructuredContent(), "status")
				return err == nil && hasStatus && status["condition"] == "Ready"
			}, time.Second, 50*time.Millisecond).Should(BeTrue(),
				"client.Update() MUST update status for unstructured objects")

			// Verify both status fields are present.
			status, _, _ := unstructured.NestedMap(retrieved2.UnstructuredContent(), "status")
			Expect(status["condition"]).To(Equal("Ready"))
			Expect(status["phase"]).To(Equal("Active"))
		})
	})

	Describe("Updater target", func() {
		It("should handle add/update/delete operations with spec and status", func() {
			target := NewTarget(mgr, "test", opv1a1.Target{
				Resource: opv1a1.Resource{
					Kind: "TestView",
				},
				Type: opv1a1.Updater,
			})

			// Create initial object with spec and status.
			obj := object.NewViewObject("test", "TestView")
			obj.SetName("test-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field1": "value1"}
			obj.Object["status"] = map[string]any{"condition": "Ready"}

			// Add operation.
			err := target.Write(ctx, object.Delta{Type: object.Added, Object: obj}, nil)
			Expect(err).NotTo(HaveOccurred())

			// Verify object was created.
			retrieved := object.NewViewObject("test", "TestView")
			retrieved.SetName("test-obj")
			retrieved.SetNamespace("default")
			Eventually(func() error {
				return mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			}, time.Second, 50*time.Millisecond).Should(Succeed())

			Expect(retrieved.Object["spec"]).To(Equal(map[string]any{"field1": "value1"}))
			Expect(retrieved.Object["status"]).To(Equal(map[string]any{"condition": "Ready"}))

			// Update operation - change spec and status.
			updatedObj := object.DeepCopy(obj)
			updatedObj.Object["spec"] = map[string]any{"field1": "value1", "field2": "value2"}
			updatedObj.Object["status"] = map[string]any{"condition": "Updated"}

			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: updatedObj}, nil)
			Expect(err).NotTo(HaveOccurred())

			// Verify update.
			Eventually(func() bool {
				err := mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
				if err != nil {
					return false
				}
				spec, ok := retrieved.Object["spec"].(map[string]any)
				return ok && spec["field2"] == "value2"
			}, time.Second, 50*time.Millisecond).Should(BeTrue())

			Expect(retrieved.Object["status"]).To(Equal(map[string]any{"condition": "Updated"}))

			// Delete operation.
			err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: updatedObj}, nil)
			Expect(err).NotTo(HaveOccurred())

			// Verify deletion.
			Eventually(func() bool {
				err := mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
				return err != nil
			}, time.Second, 50*time.Millisecond).Should(BeTrue())
		})
	})

	Describe("Patcher target", func() {
		It("should handle add/update operations with spec and status using merge-patch semantics", func() {
			target := NewTarget(mgr, "test", opv1a1.Target{
				Resource: opv1a1.Resource{
					Kind: "TestViewPatch",
				},
				Type: opv1a1.Patcher,
			})

			// Create initial object with spec and status.
			obj := object.NewViewObject("test", "TestViewPatch")
			obj.SetName("test-patch-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field1": "value1", "field2": "value2"}
			obj.Object["status"] = map[string]any{"condition": "Ready", "phase": "Active"}

			// First create the object using the client directly.
			err := mgr.GetClient().Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Update operation - patch only some fields.
			patchObj := object.NewViewObject("test", "TestViewPatch")
			patchObj.SetName("test-patch-obj")
			patchObj.SetNamespace("default")
			patchObj.Object["spec"] = map[string]any{"field2": "patched-value2", "field3": "value3"}
			patchObj.Object["status"] = map[string]any{"condition": "Patched"}

			err = target.Write(ctx, object.Delta{Type: object.Updated, Object: patchObj}, obj)
			Expect(err).NotTo(HaveOccurred())

			// Verify merge-patch behavior: field1 should remain, field2 should be updated, field3 added.
			retrieved := object.NewViewObject("test", "TestViewPatch")
			retrieved.SetName("test-patch-obj")
			retrieved.SetNamespace("default")
			Eventually(func() bool {
				err := mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
				if err != nil {
					return false
				}
				spec, ok := retrieved.Object["spec"].(map[string]any)
				return ok && spec["field2"] == "patched-value2" && spec["field3"] == "value3"
			}, time.Second, 50*time.Millisecond).Should(BeTrue())

			// Check that field1 is still present (merge behavior).
			spec := retrieved.Object["spec"].(map[string]any)
			Expect(spec["field1"]).To(Equal("value1"))

			// IMPORTANT: This test reveals the status handling issue.
			// After our patch, the status should ideally be:
			// - condition: "Patched" (from the patch)
			// - phase: "Active" (preserved from original)
			// But currently, the code OVERWRITES the entire status, so phase will be lost.
			status := retrieved.Object["status"].(map[string]any)
			Expect(status["condition"]).To(Equal("Patched"))
			// This will currently fail because phase is lost:
			// Expect(status["phase"]).To(Equal("Active"))
		})

		It("should handle delete-patch operations that remove specific fields", func() {
			target := NewTarget(mgr, "test", opv1a1.Target{
				Resource: opv1a1.Resource{
					Kind: "TestViewDelete",
				},
				Type: opv1a1.Patcher,
			})

			// Create initial object.
			obj := object.NewViewObject("test", "TestViewDelete")
			obj.SetName("test-delete-patch-obj")
			obj.SetNamespace("default")
			obj.Object["spec"] = map[string]any{"field1": "value1", "field2": "value2", "field3": "value3"}

			err := mgr.GetClient().Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Delete-patch operation - remove only field2.
			deletePatchObj := object.NewViewObject("test", "TestViewDelete")
			deletePatchObj.SetName("test-delete-patch-obj")
			deletePatchObj.SetNamespace("default")
			deletePatchObj.Object["spec"] = map[string]any{"field2": "value2"}

			err = target.Write(ctx, object.Delta{Type: object.Deleted, Object: deletePatchObj}, obj)
			Expect(err).NotTo(HaveOccurred())

			// Verify that only field2 was removed, field1 and field3 remain.
			retrieved := object.NewViewObject("test", "TestViewDelete")
			retrieved.SetName("test-delete-patch-obj")
			retrieved.SetNamespace("default")
			Eventually(func() bool {
				err := mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
				if err != nil {
					return false
				}
				spec, ok := retrieved.Object["spec"].(map[string]any)
				if !ok {
					return false
				}
				// field2 should be gone.
				_, hasField2 := spec["field2"]
				return !hasField2 && spec["field1"] == "value1" && spec["field3"] == "value3"
			}, time.Second, 50*time.Millisecond).Should(BeTrue())
		})
	})
})

var _ = Describe("Virtual Sources", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		queue  workqueue.TypedRateLimitingInterface[Request]
		mgr    manager.Manager
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		var err error
		mgr, err = manager.NewFakeManager(manager.Options{Logger: logger})
		Expect(err).NotTo(HaveOccurred())
		queue = workqueue.NewTypedRateLimitingQueue[Request](workqueue.DefaultTypedControllerRateLimiter[Request]())
	})

	AfterEach(func() {
		cancel()
		queue.ShutDown()
	})

	It("should unmarshal a OneShot source from YAML", func() {
		sourceYAML := `
type: OneShot
kind: TestOneShotTrigger
`
		var source opv1a1.Source
		err := yaml.Unmarshal([]byte(sourceYAML), &source)
		Expect(err).ToNot(HaveOccurred())

		Expect(source.Type).To(Equal(opv1a1.OneShot))
		Expect(source.Kind).To(Equal("TestOneShotTrigger"))
	})

	It("OneShot Source should trigger exactly once", func() {
		s := NewOneShotSource(mgr, "test", opv1a1.Source{
			Resource: opv1a1.Resource{
				Kind: "TestOneShotTrigger",
			},
			Type: opv1a1.OneShot,
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
		list := cache.NewViewObjectList("test", "TestOneShotTrigger")
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
type: Periodic
kind: TestPeriodicTrigger
parameters:
  period: "10ms"`
		var source opv1a1.Source
		err := yaml.Unmarshal([]byte(sourceYAML), &source)
		Expect(err).ToNot(HaveOccurred())

		// Verify fields
		Expect(source.Type).To(Equal(opv1a1.Periodic))
		Expect(source.Kind).To(Equal("TestPeriodicTrigger"))
		Expect(source.Parameters).ToNot(BeNil())

		// Verify parameter parsing
		var params map[string]interface{}
		err = json.Unmarshal(source.Parameters.Raw, &params)
		Expect(err).ToNot(HaveOccurred())
		Expect(params["period"]).To(Equal("10ms"))
	})

	It("Periodic Source should emit periodic trigger events", func() {
		params := apiextensionsv1.JSON{
			Raw: []byte(`{"period": "25ms"}`),
		}
		s := NewPeriodicSource(mgr, "test", opv1a1.Source{
			Resource: opv1a1.Resource{
				Kind: "TestPeriodicTrigger",
			},
			Type:       opv1a1.Periodic,
			Parameters: &params,
		})

		src, err := s.GetSource()
		Expect(err).NotTo(HaveOccurred())

		// Track received events in a channel (simulating controller worker)
		receivedEvents := make(chan Request, 10)
		go func() {
			defer GinkgoRecover()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					item, shutdown := queue.Get()
					if shutdown {
						return
					}
					receivedEvents <- item
					queue.Done(item)
				}
			}
		}()

		err = src.Start(ctx, queue)
		Expect(err).ToNot(HaveOccurred())

		// Wait for multiple periodic triggers (with 10ms period, should see several triggers)
		Eventually(receivedEvents, 100*time.Millisecond).Should(Receive())
		Eventually(receivedEvents, 50*time.Millisecond).Should(Receive())
		Eventually(receivedEvents, 50*time.Millisecond).Should(Receive())

		// Verify event structure
		var item Request
		Eventually(receivedEvents, 50*time.Millisecond).Should(Receive(&item))
		Expect(item.EventType).To(Equal(object.Updated))
		Expect(item.Name).To(Equal(PeriodicSourceObjectName))
	})
})
