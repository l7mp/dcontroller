package composite

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("ClientMultiplexer", func() {
	var (
		multiplexer       ClientMultiplexer
		coreClient        client.WithWatch
		testViewCache     *ViewCache
		testViewClient    client.WithWatch
		anotherViewCache  *ViewCache
		anotherViewClient client.WithWatch
		ctx               context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		// Create fake client for core API group
		coreClient = fake.NewClientBuilder().Build()

		// Create ViewCache instances for different view groups
		testViewCache = NewViewCache(CacheOptions{})
		testViewClient = testViewCache.GetClient()

		anotherViewCache = NewViewCache(CacheOptions{})
		anotherViewClient = anotherViewCache.GetClient()

		multiplexer = NewClientMultiplexer()
	})

	Describe("Client Registration", func() {
		It("should register clients for different groups", func() {
			err := multiplexer.RegisterClient("", coreClient)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.RegisterClient("another.view.dcontroller.io", anotherViewClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return error when registering duplicate group", func() {
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already registered"))
		})
	})

	Describe("Client Unregistration", func() {
		BeforeEach(func() {
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should unregister existing client", func() {
			err := multiplexer.UnregisterClient("test.view.dcontroller.io")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return error when unregistering non-existent group", func() {
			err := multiplexer.UnregisterClient("nonexistent.group")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no client registered"))
		})
	})

	Describe("CRUD Operations with View Objects", func() {
		var (
			testObj1    object.Object
			testObj2    object.Object
			anotherObj1 object.Object
		)

		BeforeEach(func() {
			// Register clients for view groups
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.RegisterClient("another.view.dcontroller.io", anotherViewClient)
			Expect(err).NotTo(HaveOccurred())

			// Create test view objects for test.view.dcontroller.io group
			testObj1 = object.NewViewObject("test", "TestView")
			testObj1.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(testObj1, "default", "test-view-1")
			object.SetContent(testObj1, map[string]interface{}{
				"spec": map[string]interface{}{
					"replicas": int64(3),
					"image":    "nginx:latest",
				},
			})

			testObj2 = object.NewViewObject("test", "TestView")
			testObj2.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(testObj2, "default", "test-view-2")
			object.SetContent(testObj2, map[string]interface{}{
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"image":    "redis:alpine",
				},
			})

			// Create test view objects for another.view.dcontroller.io group
			anotherObj1 = object.NewViewObject("another", "AnotherView")
			anotherObj1.SetGroupVersionKind(viewv1a1.GroupVersionKind("another", "AnotherView"))
			object.SetName(anotherObj1, "kube-system", "another-view-1")
			object.SetContent(anotherObj1, map[string]interface{}{
				"data": map[string]interface{}{
					"config": "value1",
				},
			})
		})

		It("should create objects in appropriate view caches", func() {
			err := multiplexer.Create(ctx, testObj1)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.Create(ctx, testObj2)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.Create(ctx, anotherObj1)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should get objects from appropriate view caches", func() {
			// Create objects first
			err := multiplexer.Create(ctx, testObj1)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.Create(ctx, anotherObj1)
			Expect(err).NotTo(HaveOccurred())

			// Get test view object
			retrieved1 := object.NewViewObject("test", "TestView")
			retrieved1.SetGroupVersionKind(testObj1.GroupVersionKind())

			err = multiplexer.Get(ctx, client.ObjectKeyFromObject(testObj1), retrieved1)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved1.GetName()).To(Equal("test-view-1"))
			Expect(retrieved1.GetNamespace()).To(Equal("default"))

			// Get another view object
			retrieved2 := object.NewViewObject("another", "AnotherView")
			retrieved2.SetGroupVersionKind(anotherObj1.GroupVersionKind())

			err = multiplexer.Get(ctx, client.ObjectKeyFromObject(anotherObj1), retrieved2)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved2.GetName()).To(Equal("another-view-1"))
			Expect(retrieved2.GetNamespace()).To(Equal("kube-system"))
		})

		It("should update objects in appropriate view caches", func() {
			// Create object first
			err := multiplexer.Create(ctx, testObj1)
			Expect(err).NotTo(HaveOccurred())

			// Update object
			updatedObj := object.DeepCopy(testObj1)
			object.SetContent(updatedObj, map[string]interface{}{
				"spec": map[string]interface{}{
					"replicas": int64(5),
					"image":    "nginx:1.20",
				},
			})
			updatedObj.SetLabels(map[string]string{"updated": "true"})

			err = multiplexer.Update(ctx, updatedObj)
			Expect(err).NotTo(HaveOccurred())

			// Verify update
			retrieved := object.NewViewObject("test", "TestView")
			retrieved.SetGroupVersionKind(testObj1.GroupVersionKind())

			err = multiplexer.Get(ctx, client.ObjectKeyFromObject(testObj1), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.GetLabels()).To(HaveKeyWithValue("updated", "true"))
		})

		It("should delete objects from appropriate view caches", func() {
			// Create object first
			err := multiplexer.Create(ctx, testObj1)
			Expect(err).NotTo(HaveOccurred())

			// Delete object
			err = multiplexer.Delete(ctx, testObj1)
			Expect(err).NotTo(HaveOccurred())

			// Verify deletion
			retrieved := object.NewViewObject("test", "TestView")
			retrieved.SetGroupVersionKind(testObj1.GroupVersionKind())

			err = multiplexer.Get(ctx, client.ObjectKeyFromObject(testObj1), retrieved)
			Expect(err).To(HaveOccurred())
		})

		It("should return error for unregistered group", func() {
			unknownObj := object.NewViewObject("unknown", "UnknownView")
			unknownObj.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "unknown.view.dcontroller.io",
				Version: "v1alpha1",
				Kind:    "UnknownView",
			})

			err := multiplexer.Create(ctx, unknownObj)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List Operations", func() {
		BeforeEach(func() {
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should list objects from appropriate view cache", func() {
			// Create multiple objects
			obj1 := object.NewViewObject("test", "TestView")
			obj1.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(obj1, "default", "test-view-1")

			obj2 := object.NewViewObject("test", "TestView")
			obj2.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(obj2, "default", "test-view-2")

			err := multiplexer.Create(ctx, obj1)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.Create(ctx, obj2)
			Expect(err).NotTo(HaveOccurred())

			// List objects
			list := NewViewObjectList("test", "TestView")
			err = multiplexer.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(2))
		})

		It("should list objects with namespace filter", func() {
			// Create objects in different namespaces
			obj1 := object.NewViewObject("test", "TestView")
			obj1.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(obj1, "default", "test-view-1")

			obj2 := object.NewViewObject("test", "TestView")
			obj2.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(obj2, "kube-system", "test-view-2")

			err := multiplexer.Create(ctx, obj1)
			Expect(err).NotTo(HaveOccurred())

			err = multiplexer.Create(ctx, obj2)
			Expect(err).NotTo(HaveOccurred())

			// List objects in default namespace only
			list := NewViewObjectList("test", "TestView")
			err = multiplexer.List(ctx, list, client.InNamespace("default"))
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1))
			Expect(list.Items[0].GetName()).To(Equal("test-view-1"))
		})

		It("should return error for list without GVK", func() {
			list := NewViewObjectList("test", "TestView")
			// Clear GVK
			list.SetGroupVersionKind(schema.GroupVersionKind{})

			err := multiplexer.List(ctx, list)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not have GroupVersionKind"))
		})
	})

	Describe("Status Operations", func() {
		var testObj object.Object

		BeforeEach(func() {
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())

			testObj = object.NewViewObject("test", "TestView")
			testObj.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))
			object.SetName(testObj, "default", "test-status")
			object.SetContent(testObj, map[string]interface{}{
				"spec": map[string]interface{}{
					"replicas": int64(3),
				},
			})

			// Create the object first
			err = multiplexer.Create(ctx, testObj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should perform status update operations", func() {
			// Update status
			statusObj := object.DeepCopy(testObj)
			object.SetContent(statusObj, map[string]interface{}{
				"spec": map[string]interface{}{
					"replicas": int64(3),
				},
				"status": map[string]interface{}{
					"phase":           "Running",
					"readyReplicas":   int64(3),
					"currentReplicas": int64(3),
				},
			})

			err := multiplexer.Status().Update(ctx, statusObj)
			Expect(err).NotTo(HaveOccurred())

			// Verify status was updated
			retrieved := object.NewViewObject("test", "TestView")
			retrieved.SetGroupVersionKind(testObj.GroupVersionKind())

			err = multiplexer.Get(ctx, client.ObjectKeyFromObject(testObj), retrieved)
			Expect(err).NotTo(HaveOccurred())

			// Check that status field exists
			content := retrieved.UnstructuredContent()
			status, exists := content["status"]
			Expect(exists).To(BeTrue())
			statusMap, ok := status.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(statusMap).To(HaveKeyWithValue("phase", "Running"))
		})
	})

	Describe("Watch Operations", func() {
		BeforeEach(func() {
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should create watch for view objects", func() {
			list := NewViewObjectList("test", "TestView")

			watcher, err := multiplexer.Watch(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(watcher).NotTo(BeNil())

			// Clean up
			watcher.Stop()
		})

		It("should return error for watch without GVK", func() {
			list := NewViewObjectList("test", "TestView")
			// Clear GVK
			list.SetGroupVersionKind(schema.GroupVersionKind{})

			_, err := multiplexer.Watch(ctx, list)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not have GroupVersionKind"))
		})
	})

	Describe("Utility Methods", func() {
		BeforeEach(func() {
			err := multiplexer.RegisterClient("test.view.dcontroller.io", testViewClient)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return scheme as nil", func() {
			scheme := multiplexer.Scheme()
			Expect(scheme).To(BeNil())
		})

		It("should return RESTMapper from first available client", func() {
			mapper := multiplexer.RESTMapper()
			// ViewCache clients don't provide RESTMapper, so this will be nil
			Expect(mapper).To(BeNil())
		})

		It("should determine GVK for objects with GVK set", func() {
			obj := object.NewViewObject("test", "TestView")
			obj.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))

			gvk, err := multiplexer.GroupVersionKindFor(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(gvk.Group).To(Equal("test.view.dcontroller.io"))
			Expect(gvk.Kind).To(Equal("TestView"))
		})

		It("should report all view objects as namespaced", func() {
			obj := object.NewViewObject("test", "TestView")
			obj.SetGroupVersionKind(viewv1a1.GroupVersionKind("test", "TestView"))

			namespaced, err := multiplexer.IsObjectNamespaced(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(namespaced).To(BeTrue())
		})
	})
})
