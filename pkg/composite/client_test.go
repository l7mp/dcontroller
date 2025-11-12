package composite

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("ViewCache Client", func() {
	var (
		viewCache  *ViewCache
		viewClient client.WithWatch
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		viewCache = NewViewCache(CacheOptions{Logger: logger})
		viewClient = viewCache.GetClient()
	})

	Describe("Auto-registration on first access", func() {
		It("should auto-register GVK when creating a view object in empty cache", func() {
			// Create a view object with a GVK that hasn't been registered yet
			obj := object.NewViewObject("test", "AutoRegisterView")
			object.SetName(obj, "default", "test-1")
			object.SetContent(obj, map[string]any{"data": "test-data"})

			// This should succeed - the GVK should be auto-registered on first Create
			err := viewClient.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred(), "Create should auto-register the GVK")

			// Verify the object was created by retrieving it
			retrieved := object.NewViewObject("test", "AutoRegisterView")
			object.SetName(retrieved, "default", "test-1")
			err = viewClient.Get(ctx, client.ObjectKeyFromObject(obj), retrieved)
			Expect(err).NotTo(HaveOccurred(), "Should be able to retrieve the created object")

			// Check metadata
			Expect(retrieved.GetName()).To(Equal("test-1"))
			Expect(retrieved.GetNamespace()).To(Equal("default"))
			Expect(retrieved.GetUID()).NotTo(BeEmpty(), "UID should be set")

			// Check content
			content := retrieved.UnstructuredContent()
			Expect(content).To(HaveKeyWithValue("data", "test-data"))
		})

		It("should auto-register GVK when getting a non-existent view object", func() {
			// Try to get an object with a GVK that hasn't been registered
			obj := object.NewViewObject("test", "GetAutoRegisterView")
			object.SetName(obj, "default", "non-existent")

			// The Get should fail because the object doesn't exist, but the GVK should be registered
			err := viewClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(err).To(HaveOccurred(), "Get should fail for non-existent object")

			// Now create an object with the same GVK - it should succeed without explicit registration
			newObj := object.NewViewObject("test", "GetAutoRegisterView")
			object.SetName(newObj, "default", "test-2")
			object.SetContent(newObj, map[string]any{"data": "test-data"})

			err = viewClient.Create(ctx, newObj)
			Expect(err).NotTo(HaveOccurred(), "Create should succeed after GVK was auto-registered by Get")
		})

		It("should auto-register GVK when listing with empty cache", func() {
			// Try to list objects with a GVK that hasn't been registered
			list := NewViewObjectList("test", "ListAutoRegisterView")

			// List should succeed and return empty list, auto-registering the GVK
			err := viewClient.List(ctx, list)
			Expect(err).NotTo(HaveOccurred(), "List should auto-register the GVK")
			Expect(list.Items).To(BeEmpty(), "List should return empty for new GVK")

			// Now create an object with the same GVK - it should succeed
			obj := object.NewViewObject("test", "ListAutoRegisterView")
			object.SetName(obj, "default", "test-3")
			object.SetContent(obj, map[string]any{"data": "test-data"})

			err = viewClient.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred(), "Create should succeed after GVK was auto-registered by List")

			// List again - should now return the created object
			list = NewViewObjectList("test", "ListAutoRegisterView")
			err = viewClient.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1), "List should return the created object")
		})
	})

	Describe("Basic CRUD operations", func() {
		BeforeEach(func() {
			// Pre-create some objects for CRUD tests
			obj := object.NewViewObject("test", "CRUDView")
			object.SetName(obj, "default", "crud-test")
			object.SetContent(obj, map[string]any{"value": "original"})
			err := viewClient.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should update an existing object", func() {
			obj := object.NewViewObject("test", "CRUDView")
			object.SetName(obj, "default", "crud-test")
			object.SetContent(obj, map[string]any{"value": "updated"})

			err := viewClient.Update(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Verify update
			retrieved := object.NewViewObject("test", "CRUDView")
			object.SetName(retrieved, "default", "crud-test")
			err = viewClient.Get(ctx, client.ObjectKeyFromObject(obj), retrieved)
			Expect(err).NotTo(HaveOccurred())

			content := retrieved.UnstructuredContent()
			Expect(content).To(HaveKeyWithValue("value", "updated"))
		})

		It("should delete an existing object", func() {
			obj := object.NewViewObject("test", "CRUDView")
			object.SetName(obj, "default", "crud-test")

			err := viewClient.Delete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// Verify deletion
			retrieved := object.NewViewObject("test", "CRUDView")
			object.SetName(retrieved, "default", "crud-test")
			err = viewClient.Get(ctx, client.ObjectKeyFromObject(obj), retrieved)
			Expect(err).To(HaveOccurred(), "Get should fail after deletion")
		})

		It("should list multiple objects", func() {
			// Create additional objects
			obj2 := object.NewViewObject("test", "CRUDView")
			object.SetName(obj2, "default", "crud-test-2")
			object.SetContent(obj2, map[string]any{"value": "second"})
			err := viewClient.Create(ctx, obj2)
			Expect(err).NotTo(HaveOccurred())

			obj3 := object.NewViewObject("test", "CRUDView")
			object.SetName(obj3, "other-ns", "crud-test-3")
			object.SetContent(obj3, map[string]any{"value": "third"})
			err = viewClient.Create(ctx, obj3)
			Expect(err).NotTo(HaveOccurred())

			// List all objects
			list := NewViewObjectList("test", "CRUDView")
			err = viewClient.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))

			// List with namespace filter
			list = NewViewObjectList("test", "CRUDView")
			err = viewClient.List(ctx, list, client.InNamespace("default"))
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(2))
		})
	})

	Describe("Multiple operators sharing ViewCache", func() {
		It("should allow views from different operators in the same cache", func() {
			// Create view from operator "test"
			obj1 := object.NewViewObject("test", "TestView")
			object.SetName(obj1, "default", "test-obj")
			object.SetContent(obj1, map[string]any{"operator": "test"})
			err := viewClient.Create(ctx, obj1)
			Expect(err).NotTo(HaveOccurred())

			// Create view from operator "other"
			obj2 := object.NewViewObject("other", "OtherView")
			object.SetName(obj2, "default", "other-obj")
			object.SetContent(obj2, map[string]any{"operator": "other"})
			err = viewClient.Create(ctx, obj2)
			Expect(err).NotTo(HaveOccurred())

			// Both should be retrievable
			retrieved1 := object.NewViewObject("test", "TestView")
			object.SetName(retrieved1, "default", "test-obj")
			err = viewClient.Get(ctx, client.ObjectKeyFromObject(obj1), retrieved1)
			Expect(err).NotTo(HaveOccurred())

			retrieved2 := object.NewViewObject("other", "OtherView")
			object.SetName(retrieved2, "default", "other-obj")
			err = viewClient.Get(ctx, client.ObjectKeyFromObject(obj2), retrieved2)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("CompositeCache Client", func() {
	var (
		compositeCache  *CompositeCache
		compositeClient client.Client
		ctx             context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		// Create a headless composite cache (no Kubernetes API server)
		var err error
		compositeCache, err = NewCompositeCache(nil, CacheOptions{Logger: logger})
		Expect(err).NotTo(HaveOccurred())

		compositeClient = compositeCache.GetViewCache().GetClient()
	})

	Describe("Auto-registration via CompositeCache", func() {
		It("should auto-register GVK when accessing through composite cache", func() {
			// Create a view object
			obj := object.NewViewObject("test", "CompositeView")
			object.SetName(obj, "default", "composite-test")
			object.SetContent(obj, map[string]any{"data": "composite-data"})

			// Create via the view cache client
			err := compositeClient.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred(), "Create should auto-register GVK")

			// Get via composite cache (which routes to view cache)
			retrieved := object.NewViewObject("test", "CompositeView")
			object.SetName(retrieved, "default", "composite-test")
			err = compositeCache.Get(ctx, client.ObjectKeyFromObject(obj), retrieved)
			Expect(err).NotTo(HaveOccurred(), "Should retrieve via composite cache")

			// Check metadata and content
			Expect(retrieved.GetName()).To(Equal("composite-test"))
			Expect(retrieved.GetNamespace()).To(Equal("default"))
			content := retrieved.UnstructuredContent()
			Expect(content).To(HaveKeyWithValue("data", "composite-data"))
		})

		It("should list through composite cache with auto-registration", func() {
			// List with new GVK - should auto-register
			list := NewViewObjectList("test", "CompositeListView")
			err := compositeCache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred(), "List should auto-register GVK")
			Expect(list.Items).To(BeEmpty())

			// Create object
			obj := object.NewViewObject("test", "CompositeListView")
			object.SetName(obj, "default", "list-test")
			object.SetContent(obj, map[string]any{"data": "list-data"})
			err = compositeClient.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			// List again - should return the object
			list = NewViewObjectList("test", "CompositeListView")
			err = compositeCache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1))
		})
	})
})
