package composite

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dcontroller/pkg/api/view/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/object"
)

const (
	timeout  = time.Second * 1
	interval = time.Millisecond * 50
)

var _ = Describe("ViewCache", func() {
	var (
		cache  *ViewCache
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		cache = NewViewCache(viewv1a1.Group("test"), CacheOptions{Logger: logger})
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Registering views", func() {
		It("should allow a view to be registered", func() {
			err := cache.RegisterCacheForKind(viewv1a1.GroupVersionKind("test", "view"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("should refuse to register foreign view", func() {
			err := cache.RegisterCacheForKind(viewv1a1.GroupVersionKind("other-op", "view"))
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Get operation", func() {
		It("should retrieve an added object", func() {
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-1")

			err := cache.Add(obj)
			Expect(err).NotTo(HaveOccurred())

			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = cache.Get(ctx, client.ObjectKeyFromObject(retrieved), retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())
		})

		It("should return an error for non-existent object", func() {
			obj := object.NewViewObject("test", "view")
			object.SetName(obj, "", "non-existent")

			err := cache.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(err).To(HaveOccurred())
		})

		It("should return an error for unknown group", func() {
			obj := object.NewViewObject("other-op", "view")
			object.SetName(obj, "default", "view")

			err := cache.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List operation", func() {
		It("should list all added objects", func() {
			objects := []object.Object{object.NewViewObject("test", "view"), object.NewViewObject("test", "view"), object.NewViewObject("test", "view")}
			object.SetName(objects[0], "ns1", "test-1")
			object.SetName(objects[1], "ns2", "test-2")
			object.SetName(objects[2], "ns3", "test-3")
			object.SetContent(objects[0], map[string]any{"a": int64(1)})
			object.SetContent(objects[1], map[string]any{"b": int64(2)})
			object.SetContent(objects[2], map[string]any{"c": int64(3)})

			for _, obj := range objects {
				err := cache.Add(obj)
				Expect(err).NotTo(HaveOccurred())
				object.WithUID(obj)
			}

			list := NewViewObjectList("test", "view")
			err := cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(3))
			Expect(list.Items).To(ContainElement(*objects[0]))
			Expect(list.Items).To(ContainElement(*objects[1]))
			Expect(list.Items).To(ContainElement(*objects[2]))
		})

		It("should list objects using a label-selector", func() {
			objects := []object.Object{object.NewViewObject("test", "view"), object.NewViewObject("test", "view")}
			object.SetName(objects[0], "ns1", "test-1")
			object.SetName(objects[1], "ns2", "test-2")
			object.SetContent(objects[0], map[string]any{"a": int64(1)})
			object.SetContent(objects[1], map[string]any{"b": int64(2)})
			objects[0].SetLabels(map[string]string{"app": "test"})

			for _, obj := range objects {
				err := cache.Add(obj)
				Expect(err).NotTo(HaveOccurred())
				object.WithUID(obj)
			}

			list := NewViewObjectList("test", "view")
			listOpts := []client.ListOption{}
			listOpts = append(listOpts, client.MatchingLabelsSelector{
				Selector: labels.SelectorFromSet(labels.Set(map[string]string{"app": "test"})),
			})

			err := cache.List(ctx, list, listOpts...)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1))
			Expect(list.Items).To(ContainElement(*objects[0]))
		})

		It("should list objects using a field-selector", func() {
			objects := []object.Object{object.NewViewObject("test", "view"), object.NewViewObject("test", "view")}
			object.SetName(objects[0], "ns1", "test-1")
			object.SetName(objects[1], "ns2", "test-2")
			object.SetContent(objects[0], map[string]any{"a": int64(1)})
			object.SetContent(objects[1], map[string]any{"b": int64(2)})

			for _, obj := range objects {
				err := cache.Add(obj)
				Expect(err).NotTo(HaveOccurred())
				object.WithUID(obj)
			}

			list := NewViewObjectList("test", "view")
			listOpts := []client.ListOption{}
			selector, err := fields.ParseSelector("metadata.name=test-1")
			Expect(err).NotTo(HaveOccurred())
			listOpts = append(listOpts, client.MatchingFieldsSelector{Selector: selector})

			err = cache.List(ctx, list, listOpts...)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(HaveLen(1))
			Expect(list.Items).To(ContainElement(*objects[0]))
		})

		It("should return an empty list when cache is empty", func() {
			list := NewViewObjectList("test", "view")
			err := cache.List(ctx, list)
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Items).To(BeEmpty())
		})
	})

	Describe("View cache client operations", func() {
		It("should retrieve an added object", func() {
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			object.SetName(obj, "ns", "test-1")

			c := cache.GetClient()
			err := c.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "test-1"}, retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())

			object.SetContent(obj, map[string]any{"a": int64(2)})
			object.SetName(obj, "ns", "test-1")
			err = c.Update(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			object.WithUID(obj)
			retrieved = object.DeepCopy(obj)
			err = c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "test-1"}, retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())

			err = c.Delete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			err = c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "test-1"}, retrieved)
			Expect(err).To(HaveOccurred())
		})

		It("should retrieve an added object with an empty namespace", func() {
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"a": int64(1)})
			obj.SetName("test-1")

			c := cache.GetClient()
			err := c.Create(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			object.WithUID(obj)
			retrieved := object.DeepCopy(obj)
			err = c.Get(ctx, client.ObjectKey{Name: "test-1"}, retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())

			object.SetContent(obj, map[string]any{"a": int64(2)})
			obj.SetName("test-1")
			err = c.Update(ctx, obj)
			Expect(err).NotTo(HaveOccurred())

			object.WithUID(obj)
			retrieved = object.DeepCopy(obj)
			err = c.Get(ctx, client.ObjectKey{Name: "test-1"}, retrieved)
			Expect(err).NotTo(HaveOccurred())
			Expect(object.DeepEqual(retrieved, obj)).To(BeTrue())

			err = c.Delete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			err = c.Get(ctx, client.ObjectKey{Name: "test-1"}, retrieved)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Watch operation", func() {
		It("should notify of existing objects", func() {
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "watch-data"})
			object.SetName(obj, "ns", "test-watch")
			cache.Add(obj)
			object.WithUID(obj)

			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of added objects", func() {
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "watch-data"})
			object.SetName(obj, "ns", "test-watch")
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(obj)
				object.WithUID(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of updated objects", func() {
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "original data"})
			object.SetName(obj, "ns", "test-update")
			cache.Add(obj)
			object.WithUID(obj)

			updatedObj := object.NewViewObject("test", "view")
			object.SetContent(updatedObj, map[string]any{"data": "updated data"})
			object.SetName(updatedObj, "ns", "test-update")
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Update(obj, updatedObj)
				object.WithUID(updatedObj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(obj).To(Equal(event.Object.(object.Object)))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))
			Expect(updatedObj).To(Equal(event.Object.(object.Object)))
			Expect(object.DeepEqual(updatedObj, event.Object.(object.Object))).To(BeTrue())
		})

		It("should notify of deleted objects", func() {
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())

			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "original data"})
			object.SetName(obj, "ns", "test-delete")
			cache.Add(obj)
			object.WithUID(obj)

			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Delete(obj)
			}()

			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())

			event, ok = tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Deleted))
			Expect(object.DeepEqual(obj, event.Object.(object.Object))).To(BeTrue())
		})

		// Add these tests after the existing "Watch operation" tests in pkg/composite/view_cache_test.go

		It("should filter watch events by namespace", func() {
			// Create objects in different namespaces
			obj1 := object.NewViewObject("test", "view")
			object.SetContent(obj1, map[string]any{"data": "ns1-data"})
			object.SetName(obj1, "namespace1", "test-watch-ns1")

			obj2 := object.NewViewObject("test", "view")
			object.SetContent(obj2, map[string]any{"data": "ns2-data"})
			object.SetName(obj2, "namespace2", "test-watch-ns2")

			// Start watching only namespace1
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"),
				client.InNamespace("namespace1"))
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Add objects to both namespaces
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(obj1) // Should generate event
				cache.Add(obj2) // Should NOT generate event
			}()

			// Should only receive event for namespace1 object
			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))

			watchedObj := event.Object.(object.Object)
			Expect(watchedObj.GetNamespace()).To(Equal("namespace1"))
			Expect(watchedObj.GetName()).To(Equal("test-watch-ns1"))

			// Should not receive any more events
			_, ok = tryWatch(watcher, interval/2)
			Expect(ok).To(BeFalse())
		})

		It("should filter watch events by label selector", func() {
			// Create objects with different labels
			labeledObj := object.NewViewObject("test", "view")
			object.SetContent(labeledObj, map[string]any{"data": "labeled-data"})
			object.SetName(labeledObj, "ns", "labeled-obj")
			labeledObj.SetLabels(map[string]string{"app": "test", "env": "prod"})

			unlabeledObj := object.NewViewObject("test", "view")
			object.SetContent(unlabeledObj, map[string]any{"data": "unlabeled-data"})
			object.SetName(unlabeledObj, "ns", "unlabeled-obj")

			wrongLabelObj := object.NewViewObject("test", "view")
			object.SetContent(wrongLabelObj, map[string]any{"data": "wrong-label-data"})
			object.SetName(wrongLabelObj, "ns", "wrong-label-obj")
			wrongLabelObj.SetLabels(map[string]string{"app": "other", "env": "dev"})

			// Start watching with label selector
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"),
				client.MatchingLabels{"app": "test"})
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Add all objects
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(labeledObj)    // Should generate event (app=test)
				cache.Add(unlabeledObj)  // Should NOT generate event (no labels)
				cache.Add(wrongLabelObj) // Should NOT generate event (app=other)
			}()

			// Should only receive event for the labeled object
			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))

			watchedObj := event.Object.(object.Object)
			Expect(watchedObj.GetName()).To(Equal("labeled-obj"))
			Expect(watchedObj.GetLabels()).To(HaveKeyWithValue("app", "test"))

			// Should not receive any more events
			_, ok = tryWatch(watcher, interval/2)
			Expect(ok).To(BeFalse())
		})

		It("should filter watch events by field selector", func() {
			// Create objects with different names
			obj1 := object.NewViewObject("test", "view")
			object.SetContent(obj1, map[string]any{"data": "obj1-data"})
			object.SetName(obj1, "ns", "target-object")

			obj2 := object.NewViewObject("test", "view")
			object.SetContent(obj2, map[string]any{"data": "obj2-data"})
			object.SetName(obj2, "ns", "other-object")

			// Start watching with field selector
			selector, err := fields.ParseSelector("metadata.name=target-object")
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"),
				client.MatchingFieldsSelector{Selector: selector})
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Add both objects
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(obj1) // Should generate event (name=target-object)
				cache.Add(obj2) // Should NOT generate event (name=other-object)
			}()

			// Should only receive event for the target object
			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))

			watchedObj := event.Object.(object.Object)
			Expect(watchedObj.GetName()).To(Equal("target-object"))

			// Should not receive any more events
			_, ok = tryWatch(watcher, interval/2)
			Expect(ok).To(BeFalse())
		})

		It("should combine multiple selectors", func() {
			// Create objects with different combinations of properties
			matchingObj := object.NewViewObject("test", "view")
			object.SetContent(matchingObj, map[string]any{"data": "matching"})
			object.SetName(matchingObj, "target-ns", "target-name")
			matchingObj.SetLabels(map[string]string{"app": "test"})

			wrongNamespaceObj := object.NewViewObject("test", "view")
			object.SetContent(wrongNamespaceObj, map[string]any{"data": "wrong-ns"})
			object.SetName(wrongNamespaceObj, "other-ns", "target-name")
			wrongNamespaceObj.SetLabels(map[string]string{"app": "test"})

			wrongLabelObj := object.NewViewObject("test", "view")
			object.SetContent(wrongLabelObj, map[string]any{"data": "wrong-label"})
			object.SetName(wrongLabelObj, "target-ns", "target-name")
			wrongLabelObj.SetLabels(map[string]string{"app": "other"})

			wrongNameObj := object.NewViewObject("test", "view")
			object.SetContent(wrongNameObj, map[string]any{"data": "wrong-name"})
			object.SetName(wrongNameObj, "target-ns", "other-name")
			wrongNameObj.SetLabels(map[string]string{"app": "test"})

			// Start watching with multiple selectors: namespace + label + field
			selector, err := fields.ParseSelector("metadata.name=target-name")
			Expect(err).NotTo(HaveOccurred())

			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"),
				client.InNamespace("target-ns"),
				client.MatchingLabels{"app": "test"},
				client.MatchingFieldsSelector{Selector: selector})
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Add all objects
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(matchingObj)       // Should generate event (matches all)
				cache.Add(wrongNamespaceObj) // Should NOT generate event (wrong namespace)
				cache.Add(wrongLabelObj)     // Should NOT generate event (wrong label)
				cache.Add(wrongNameObj)      // Should NOT generate event (wrong name)
			}()

			// Should only receive event for the fully matching object
			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Added))

			watchedObj := event.Object.(object.Object)
			Expect(watchedObj.GetName()).To(Equal("target-name"))
			Expect(watchedObj.GetNamespace()).To(Equal("target-ns"))
			Expect(watchedObj.GetLabels()).To(HaveKeyWithValue("app", "test"))

			// Should not receive any more events
			_, ok = tryWatch(watcher, interval/2)
			Expect(ok).To(BeFalse())
		})

		It("should handle update events with selectors", func() {
			// Create an object that initially doesn't match selectors
			obj := object.NewViewObject("test", "view")
			object.SetContent(obj, map[string]any{"data": "initial"})
			object.SetName(obj, "ns", "test-update")
			obj.SetLabels(map[string]string{"app": "other"}) // Initially doesn't match

			cache.Add(obj)

			// Start watching with label selector
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"),
				client.MatchingLabels{"app": "test"})
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Clear any initial events
			timeout := time.NewTimer(100 * time.Millisecond)
			done := false
			for !done {
				select {
				case <-watcher.ResultChan():
					continue
				case <-timeout.C:
					done = true
				}
			}

			// Update object to match selector
			updatedObj := object.DeepCopy(obj)
			object.SetContent(updatedObj, map[string]any{"data": "updated"})
			updatedObj.SetLabels(map[string]string{"app": "test"}) // Now matches
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Update(obj, updatedObj)
			}()

			// Should receive update event since object now matches
			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Modified))

			watchedObj := event.Object.(object.Object)
			Expect(watchedObj.GetLabels()).To(HaveKeyWithValue("app", "test"))

			data, found, err := unstructured.NestedString(watchedObj.Object, "data")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(data).To(Equal("updated"))
		})

		It("should handle delete events with selectors", func() {
			// Create objects, some matching selectors
			matchingObj := object.NewViewObject("test", "view")
			object.SetContent(matchingObj, map[string]any{"data": "matching"})
			object.SetName(matchingObj, "ns", "matching-delete")
			matchingObj.SetLabels(map[string]string{"app": "test"})

			nonMatchingObj := object.NewViewObject("test", "view")
			object.SetContent(nonMatchingObj, map[string]any{"data": "non-matching"})
			object.SetName(nonMatchingObj, "ns", "non-matching-delete")
			nonMatchingObj.SetLabels(map[string]string{"app": "other"})

			cache.Add(matchingObj)
			cache.Add(nonMatchingObj)

			// Start watching with label selector
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"),
				client.MatchingLabels{"app": "test"})
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Clear any initial events
			timeout := time.NewTimer(100 * time.Millisecond)
		clearEvents:
			for {
				select {
				case <-watcher.ResultChan():
					continue
				case <-timeout.C:
					break clearEvents
				}
			}

			// Delete both objects
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Delete(matchingObj)    // Should generate delete event
				cache.Delete(nonMatchingObj) // Should NOT generate delete event
			}()

			// Should only receive delete event for the matching object
			event, ok := tryWatch(watcher, interval)
			Expect(ok).To(BeTrue())
			Expect(event.Type).To(Equal(watch.Deleted))

			watchedObj := event.Object.(object.Object)
			Expect(watchedObj.GetName()).To(Equal("matching-delete"))
			Expect(watchedObj.GetLabels()).To(HaveKeyWithValue("app", "test"))

			// Should not receive any more events
			_, ok = tryWatch(watcher, interval/2)
			Expect(ok).To(BeFalse())
		})

		It("should handle empty namespace selector (all namespaces)", func() {
			// Create objects in different namespaces
			obj1 := object.NewViewObject("test", "view")
			object.SetContent(obj1, map[string]any{"data": "data1"})
			object.SetName(obj1, "ns1", "obj1")

			obj2 := object.NewViewObject("test", "view")
			object.SetContent(obj2, map[string]any{"data": "data2"})
			object.SetName(obj2, "ns2", "obj2")

			// Start watching without namespace restriction
			watcher, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())
			defer watcher.Stop()

			// Add objects in different namespaces
			go func() {
				time.Sleep(25 * time.Millisecond)
				cache.Add(obj1)
				cache.Add(obj2)
			}()

			// Should receive events for both objects
			receivedNames := make(map[string]bool)

			for i := 0; i < 2; i++ {
				event, ok := tryWatch(watcher, interval)
				Expect(ok).To(BeTrue())
				Expect(event.Type).To(Equal(watch.Added))

				watchedObj := event.Object.(object.Object)
				receivedNames[watchedObj.GetName()] = true
			}

			Expect(receivedNames).To(HaveKey("obj1"))
			Expect(receivedNames).To(HaveKey("obj2"))
			Expect(receivedNames).To(HaveLen(2))
		})

		It("should not re-trigger events on existing watches when adding a new watch", func() {
			// Create and add some objects to the cache
			obj1 := object.NewViewObject("test", "view")
			object.SetContent(obj1, map[string]any{"data": "test-data-1"})
			object.SetName(obj1, "ns1", "test-1")

			obj2 := object.NewViewObject("test", "view")
			object.SetContent(obj2, map[string]any{"data": "test-data-2"})
			object.SetName(obj2, "ns2", "test-2")

			err := cache.Add(obj1)
			Expect(err).NotTo(HaveOccurred())
			err = cache.Add(obj2)
			Expect(err).NotTo(HaveOccurred())

			// Create first watcher and consume initial events
			watcher1, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())
			defer watcher1.Stop()

			// Consume initial events from first watcher (should get 2 events for existing objects)
			event1, ok := tryWatch(watcher1, interval)
			Expect(ok).To(BeTrue())
			Expect(event1.Type).To(Equal(watch.Added))

			event2, ok := tryWatch(watcher1, interval)
			Expect(ok).To(BeTrue())
			Expect(event2.Type).To(Equal(watch.Added))

			// Ensure no more events are pending for watcher1
			_, ok = tryWatch(watcher1, time.Millisecond*10)
			Expect(ok).To(BeFalse())

			// Create second watcher - this should NOT trigger new events on watcher1
			watcher2, err := cache.Watch(ctx, NewViewObjectList("test", "view"))
			Expect(err).NotTo(HaveOccurred())
			defer watcher2.Stop()

			// watcher2 should get initial events for existing objects
			event3, ok := tryWatch(watcher2, interval)
			Expect(ok).To(BeTrue())
			Expect(event3.Type).To(Equal(watch.Added))

			event4, ok := tryWatch(watcher2, interval)
			Expect(ok).To(BeTrue())
			Expect(event4.Type).To(Equal(watch.Added))

			// CRITICAL: watcher1 should NOT receive any additional events
			// when watcher2 was created
			_, ok = tryWatch(watcher1, time.Millisecond*50)
			Expect(ok).To(BeFalse(), "watcher1 should not receive additional events when watcher2 is created")

			// Add a new object to verify both watchers still work independently
			obj3 := object.NewViewObject("test", "view")
			object.SetContent(obj3, map[string]any{"data": "test-data-3"})
			object.SetName(obj3, "ns3", "test-3")

			err = cache.Add(obj3)
			Expect(err).NotTo(HaveOccurred())

			// Both watchers should receive the new object event
			event5, ok := tryWatch(watcher1, interval)
			Expect(ok).To(BeTrue())
			Expect(event5.Type).To(Equal(watch.Added))

			event6, ok := tryWatch(watcher2, interval)
			Expect(ok).To(BeTrue())
			Expect(event6.Type).To(Equal(watch.Added))
		})
	})
})

func tryWatch(watcher watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}
